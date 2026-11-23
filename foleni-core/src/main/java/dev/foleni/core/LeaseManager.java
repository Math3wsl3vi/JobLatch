package dev.foleni.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.Optional;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Renews leases (heartbeats) and takes back jobs whose leases have expired.
 *
 * <p>A lease is "worker W owns this job until time T". A live worker keeps pushing T forward by
 * calling {@link #renew}. A dead worker stops, T passes, and {@link #reclaimExpired} makes the job
 * available again. Nobody has to detect a crash: silence is enough.
 *
 * <p>Both operations are single SQL statements and safe to run concurrently with each other, with
 * claims, with completions, and with other reapers in other processes.
 *
 * <p>This class is thread-safe.
 */
public final class LeaseManager {

    private static final Logger log = LoggerFactory.getLogger(LeaseManager.class);

    /** Upper limit for one reaper pass, for the same reason as {@link JobClaimer#MAX_BATCH_SIZE}. */
    public static final int MAX_RECLAIM_BATCH = 1000;

    /**
     * Heartbeat. Same fencing check as complete/fail, so only the current owner can extend.
     *
     * <p>The new expiry is counted from the database's {@code now()}, not added to the old expiry.
     * That way a late heartbeat cannot build up a lease far into the future.
     */
    private static final String RENEW_SQL = """
            UPDATE foleni_jobs
            SET locked_until = now() + ? * interval '1 millisecond',
                updated_at   = now()
            WHERE id = ?
              AND state = 'running'
              AND locked_by = ?
              AND attempts = ?
            RETURNING locked_until
            """;

    /**
     * Reaper. Same shape as the claim statement: pick and lock rows with SKIP LOCKED, then update
     * exactly those rows.
     *
     * <ul>
     *   <li>{@code locked_until < now()}: only leases that have ended. The partial index
     *       {@code foleni_jobs_lease_expiry_idx} covers exactly this search.
     *   <li>{@code SKIP LOCKED}: a row locked by someone else is being changed right now (a
     *       heartbeat, a completion, another reaper). Leave it alone. If it is still expired
     *       next time, we will get it then.
     *   <li>Attempts were counted when the job was claimed. An expired attempt therefore uses up
     *       one attempt, so a job that crashes its worker every time ends up 'dead' instead of
     *       looping forever.
     *   <li>{@code last_error} records why the job came back. The worker ID is safe to store: it
     *       was validated against the name pattern when the job was claimed.
     * </ul>
     */
    private static final String RECLAIM_SQL = """
            WITH expired AS (
                SELECT id
                FROM foleni_jobs
                WHERE state = 'running'
                  AND locked_until < now()
                ORDER BY locked_until
                LIMIT ?
                FOR UPDATE SKIP LOCKED
            )
            UPDATE foleni_jobs AS j
            SET state        = CASE WHEN j.attempts >= j.max_attempts THEN 'dead' ELSE 'available' END,
                finished_at  = CASE WHEN j.attempts >= j.max_attempts THEN now() ELSE NULL END,
                run_at       = CASE WHEN j.attempts >= j.max_attempts THEN j.run_at ELSE now() END,
                last_error   = 'lease expired: worker ' || j.locked_by || ' stopped heartbeating',
                last_failed_at = now(),
                locked_by    = NULL,
                locked_until = NULL,
                updated_at   = now()
            FROM expired
            WHERE j.id = expired.id
            RETURNING j.state
            """;

    private final DataSource dataSource;

    public LeaseManager(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    /**
     * Extends the caller's ownership of {@code job} to {@code now() + leaseDuration}.
     *
     * <p>Like complete/fail, this still works after the lease has expired, as long as no reaper
     * has taken the job back yet.
     *
     * <p>The {@link ClaimedJob} you hold keeps its original {@code lockedUntil}: records never
     * change. That is fine, because ownership checks use {@code attempts}, not the expiry time.
     *
     * @return the new expiry time, or empty if the caller no longer owns the job. In that case
     *     stop working on it if you can: another worker may already be running it.
     */
    public Optional<Instant> renew(ClaimedJob job, Duration leaseDuration) throws SQLException {
        Objects.requireNonNull(job, "job");
        Objects.requireNonNull(leaseDuration, "leaseDuration");
        if (leaseDuration.toMillis() < 1) {
            throw new IllegalArgumentException("leaseDuration must be at least 1 millisecond");
        }

        // One UPDATE in auto-commit mode: its own transaction, no BEGIN/COMMIT round trips.
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(RENEW_SQL)) {
            conn.setAutoCommit(true); // pools may hand out connections with it off
            ps.setLong(1, leaseDuration.toMillis());
            ps.setObject(2, job.id());
            ps.setString(3, job.lockedBy());
            ps.setInt(4, job.attempts());
            Optional<Instant> newExpiry;
            try (ResultSet rs = ps.executeQuery()) {
                newExpiry = rs.next()
                        ? Optional.of(rs.getObject(1, OffsetDateTime.class).toInstant())
                        : Optional.empty();
            }
            if (newExpiry.isEmpty()) {
                log.info("Lease on job {} not renewed: worker {} no longer owns attempt {}",
                        job.id(), job.lockedBy(), job.attempts());
            }
            return newExpiry;
        }
    }

    /**
     * Takes back up to {@code maxJobs} jobs whose leases have expired, in one transaction.
     *
     * <p>Safe to call from many threads and processes at once: each expired job is reclaimed by
     * exactly one caller.
     *
     * @param maxJobs between 1 and {@link #MAX_RECLAIM_BATCH}
     * @return how many jobs were requeued and how many became dead
     */
    public ReclaimResult reclaimExpired(int maxJobs) throws SQLException {
        if (maxJobs < 1 || maxJobs > MAX_RECLAIM_BATCH) {
            throw new IllegalArgumentException(
                    "maxJobs must be between 1 and " + MAX_RECLAIM_BATCH + ", was " + maxJobs);
        }

        // One statement (the CTE and its UPDATE) in auto-commit mode: one transaction.
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(RECLAIM_SQL)) {
            conn.setAutoCommit(true); // pools may hand out connections with it off
            ps.setInt(1, maxJobs);
            int requeued = 0;
            int dead = 0;
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    if ("dead".equals(rs.getString(1))) {
                        dead++;
                    } else {
                        requeued++;
                    }
                }
            }
            ReclaimResult result = new ReclaimResult(requeued, dead);
            if (result.total() > 0) {
                // WARN: expired leases mean workers crashed or stalled. Operators should know.
                log.warn("Reclaimed {} job(s) with expired leases ({} requeued, {} dead)",
                        result.total(), requeued, dead);
            }
            return result;
        }
    }
}