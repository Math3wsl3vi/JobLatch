package dev.joblatch.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes ownership of available jobs so a worker can run them.
 *
 * <h2>How a claim works</h2>
 *
 * <p>One SQL statement, in its own short transaction:
 *
 * <ol>
 *   <li>Find up to N jobs in the queue that are available and due, best first.
 *   <li>Lock them with {@code FOR UPDATE SKIP LOCKED}. Rows another worker is claiming right now
 *       are skipped instead of waited on.
 *   <li>Mark them as running, owned by this worker, with a lease that ends at
 *       {@code now() + leaseDuration}, and add 1 to attempts.
 *   <li>Commit. The row locks are released, but ownership stays recorded in the rows.
 * </ol>
 *
 * <p>Two workers can never get the same job from one round of claims. The second worker either
 * skips the locked row, or, if the first has already committed, sees that it is no longer
 * 'available' and does not select it.
 *
 * <p>This class is thread-safe. Many threads may call {@link #claim} at the same time.
 */
public final class JobClaimer {

    private static final Logger log = LoggerFactory.getLogger(JobClaimer.class);

    /** Upper limit for one claim. Bigger batches hold row locks longer and starve other workers. */
    public static final int MAX_BATCH_SIZE = 1000;

    

    /**
     * The claim statement. Read it inside out.
     *
     * <p><b>The {@code WITH candidates AS (...)} part</b> picks and locks rows:
     *
     * <ul>
     *   <li>{@code state = 'available' AND run_at <= now()}: only due jobs. {@code now()} is the
     *       database's clock, the same for every worker.
     *   <li>{@code attempts < max_attempts}: defensive. If a job ever ended up available with no
     *       attempts left, adding 1 would break the CHECK constraint and fail the whole batch,
     *       every time. Skipping it keeps the queue moving. Milestone 5 moves such jobs to 'dead'.
     *   <li>{@code ORDER BY priority DESC, run_at}: best first. It matches the partial index
     *       {@code joblatch_jobs_claim_idx}, so Postgres reads jobs straight from the index in order.
     *   <li>{@code LIMIT ?}: batch size.
     *   <li>{@code FOR UPDATE SKIP LOCKED}: lock what we picked, and step over rows someone else
     *       has locked.
     * </ul>
     *
     * <p><b>The {@code UPDATE ... FROM candidates} part</b> changes exactly the locked rows, and
     * {@code RETURNING} sends them back so no second query is needed.
     *
     * <p>{@code ? * interval '1 millisecond'} turns the lease length (sent as a number of
     * milliseconds) into a Postgres interval. It is still a parameter, never pasted text.
     *
     * <p>Note: {@code RETURNING} does not keep the {@code ORDER BY} order. Callers must not rely on
     * the order of the returned list.
     */
    private static final String CLAIM_SQL = """
            WITH candidates AS (
                SELECT id
                FROM joblatch_jobs
                WHERE queue = ?
                  AND state = 'available'
                  AND run_at <= now()
                  AND attempts < max_attempts
                ORDER BY priority DESC, run_at
                LIMIT ?
                FOR UPDATE SKIP LOCKED
            )
            UPDATE joblatch_jobs AS j
            SET state        = 'running',
                locked_by    = ?,
                locked_until = now() + ? * interval '1 millisecond',
                attempts     = j.attempts + 1,
                updated_at   = now()
            FROM candidates
            WHERE j.id = candidates.id
            RETURNING j.id, j.queue, j.job_type, j.payload::text AS payload,
                      j.attempts, j.max_attempts, j.locked_by, j.locked_until
            """;

    /**
     * How long until the earliest available job in a queue is due. Negative values mean "already
     * due". NULL (no available jobs at all) comes back as SQL NULL. Served by the V3 index
     * joblatch_jobs_next_due_idx.
     *
     * <p>{@code EXTRACT(EPOCH FROM interval)} turns an interval into seconds. Times 1000 and cast to
     * bigint gives whole milliseconds.
     */
    private static final String NEXT_DUE_SQL = """
            SELECT (EXTRACT(EPOCH FROM (min(run_at) - now())) * 1000)::bigint
            FROM joblatch_jobs
            WHERE queue = ?
              AND state = 'available'
              AND attempts < max_attempts
            """;

    private final DataSource dataSource;

    /**
     * @param dataSource where connections come from. The claimer borrows one per claim and returns
     *     it straight away.
     */
    public JobClaimer(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    /**
     * Claims up to {@code batchSize} due jobs from {@code queue} for {@code workerId}.
     *
     * @param queue         which queue to take from
     * @param workerId      who is claiming. Must be unique per worker (for example
     *                      "host-pid-thread"), because it is how ownership is checked later.
     * @param batchSize     at most this many jobs, between 1 and {@link #MAX_BATCH_SIZE}
     * @param leaseDuration how long the worker owns each job before it may be taken back
     * @return the claimed jobs, possibly empty, in no particular order. They are already committed
     *     as owned by {@code workerId}.
     * @throws SQLException if the database call fails. Nothing was claimed in that case.
     */
    public List<ClaimedJob> claim(String queue, String workerId, int batchSize, Duration leaseDuration)
            throws SQLException {
        validate(queue, workerId, batchSize, leaseDuration);

        // We open the connection here, so we own it. The claim is ONE statement, and in
        // auto-commit mode one statement is one transaction: the SELECT ... FOR UPDATE and the
        // UPDATE succeed or fail together, and commit before executeQuery returns. Ownership
        // becomes real at that moment. A crash before it leaves the jobs available: nothing lost.
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(true); // pools may hand out connections with it off
            List<ClaimedJob> claimed = runClaim(conn, queue, workerId, batchSize, leaseDuration);
            if (!claimed.isEmpty()) {
                log.debug("Worker {} claimed {} job(s) from queue {}", workerId, claimed.size(), queue);
            }
            return claimed;
        }
    }

    /**
     * How long until the next available job in {@code queue} may run, on the database's clock.
     * Lets an idle worker sleep exactly until a scheduled job is due, instead of waking up
     * repeatedly to find nothing.
     *
     * @return empty if the queue has no available jobs at all, {@link Duration#ZERO} if one is
     *     already due
     */
    public Optional<Duration> timeUntilNextDue(String queue) throws SQLException {
        Names.requireValid("queue", queue);
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(NEXT_DUE_SQL)) {
            ps.setString(1, queue);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next(); // an aggregate like min() always returns exactly one row
                long millis = rs.getLong(1);
                if (rs.wasNull()) { // getLong returns 0 for NULL. wasNull tells the two apart.
                    return Optional.empty();
                }
                return Optional.of(Duration.ofMillis(Math.max(0, millis)));
            }
        }
    }

    private static List<ClaimedJob> runClaim(
            Connection conn, String queue, String workerId, int batchSize, Duration leaseDuration)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(CLAIM_SQL)) {
            ps.setString(1, queue);
            ps.setInt(2, batchSize);
            ps.setString(3, workerId);
            ps.setLong(4, leaseDuration.toMillis());

            List<ClaimedJob> claimed = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    claimed.add(new ClaimedJob(
                            rs.getObject("id", UUID.class),
                            rs.getString("queue"),
                            rs.getString("job_type"),
                            rs.getString("payload"),
                            rs.getInt("attempts"),
                            rs.getInt("max_attempts"),
                            rs.getString("locked_by"),
                            rs.getObject("locked_until", OffsetDateTime.class).toInstant()));
                }
            }
            return claimed;
        }
    }

    private static void validate(String queue, String workerId, int batchSize, Duration leaseDuration) {
        Names.requireValid("queue", queue);
        Names.requireValid("workerId", workerId);
        if (batchSize < 1 || batchSize > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException(
                    "batchSize must be between 1 and " + MAX_BATCH_SIZE + ", was " + batchSize);
        }
        Objects.requireNonNull(leaseDuration, "leaseDuration");
        // Under 1 ms would round down to a zero-length lease: expired the moment it is granted.
        if (leaseDuration.toMillis() < 1) {
            throw new IllegalArgumentException("leaseDuration must be at least 1 millisecond");
        }
    }
}
