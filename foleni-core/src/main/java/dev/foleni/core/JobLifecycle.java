package dev.foleni.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Moves a claimed job to its next state: succeeded, back to available (retry later), or dead.
 *
 * <h2>Ownership check: the fencing token</h2>
 *
 * <p>Every transition is a single {@code UPDATE} whose {@code WHERE} clause carries the worker's
 * proof of ownership, taken from the {@link ClaimedJob} it received when it claimed the job:
 *
 * <pre>
 *   WHERE id = ? AND state = 'running' AND locked_by = ? AND attempts = ?
 * </pre>
 *
 * <p>{@code attempts} goes up by one on every claim, so (id, attempts) identifies exactly one
 * ownership period. If anyone else has claimed the job since, even the same worker ID claiming it
 * again, {@code attempts} no longer matches and the update changes 0 rows. The answer is then
 * {@link TransitionResult#NOT_OWNER}.
 *
 * <p>Check and write happen in one statement, so there is no gap between "am I still the owner?"
 * and "write the new state" for another worker to slip into.
 *
 * <h2>Expired but not reclaimed</h2>
 *
 * <p>A worker whose lease has expired may still complete or fail the job, as long as nobody has
 * claimed it again. The lease exists to allow recovery, and the fencing token tells us exactly
 * whether recovery has happened.
 *
 * <h2>Retries</h2>
 *
 * <p>A failed job with attempts left is scheduled again at {@code now() + delay}, where the delay
 * comes from the {@link RetryPolicy}. Java decides how long to wait. The database decides what
 * time that is, so every worker uses the same clock.
 *
 * <p>This class is thread-safe.
 */
public final class JobLifecycle {

    private static final Logger log = LoggerFactory.getLogger(JobLifecycle.class);

    /**
     * Longest error text stored in {@code last_error}. Stack traces can be huge, and only the
     * start is useful for a person deciding what went wrong.
     */
    public static final int MAX_ERROR_LENGTH = 2000;

    /** Success: finished, lease cleared. The CHECK constraints require all of these together. */
    private static final String COMPLETE_SQL = """
            UPDATE foleni_jobs
            SET state        = 'succeeded',
                locked_by    = NULL,
                locked_until = NULL,
                finished_at  = now(),
                updated_at   = now()
            WHERE id = ?
              AND state = 'running'
              AND locked_by = ?
              AND attempts = ?
            """;

    /**
     * Failure: one statement decides between "retry later" and "dead", so the decision and the
     * write can never disagree.
     *
     * <p>The {@code FROM (SELECT ?::boolean AS permanent, ?::bigint AS retry_delay_ms) AS p} part
     * is a trick to give parameters names. JDBC parameters are positional {@code ?} marks, and we
     * need "permanent" in three places. Selecting the parameters once into a one-row table
     * {@code p} lets us write {@code p.permanent} as often as we like.
     *
     * <p>A job gives up (becomes dead) when it has used its last attempt OR the caller said the
     * failure is permanent. Otherwise it becomes available again at {@code now() + delay}.
     */
    private static final String FAIL_SQL = """
            UPDATE foleni_jobs AS j
            SET state          = CASE WHEN j.attempts >= j.max_attempts OR p.permanent
                                      THEN 'dead' ELSE 'available' END,
                finished_at    = CASE WHEN j.attempts >= j.max_attempts OR p.permanent
                                      THEN now() ELSE NULL END,
                run_at         = CASE WHEN j.attempts >= j.max_attempts OR p.permanent
                                      THEN j.run_at
                                      ELSE now() + p.retry_delay_ms * interval '1 millisecond' END,
                locked_by      = NULL,
                locked_until   = NULL,
                last_error     = ?,
                last_failed_at = now(),
                updated_at     = now()
            FROM (SELECT ?::boolean AS permanent, ?::bigint AS retry_delay_ms) AS p
            WHERE j.id = ?
              AND j.state = 'running'
              AND j.locked_by = ?
              AND j.attempts = ?
            RETURNING j.state
            """;

    private final DataSource dataSource;
    private final RetryPolicy retryPolicy;

    /** Uses {@link RetryPolicy#defaults()}. */
    public JobLifecycle(DataSource dataSource) {
        this(dataSource, RetryPolicy.defaults());
    }

    /**
     * @param dataSource  used by the methods that manage their own transaction
     * @param retryPolicy how long failed jobs wait before their next attempt
     */
    public JobLifecycle(DataSource dataSource, RetryPolicy retryPolicy) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy");
    }

    /**
     * Marks the job succeeded, in its own transaction.
     *
     * @return {@link TransitionResult#SUCCEEDED}, or {@link TransitionResult#NOT_OWNER} if the
     *     caller no longer owns the job (nothing was changed)
     */
    public TransitionResult complete(ClaimedJob job) throws SQLException {
        Objects.requireNonNull(job, "job");
        // One UPDATE in auto-commit mode: its own transaction, no BEGIN/COMMIT round trips.
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(true); // pools may hand out connections with it off
            return runComplete(conn, job);
        }
    }

    /**
     * Marks the job succeeded inside the caller's transaction. Same rule as {@link
     * JobQueue#enqueue(Connection, NewJob)}: we never commit, roll back or close your connection.
     *
     * <p>Use this when the handler's work is in the same database. The work and the completion
     * then commit together or not at all, so a crash can never leave "work done, job still
     * running" (which would cause a rerun).
     *
     * <p>If this returns {@link TransitionResult#NOT_OWNER}, someone else owns the job now. You
     * should roll back your work, because the new owner will do it.
     *
     * @throws IllegalArgumentException if the connection is in auto-commit mode
     */
    public TransitionResult complete(Connection connection, ClaimedJob job) throws SQLException {
        Objects.requireNonNull(connection, "connection");
        Objects.requireNonNull(job, "job");
        if (connection.getAutoCommit()) {
            throw new IllegalArgumentException(
                    "complete(Connection, ...) needs a connection with auto-commit off so the "
                            + "completion joins your transaction. Use complete(ClaimedJob) otherwise.");
        }
        return runComplete(connection, job);
    }

    /**
     * Records a failed attempt that is worth retrying (a timeout, an unavailable service...). If
     * attempts remain, the job runs again after a backoff delay. Otherwise it becomes dead.
     *
     * @param errorMessage a short description of what went wrong. It is stored in the database
     *     (truncated to {@link #MAX_ERROR_LENGTH}) and never logged by Foleni. Still, do not put
     *     secrets or personal data in it: operators will read it.
     * @return {@link TransitionResult#RETRY_SCHEDULED}, {@link TransitionResult#DEAD} or {@link
     *     TransitionResult#NOT_OWNER}
     */
    public TransitionResult fail(ClaimedJob job, String errorMessage) throws SQLException {
        return recordFailure(job, errorMessage, false);
    }

    /**
     * Records a failure that retrying cannot fix (invalid payload, missing customer...). The job
     * becomes dead immediately, whatever attempts it has left, and waits there for a person.
     *
     * @return {@link TransitionResult#DEAD} or {@link TransitionResult#NOT_OWNER}
     */
    public TransitionResult failPermanently(ClaimedJob job, String errorMessage) throws SQLException {
        return recordFailure(job, errorMessage, true);
    }

    private TransitionResult recordFailure(ClaimedJob job, String errorMessage, boolean permanent)
            throws SQLException {
        Objects.requireNonNull(job, "job");
        Objects.requireNonNull(errorMessage, "errorMessage");
        String stored = cleanError(errorMessage);
        // Computed even when it will not be used (last attempt or permanent): it is cheap, and
        // it keeps the SQL to a single statement.
        Duration delay = retryPolicy.delayAfter(job.attempts());

        // One UPDATE in auto-commit mode: its own transaction, no BEGIN/COMMIT round trips.
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(true); // pools may hand out connections with it off
            return runFail(conn, job, stored, permanent, delay);
        }
    }

    /**
     * Postgres text columns cannot hold the NUL character (code 0). An exception message
     * containing one would make the UPDATE itself fail, and the failure would go unrecorded. Long
     * messages are cut to {@link #MAX_ERROR_LENGTH}.
     */
    private static String cleanError(String errorMessage) {
        String cleaned = errorMessage.replace('\0', ' ');
        return cleaned.length() > MAX_ERROR_LENGTH ? cleaned.substring(0, MAX_ERROR_LENGTH) : cleaned;
    }

    private static TransitionResult runComplete(Connection conn, ClaimedJob job) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(COMPLETE_SQL)) {
            ps.setObject(1, job.id());
            ps.setString(2, job.lockedBy());
            ps.setInt(3, job.attempts());
            // executeUpdate returns how many rows changed: 1 = we owned it, 0 = we did not.
            int changed = ps.executeUpdate();
            if (changed == 0) {
                log.info("Job {} not completed: worker {} no longer owns attempt {}",
                        job.id(), job.lockedBy(), job.attempts());
                return TransitionResult.NOT_OWNER;
            }
            log.debug("Job {} succeeded on attempt {}", job.id(), job.attempts());
            return TransitionResult.SUCCEEDED;
        }
    }

    private static TransitionResult runFail(
            Connection conn, ClaimedJob job, String error, boolean permanent, Duration delay)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(FAIL_SQL)) {
            // Parameters are numbered in the order the ? marks appear in the SQL text.
            ps.setString(1, error);           // last_error = ?
            ps.setBoolean(2, permanent);      // ?::boolean AS permanent
            ps.setLong(3, delay.toMillis());  // ?::bigint AS retry_delay_ms
            ps.setObject(4, job.id());        // j.id = ?
            ps.setString(5, job.lockedBy());  // j.locked_by = ?
            ps.setInt(6, job.attempts());     // j.attempts = ?
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) { // no row returned = WHERE did not match = not the owner
                    log.info("Job {} not failed: worker {} no longer owns attempt {}",
                            job.id(), job.lockedBy(), job.attempts());
                    return TransitionResult.NOT_OWNER;
                }
                if ("dead".equals(rs.getString(1))) {
                    // WARN: a person may need to look at this job. The error text stays in the
                    // database, not in the log line.
                    log.warn("Job {} is dead after {} attempt(s){}", job.id(), job.attempts(),
                            permanent ? " (permanent failure)" : "");
                    return TransitionResult.DEAD;
                }
                log.debug("Job {} failed attempt {}, retrying in {} ms",
                        job.id(), job.attempts(), delay.toMillis());
                return TransitionResult.RETRY_SCHEDULED;
            }
        }
    }
}
