package dev.foleni.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Moves a claimed job to its next state: succeeded, back to available (retry), or dead.
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
 * whether recovery has happened. Rejecting finished work only because a clock ticked past
 * {@code locked_until} would force a pointless rerun.
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
     * Failure: one statement decides between retry and dead, so the decision and the write can
     * never disagree.
     *
     * <p>{@code CASE WHEN ... THEN ... ELSE ... END} is SQL's if/else. On the right of
     * {@code SET}, column names refer to the row's values before this update.
     *
     * <p>Retry happens immediately ({@code run_at = now()}) for now. Milestone 5 adds backoff.
     */
    private static final String FAIL_SQL = """
            UPDATE foleni_jobs
            SET state        = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'available' END,
                finished_at  = CASE WHEN attempts >= max_attempts THEN now() ELSE NULL END,
                run_at       = CASE WHEN attempts >= max_attempts THEN run_at ELSE now() END,
                locked_by    = NULL,
                locked_until = NULL,
                last_error   = ?,
                updated_at   = now()
            WHERE id = ?
              AND state = 'running'
              AND locked_by = ?
              AND attempts = ?
            RETURNING state
            """;

    private final DataSource dataSource;

    /** @param dataSource used by the methods that manage their own transaction */
    public JobLifecycle(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    /**
     * Marks the job succeeded, in its own transaction.
     *
     * @return {@link TransitionResult#SUCCEEDED}, or {@link TransitionResult#NOT_OWNER} if the
     *     caller no longer owns the job (nothing was changed)
     */
    public TransitionResult complete(ClaimedJob job) throws SQLException {
        Objects.requireNonNull(job, "job");
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                TransitionResult result = runComplete(conn, job);
                conn.commit();
                return result;
            } catch (SQLException | RuntimeException e) {
                rollbackQuietly(conn, e);
                throw e;
            }
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
     * Records a failed attempt, in its own transaction. If attempts remain, the job becomes
     * available again. Otherwise it becomes dead and is kept for inspection.
     *
     * @param errorMessage a short description of what went wrong. It is stored in the database
     *     (truncated to {@link #MAX_ERROR_LENGTH}) and never logged by Foleni. Still, do not put
     *     secrets or personal data in it: operators will read it.
     * @return {@link TransitionResult#RETRY_SCHEDULED}, {@link TransitionResult#DEAD} or {@link
     *     TransitionResult#NOT_OWNER}
     */
    public TransitionResult fail(ClaimedJob job, String errorMessage) throws SQLException {
        Objects.requireNonNull(job, "job");
        Objects.requireNonNull(errorMessage, "errorMessage");
        // Postgres text columns cannot hold the NUL character (code 0). An exception message
        // containing one would make the UPDATE itself fail, and the failure would go unrecorded.
        String cleaned = errorMessage.replace('\0', ' ');
        String stored = cleaned.length() > MAX_ERROR_LENGTH
                ? cleaned.substring(0, MAX_ERROR_LENGTH)
                : cleaned;

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                TransitionResult result = runFail(conn, job, stored);
                conn.commit();
                return result;
            } catch (SQLException | RuntimeException e) {
                rollbackQuietly(conn, e);
                throw e;
            }
        }
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

    private static TransitionResult runFail(Connection conn, ClaimedJob job, String error)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(FAIL_SQL)) {
            ps.setString(1, error);
            ps.setObject(2, job.id());
            ps.setString(3, job.lockedBy());
            ps.setInt(4, job.attempts());
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) { // no row returned = WHERE did not match = not the owner
                    log.info("Job {} not failed: worker {} no longer owns attempt {}",
                            job.id(), job.lockedBy(), job.attempts());
                    return TransitionResult.NOT_OWNER;
                }
                String newState = rs.getString("state");
                if ("dead".equals(newState)) {
                    // WARN: a person may need to look at this job. The error text stays in the
                    // database, not in the log line.
                    log.warn("Job {} is dead after {} attempt(s)", job.id(), job.attempts());
                    return TransitionResult.DEAD;
                }
                log.debug("Job {} failed attempt {}, will retry", job.id(), job.attempts());
                return TransitionResult.RETRY_SCHEDULED;
            }
        }
    }

    private static void rollbackQuietly(Connection conn, Exception original) {
        try {
            conn.rollback();
        } catch (SQLException rollbackFailure) {
            original.addSuppressed(rollbackFailure);
        }
    }
}