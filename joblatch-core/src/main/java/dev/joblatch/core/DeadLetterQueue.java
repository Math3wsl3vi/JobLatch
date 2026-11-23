package dev.joblatch.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operator tools for dead jobs: list them, and put them back in the queue once the cause is fixed.
 *
 * <p>Dead jobs are never deleted by JobLatch. They stay in {@code joblatch_jobs} with state 'dead',
 * their payload, their last error and their attempt count, until someone requeues them or a
 * retention job removes them.
 *
 * <p>This class is thread-safe.
 */
public final class DeadLetterQueue {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueue.class);

    /** Most rows one {@link #list} call returns. Page through bigger sets. */
    public static final int MAX_LIST_LIMIT = 1000;

    /** Newest deaths first. Served by the partial index joblatch_jobs_dead_idx from V2. */
    private static final String LIST_SQL = """
            SELECT id, queue, job_type, payload::text AS payload, attempts, max_attempts,
                   last_error, last_failed_at, finished_at
            FROM joblatch_jobs
            WHERE queue = ?
              AND state = 'dead'
            ORDER BY finished_at DESC
            LIMIT ?
            """;

    /**
     * Back to the start. attempts goes back to 0 so the job gets its full attempt budget again
     * (the claim query only picks jobs with attempts < max_attempts). last_error and
     * last_failed_at are kept as history until the next failure overwrites them.
     *
     * <p>{@code AND state = 'dead'} makes this safe to call twice, or on the wrong job: it only
     * ever changes a dead job.
     */
    private static final String REQUEUE_SQL = """
            UPDATE joblatch_jobs
            SET state       = 'available',
                attempts    = 0,
                run_at      = now(),
                finished_at = NULL,
                updated_at  = now()
            WHERE id = ?
              AND state = 'dead'
            """;

    private final DataSource dataSource;

    public DeadLetterQueue(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    /**
     * Lists dead jobs in a queue, most recently died first.
     *
     * @param limit between 1 and {@link #MAX_LIST_LIMIT}
     */
    public List<DeadJob> list(String queue, int limit) throws SQLException {
        Names.requireValid("queue", queue);
        if (limit < 1 || limit > MAX_LIST_LIMIT) {
            throw new IllegalArgumentException("limit must be between 1 and " + MAX_LIST_LIMIT);
        }

        // A single SELECT needs no explicit transaction: in auto-commit mode it runs as its own.
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(LIST_SQL)) {
            ps.setString(1, queue);
            ps.setInt(2, limit);
            List<DeadJob> dead = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    dead.add(new DeadJob(
                            rs.getObject("id", UUID.class),
                            rs.getString("queue"),
                            rs.getString("job_type"),
                            rs.getString("payload"),
                            rs.getInt("attempts"),
                            rs.getInt("max_attempts"),
                            rs.getString("last_error"),
                            toInstant(rs.getObject("last_failed_at", OffsetDateTime.class)),
                            toInstant(rs.getObject("finished_at", OffsetDateTime.class))));
                }
            }
            return dead;
        }
    }

    /**
     * Puts a dead job back in its queue, with a fresh set of attempts, to run as soon as possible.
     *
     * @return true if the job was dead and is now available again. False if no dead job has
     *     that ID (already requeued, never existed, or not dead).
     */
    public boolean requeue(UUID jobId) throws SQLException {
        Objects.requireNonNull(jobId, "jobId");
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(REQUEUE_SQL)) {
            ps.setObject(1, jobId);
            boolean requeued = ps.executeUpdate() == 1;
            if (requeued) {
                // INFO, not DEBUG: a person took a deliberate action. Worth an audit trail.
                log.info("Dead job {} requeued", jobId);
            }
            return requeued;
        }
    }

    private static Instant toInstant(OffsetDateTime value) {
        return value == null ? null : value.toInstant();
    }
}
