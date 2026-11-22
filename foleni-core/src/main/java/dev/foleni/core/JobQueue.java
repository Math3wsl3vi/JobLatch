package dev.foleni.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.UUID;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The entry point for submitting jobs.
 *
 * <h2>Transaction ownership: the single most important rule in this class</h2>
 *
 * <p><b>Whoever starts a transaction is the one who ends it.</b>
 *
 * <ul>
 *   <li>{@link #enqueue(Connection, NewJob)} uses <em>your</em> connection and <em>your</em>
 *       transaction. It only runs one {@code INSERT}. It never commits, rolls back, closes the
 *       connection or changes its settings. The job becomes real only when you commit, and
 *       disappears if you roll back. That is what lets a job and your business data be saved
 *       together, or not at all.
 *   <li>{@link #enqueue(DataSource, NewJob)} borrows its own connection and commits by itself.
 *       Use it only when the job does not need to be atomic with anything else.
 * </ul>
 *
 * <p>This class is thread-safe: it holds no mutable state.
 */
public final class JobQueue {

    // SLF4J is a logging facade. The library only depends on its API, and the application picks
    // the real logging backend (Logback, Log4j, ...).
    private static final Logger log = LoggerFactory.getLogger(JobQueue.class);

    /**
     * The one SQL statement Milestone 1 needs.
     *
     * <ul>
     *   <li>Every value is a {@code ?} placeholder. The JDBC driver sends values separately from
     *       the SQL text, so a payload like {@code '); DROP TABLE ...} is just data. Never build
     *       SQL by gluing strings together.
     *   <li>{@code ?::jsonb} asks Postgres to parse the string as JSON. Invalid JSON is rejected
     *       by the database.
     *   <li>{@code COALESCE(?, now())}: if run_at is null, use the database clock. One clock for
     *       every machine means no clock-skew bugs.
     *   <li>{@code RETURNING id}: hands back the ID Postgres generated, with no second query.
     * </ul>
     */
    private static final String INSERT_SQL = """
            INSERT INTO foleni_jobs (queue, job_type, payload, run_at, priority, max_attempts)
            VALUES (?, ?, ?::jsonb, COALESCE(?, now()), ?, ?)
            RETURNING id
            """;

    /** Creates a job queue. It holds no resources, so there is nothing to close. */
    public JobQueue() {}

    /**
     * Adds a job inside the caller's transaction.
     *
     * <p>Typical use:
     *
     * <pre>{@code
     * try (Connection conn = dataSource.getConnection()) {
     *     conn.setAutoCommit(false);              // start a transaction
     *     try {
     *         insertOrder(conn, order);           // your business write
     *         jobQueue.enqueue(conn, NewJob.of("emails", "send-receipt", json));
     *         conn.commit();                      // order + job saved together
     *     } catch (Exception e) {
     *         conn.rollback();                    // neither is saved
     *         throw e;
     *     }
     * }
     * }</pre>
     *
     * @param connection an open connection with auto-commit turned off. Still owned by the
     *                   caller after this method returns or throws.
     * @param job        the job to add
     * @return the new job's ID. It is only visible to workers after the caller commits.
     * @throws IllegalArgumentException if the connection is in auto-commit mode
     * @throws SQLException if the insert fails. The caller's transaction is then aborted by
     *     Postgres and must be rolled back. The message never contains the payload.
     */
    public UUID enqueue(Connection connection, NewJob job) throws SQLException {
        Objects.requireNonNull(connection, "connection");
        Objects.requireNonNull(job, "job");

        // In auto-commit mode every statement commits immediately, so the job would be saved
        // even if the caller's later work fails. That silently breaks the promise of this
        // method, so we refuse loudly instead.
        if (connection.getAutoCommit()) {
            throw new IllegalArgumentException(
                    "enqueue(Connection, ...) needs a connection with auto-commit off so the job "
                            + "joins your transaction. Use enqueue(DataSource, ...) for a "
                            + "standalone job.");
        }

        return insert(connection, job);
    }

    /**
     * Adds a job in its own short transaction, committed before this method returns.
     *
     * <p>Not atomic with anything else you do. If you also write business data, use {@link
     * #enqueue(Connection, NewJob)} instead.
     *
     * @return the ID of the committed job
     * @throws SQLException if the job could not be saved. Nothing was committed in that case.
     */
    public UUID enqueue(DataSource dataSource, NewJob job) throws SQLException {
        Objects.requireNonNull(dataSource, "dataSource");
        Objects.requireNonNull(job, "job");

        // Here WE opened the connection, so WE own it: we start the transaction, end it,
        // and close the connection. try-with-resources closes it even if an exception is thrown.
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                UUID id = insert(connection, job);
                connection.commit();
                return id;
            } catch (SQLException | RuntimeException e) {
                rollbackQuietly(connection, e);
                throw e;
            }
        }
    }

    /** Runs the INSERT on the given connection. Does not touch the transaction. */
    private UUID insert(Connection connection, NewJob job) throws SQLException {
        // try-with-resources: the statement and result set are closed automatically
        // (this frees driver resources). Closing them does NOT end the transaction.
        try (PreparedStatement ps = connection.prepareStatement(INSERT_SQL)) {
            ps.setString(1, job.queue());
            ps.setString(2, job.jobType());
            ps.setString(3, job.payloadJson());
            if (job.runAt() == null) {
                ps.setNull(4, Types.TIMESTAMP_WITH_TIMEZONE);
            } else {
                // The Postgres JDBC driver maps OffsetDateTime to timestamptz exactly.
                ps.setObject(4, OffsetDateTime.ofInstant(job.runAt(), ZoneOffset.UTC));
            }
            ps.setInt(5, job.priority());
            ps.setInt(6, job.maxAttempts());

            try (ResultSet rs = ps.executeQuery()) {
                rs.next(); // INSERT ... RETURNING always returns exactly one row on success
                UUID id = rs.getObject("id", UUID.class);
                // Log identifiers only. Never the payload.
                log.debug("Enqueued job id={} queue={} type={}", id, job.queue(), job.jobType());
                return id;
            }
        } catch (SQLException e) {
            throw sanitize(e, job);
        }
    }

    /**
     * Postgres error messages can quote the data that caused them. For example, a constraint
     * violation includes "Failing row contains (...)" with the whole row, payload included. If
     * that exception were logged, the payload would leak into the logs.
     *
     * <p>So we throw a fresh exception that keeps the useful parts (the SQLState code, which
     * says what kind of error it was) and drops the message and cause that might contain data.
     *
     * <p>SQLState examples: {@code 22P02} = invalid JSON, {@code 23514} = a CHECK constraint
     * failed, {@code 42P01} = table missing (migrations not run), {@code 08xxx} = connection
     * problem.
     */
    private static SQLException sanitize(SQLException original, NewJob job) {
        return new SQLException(
                "Failed to enqueue job (queue=" + job.queue()
                        + ", type=" + job.jobType()
                        + ", SQLState=" + original.getSQLState() + ")",
                original.getSQLState(),
                original.getErrorCode());
    }

    /**
     * Rolls back, but never hides the original error. If the rollback itself fails (for
     * example because the connection is dead), that failure is attached to the original
     * exception as "suppressed" and the original is what gets thrown.
     */
    private static void rollbackQuietly(Connection connection, Exception original) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            original.addSuppressed(rollbackFailure);
        }
    }
}