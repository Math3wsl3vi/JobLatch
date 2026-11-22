package dev.foleni.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Integration tests against a real Postgres started in Docker by Testcontainers.
 *
 * <p>Each test is named after the promise it proves. Read the names as the spec.
 */
@Testcontainers
class JobQueueTest {

    // "static" means one container for all tests in this class (starting one takes seconds).
    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    static DataSource dataSource;

    private final JobQueue jobQueue = new JobQueue();

    @BeforeAll
    static void setUpDatabase() throws SQLException {
        // PGSimpleDataSource is the driver's own DataSource with no pooling.
        // That is fine for tests. Real apps would use a pool such as HikariCP.
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(POSTGRES.getJdbcUrl());
        ds.setUser(POSTGRES.getUsername());
        ds.setPassword(POSTGRES.getPassword());
        dataSource = ds;

        FoleniMigrations.migrate(dataSource);

        // A pretend "business" table, to prove a job commits or rolls back together with app data.
        try (Connection conn = dataSource.getConnection(); Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE orders (id bigint PRIMARY KEY)");
        }
    }

    @BeforeEach
    void emptyTables() throws SQLException {
        // Every test starts from empty tables so tests cannot affect each other.
        try (Connection conn = dataSource.getConnection(); Statement st = conn.createStatement()) {
            st.execute("TRUNCATE foleni_jobs, orders");
        }
    }

    // ---------------------------------------------------------------- happy path

    @Test
    void committedJobIsStoredWithCorrectFields() throws SQLException {
        UUID id;
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            id = jobQueue.enqueue(conn, NewJob.of("emails", "send-receipt", "{\"orderId\":42}")
                    .withPriority(7)
                    .withMaxAttempts(3));
            conn.commit();
        }

        // Read it back on a DIFFERENT connection: proves it was really committed to the database,
        // not just visible inside the first connection's transaction.
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT queue, job_type, payload->>'orderId' AS order_id, state, priority,"
                                + " attempts, max_attempts, locked_by, finished_at"
                                + " FROM foleni_jobs WHERE id = ?")) {
            ps.setObject(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("queue")).isEqualTo("emails");
                assertThat(rs.getString("job_type")).isEqualTo("send-receipt");
                assertThat(rs.getString("order_id")).isEqualTo("42");
                assertThat(rs.getString("state")).isEqualTo("available");
                assertThat(rs.getInt("priority")).isEqualTo(7);
                assertThat(rs.getInt("attempts")).isZero();
                assertThat(rs.getInt("max_attempts")).isEqualTo(3);
                assertThat(rs.getString("locked_by")).isNull();
                assertThat(rs.getObject("finished_at")).isNull();
            }
        }
    }

    @Test
    void runAtDefaultsToDatabaseNowAndHonoursExplicitValue() throws SQLException {
        Instant future = Instant.now().plus(1, ChronoUnit.HOURS).truncatedTo(ChronoUnit.MICROS);
        UUID immediate;
        UUID scheduled;
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            immediate = jobQueue.enqueue(conn, NewJob.of("q", "t", "{}"));
            scheduled = jobQueue.enqueue(conn, NewJob.of("q", "t", "{}").withRunAt(future));
            conn.commit();
        }

        assertThat(runAtOf(immediate)).isBeforeOrEqualTo(Instant.now().plusSeconds(5));
        assertThat(runAtOf(scheduled)).isEqualTo(future);
    }

    @Test
    void dataSourceOverloadCommitsByItself() throws SQLException {
        UUID id = jobQueue.enqueue(dataSource, NewJob.of("emails", "send-receipt", "{}"));

        assertThat(jobExists(id)).isTrue();
    }

    // ---------------------------------------------------------------- transaction semantics

    @Test
    void callerRollbackRemovesTheJob() throws SQLException {
        UUID id;
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            id = jobQueue.enqueue(conn, NewJob.of("emails", "send-receipt", "{}"));
            conn.rollback();
        }

        assertThat(jobExists(id)).isFalse();
    }

    @Test
    void jobAndBusinessRowCommitTogether() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            insertOrder(conn, 1);
            jobQueue.enqueue(conn, NewJob.of("emails", "send-receipt", "{\"orderId\":1}"));
            conn.commit();
        }

        assertThat(count("orders")).isEqualTo(1);
        assertThat(count("foleni_jobs")).isEqualTo(1);
    }

    @Test
    void jobIsInvisibleToOthersUntilCallerCommits() throws SQLException {
        try (Connection writer = dataSource.getConnection()) {
            writer.setAutoCommit(false);
            UUID id = jobQueue.enqueue(writer, NewJob.of("emails", "send-receipt", "{}"));

            // Another connection (think: a worker) must not see an uncommitted job.
            assertThat(jobExists(id)).isFalse();

            writer.commit();
            assertThat(jobExists(id)).isTrue();
        }
    }

    /**
     * The scenario from question 1: the enqueue fails, the caller ignores the error and commits
     * anyway. Postgres has already marked the transaction as aborted, so the order must NOT be
     * saved either. "An enqueue failure does not silently commit the caller's transaction."
     */
    @Test
    void failedEnqueueAbortsCallersTransactionEvenIfCallerCommits() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            insertOrder(conn, 1);

            // "{not json" passes Java validation (it is non-blank) but Postgres rejects it.
            assertThatThrownBy(() -> jobQueue.enqueue(conn, NewJob.of("q", "t", "{not json")))
                    .isInstanceOf(SQLException.class);

            // A badly written caller swallows the error and commits.
            // Depending on driver settings, commit() either throws or quietly rolls back.
            // Either way, nothing may be saved.
            try {
                conn.commit();
            } catch (SQLException expected) {
                conn.rollback();
            }
        }

        assertThat(count("orders")).isZero();
        assertThat(count("foleni_jobs")).isZero();
    }

    @Test
    void autoCommitConnectionIsRejectedAndNothingIsInserted() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            assertThat(conn.getAutoCommit()).isTrue(); // JDBC default

            assertThatIllegalArgumentException()
                    .isThrownBy(() -> jobQueue.enqueue(conn, NewJob.of("q", "t", "{}")))
                    .withMessageContaining("auto-commit");
        }

        assertThat(count("foleni_jobs")).isZero();
    }

    @Test
    void enqueueNeverChangesOrClosesTheCallersConnection() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            // On success...
            jobQueue.enqueue(conn, NewJob.of("q", "t", "{}"));
            assertThat(conn.isClosed()).isFalse();
            assertThat(conn.getAutoCommit()).isFalse();

            // ...and on failure, the connection is still the caller's, untouched.
            assertThatThrownBy(() -> jobQueue.enqueue(conn, NewJob.of("q", "t", "nope")))
                    .isInstanceOf(SQLException.class);
            assertThat(conn.isClosed()).isFalse();
            assertThat(conn.getAutoCommit()).isFalse();

            conn.rollback();
        }
    }

    // ---------------------------------------------------------------- security

    @Test
    void errorMessagesNeverContainThePayload() throws SQLException {
        String secret = "SSN-987-65-4321";
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            assertThatThrownBy(() -> jobQueue.enqueue(conn,
                            NewJob.of("q", "t", "{\"ssn\": \"" + secret + "\", broken")))
                    .isInstanceOf(SQLException.class)
                    .hasMessageNotContaining(secret)
                    .hasNoCause() // the original exception, which may quote the data, is dropped
                    .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo("22P02"));

            conn.rollback();
        }
    }

    // ---------------------------------------------------------------- schema safety net

    @Test
    void databaseRejectsImpossibleRowsEvenWithoutJava() throws SQLException {
        // Bypass Java validation entirely, as a buggy future change or a manual SQL session might.
        try (Connection conn = dataSource.getConnection(); Statement st = conn.createStatement()) {
            assertThatThrownBy(() -> st.execute(
                            "INSERT INTO foleni_jobs (queue, job_type, payload, state)"
                                    + " VALUES ('q', 't', '{}', 'running')"))
                    .as("running without an owner")
                    .isInstanceOf(SQLException.class)
                    .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo("23514"));

            assertThatThrownBy(() -> st.execute(
                            "INSERT INTO foleni_jobs (queue, job_type, payload, attempts, max_attempts)"
                                    + " VALUES ('q', 't', '{}', 6, 5)"))
                    .as("more attempts than allowed")
                    .isInstanceOf(SQLException.class);

            assertThatThrownBy(() -> st.execute(
                            "INSERT INTO foleni_jobs (queue, job_type, payload, state)"
                                    + " VALUES ('q', 't', '{}', 'banana')"))
                    .as("unknown state")
                    .isInstanceOf(SQLException.class);
        }
    }

    @Test
    void migrationsAreSafeToRunTwice() throws SQLException {
        FoleniMigrations.migrate(dataSource); // already ran in @BeforeAll

        assertThat(count("foleni_schema_history")).isEqualTo(1);
    }

    // ---------------------------------------------------------------- helpers

    private static void insertOrder(Connection conn, long id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO orders (id) VALUES (?)")) {
            ps.setLong(1, id);
            ps.executeUpdate();
        }
    }

    private static boolean jobExists(UUID id) throws SQLException {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT 1 FROM foleni_jobs WHERE id = ?")) {
            ps.setObject(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private static Instant runAtOf(UUID id) throws SQLException {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT run_at FROM foleni_jobs WHERE id = ?")) {
            ps.setObject(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getObject(1, OffsetDateTime.class).toInstant();
            }
        }
    }

    /** Table names here are fixed strings from this file, never user input. */
    private static long count(String table) throws SQLException {
        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT count(*) FROM " + table)) {
            rs.next();
            return rs.getLong(1);
        }
    }
}