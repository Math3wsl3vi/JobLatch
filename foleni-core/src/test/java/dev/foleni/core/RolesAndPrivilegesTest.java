package dev.foleni.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

/**
 * Applies the role scripts from {@code docs/sql/} exactly as the security guide tells operators to
 * (with psql, inside the database container), then proves both halves of least privilege:
 *
 * <ul>
 *   <li>the application role can do everything Foleni needs at runtime
 *   <li>it cannot do anything else: no DELETE, no DDL, no migrations
 * </ul>
 *
 * <p>If someone edits the documented SQL and breaks it, this test fails.
 */
@Testcontainers
class RolesAndPrivilegesTest {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    /** The docs folder, relative to the foleni-core module (Maven runs tests from there). */
    private static final Path SQL_DIR = Path.of("..", "docs", "sql");

    /** SQLState for "insufficient privilege" (also used for "must be owner"). */
    private static final String INSUFFICIENT_PRIVILEGE = "42501";

    // Synthetic test-only credentials for a throwaway container.
    private static final String OWNER_PASSWORD = "test-only-owner-password";
    private static final String APP_PASSWORD = "test-only-app-password";
    private static final String MAINTENANCE_PASSWORD = "test-only-maintenance-password";

    static DataSource owner;
    static DataSource app;
    static DataSource maintenance;

    @BeforeAll
    static void applyDocumentedRoleScripts() throws Exception {
        // Step 1, as the superuser, with the password passed as a psql variable (as documented).
        runPsql(POSTGRES.getUsername(), "01-create-roles.sql", "owner_password=" + OWNER_PASSWORD);

        // Migrations run as the owner.
        owner = dataSource("foleni_owner", OWNER_PASSWORD);
        FoleniMigrations.migrate(owner);

        // Step 2, as the owner.
        runPsql("foleni_owner", "02-grant-privileges.sql", null);

        // The application's own login role, and a retention job's login role, as the guide shows.
        // DDL cannot take ? parameters; these are fixed test constants, never input.
        try (Connection conn = superuserConnection(); Statement st = conn.createStatement()) {
            st.execute("CREATE ROLE orders_app LOGIN PASSWORD '" + APP_PASSWORD + "' IN ROLE foleni_runtime");
            st.execute("CREATE ROLE retention_job LOGIN PASSWORD '" + MAINTENANCE_PASSWORD + "' IN ROLE foleni_maintenance");
        }
        app = dataSource("orders_app", APP_PASSWORD);
        maintenance = dataSource("retention_job", MAINTENANCE_PASSWORD);
    }

    @BeforeEach
    void emptyTable() throws SQLException {
        try (Connection conn = superuserConnection(); Statement st = conn.createStatement()) {
            st.execute("TRUNCATE foleni_jobs");
        }
    }

    // ---------------------------------------------------------------- everything the app needs

    @Test
    void runtimeRoleCanRunTheWholeJobLifecycle() throws Exception {
        JobQueue queue = new JobQueue();
        JobClaimer claimer = new JobClaimer(app);
        JobLifecycle lifecycle = new JobLifecycle(app, RetryPolicy.noDelay());
        LeaseManager leases = new LeaseManager(app);
        DeadLetterQueue deadLetters = new DeadLetterQueue(app);

        // enqueue, both ways
        queue.enqueue(app, NewJob.of("q", "t", "{}"));
        try (Connection conn = app.getConnection()) {
            conn.setAutoCommit(false);
            queue.enqueue(conn, NewJob.of("q", "t", "{}").withMaxAttempts(1));
            conn.commit();
        }
        assertThat(claimer.timeUntilNextDue("q")).contains(Duration.ZERO);

        // claim, heartbeat, complete
        List<ClaimedJob> claimed = claimer.claim("q", "w", 2, Duration.ofMinutes(5));
        assertThat(claimed).hasSize(2);
        assertThat(leases.renew(claimed.get(0), Duration.ofMinutes(5))).isPresent();

        // one succeeds, the other fails on its only attempt and goes dead
        ClaimedJob ok = claimed.stream().filter(j -> j.maxAttempts() > 1).findFirst().orElseThrow();
        ClaimedJob bad = claimed.stream().filter(j -> j.maxAttempts() == 1).findFirst().orElseThrow();
        assertThat(lifecycle.complete(ok)).isEqualTo(TransitionResult.SUCCEEDED);
        assertThat(lifecycle.fail(bad, "boom")).isEqualTo(TransitionResult.DEAD);

        // dead-letter queue
        assertThat(deadLetters.list("q", 10)).hasSize(1);
        assertThat(deadLetters.requeue(bad.id())).isTrue();

        // reaper
        claimer.claim("q", "w", 1, Duration.ofMillis(1));
        Thread.sleep(20);
        assertThat(leases.reclaimExpired(10).total()).isEqualTo(1);
    }

    @Test
    void runtimeRoleCanListenAndRunAWorkerPool() throws Exception {
        AtomicInteger wakes = new AtomicInteger();
        try (JobNotificationListener listener = new JobNotificationListener(app, "q", wakes::incrementAndGet)) {
            listener.start();
            waitFor(() -> listener.isListening());
            CountDownLatch ran = new CountDownLatch(1);
            try (WorkerPool pool = new WorkerPool(app, WorkerPoolConfig.defaults("q"), Map.of("t", job -> ran.countDown()))) {
                pool.start();
                new JobQueue().enqueue(app, NewJob.of("q", "t", "{}"));
                assertThat(ran.await(10, TimeUnit.SECONDS)).isTrue();
            }
            assertThat(wakes.get()).as("NOTIFY reached the runtime role").isGreaterThanOrEqualTo(2);
        }
    }

    @Test
    void documentedMonitoringQueriesRunAsRuntimeRole() throws Exception {
        new JobQueue().enqueue(app, NewJob.of("q", "t", "{}"));
        List<String> statements = sqlStatements("monitoring.sql");
        assertThat(statements).hasSize(5);
        try (Connection conn = app.getConnection(); Statement st = conn.createStatement()) {
            for (String sql : statements) {
                try (ResultSet rs = st.executeQuery(sql)) {
                    rs.next(); // ran without an error is what matters
                }
            }
        }
    }

    // ---------------------------------------------------------------- and nothing more

    @Test
    void runtimeRoleCannotDeleteTruncateOrChangeTheSchema() {
        assertDenied(app, "DELETE FROM foleni_jobs");
        assertDenied(app, "TRUNCATE foleni_jobs");
        assertDenied(app, "DROP TABLE foleni_jobs");
        assertDenied(app, "ALTER TABLE foleni_jobs ADD COLUMN x int");
        assertDenied(app, "CREATE TABLE sneaky (id int)");
        assertDenied(app, "SELECT * FROM foleni_schema_history");
    }

    @Test
    void runtimeRoleCannotRunMigrations() {
        assertThatThrownBy(() -> FoleniMigrations.migrate(app))
                .isInstanceOf(SQLException.class)
                .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo(INSUFFICIENT_PRIVILEGE));
    }

    // ---------------------------------------------------------------- retention

    @Test
    void maintenanceRoleRunsDocumentedRetentionAndNothingElse() throws Exception {
        UUID old = insertSucceeded("now() - interval '30 days'");
        UUID recent = insertSucceeded("now() - interval '1 hour'");

        String retention = sqlStatements("03-retention.sql").get(0);
        try (Connection conn = maintenance.getConnection(); Statement st = conn.createStatement()) {
            assertThat(st.executeUpdate(retention)).isEqualTo(1);
        }
        assertThat(exists(old)).isFalse();
        assertThat(exists(recent)).isTrue();

        assertDenied(maintenance, "UPDATE foleni_jobs SET priority = 1");
        assertDenied(maintenance, "INSERT INTO foleni_jobs (queue, job_type, payload) VALUES ('q', 't', '{}')");
    }

    // ---------------------------------------------------------------- helpers

    private static void assertDenied(DataSource ds, String sql) {
        assertThatThrownBy(() -> {
            try (Connection conn = ds.getConnection(); Statement st = conn.createStatement()) {
                st.execute(sql);
            }
        }).as(sql)
                .isInstanceOf(SQLException.class)
                .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo(INSUFFICIENT_PRIVILEGE));
    }

    /** Copies a documented script into the container and runs it with psql, like an operator would. */
    private static void runPsql(String user, String file, String variable) throws Exception {
        String target = "/tmp/" + file;
        POSTGRES.copyFileToContainer(MountableFile.forHostPath(SQL_DIR.resolve(file)), target);
        List<String> cmd = new ArrayList<>(List.of(
                "psql", "-v", "ON_ERROR_STOP=1", "-U", user, "-d", POSTGRES.getDatabaseName()));
        if (variable != null) {
            cmd.add("-v");
            cmd.add(variable);
        }
        cmd.add("-f");
        cmd.add(target);
        ExecResult result = POSTGRES.execInContainer(cmd.toArray(String[]::new));
        assertThat(result.getExitCode()).as(file + ": " + result.getStderr()).isZero();
    }

    /** Splits a documented SQL file into statements. The files keep semicolons out of comments. */
    private static List<String> sqlStatements(String file) throws Exception {
        String text = Files.readString(SQL_DIR.resolve(file));
        String withoutComments = text.replaceAll("(?m)^\\s*--.*$", "");
        return Arrays.stream(withoutComments.split(";"))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }

    private static UUID insertSucceeded(String finishedAt) throws SQLException {
        try (Connection conn = superuserConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO foleni_jobs (queue, job_type, payload, state, finished_at)"
                                + " VALUES ('q', 't', '{}', 'succeeded', " + finishedAt + ") RETURNING id")) {
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getObject(1, UUID.class);
            }
        }
    }

    private static boolean exists(UUID id) throws SQLException {
        try (Connection conn = superuserConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT 1 FROM foleni_jobs WHERE id = ?")) {
            ps.setObject(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private static DataSource dataSource(String user, String password) {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(POSTGRES.getJdbcUrl());
        ds.setUser(user);
        ds.setPassword(password);
        return ds;
    }

    @FunctionalInterface
    private interface Condition {
        boolean isTrue() throws Exception;
    }

    private static void waitFor(Condition condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!condition.isTrue()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within 20 s");
            }
            Thread.sleep(20);
        }
    }

    /** A connection as the container's superuser, for test setup only. */
    private static Connection superuserConnection() throws SQLException {
        return dataSource(POSTGRES.getUsername(), POSTGRES.getPassword()).getConnection();
    }
}
