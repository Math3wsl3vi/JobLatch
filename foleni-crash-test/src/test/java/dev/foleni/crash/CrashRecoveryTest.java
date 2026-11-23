package dev.foleni.crash;

import static org.assertj.core.api.Assertions.assertThat;

import dev.foleni.core.FoleniMigrations;
import dev.foleni.core.JobQueue;
import dev.foleni.core.NewJob;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Starts real worker processes ({@link CrashWorkerMain}) and kills them.
 *
 * <p>Recovery assumptions these tests rely on, and that production must provide too:
 *
 * <ol>
 *   <li>at least one worker process is running, or gets restarted
 *   <li>the database stays available
 *   <li>max_attempts is larger than the number of kills a single job can be caught in
 * </ol>
 *
 * <p>Worker output goes to {@code target/crash-logs/} for debugging.
 */
@Testcontainers
class CrashRecoveryTest {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    private static final Path LOG_DIR = Path.of("target", "crash-logs");
    private static final Duration READY_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration DRAIN_TIMEOUT = Duration.ofMinutes(3);

    static DataSource dataSource;

    private final List<Process> workers = new ArrayList<>();
    private int workersStarted = 0;

    @BeforeAll
    static void setUpDatabase() throws Exception {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(POSTGRES.getJdbcUrl());
        ds.setUser(POSTGRES.getUsername());
        ds.setPassword(POSTGRES.getPassword());
        dataSource = ds;
        FoleniMigrations.migrate(dataSource);
        // The application's side-effect tables.
        // crash_executions: one row per handler run that committed. NO unique key, so duplicates show.
        // crash_results:    one row per job. The primary key is the idempotency guard.
        execute("CREATE TABLE crash_executions (id bigserial PRIMARY KEY, job_id uuid NOT NULL,"
                + " attempt int NOT NULL, worker text NOT NULL, at timestamptz NOT NULL DEFAULT now())");
        execute("CREATE TABLE crash_results (job_id uuid PRIMARY KEY, worker text NOT NULL,"
                + " at timestamptz NOT NULL DEFAULT now())");
        Files.createDirectories(LOG_DIR);
    }

    @BeforeEach
    void reset() throws SQLException {
        execute("TRUNCATE foleni_jobs, crash_executions, crash_results");
    }

    @AfterEach
    void killAllWorkers() throws InterruptedException {
        for (Process p : workers) {
            p.destroyForcibly();
            p.waitFor(10, TimeUnit.SECONDS);
        }
        workers.clear();
    }

    // ---------------------------------------------------------------- the chaos test

    /**
     * Acceptance for Milestone 8. 3,000 jobs, 4 worker processes, and a dozen kill -9s at random
     * moments, each followed by a restart. Afterwards every job must have succeeded and produced
     * exactly one result.
     */
    @Test
    void randomKillsNeverLoseAJob() throws Exception {
        final int jobCount = 3000;
        final int processCount = 4;
        final int kills = 12;
        long seed = Long.getLong("crash.seed", System.nanoTime());
        Random random = new Random(seed);
        System.out.println("crash test seed = " + seed + " (rerun with -Dcrash.seed=" + seed + ")");

        insertJobs(jobCount, CrashWorkerMain.WORK);
        for (int i = 0; i < processCount; i++) {
            startWorker();
        }

        for (int k = 0; k < kills; k++) {
            Thread.sleep(300 + random.nextInt(600));
            int victim = random.nextInt(workers.size());
            Process p = workers.remove(victim);
            p.destroyForcibly();                     // SIGKILL: no cleanup of any kind runs
            p.waitFor(10, TimeUnit.SECONDS);
            startWorker();                           // the "orchestrator" restarts it
        }

        waitUntil(() -> countWhere("state IN ('succeeded', 'dead')") == jobCount, DRAIN_TIMEOUT);

        // 1. Every job reached a terminal state, and under our assumptions that means succeeded.
        assertThat(countWhere("state = 'succeeded'")).isEqualTo(jobCount);
        assertThat(countWhere("state <> 'succeeded'")).isZero();

        // 2. Every expected job ID appears in the results table. Nothing silently lost.
        assertThat(queryLong("SELECT count(*) FROM foleni_jobs j"
                + " LEFT JOIN crash_results r ON r.job_id = j.id WHERE r.job_id IS NULL")).isZero();
        // ...and exactly once each (the primary key guarantees it, the count proves it).
        assertThat(queryLong("SELECT count(*) FROM crash_results")).isEqualTo(jobCount);

        // 3. The kills really interrupted work: some jobs needed more than one attempt.
        long recovered = countWhere("attempts > 1");
        assertThat(recovered).as("jobs recovered after a kill").isPositive();

        long executions = queryLong("SELECT count(*) FROM crash_executions");
        System.out.printf("kills=%d, jobs=%d, executions=%d, duplicate executions=%d, jobs needing >1 attempt=%d%n",
                kills, jobCount, executions, executions - jobCount, recovered);
    }

    // ---------------------------------------------------------------- duplicates, made visible

    /**
     * At-least-once in action. The handler commits its side effect, then the process is killed
     * before the job is marked complete. The job is recovered and run AGAIN: two executions. The
     * idempotent insert keeps the result at exactly one.
     */
    @Test
    void killAfterSideEffectCausesDuplicateExecutionThatIdempotencyAbsorbs() throws Exception {
        UUID id = new JobQueue().enqueue(dataSource,
                NewJob.of(CrashWorkerMain.QUEUE, CrashWorkerMain.EFFECT_THEN_HANG, "{}").withMaxAttempts(5));

        Process first = startWorker();
        // Wait until the side effect of attempt 1 is committed. The handler is now hanging.
        waitUntil(() -> executionsOf(id) == 1, Duration.ofSeconds(30));
        assertThat(stateOf(id)).isEqualTo("running");

        workers.remove(first);
        first.destroyForcibly(); // dies between "work done" and "job completed"
        first.waitFor(10, TimeUnit.SECONDS);

        startWorker(); // its reaper reclaims the job once the lease runs out, then runs it again
        waitUntil(() -> "succeeded".equals(stateOf(id)), Duration.ofSeconds(60));

        assertThat(executionsOf(id)).as("the handler ran twice").isEqualTo(2);
        assertThat(queryLong("SELECT count(*) FROM crash_results")).as("but the effect happened once").isEqualTo(1);
        assertThat(attemptsOf(id)).isEqualTo(2);
        assertThat(lastErrorOf(id)).contains("lease expired");
    }

    // ---------------------------------------------------------------- graceful stop

    /**
     * SIGTERM is not SIGKILL: the shutdown hook runs, running jobs finish, and nothing is left
     * 'running' for the reaper to clean up. No job needs a second attempt.
     */
    @Test
    void sigtermDrainsRunningJobsWithoutDuplicates() throws Exception {
        insertJobs(400, CrashWorkerMain.WORK);
        Process worker = startWorker();
        waitUntil(() -> countWhere("state = 'succeeded'") > 20, Duration.ofSeconds(30));

        workers.remove(worker);
        worker.destroy(); // SIGTERM
        assertThat(worker.waitFor(30, TimeUnit.SECONDS)).as("exited after draining").isTrue();

        assertThat(countWhere("state = 'running'")).as("nothing left mid-flight").isZero();
        assertThat(countWhere("attempts > 1")).as("no job needed recovery").isZero();
        assertThat(queryLong("SELECT count(*) FROM crash_executions"))
                .as("no duplicate executions")
                .isEqualTo(queryLong("SELECT count(*) FROM crash_results"));
    }

    // ---------------------------------------------------------------- process management

    /** Starts a worker JVM and waits until it prints READY. */
    private Process startWorker() throws Exception {
        int n = ++workersStarted;
        Path log = LOG_DIR.resolve("worker-" + n + ".log");
        Files.deleteIfExists(log);

        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        // Surefire puts the full test classpath (our classes, foleni-core, HikariCP, the driver...)
        // in java.class.path. The child gets the same.
        ProcessBuilder pb = new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"),
                CrashWorkerMain.class.getName());
        pb.environment().put("FOLENI_DB_URL", POSTGRES.getJdbcUrl());
        pb.environment().put("FOLENI_DB_USER", POSTGRES.getUsername());
        pb.environment().put("FOLENI_DB_PASSWORD", POSTGRES.getPassword());
        pb.redirectErrorStream(true);
        pb.redirectOutput(log.toFile());

        Process p = pb.start();
        workers.add(p);
        waitUntil(() -> Files.exists(log) && Files.readString(log).contains("READY"), READY_TIMEOUT);
        return p;
    }

    private static void insertJobs(int count, String jobType) throws SQLException {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO foleni_jobs (queue, job_type, payload, max_attempts)"
                                + " SELECT ?, ?, jsonb_build_object('n', n), 25 FROM generate_series(1, ?) AS n")) {
            ps.setString(1, CrashWorkerMain.QUEUE);
            ps.setString(2, jobType);
            ps.setInt(3, count);
            ps.executeUpdate();
        }
    }

    // ---------------------------------------------------------------- queries

    @FunctionalInterface
    private interface Condition {
        boolean isTrue() throws Exception;
    }

    private static void waitUntil(Condition condition, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.isTrue()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within " + timeout
                        + " (worker logs are in " + LOG_DIR.toAbsolutePath() + ")");
            }
            Thread.sleep(100);
        }
    }

    /** {@code where} is always a fixed string written in this file, never input. */
    private static long countWhere(String where) throws SQLException {
        return queryLong("SELECT count(*) FROM foleni_jobs WHERE " + where);
    }

    private static long executionsOf(UUID id) throws SQLException {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM crash_executions WHERE job_id = ?")) {
            ps.setObject(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private static String stateOf(UUID id) throws SQLException {
        return jobColumn("state", id);
    }

    private static String lastErrorOf(UUID id) throws SQLException {
        return jobColumn("last_error", id);
    }

    private static int attemptsOf(UUID id) throws SQLException {
        return Integer.parseInt(jobColumn("attempts", id));
    }

    /** {@code column} is always a fixed name written in this file. */
    private static String jobColumn(String column, UUID id) throws SQLException {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT " + column + "::text FROM foleni_jobs WHERE id = ?")) {
            ps.setObject(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getString(1);
            }
        }
    }

    private static long queryLong(String sql) throws SQLException {
        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static void execute(String sql) throws SQLException {
        try (Connection conn = dataSource.getConnection(); Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }
}
