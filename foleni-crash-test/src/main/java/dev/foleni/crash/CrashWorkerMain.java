package dev.foleni.crash;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.foleni.core.ClaimedJob;
import dev.foleni.core.JobHandler;
import dev.foleni.core.LeaseManager;
import dev.foleni.core.LeaseReaper;
import dev.foleni.core.RetryPolicy;
import dev.foleni.core.WorkerPool;
import dev.foleni.core.WorkerPoolConfig;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import javax.sql.DataSource;

/**
 * A real worker process, the kind you would deploy. The crash tests start several of these as
 * separate JVMs and kill them.
 *
 * <p>Configuration comes from environment variables, never command-line arguments: anyone on the
 * machine can read another process's arguments (with {@code ps}), but not its environment.
 *
 * <ul>
 *   <li>{@code FOLENI_DB_URL}: JDBC URL
 *   <li>{@code FOLENI_DB_USER}, {@code FOLENI_DB_PASSWORD}: credentials
 * </ul>
 *
 * <p>Prints {@code READY} to standard output once it is processing jobs.
 *
 * <p>Expects the tables {@code crash_executions} and {@code crash_results} to exist (the test
 * creates them).
 */
public final class CrashWorkerMain {

    static final String QUEUE = "crash";

    /** A normal job: a little work, then an idempotent side effect. */
    static final String WORK = "work";

    /**
     * A job built to show duplicate execution: on its first attempt it commits its side effect and
     * then hangs, so the test can kill the process between "work done" and "job completed".
     */
    static final String EFFECT_THEN_HANG = "effect-then-hang";

    /** Short lease so recovery after a kill is quick: heartbeat every ~667 ms. */
    private static final Duration LEASE = Duration.ofSeconds(2);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(500);
    private static final Duration REAPER_INTERVAL = Duration.ofMillis(500);
    private static final int REAPER_BATCH = 100;
    private static final int CONCURRENCY = 4;
    /** Handler threads + poller + heartbeats + listener + reaper + outcome writes, with headroom. */
    private static final int CONNECTION_POOL_SIZE = 16;
    /** Simulated work per job, so that kills land while jobs are in flight. */
    private static final int MIN_WORK_MS = 10;
    private static final int MAX_WORK_MS = 50;
    /** "Forever" for a hanging handler: far longer than any test runs. */
    private static final Duration HANG = Duration.ofHours(1);

    private static final String RECORD_EXECUTION_SQL =
            "INSERT INTO crash_executions (job_id, attempt, worker) VALUES (?, ?, ?)";

    /**
     * The idempotent side effect. The job ID is the idempotency key: the primary key on job_id
     * turns a second execution's insert into a no-op instead of a second result.
     */
    private static final String RECORD_RESULT_SQL =
            "INSERT INTO crash_results (job_id, worker) VALUES (?, ?) ON CONFLICT (job_id) DO NOTHING";

    private CrashWorkerMain() {}

    public static void main(String[] args) throws Exception {
        HikariConfig hikari = new HikariConfig();
        hikari.setJdbcUrl(requireEnv("FOLENI_DB_URL"));
        hikari.setUsername(requireEnv("FOLENI_DB_USER"));
        hikari.setPassword(requireEnv("FOLENI_DB_PASSWORD"));
        hikari.setMaximumPoolSize(CONNECTION_POOL_SIZE);
        hikari.setPoolName("crash-worker");
        HikariDataSource dataSource = new HikariDataSource(hikari);

        String workerName = "pid-" + ProcessHandle.current().pid();
        Map<String, JobHandler> handlers = Map.of(
                WORK, job -> {
                    Thread.sleep(ThreadLocalRandom.current().nextInt(MIN_WORK_MS, MAX_WORK_MS + 1));
                    recordEffect(dataSource, job, workerName);
                },
                EFFECT_THEN_HANG, job -> {
                    recordEffect(dataSource, job, workerName);
                    if (job.attempts() == 1) {
                        Thread.sleep(HANG.toMillis()); // the test kills us here
                    }
                });

        WorkerPoolConfig config = WorkerPoolConfig.defaults(QUEUE)
                .withConcurrency(CONCURRENCY)
                .withLeaseDuration(LEASE)
                .withPollInterval(POLL_INTERVAL)
                .withShutdownTimeout(Duration.ofSeconds(10))
                .withRetryPolicy(new RetryPolicy(Duration.ofMillis(50), Duration.ofSeconds(1), 2.0, 0.5));

        WorkerPool pool = new WorkerPool(dataSource, config, handlers);
        LeaseReaper reaper = new LeaseReaper(new LeaseManager(dataSource), REAPER_INTERVAL, REAPER_BATCH);

        // Runs on SIGTERM (Process.destroy(), Ctrl+C, Kubernetes stopping a pod).
        // Does NOT run on SIGKILL (Process.destroyForcibly(), kill -9): nothing does.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            pool.close();   // stop claiming, let running jobs finish
            reaper.close();
            dataSource.close();
        }, "foleni-shutdown"));

        reaper.start();
        pool.start();
        System.out.println("READY " + workerName);
        System.out.flush();

        // The pool's threads are daemons, so something must keep the JVM alive. The main thread
        // waits for itself to finish, which never happens: the process runs until it is signalled.
        Thread.currentThread().join();
    }

    /** Records the execution (every time) and the result (once per job), in one transaction. */
    private static void recordEffect(DataSource dataSource, ClaimedJob job, String workerName) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement ps = conn.prepareStatement(RECORD_EXECUTION_SQL)) {
                    ps.setObject(1, job.id());
                    ps.setInt(2, job.attempts());
                    ps.setString(3, workerName);
                    ps.executeUpdate();
                }
                try (PreparedStatement ps = conn.prepareStatement(RECORD_RESULT_SQL)) {
                    ps.setObject(1, job.id());
                    ps.setString(2, workerName);
                    ps.executeUpdate();
                }
                conn.commit();
            } catch (SQLException | RuntimeException e) {
                try {
                    conn.rollback();
                } catch (SQLException rollbackFailure) {
                    e.addSuppressed(rollbackFailure); // never hide the original error
                }
                throw e;
            }
        }
    }

    private static String requireEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            // Name the variable, never print a value: it might be the password.
            throw new IllegalStateException("Missing environment variable " + name);
        }
        return value;
    }
}
