package dev.joblatch.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** The worker pool end to end, against real Postgres. */
@Testcontainers
class WorkerPoolTest {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    private static final Duration WAIT = Duration.ofSeconds(20);

    static DataSource dataSource;

    private final JobQueue jobQueue = new JobQueue();

    @BeforeAll
    static void setUpDatabase() throws SQLException {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(POSTGRES.getJdbcUrl());
        ds.setUser(POSTGRES.getUsername());
        ds.setPassword(POSTGRES.getPassword());
        dataSource = ds;
        JobLatchMigrations.migrate(dataSource);
    }

    @BeforeEach
    void reset() throws SQLException {
        execute("TRUNCATE joblatch_jobs");
    }

    /** Fast settings for tests: short polling, no retry delay. */
    private static WorkerPoolConfig fastConfig() {
        return WorkerPoolConfig.defaults("q")
                .withPollInterval(Duration.ofMillis(20))
                .withShutdownTimeout(Duration.ofSeconds(10))
                .withRetryPolicy(RetryPolicy.noDelay());
    }

    // ---------------------------------------------------------------- happy path

    @Test
    void runsEveryJobAndMarksItSucceeded() throws Exception {
        int jobCount = 200;
        for (int i = 0; i < jobCount; i++) {
            enqueue("t", 5);
        }
        Set<UUID> seen = ConcurrentHashMap.newKeySet();

        try (WorkerPool pool = new WorkerPool(dataSource, fastConfig().withConcurrency(8),
                Map.of("t", job -> seen.add(job.id())))) {
            pool.start();
            waitUntil(() -> count("succeeded") == jobCount);
        }

        assertThat(seen).hasSize(jobCount);
    }

    // ---------------------------------------------------------------- outcomes

    @Test
    void exceptionIsRetriedThenSucceeds() throws Exception {
        UUID id = enqueue("flaky", 5);
        AtomicInteger calls = new AtomicInteger();

        try (WorkerPool pool = new WorkerPool(dataSource, fastConfig(), Map.of("flaky", job -> {
            if (calls.incrementAndGet() == 1) {
                throw new java.io.IOException("connection reset");
            }
        }))) {
            pool.start();
            waitUntil(() -> "succeeded".equals(stateOf(id)));
        }

        assertThat(calls.get()).isEqualTo(2);
        assertThat(attemptsOf(id)).isEqualTo(2);
        assertThat(lastErrorOf(id)).isEqualTo("java.io.IOException: connection reset");
    }

    @Test
    void permanentFailureGoesStraightToDead() throws Exception {
        UUID id = enqueue("t", 10);

        try (WorkerPool pool = new WorkerPool(dataSource, fastConfig(), Map.of("t", job -> {
            throw new PermanentJobFailureException("payload has no orderId");
        }))) {
            pool.start();
            waitUntil(() -> "dead".equals(stateOf(id)));
        }

        assertThat(attemptsOf(id)).isEqualTo(1);
        assertThat(lastErrorOf(id)).contains("payload has no orderId");
    }

    @Test
    void unknownJobTypeIsFailedNotLost() throws Exception {
        UUID id = enqueue("nobody-handles-this", 1);

        try (WorkerPool pool = new WorkerPool(dataSource, fastConfig(), Map.of("t", job -> { }))) {
            pool.start();
            waitUntil(() -> "dead".equals(stateOf(id)));
        }

        assertThat(lastErrorOf(id)).contains("no handler registered");
    }

    /** Acceptance: unexpected worker failures do not kill the pool. */
    @Test
    void handlerThrowingAnErrorDoesNotKillThePool() throws Exception {
        UUID bad = enqueue("explodes", 1);
        UUID good = enqueue("t", 1);

        try (WorkerPool pool = new WorkerPool(dataSource, fastConfig().withConcurrency(1), Map.of(
                "explodes", job -> {
                    throw new AssertionError("bug in handler"); // an Error, not an Exception
                },
                "t", job -> { }))) {
            pool.start();
            waitUntil(() -> "dead".equals(stateOf(bad)) && "succeeded".equals(stateOf(good)));
        }

        assertThat(lastErrorOf(bad)).contains("AssertionError");
    }

    // ---------------------------------------------------------------- backpressure

    /**
     * Acceptance: no unbounded growth. With concurrency 3, at most 3 handlers run at once AND at
     * most 3 jobs are ever claimed in the database. Nothing waits in a hidden in-memory queue.
     */
    @Test
    void neverRunsOrClaimsMoreThanConcurrency() throws Exception {
        int concurrency = 3;
        int jobCount = 30;
        for (int i = 0; i < jobCount; i++) {
            enqueue("t", 5);
        }
        AtomicInteger runningNow = new AtomicInteger();
        AtomicInteger maxRunning = new AtomicInteger();
        AtomicInteger maxClaimed = new AtomicInteger();

        try (WorkerPool pool = new WorkerPool(dataSource, fastConfig().withConcurrency(concurrency),
                Map.of("t", job -> {
                    int now = runningNow.incrementAndGet();
                    maxRunning.accumulateAndGet(now, Math::max);
                    maxClaimed.accumulateAndGet((int) count("running"), Math::max);
                    Thread.sleep(30);
                    runningNow.decrementAndGet();
                }))) {
            pool.start();
            waitUntil(() -> count("succeeded") == jobCount);
            // Wait, don't assert instantly: a job is marked succeeded a moment before its
            // worker finishes bookkeeping.
            waitUntil(() -> pool.activeJobs() == 0);
        }

        assertThat(maxRunning.get()).isLessThanOrEqualTo(concurrency);
        assertThat(maxClaimed.get()).isLessThanOrEqualTo(concurrency);
        assertThat(maxRunning.get()).as("work actually ran in parallel").isGreaterThan(1);
    }

    // ---------------------------------------------------------------- heartbeats

    /** A job that runs 3x longer than its lease keeps it, because the pool renews it. */
    @Test
    void longJobIsHeartbeatedAndNotReclaimed() throws Exception {
        UUID id = enqueue("slow", 5);
        LeaseManager reaper = new LeaseManager(dataSource);
        AtomicBoolean done = new AtomicBoolean();

        WorkerPoolConfig config = fastConfig().withLeaseDuration(Duration.ofMillis(600)); // heartbeat every 200 ms
        try (WorkerPool pool = new WorkerPool(dataSource, config, Map.of("slow", job -> {
            Thread.sleep(1800);
            done.set(true);
        }))) {
            pool.start();
            // Run the reaper over and over while the job is running. It must never reclaim it.
            while (!done.get()) {
                assertThat(reaper.reclaimExpired(10).total()).isZero();
                Thread.sleep(50);
            }
            waitUntil(() -> "succeeded".equals(stateOf(id)));
        }

        assertThat(attemptsOf(id)).isEqualTo(1);
    }

    /** If another worker takes the job over, the heartbeat notices and interrupts the handler. */
    @Test
    void lostOwnershipInterruptsTheHandler() throws Exception {
        UUID id = enqueue("t", 5);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();

        WorkerPoolConfig config = fastConfig().withLeaseDuration(Duration.ofMillis(300));
        try (WorkerPool pool = new WorkerPool(dataSource, config, Map.of("t", job -> {
            if (calls.incrementAndGet() > 1) {
                return; // second run (after recovery) just succeeds
            }
            started.countDown();
            try {
                Thread.sleep(30_000);
            } catch (InterruptedException e) {
                interrupted.countDown();
                throw e;
            }
        }))) {
            pool.start();
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
            // Simulate another worker having taken the job: it now belongs to "someone-else".
            execute("UPDATE joblatch_jobs SET locked_by = 'someone-else'");

            assertThat(interrupted.await(10, TimeUnit.SECONDS)).as("handler interrupted").isTrue();
        }
        // The stale worker's failure was rejected: the job still belongs to someone-else.
        assertThat(queryString("SELECT locked_by FROM joblatch_jobs WHERE id = ?", id)).isEqualTo("someone-else");
    }

    // ---------------------------------------------------------------- timeouts

    @Test
    void jobTimeoutInterruptsAndFailsTheJob() throws Exception {
        UUID id = enqueue("t", 1);

        WorkerPoolConfig config = fastConfig().withJobTimeout(Duration.ofMillis(200));
        try (WorkerPool pool = new WorkerPool(dataSource, config, Map.of("t", job -> Thread.sleep(30_000)))) {
            pool.start();
            waitUntil(() -> "dead".equals(stateOf(id)));
        }

        assertThat(lastErrorOf(id)).isEqualTo("timed out after 200 ms");
    }

    // ---------------------------------------------------------------- shutdown

    /** Acceptance: shutdown stops new claims and lets running jobs finish. */
    @Test
    void gracefulShutdownFinishesRunningJobsAndStopsClaiming() throws Exception {
        UUID running = enqueue("t", 5);
        CountDownLatch started = new CountDownLatch(1);

        WorkerPool pool = new WorkerPool(dataSource, fastConfig().withConcurrency(1), Map.of("t", job -> {
            started.countDown();
            Thread.sleep(500);
        }));
        pool.start();
        assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();

        pool.close(); // blocks until the running job finishes

        assertThat(stateOf(running)).isEqualTo("succeeded");
        UUID afterClose = enqueue("t", 5);
        Thread.sleep(200);
        assertThat(stateOf(afterClose)).as("no claims after close").isEqualTo("available");
        assertThatIllegalStateException().isThrownBy(pool::start);
    }

    @Test
    void shutdownTimeoutInterruptsStuckJobsAndRecordsFailure() throws Exception {
        UUID id = enqueue("t", 5);
        CountDownLatch started = new CountDownLatch(1);

        WorkerPoolConfig config = fastConfig().withShutdownTimeout(Duration.ofMillis(200));
        WorkerPool pool = new WorkerPool(dataSource, config, Map.of("t", job -> {
            started.countDown();
            Thread.sleep(30_000);
        }));
        pool.start();
        assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();

        long t0 = System.nanoTime();
        pool.close();
        long tookMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);

        assertThat(tookMs).as("close did not wait 30 s").isLessThan(5_000);
        // The interrupted job was failed and will be retried, not left 'running'.
        assertThat(stateOf(id)).isEqualTo("available");
        assertThat(lastErrorOf(id)).contains("InterruptedException");
    }

    @Test
    void closeWithoutStartAndDoubleCloseAreHarmless() {
        WorkerPool pool = new WorkerPool(dataSource, fastConfig(), Map.of("t", job -> { }));
        pool.close();
        pool.close();
    }

    // ---------------------------------------------------------------- database outage

    @Test
    void poolSurvivesDatabaseOutageWhilePolling() throws Exception {
        UUID id = enqueue("t", 5);
        AtomicInteger failuresLeft = new AtomicInteger(5);

        try (WorkerPool pool = new WorkerPool(failingFirst(failuresLeft), fastConfig(), Map.of("t", job -> { }))) {
            pool.start();
            waitUntil(() -> "succeeded".equals(stateOf(id)));
        }
        assertThat(failuresLeft.get()).isLessThanOrEqualTo(0);
    }

    // ---------------------------------------------------------------- validation

    @Test
    void rejectsInvalidConfiguration() {
        assertThatIllegalArgumentException().isThrownBy(() -> WorkerPoolConfig.defaults("bad queue"));
        assertThatIllegalArgumentException().isThrownBy(() -> fastConfig().withConcurrency(0));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> fastConfig().withConcurrency(WorkerPoolConfig.MAX_CONCURRENCY + 1));
        assertThatIllegalArgumentException().isThrownBy(() -> fastConfig().withPollInterval(Duration.ZERO));
        assertThatIllegalArgumentException().isThrownBy(() -> fastConfig().withLeaseDuration(Duration.ofMillis(2)));
        assertThatIllegalArgumentException().isThrownBy(() -> fastConfig().withJobTimeout(Duration.ZERO));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> fastConfig().withShutdownTimeout(Duration.ofSeconds(-1)));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new WorkerPool(dataSource, fastConfig(), Map.of("bad type", job -> { })));
    }

    // ---------------------------------------------------------------- helpers

    private UUID enqueue(String jobType, int maxAttempts) throws SQLException {
        return jobQueue.enqueue(dataSource, NewJob.of("q", jobType, "{}").withMaxAttempts(maxAttempts));
    }

    @FunctionalInterface
    private interface Condition {
        boolean isTrue() throws Exception;
    }

    private static void waitUntil(Condition condition) throws Exception {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (!condition.isTrue()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within " + WAIT);
            }
            Thread.sleep(20);
        }
    }

    /** First N getConnection() calls fail, like a database that is briefly down. See LeaseTest. */
    private static DataSource failingFirst(AtomicInteger failuresLeft) {
        return (DataSource) Proxy.newProxyInstance(
                WorkerPoolTest.class.getClassLoader(),
                new Class<?>[] {DataSource.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getConnection") && failuresLeft.getAndDecrement() > 0) {
                        throw new SQLException("simulated outage", "08001");
                    }
                    try {
                        return method.invoke(dataSource, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private static long count(String state) throws SQLException {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM joblatch_jobs WHERE state = ?")) {
            ps.setString(1, state);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private static String stateOf(UUID id) throws SQLException {
        return queryString("SELECT state FROM joblatch_jobs WHERE id = ?", id);
    }

    private static String lastErrorOf(UUID id) throws SQLException {
        return queryString("SELECT last_error FROM joblatch_jobs WHERE id = ?", id);
    }

    private static int attemptsOf(UUID id) throws SQLException {
        return Integer.parseInt(queryString("SELECT attempts::text FROM joblatch_jobs WHERE id = ?", id));
    }

    private static String queryString(String sql, UUID id) throws SQLException {
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getString(1);
            }
        }
    }

    private static void execute(String sql) throws SQLException {
        try (Connection conn = dataSource.getConnection(); Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }
}
