package dev.joblatch.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
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

/** Every event a WorkerPool and a LeaseReaper report, and that a bad listener cannot hurt them. */
@Testcontainers
class JobEventsTest {

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

    /** Records every event, from any thread. */
    static final class Recorder implements JobEventListener {
        final List<JobExecution> finished = new CopyOnWriteArrayList<>();
        final List<Integer> claimedCounts = new CopyOnWriteArrayList<>();
        final List<Boolean> claimEventsValid = new CopyOnWriteArrayList<>();
        final List<ClaimedJob> lost = new CopyOnWriteArrayList<>();
        final List<ReclaimResult> reclaimed = new CopyOnWriteArrayList<>();
        final List<Component> errors = new CopyOnWriteArrayList<>();

        @Override
        public void jobsClaimed(String queue, int requested, int claimed, Duration duration) {
            // No assertions here: they would run on the poller thread. Record, assert in the test.
            claimEventsValid.add(claimed >= 0 && claimed <= requested && !duration.isNegative());
            claimedCounts.add(claimed);
        }

        @Override
        public void jobFinished(JobExecution execution) {
            finished.add(execution);
        }

        @Override
        public void leaseLost(ClaimedJob job) {
            lost.add(job);
        }

        @Override
        public void leasesReclaimed(ReclaimResult result) {
            reclaimed.add(result);
        }

        @Override
        public void backgroundError(Component component, String queue, Throwable error) {
            errors.add(component);
        }

        List<JobExecution.Outcome> outcomes() {
            return finished.stream().map(JobExecution::outcome).toList();
        }
    }

    private static WorkerPoolConfig fastConfig() {
        return WorkerPoolConfig.defaults("q")
                .withPollInterval(Duration.ofMillis(20))
                .withRetryPolicy(RetryPolicy.noDelay());
    }

    // ---------------------------------------------------------------- outcomes

    @Test
    void successReportsClaimAndFinishWithHandlerDuration() throws Exception {
        enqueue("t", 5);
        Recorder events = new Recorder();

        try (WorkerPool pool = new WorkerPool(dataSource, fastConfig(), Map.of("t", job -> Thread.sleep(50)), events)) {
            pool.start();
            waitUntil(() -> events.finished.size() == 1);
        }

        JobExecution e = events.finished.get(0);
        assertThat(e.outcome()).isEqualTo(JobExecution.Outcome.SUCCEEDED);
        assertThat(e.handlerDuration()).isGreaterThanOrEqualTo(Duration.ofMillis(50));
        assertThat(e.failure()).isNull();
        assertThat(e.failureClass()).isEqualTo("none");
        assertThat(e.queue()).isEqualTo("q");
        assertThat(e.jobType()).isEqualTo("t");
        assertThat(events.claimedCounts).contains(1);
        assertThat(events.claimEventsValid).isNotEmpty().allMatch(valid -> valid);
    }

    @Test
    void retryThenSuccessThenPermanentFailureAreReportedAsSuch() throws Exception {
        enqueue("flaky", 5);
        enqueue("broken", 5);
        AtomicInteger calls = new AtomicInteger();
        Recorder events = new Recorder();

        try (WorkerPool pool = new WorkerPool(dataSource, fastConfig().withConcurrency(1), Map.of(
                "flaky", job -> {
                    if (calls.incrementAndGet() == 1) {
                        throw new java.io.IOException("connection reset");
                    }
                },
                "broken", job -> {
                    throw new PermanentJobFailureException("bad payload");
                }), events)) {
            pool.start();
            waitUntil(() -> events.finished.size() == 3);
        }

        assertThat(events.outcomes()).containsExactlyInAnyOrder(
                JobExecution.Outcome.RETRY_SCHEDULED, JobExecution.Outcome.SUCCEEDED, JobExecution.Outcome.DEAD);
        JobExecution retry = events.finished.stream()
                .filter(e -> e.outcome() == JobExecution.Outcome.RETRY_SCHEDULED).findFirst().orElseThrow();
        assertThat(retry.failureClass()).isEqualTo("java.io.IOException");
    }

    @Test
    void timeoutIsFlagged() throws Exception {
        enqueue("t", 1);
        Recorder events = new Recorder();

        WorkerPoolConfig config = fastConfig().withJobTimeout(Duration.ofMillis(100));
        try (WorkerPool pool = new WorkerPool(dataSource, config, Map.of("t", job -> Thread.sleep(30_000)), events)) {
            pool.start();
            waitUntil(() -> events.finished.size() == 1);
        }

        assertThat(events.finished.get(0).timedOut()).isTrue();
        assertThat(events.finished.get(0).outcome()).isEqualTo(JobExecution.Outcome.DEAD);
    }

    @Test
    void lostLeaseIsReportedAndTheOutcomeIsNotOwner() throws Exception {
        enqueue("t", 5);
        Recorder events = new Recorder();
        CountDownLatch started = new CountDownLatch(1);

        WorkerPoolConfig config = fastConfig().withLeaseDuration(Duration.ofMillis(300));
        try (WorkerPool pool = new WorkerPool(dataSource, config, Map.of("t", job -> {
            started.countDown();
            Thread.sleep(30_000);
        }), events)) {
            pool.start();
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
            execute("UPDATE joblatch_jobs SET locked_by = 'someone-else'"); // another worker took it

            waitUntil(() -> events.finished.size() == 1);
        }

        assertThat(events.lost).hasSize(1);
        assertThat(events.outcomes()).containsExactly(JobExecution.Outcome.NOT_OWNER);
    }

    // ---------------------------------------------------------------- reaper

    @Test
    void reaperReportsReclaimedLeases() throws Exception {
        enqueue("t", 5);
        enqueue("t", 1);
        new JobClaimer(dataSource).claim("q", "crashed-worker", 2, Duration.ofMillis(1));
        Recorder events = new Recorder();

        try (LeaseReaper reaper = new LeaseReaper(new LeaseManager(dataSource), Duration.ofMillis(50), 100, events)) {
            reaper.start();
            waitUntil(() -> !events.reclaimed.isEmpty());
        }

        assertThat(events.reclaimed).containsExactly(new ReclaimResult(1, 1));
    }

    // ---------------------------------------------------------------- errors

    /**
     * The handler takes the database "down" just before the pool records its outcome. The outcome
     * cannot be written (NOT_RECORDED) and both the failed write and the failing claims are
     * reported. Then the database "comes back" and the reaper-free pool simply carries on.
     */
    @Test
    void databaseErrorsAreReportedByComponent() throws Exception {
        AtomicBoolean databaseDown = new AtomicBoolean(false);
        DataSource flaky = failingWhile(databaseDown);
        enqueue("t", 5);
        Recorder events = new Recorder();

        WorkerPoolConfig config = fastConfig().withNotifications(false);
        try (WorkerPool pool = new WorkerPool(flaky, config, Map.of("t", job -> databaseDown.set(true)), events)) {
            pool.start();
            waitUntil(() -> events.finished.size() == 1 && events.errors.contains(JobEventListener.Component.CLAIM));
            databaseDown.set(false);
        }

        assertThat(events.outcomes()).containsExactly(JobExecution.Outcome.NOT_RECORDED);
        assertThat(events.errors).contains(JobEventListener.Component.RECORD_OUTCOME, JobEventListener.Component.CLAIM);
    }

    @Test
    void lostNotificationConnectionIsReported() throws Exception {
        Recorder events = new Recorder();
        try (WorkerPool pool = new WorkerPool(dataSource, fastConfig(), Map.of("t", job -> { }), events)) {
            pool.start();
            waitUntil(() -> listenerPid() != null);
            execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query = 'LISTEN joblatch_jobs'"
                    + " AND pid <> pg_backend_pid()");
            waitUntil(() -> events.errors.contains(JobEventListener.Component.NOTIFICATION_LISTENER));
        }
    }

    // ---------------------------------------------------------------- a bad listener

    /**
     * A listener that throws on every event, including an Error, must not stop jobs from being
     * processed.
     */
    @Test
    void throwingListenerCannotBreakThePool() throws Exception {
        for (int i = 0; i < 20; i++) {
            enqueue("t", 5);
        }
        AtomicInteger calls = new AtomicInteger();
        JobEventListener hostile = new JobEventListener() {
            @Override
            public void jobsClaimed(String queue, int requested, int claimed, Duration duration) {
                calls.incrementAndGet();
                throw new IllegalStateException("metrics bug");
            }

            @Override
            public void jobFinished(JobExecution execution) {
                calls.incrementAndGet();
                throw new AssertionError("metrics bug that is an Error, not an Exception");
            }
        };

        try (WorkerPool pool = new WorkerPool(dataSource, fastConfig().withConcurrency(4), Map.of("t", job -> { }), hostile)) {
            pool.start();
            waitUntil(() -> countSucceeded() == 20);
        }
        assertThat(calls.get()).as("the listener really was called (and threw)").isGreaterThanOrEqualTo(20);
    }

    // ---------------------------------------------------------------- JobCounters

    @Test
    void jobCountersAddUpOutcomes() throws Exception {
        for (int i = 0; i < 10; i++) {
            enqueue("ok", 5);
        }
        enqueue("bad", 1);
        JobCounters counters = new JobCounters();

        try (WorkerPool pool = new WorkerPool(dataSource, fastConfig(), Map.of(
                "ok", job -> { },
                "bad", job -> {
                    throw new IllegalStateException("x");
                }), counters)) {
            pool.start();
            waitUntil(() -> counters.snapshot().finished() == 11);
        }

        JobCounters.Snapshot s = counters.snapshot();
        assertThat(s.succeeded()).isEqualTo(10);
        assertThat(s.dead()).isEqualTo(1);
        assertThat(s.retryScheduled()).isZero();
        assertThat(s.claimed()).isEqualTo(11);
        assertThat(s.emptyClaims()).isPositive(); // the pool polled again after the queue emptied
        assertThat(s.maxHandlerTime()).isGreaterThanOrEqualTo(s.averageHandlerTime());
        assertThat(s.errors().values()).allMatch(count -> count == 0);
    }

    @Test
    void jobExecutionToStringHidesPayloadAndMessage() {
        ClaimedJob job = new ClaimedJob(UUID.randomUUID(), "q", "t", "{\"card\":\"SECRET-1\"}", 1, 5, "w",
                java.time.Instant.now());
        JobExecution e = new JobExecution(job, JobExecution.Outcome.DEAD, Duration.ofMillis(5),
                new IllegalStateException("card SECRET-2 declined"), false);

        assertThat(e.toString()).doesNotContain("SECRET-1").doesNotContain("SECRET-2")
                .contains("java.lang.IllegalStateException");
    }

    // ---------------------------------------------------------------- helpers

    private void enqueue(String jobType, int maxAttempts) throws SQLException {
        jobQueue.enqueue(dataSource, NewJob.of("q", jobType, "{}").withMaxAttempts(maxAttempts));
    }

    /** getConnection() fails while {@code down} is true, like an unreachable database. */
    private static DataSource failingWhile(AtomicBoolean down) {
        return (DataSource) Proxy.newProxyInstance(
                JobEventsTest.class.getClassLoader(),
                new Class<?>[] {DataSource.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getConnection") && down.get()) {
                        throw new SQLException("simulated outage", "08001");
                    }
                    try {
                        return method.invoke(dataSource, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private static Integer listenerPid() throws SQLException {
        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement();
                var rs = st.executeQuery("SELECT pid FROM pg_stat_activity WHERE query = 'LISTEN joblatch_jobs'")) {
            return rs.next() ? rs.getInt(1) : null;
        }
    }

    private static long countSucceeded() throws SQLException {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM joblatch_jobs WHERE state = 'succeeded'");
                var rs = ps.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
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

    private static void execute(String sql) throws SQLException {
        try (Connection conn = dataSource.getConnection(); Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }
}
