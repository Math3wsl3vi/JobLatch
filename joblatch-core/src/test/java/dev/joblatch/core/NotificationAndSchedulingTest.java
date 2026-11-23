package dev.joblatch.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
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

/** LISTEN/NOTIFY wake-ups, reconnection, polling fallback and on-time scheduled jobs. */
@Testcontainers
class NotificationAndSchedulingTest {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    private static final Duration WAIT = Duration.ofSeconds(20);
    /** Long enough that if a test passes quickly, it cannot have been the polling that did it. */
    private static final Duration SLOW_POLL = Duration.ofSeconds(30);
    /** How long we wait to be sure something did NOT happen. */
    private static final Duration QUIET_PERIOD = Duration.ofMillis(700);

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
        execute("ALTER TABLE joblatch_jobs ENABLE TRIGGER USER"); // in case a test disabled them
    }

    // ---------------------------------------------------------------- the listener on its own

    @Test
    void wakesOnceWhenConnectedThenOnEachCommittedJob() throws Exception {
        AtomicInteger wakes = new AtomicInteger();
        try (JobNotificationListener listener = new JobNotificationListener(dataSource, "q", wakes::incrementAndGet)) {
            listener.start();
            waitUntil(() -> wakes.get() == 1); // the "might have missed something" wake on connect

            jobQueue.enqueue(dataSource, NewJob.of("q", "t", "{}"));

            waitUntil(() -> wakes.get() == 2);
        }
    }

    @Test
    void rolledBackEnqueueSendsNothing() throws Exception {
        AtomicInteger wakes = new AtomicInteger();
        try (JobNotificationListener listener = startedListener("q", wakes)) {
            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);
                jobQueue.enqueue(conn, NewJob.of("q", "t", "{}"));
                conn.rollback();
            }
            Thread.sleep(QUIET_PERIOD.toMillis());
            assertThat(wakes.get()).as("no wake after rollback").isEqualTo(1);
            assertThat(listener.isListening()).isTrue();
        }
    }

    @Test
    void otherQueuesDoNotWakeUs() throws Exception {
        AtomicInteger wakes = new AtomicInteger();
        try (JobNotificationListener listener = startedListener("q", wakes)) {
            jobQueue.enqueue(dataSource, NewJob.of("other-queue", "t", "{}"));
            Thread.sleep(QUIET_PERIOD.toMillis());
            assertThat(wakes.get()).isEqualTo(1);
            assertThat(listener.isListening()).isTrue();
        }
    }

    /** 1,000 jobs inserted in one transaction: Postgres merges identical notifications into one. */
    @Test
    void bulkInsertWakesOnce() throws Exception {
        AtomicInteger wakes = new AtomicInteger();
        try (JobNotificationListener listener = startedListener("q", wakes)) {
            execute("INSERT INTO joblatch_jobs (queue, job_type, payload)"
                    + " SELECT 'q', 't', '{}' FROM generate_series(1, 1000)");
            waitUntil(() -> wakes.get() >= 2);
            Thread.sleep(QUIET_PERIOD.toMillis());
            assertThat(wakes.get()).isEqualTo(2);
            assertThat(listener.isListening()).isTrue();
        }
    }

    @Test
    void jobBecomingAvailableAgainAlsoNotifies() throws Exception {
        AtomicInteger wakes = new AtomicInteger();
        UUID id = jobQueue.enqueue(dataSource, NewJob.of("q", "t", "{}").withMaxAttempts(1));
        ClaimedJob job = new JobClaimer(dataSource).claim("q", "w", 1, Duration.ofMinutes(5)).get(0);
        new JobLifecycle(dataSource, RetryPolicy.noDelay()).fail(job, "x"); // now dead

        try (JobNotificationListener listener = startedListener("q", wakes)) {
            new DeadLetterQueue(dataSource).requeue(id); // dead -> available
            waitUntil(() -> wakes.get() == 2);
            assertThat(listener.isListening()).isTrue();
        }
    }

    /** Acceptance: reconnection restores notifications after the connection is killed. */
    @Test
    void reconnectsAfterItsConnectionIsKilled() throws Exception {
        AtomicInteger wakes = new AtomicInteger();
        try (JobNotificationListener listener = startedListener("q", wakes)) {
            int oldPid = listener.backendPid();

            // An admin (or a failover, or a network blip) kills the listening session.
            terminateBackend(oldPid);

            waitUntil(() -> listener.isListening() && listener.backendPid() != oldPid);
            int wakesAfterReconnect = wakes.get();
            assertThat(wakesAfterReconnect).as("woke on reconnect").isGreaterThanOrEqualTo(2);

            jobQueue.enqueue(dataSource, NewJob.of("q", "t", "{}"));
            waitUntil(() -> wakes.get() > wakesAfterReconnect);
        }
    }

    // ---------------------------------------------------------------- the pool

    /** Acceptance: notifications reduce polling. A 30 s poll interval, yet the job runs at once. */
    @Test
    void idlePoolPicksUpNewJobImmediately() throws Exception {
        CountDownLatch ran = new CountDownLatch(1);
        WorkerPoolConfig config = WorkerPoolConfig.defaults("q").withPollInterval(SLOW_POLL);

        try (WorkerPool pool = new WorkerPool(dataSource, config, Map.of("t", job -> ran.countDown()))) {
            pool.start();
            Thread.sleep(QUIET_PERIOD.toMillis()); // let the pool go idle (first poll found nothing)

            jobQueue.enqueue(dataSource, NewJob.of("q", "t", "{}"));

            assertThat(ran.await(5, TimeUnit.SECONDS)).as("woken by NOTIFY, not by the 30 s poll").isTrue();
        }
    }

    /**
     * Acceptance: missing notifications do not lose jobs. Triggers disabled = no NOTIFY at all.
     * Polling still finds the job.
     */
    @Test
    void jobIsStillProcessedWhenNoNotificationIsSent() throws Exception {
        execute("ALTER TABLE joblatch_jobs DISABLE TRIGGER USER");
        CountDownLatch ran = new CountDownLatch(1);
        WorkerPoolConfig config = WorkerPoolConfig.defaults("q").withPollInterval(Duration.ofMillis(300));

        try (WorkerPool pool = new WorkerPool(dataSource, config, Map.of("t", job -> ran.countDown()))) {
            pool.start();
            Thread.sleep(QUIET_PERIOD.toMillis());
            jobQueue.enqueue(dataSource, NewJob.of("q", "t", "{}"));

            assertThat(ran.await(10, TimeUnit.SECONDS)).as("found by polling").isTrue();
        }
    }

    @Test
    void poolWorksWithNotificationsTurnedOff() throws Exception {
        CountDownLatch ran = new CountDownLatch(1);
        WorkerPoolConfig config = WorkerPoolConfig.defaults("q")
                .withPollInterval(Duration.ofMillis(100))
                .withNotifications(false);

        try (WorkerPool pool = new WorkerPool(dataSource, config, Map.of("t", job -> ran.countDown()))) {
            pool.start();
            jobQueue.enqueue(dataSource, NewJob.of("q", "t", "{}"));
            assertThat(ran.await(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    // ---------------------------------------------------------------- scheduled jobs

    /**
     * Acceptance: scheduled jobs do not run early, and run on time even though the poll interval
     * is 30 s. The check "not early" uses the database clock, the same clock the claim uses.
     */
    @Test
    void scheduledJobRunsOnTimeAndNeverEarly() throws Exception {
        AtomicBoolean ranEarly = new AtomicBoolean();
        CountDownLatch ran = new CountDownLatch(1);
        WorkerPoolConfig config = WorkerPoolConfig.defaults("q").withPollInterval(SLOW_POLL);

        try (WorkerPool pool = new WorkerPool(dataSource, config, Map.of("t", job -> {
            ranEarly.set(dbNowIsBeforeRunAt(job.id()));
            ran.countDown();
        }))) {
            pool.start();
            Thread.sleep(QUIET_PERIOD.toMillis());

            Instant runAt = Instant.now().plusMillis(1500);
            jobQueue.enqueue(dataSource, NewJob.of("q", "t", "{}").withRunAt(runAt));

            // The NOTIFY wakes the pool, it finds the job is not due yet, asks "when is it due?"
            // and sleeps exactly that long, instead of 30 s.
            assertThat(ran.await(6, TimeUnit.SECONDS)).as("ran close to its run_at").isTrue();
        }
        assertThat(ranEarly.get()).as("ran before run_at").isFalse();
    }

    @Test
    void timeUntilNextDueReportsTheEarliestJob() throws Exception {
        JobClaimer claimer = new JobClaimer(dataSource);
        assertThat(claimer.timeUntilNextDue("q")).isEmpty();

        jobQueue.enqueue(dataSource, NewJob.of("q", "t", "{}").withRunAt(Instant.now().plusSeconds(600)));
        jobQueue.enqueue(dataSource, NewJob.of("q", "t", "{}").withRunAt(Instant.now().plusSeconds(60)));

        Optional<Duration> next = claimer.timeUntilNextDue("q");
        assertThat(next).isPresent();
        assertThat(next.get()).isBetween(Duration.ofSeconds(50), Duration.ofSeconds(61));

        jobQueue.enqueue(dataSource, NewJob.of("q", "t", "{}")); // due now
        assertThat(claimer.timeUntilNextDue("q")).contains(Duration.ZERO);
    }

    // ---------------------------------------------------------------- helpers

    private JobNotificationListener startedListener(String queue, AtomicInteger wakes) throws Exception {
        JobNotificationListener listener = new JobNotificationListener(dataSource, queue, wakes::incrementAndGet);
        listener.start();
        waitUntil(() -> wakes.get() == 1 && listener.isListening());
        return listener;
    }

    /** Kills a database session, like an admin or a failover would. */
    private static void terminateBackend(int pid) throws SQLException {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT pg_terminate_backend(?)")) {
            ps.setInt(1, pid);
            ps.execute();
        }
    }

    private static boolean dbNowIsBeforeRunAt(UUID id) throws SQLException {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT now() < run_at FROM joblatch_jobs WHERE id = ?")) {
            ps.setObject(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
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
