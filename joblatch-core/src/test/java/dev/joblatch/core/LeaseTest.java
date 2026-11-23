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
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** Leases, heartbeats and the reaper, against real Postgres. */
@Testcontainers
class LeaseTest {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    private static final Duration LONG_LEASE = Duration.ofMinutes(5);
    /** A lease that is over almost immediately, to simulate a worker that died. */
    private static final Duration TINY_LEASE = Duration.ofMillis(1);

    static DataSource dataSource;
    static ExecutorService threads;

    private final JobQueue jobQueue = new JobQueue();
    private JobClaimer claimer;
    private JobLifecycle lifecycle;
    private LeaseManager leases;

    @BeforeAll
    static void setUpDatabase() throws SQLException {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(POSTGRES.getJdbcUrl());
        ds.setUser(POSTGRES.getUsername());
        ds.setPassword(POSTGRES.getPassword());
        dataSource = ds;
        JobLatchMigrations.migrate(dataSource);
        threads = Executors.newFixedThreadPool(8);
    }

    @AfterAll
    static void tearDown() {
        threads.shutdownNow();
    }

    @BeforeEach
    void reset() throws SQLException {
        execute("TRUNCATE joblatch_jobs");
        claimer = new JobClaimer(dataSource);
        lifecycle = new JobLifecycle(dataSource);
        leases = new LeaseManager(dataSource);
    }

    // ---------------------------------------------------------------- heartbeat (renew)

    @Test
    void ownerCanRenewLease() throws SQLException {
        ClaimedJob job = enqueueAndClaim("worker-a", Duration.ofSeconds(30), 5);

        Optional<Instant> renewed = leases.renew(job, Duration.ofMinutes(10));

        assertThat(renewed).isPresent();
        assertThat(renewed.get()).isAfter(job.lockedUntil()); // pushed forward
        assertThat(lockedUntilInDb(job.id())).isEqualTo(renewed.get());
    }

    @Test
    void nonOwnerCannotRenew() throws SQLException {
        ClaimedJob mine = enqueueAndClaim("worker-a", LONG_LEASE, 5);
        ClaimedJob impostor = new ClaimedJob(mine.id(), mine.queue(), mine.jobType(), mine.payloadJson(),
                mine.attempts(), mine.maxAttempts(), "worker-b", mine.lockedUntil());

        assertThat(leases.renew(impostor, LONG_LEASE)).isEmpty();
        assertThat(lockedUntilInDb(mine.id())).isEqualTo(mine.lockedUntil()); // unchanged
    }

    @Test
    void cannotRenewACompletedJob() throws SQLException {
        ClaimedJob job = enqueueAndClaim("worker-a", LONG_LEASE, 5);
        lifecycle.complete(job);

        assertThat(leases.renew(job, LONG_LEASE)).isEmpty();
    }

    // ---------------------------------------------------------------- reclaim

    @Test
    void expiredLeaseIsReclaimedAndJobCanRunAgain() throws Exception {
        ClaimedJob crashed = enqueueAndClaim("worker-a", TINY_LEASE, 5);
        Thread.sleep(50); // worker-a "died": nobody renews, the lease runs out

        ReclaimResult result = leases.reclaimExpired(100);

        assertThat(result).isEqualTo(new ReclaimResult(1, 0));
        assertThat(stateOf(crashed.id())).isEqualTo("available");
        assertThat(lastErrorOf(crashed.id())).contains("lease expired").contains("worker-a");

        ClaimedJob retry = claimer.claim("q", "worker-b", 1, LONG_LEASE).get(0);
        assertThat(retry.id()).isEqualTo(crashed.id());
        assertThat(retry.attempts()).isEqualTo(2);
    }

    @Test
    void liveLeasesAreNotReclaimed() throws SQLException {
        enqueueAndClaim("worker-a", LONG_LEASE, 5);

        assertThat(leases.reclaimExpired(100).total()).isZero();
    }

    @Test
    void renewedLeaseIsNotReclaimed() throws Exception {
        ClaimedJob job = enqueueAndClaim("worker-a", Duration.ofMillis(300), 5);
        leases.renew(job, LONG_LEASE); // heartbeat before expiry
        Thread.sleep(400);             // the ORIGINAL lease would be over by now

        assertThat(leases.reclaimExpired(100).total()).isZero();
        assertThat(stateOf(job.id())).isEqualTo("running");
    }

    @Test
    void expiredLeaseOnLastAttemptMakesJobDead() throws Exception {
        ClaimedJob job = enqueueAndClaim("worker-a", TINY_LEASE, 1);
        Thread.sleep(50);

        assertThat(leases.reclaimExpired(100)).isEqualTo(new ReclaimResult(0, 1));
        assertThat(stateOf(job.id())).isEqualTo("dead");
        assertThat(claimer.claim("q", "worker-b", 1, LONG_LEASE)).isEmpty();
    }

    /**
     * Acceptance: expired ownership cannot overwrite the new owner's state. This is the same
     * scenario as in Milestone 3, but now the real reaper does the recovery.
     */
    @Test
    void staleWorkerCannotTouchJobAfterRealReclaim() throws Exception {
        ClaimedJob stale = enqueueAndClaim("worker-a", TINY_LEASE, 5);
        Thread.sleep(50);
        leases.reclaimExpired(100);
        ClaimedJob fresh = claimer.claim("q", "worker-b", 1, LONG_LEASE).get(0);

        assertThat(leases.renew(stale, LONG_LEASE)).isEmpty();
        assertThat(lifecycle.complete(stale)).isEqualTo(TransitionResult.NOT_OWNER);
        assertThat(lifecycle.fail(stale, "late")).isEqualTo(TransitionResult.NOT_OWNER);

        assertThat(lifecycle.complete(fresh)).isEqualTo(TransitionResult.SUCCEEDED);
    }

    // ---------------------------------------------------------------- concurrency

    /**
     * Acceptance: concurrent reapers cannot corrupt state. 8 threads reap 2,000 expired jobs at
     * once. If any job were reclaimed twice, the counts would add up to more than 2,000.
     */
    @Test
    void concurrentReapersReclaimEachJobExactlyOnce() throws Exception {
        final int jobCount = 2000;
        final int reaperCount = 8;
        insertExpiredRunningJobs(jobCount);

        CountDownLatch go = new CountDownLatch(1);
        List<Future<Integer>> results = new ArrayList<>();
        for (int r = 0; r < reaperCount; r++) {
            results.add(threads.submit(() -> {
                go.await();
                int mine = 0;
                while (true) {
                    int n = leases.reclaimExpired(50).total();
                    if (n == 0) {
                        return mine;
                    }
                    mine += n;
                }
            }));
        }
        go.countDown();

        int total = 0;
        for (Future<Integer> f : results) {
            total += f.get(2, TimeUnit.MINUTES);
        }

        assertThat(total).isEqualTo(jobCount);
        assertThat(queryLong("SELECT count(*) FROM joblatch_jobs WHERE state = 'available' AND attempts = 1"))
                .isEqualTo(jobCount);
    }

    /**
     * A heartbeat and a reaper hit the same expired job at the same moment. Exactly one may win:
     * either the heartbeat renews (and the job stays running), or the reaper reclaims it (and the
     * heartbeat reports "not owner"). Never both, never neither.
     */
    @Test
    void heartbeatAndReaperRaceHasExactlyOneWinner() throws Exception {
        for (int round = 0; round < 100; round++) {
            execute("TRUNCATE joblatch_jobs");
            ClaimedJob job = enqueueAndClaim("worker-a", TINY_LEASE, 5);
            Thread.sleep(5); // lease is now expired, but nobody has reclaimed it yet
            CountDownLatch go = new CountDownLatch(1);

            Future<Optional<Instant>> heartbeat = threads.submit(() -> {
                go.await();
                return leases.renew(job, LONG_LEASE);
            });
            Future<ReclaimResult> reaper = threads.submit(() -> {
                go.await();
                return leases.reclaimExpired(10);
            });
            go.countDown();

            boolean renewed = heartbeat.get(30, TimeUnit.SECONDS).isPresent();
            boolean reclaimed = reaper.get(30, TimeUnit.SECONDS).total() == 1;

            assertThat(renewed ^ reclaimed).as("round %d: exactly one winner", round).isTrue();
            assertThat(stateOf(job.id())).isEqualTo(renewed ? "running" : "available");
        }
    }

    // ---------------------------------------------------------------- background reaper

    @Test
    void backgroundReaperRecoversCrashedJobs() throws Exception {
        ClaimedJob crashed = enqueueAndClaim("worker-a", TINY_LEASE, 5);

        try (LeaseReaper reaper = new LeaseReaper(leases, Duration.ofMillis(50), 100)) {
            reaper.start();
            waitUntil(() -> "available".equals(stateOf(crashed.id())), Duration.ofSeconds(10));
        }
    }

    /**
     * Acceptance: the reaper survives the database being unavailable. The first 3 attempts to get
     * a connection fail. The reaper must log, keep going, and recover the job once the database
     * is back.
     */
    @Test
    void backgroundReaperSurvivesDatabaseOutage() throws Exception {
        ClaimedJob crashed = enqueueAndClaim("worker-a", TINY_LEASE, 5);
        AtomicInteger failuresLeft = new AtomicInteger(3);
        LeaseManager flakyLeases = new LeaseManager(failingFirst(failuresLeft));

        try (LeaseReaper reaper = new LeaseReaper(flakyLeases, Duration.ofMillis(50), 100)) {
            reaper.start();
            waitUntil(() -> "available".equals(stateOf(crashed.id())), Duration.ofSeconds(10));
        }
        assertThat(failuresLeft.get()).as("all simulated outages happened").isLessThanOrEqualTo(0);
    }

    @Test
    void reaperStopsOnCloseAndCannotStartTwice() {
        LeaseReaper reaper = new LeaseReaper(leases, Duration.ofMillis(50), 100);
        reaper.start();
        assertThatIllegalStateException().isThrownBy(reaper::start);

        reaper.close();
        assertThat(reaper.isTerminated()).isTrue();
        reaper.close(); // second close is harmless
    }

    @Test
    void rejectsInvalidArguments() throws SQLException {
        ClaimedJob job = enqueueAndClaim("worker-a", LONG_LEASE, 5);
        assertThatIllegalArgumentException().isThrownBy(() -> leases.renew(job, Duration.ZERO));
        assertThatIllegalArgumentException().isThrownBy(() -> leases.reclaimExpired(0));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> leases.reclaimExpired(LeaseManager.MAX_RECLAIM_BATCH + 1));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new LeaseReaper(leases, Duration.ZERO, 10));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new LeaseReaper(leases, Duration.ofSeconds(1), 0));
    }

    // ---------------------------------------------------------------- helpers

    private ClaimedJob enqueueAndClaim(String workerId, Duration lease, int maxAttempts) throws SQLException {
        jobQueue.enqueue(dataSource, NewJob.of("q", "t", "{}").withMaxAttempts(maxAttempts));
        return claimer.claim("q", workerId, 1, lease).get(0);
    }

    /** Inserts jobs that look like they were claimed by a worker that died a minute ago. */
    private static void insertExpiredRunningJobs(int count) throws SQLException {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO joblatch_jobs (queue, job_type, payload, state, attempts, locked_by, locked_until)"
                                + " SELECT 'q', 't', '{}', 'running', 1, 'dead-worker', now() - interval '1 minute'"
                                + " FROM generate_series(1, ?)")) {
            ps.setInt(1, count);
            ps.executeUpdate();
        }
    }

    /**
     * Wraps the real DataSource so that the first N calls to getConnection() throw, like a database
     * that is briefly down.
     *
     * <p>{@code Proxy.newProxyInstance} builds an object that implements an interface at runtime
     * and sends every method call to the lambda below. That saves writing a class with all of
     * DataSource's methods just to change one.
     */
    private static DataSource failingFirst(AtomicInteger failuresLeft) {
        return (DataSource) Proxy.newProxyInstance(
                LeaseTest.class.getClassLoader(),
                new Class<?>[] {DataSource.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getConnection") && failuresLeft.getAndDecrement() > 0) {
                        throw new SQLException("simulated outage", "08001"); // 08xxx = connection error
                    }
                    try {
                        return method.invoke(dataSource, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause(); // unwrap so callers see the real exception
                    }
                });
    }

    /** A condition that may throw SQLException (so we can call stateOf inside it). */
    @FunctionalInterface
    private interface Condition {
        boolean isTrue() throws Exception;
    }

    /** Polls until the condition is true, or fails the test after the timeout. */
    private static void waitUntil(Condition condition, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.isTrue()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within " + timeout);
            }
            Thread.sleep(20);
        }
    }

    private static String stateOf(UUID id) throws SQLException {
        return queryString("SELECT state FROM joblatch_jobs WHERE id = ?", id);
    }

    private static String lastErrorOf(UUID id) throws SQLException {
        return queryString("SELECT last_error FROM joblatch_jobs WHERE id = ?", id);
    }

    private static Instant lockedUntilInDb(UUID id) throws SQLException {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT locked_until FROM joblatch_jobs WHERE id = ?")) {
            ps.setObject(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getObject(1, OffsetDateTime.class).toInstant();
            }
        }
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