package dev.foleni.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** Claiming tests against real Postgres, including the 20-thread / 10,000-job concurrency test. */
@Testcontainers
class JobClaimerTest {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    private static final Duration LEASE = Duration.ofMinutes(5);

    static DataSource dataSource;

    private final JobQueue jobQueue = new JobQueue();
    private JobClaimer claimer;

    @BeforeAll
    static void setUpDatabase() throws SQLException {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(POSTGRES.getJdbcUrl());
        ds.setUser(POSTGRES.getUsername());
        ds.setPassword(POSTGRES.getPassword());
        dataSource = ds;
        FoleniMigrations.migrate(dataSource);
    }

    @BeforeEach
    void reset() throws SQLException {
        execute("TRUNCATE foleni_jobs");
        claimer = new JobClaimer(dataSource);
    }

    // ---------------------------------------------------------------- basic behaviour

    @Test
    void claimMarksJobRunningOwnedByWorkerWithLease() throws SQLException {
        UUID id = jobQueue.enqueue(dataSource, NewJob.of("emails", "send-receipt", "{\"orderId\":7}"));
        Instant before = Instant.now();

        List<ClaimedJob> claimed = claimer.claim("emails", "worker-1", 10, LEASE);

        assertThat(claimed).hasSize(1);
        ClaimedJob job = claimed.get(0);
        assertThat(job.id()).isEqualTo(id);
        assertThat(job.jobType()).isEqualTo("send-receipt");
        assertThat(job.payloadJson()).contains("\"orderId\": 7"); // jsonb normalises spacing
        assertThat(job.attempts()).isEqualTo(1);
        assertThat(job.lockedBy()).isEqualTo("worker-1");
        // Lease ends roughly LEASE from now. Allow slack: the lease uses the database clock.
        assertThat(job.lockedUntil())
                .isAfter(before.plus(LEASE).minusSeconds(30))
                .isBefore(before.plus(LEASE).plusSeconds(30));

        // And it is committed: a separate connection sees the row as running.
        assertThat(stateOf(id)).isEqualTo("running");
    }

    @Test
    void claimedJobCannotBeClaimedAgain() throws SQLException {
        jobQueue.enqueue(dataSource, NewJob.of("q", "t", "{}"));

        assertThat(claimer.claim("q", "worker-1", 10, LEASE)).hasSize(1);
        assertThat(claimer.claim("q", "worker-2", 10, LEASE)).isEmpty();
    }

    @Test
    void emptyQueueReturnsEmptyList() throws SQLException {
        assertThat(claimer.claim("q", "worker-1", 10, LEASE)).isEmpty();
    }

    @Test
    void batchSizeLimitsHowManyAreClaimed() throws SQLException {
        for (int i = 0; i < 5; i++) {
            jobQueue.enqueue(dataSource, NewJob.of("q", "t", "{}"));
        }

        assertThat(claimer.claim("q", "worker-1", 3, LEASE)).hasSize(3);
        assertThat(claimer.claim("q", "worker-1", 3, LEASE)).hasSize(2);
    }

    @Test
    void onlyClaimsFromTheRequestedQueue() throws SQLException {
        jobQueue.enqueue(dataSource, NewJob.of("emails", "t", "{}"));

        assertThat(claimer.claim("reports", "worker-1", 10, LEASE)).isEmpty();
        assertThat(claimer.claim("emails", "worker-1", 10, LEASE)).hasSize(1);
    }

    @Test
    void futureJobsAreNotClaimedEarly() throws SQLException {
        jobQueue.enqueue(dataSource, NewJob.of("q", "t", "{}").withRunAt(Instant.now().plusSeconds(3600)));

        assertThat(claimer.claim("q", "worker-1", 10, LEASE)).isEmpty();
    }

    @Test
    void higherPriorityIsClaimedFirstThenOlderRunAt() throws SQLException {
        Instant now = Instant.now();
        UUID lowOld = jobQueue.enqueue(dataSource, NewJob.of("q", "t", "{}").withRunAt(now.minusSeconds(60)));
        UUID highNew = jobQueue.enqueue(dataSource,
                NewJob.of("q", "t", "{}").withPriority(10).withRunAt(now.minusSeconds(1)));
        UUID highOld = jobQueue.enqueue(dataSource,
                NewJob.of("q", "t", "{}").withPriority(10).withRunAt(now.minusSeconds(30)));

        // Claim one at a time so the order is visible (RETURNING itself does not preserve order).
        assertThat(claimer.claim("q", "w", 1, LEASE).get(0).id()).isEqualTo(highOld);
        assertThat(claimer.claim("q", "w", 1, LEASE).get(0).id()).isEqualTo(highNew);
        assertThat(claimer.claim("q", "w", 1, LEASE).get(0).id()).isEqualTo(lowOld);
    }

    @Test
    void jobWithNoAttemptsLeftIsSkippedWithoutBreakingTheBatch() throws SQLException {
        // A row that should never exist in normal operation: available but out of attempts.
        execute("INSERT INTO foleni_jobs (queue, job_type, payload, attempts, max_attempts)"
                + " VALUES ('q', 't', '{}', 5, 5)");
        UUID healthy = jobQueue.enqueue(dataSource, NewJob.of("q", "t", "{}"));

        List<ClaimedJob> claimed = claimer.claim("q", "worker-1", 10, LEASE);

        assertThat(claimed).extracting(ClaimedJob::id).containsExactly(healthy);
    }

    // ---------------------------------------------------------------- SKIP LOCKED

    /**
     * Proves SKIP LOCKED: another transaction holds a lock on job A. The claim must not wait for
     * it. It must immediately take job B instead. Without SKIP LOCKED this test would hang until
     * the timeout fails it.
     */
    @Test
    void lockedRowsAreSkippedNotWaitedOn() throws Exception {
        UUID a = jobQueue.enqueue(dataSource, NewJob.of("q", "t", "{}").withPriority(10));
        UUID b = jobQueue.enqueue(dataSource, NewJob.of("q", "t", "{}"));

        try (Connection blocker = dataSource.getConnection()) {
            blocker.setAutoCommit(false);
            try (PreparedStatement ps = blocker.prepareStatement(
                    "SELECT id FROM foleni_jobs WHERE id = ? FOR UPDATE")) {
                ps.setObject(1, a);
                ps.executeQuery().close(); // job A is now locked until `blocker` ends its transaction
            }

            List<ClaimedJob> claimed = assertTimeoutPreemptively(
                    Duration.ofSeconds(5), () -> claimer.claim("q", "worker-1", 10, LEASE));

            assertThat(claimed).extracting(ClaimedJob::id).containsExactly(b);
            blocker.rollback();
        }

        // Once the lock is gone, A is claimable again.
        assertThat(claimer.claim("q", "worker-2", 10, LEASE)).extracting(ClaimedJob::id).containsExactly(a);
    }

    // ---------------------------------------------------------------- the big one

    /**
     * Acceptance test for Milestone 2: 20 threads race to claim 10,000 jobs.
     *
     * <p>Every claimed ID goes into a map with {@code putIfAbsent}, which returns the previous
     * owner if the ID was already there. Any non-null return would mean two workers got the same
     * job, and the test records it as a duplicate.
     */
    @Test
    void twentyWorkersClaimTenThousandJobsWithNoDuplicates() throws Exception {
        final int jobCount = 10_000;
        final int workerCount = 20;
        final int batchSize = 10;

        // Insert all jobs with one statement. generate_series(1, N) produces N rows.
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO foleni_jobs (queue, job_type, payload)"
                                + " SELECT 'load', 't', jsonb_build_object('n', n) FROM generate_series(1, ?) AS n")) {
            ps.setInt(1, jobCount);
            ps.executeUpdate();
        }

        Map<UUID, String> owners = new ConcurrentHashMap<>();
        AtomicInteger duplicates = new AtomicInteger();
        CountDownLatch startTogether = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(workerCount);

        try {
            List<Future<Integer>> results = new ArrayList<>();
            for (int w = 0; w < workerCount; w++) {
                String workerId = "worker-" + w;
                results.add(pool.submit(() -> {
                    startTogether.await(); // all threads start at the same instant: maximum contention
                    int mine = 0;
                    while (true) {
                        List<ClaimedJob> batch = claimer.claim("load", workerId, batchSize, LEASE);
                        if (batch.isEmpty()) {
                            return mine; // queue drained
                        }
                        for (ClaimedJob job : batch) {
                            if (owners.putIfAbsent(job.id(), workerId) != null) {
                                duplicates.incrementAndGet();
                            }
                        }
                        mine += batch.size();
                    }
                }));
            }

            startTogether.countDown();

            int total = 0;
            int workersThatGotWork = 0;
            for (Future<Integer> f : results) {
                int n = f.get(2, TimeUnit.MINUTES); // rethrows any exception from the worker thread
                total += n;
                if (n > 0) {
                    workersThatGotWork++;
                }
            }

            assertThat(duplicates.get()).as("jobs claimed by two workers").isZero();
            assertThat(owners).hasSize(jobCount);
            assertThat(total).isEqualTo(jobCount);
            // Not a correctness rule, but SKIP LOCKED should spread the work out.
            assertThat(workersThatGotWork).isGreaterThan(1);
        } finally {
            pool.shutdownNow();
        }

        // The database agrees: every job is running, claimed exactly once, by the worker we recorded.
        assertThat(queryLong("SELECT count(*) FROM foleni_jobs WHERE state = 'running' AND attempts = 1"))
                .isEqualTo(jobCount);
        assertThat(queryLong("SELECT count(*) FROM foleni_jobs WHERE state <> 'running' OR attempts <> 1"))
                .isZero();
    }

    // ---------------------------------------------------------------- validation and security

    @Test
    void rejectsInvalidArguments() {
        assertThatIllegalArgumentException().isThrownBy(() -> claimer.claim("bad queue", "w", 1, LEASE));
        assertThatIllegalArgumentException().isThrownBy(() -> claimer.claim("q", "", 1, LEASE));
        assertThatIllegalArgumentException().isThrownBy(() -> claimer.claim("q", "w", 0, LEASE));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> claimer.claim("q", "w", JobClaimer.MAX_BATCH_SIZE + 1, LEASE));
        assertThatIllegalArgumentException().isThrownBy(() -> claimer.claim("q", "w", 1, Duration.ZERO));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> claimer.claim("q", "w", 1, Duration.ofSeconds(-1)));
    }

    @Test
    void claimedJobToStringHidesPayload() throws SQLException {
        jobQueue.enqueue(dataSource, NewJob.of("q", "t", "{\"card\":\"SECRET-4242\"}"));

        ClaimedJob job = claimer.claim("q", "w", 1, LEASE).get(0);

        assertThat(job.toString()).doesNotContain("SECRET-4242").contains("payload=<redacted>");
    }

    // ---------------------------------------------------------------- helpers

    private static void execute(String sql) throws SQLException {
        try (Connection conn = dataSource.getConnection(); Statement st = conn.createStatement()) {
            st.execute(sql);
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

    private static String stateOf(UUID id) throws SQLException {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT state FROM foleni_jobs WHERE id = ?")) {
            ps.setObject(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getString(1);
            }
        }
    }
}