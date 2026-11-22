package dev.foleni.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** Completion, failure and stale-worker protection, against real Postgres. */
@Testcontainers
class JobLifecycleTest {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    private static final Duration LEASE = Duration.ofMinutes(5);

    static DataSource dataSource;
    static ExecutorService racePool;

    private final JobQueue jobQueue = new JobQueue();
    private JobClaimer claimer;
    private JobLifecycle lifecycle;

    @BeforeAll
    static void setUpDatabase() throws SQLException {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(POSTGRES.getJdbcUrl());
        ds.setUser(POSTGRES.getUsername());
        ds.setPassword(POSTGRES.getPassword());
        dataSource = ds;
        FoleniMigrations.migrate(dataSource);
        execute("CREATE TABLE invoices (id bigint PRIMARY KEY, paid boolean NOT NULL)");
        racePool = Executors.newFixedThreadPool(2);
    }

    @AfterAll
    static void tearDown() {
        racePool.shutdownNow();
    }

    @BeforeEach
    void reset() throws SQLException {
        execute("TRUNCATE foleni_jobs, invoices");
        claimer = new JobClaimer(dataSource);
        lifecycle = new JobLifecycle(dataSource);
    }

    // ---------------------------------------------------------------- complete

    @Test
    void ownerCanComplete() throws SQLException {
        ClaimedJob job = enqueueAndClaim("worker-a", 5);

        assertThat(lifecycle.complete(job)).isEqualTo(TransitionResult.SUCCEEDED);

        Row row = row(job.id());
        assertThat(row.state()).isEqualTo("succeeded");
        assertThat(row.lockedBy()).isNull();
        assertThat(row.finished()).isTrue();
    }

    @Test
    void completingTwiceIsRejected() throws SQLException {
        ClaimedJob job = enqueueAndClaim("worker-a", 5);

        assertThat(lifecycle.complete(job)).isEqualTo(TransitionResult.SUCCEEDED);
        // succeeded -> succeeded is not a valid transition.
        assertThat(lifecycle.complete(job)).isEqualTo(TransitionResult.NOT_OWNER);
    }

    @Test
    void anotherWorkerCannotComplete() throws SQLException {
        ClaimedJob mine = enqueueAndClaim("worker-a", 5);
        // A worker pretending to own the job (same id and attempt, wrong worker).
        ClaimedJob impostor = withOwner(mine, "worker-b", mine.attempts());

        assertThat(lifecycle.complete(impostor)).isEqualTo(TransitionResult.NOT_OWNER);
        assertThat(row(mine.id()).state()).isEqualTo("running");
    }

    /**
     * The scenario from the explanation: worker A stalls, the job is recovered and claimed by B.
     * A's late completion must not overwrite B's ownership.
     */
    @Test
    void staleWorkerCannotCompleteAfterJobIsReclaimed() throws SQLException {
        ClaimedJob staleA = enqueueAndClaim("worker-a", 5);

        simulateReaper(staleA.id()); // Milestone 4 will do this automatically on lease expiry
        ClaimedJob freshB = claimer.claim("q", "worker-b", 1, LEASE).get(0);
        assertThat(freshB.attempts()).isEqualTo(2);

        assertThat(lifecycle.complete(staleA)).isEqualTo(TransitionResult.NOT_OWNER);
        assertThat(lifecycle.fail(staleA, "late")).isEqualTo(TransitionResult.NOT_OWNER);
        assertThat(row(freshB.id()).lockedBy()).isEqualTo("worker-b"); // B is untouched

        assertThat(lifecycle.complete(freshB)).isEqualTo(TransitionResult.SUCCEEDED);
    }

    /**
     * Why the attempt number is part of the check: the SAME worker ID claims the job again after
     * recovery. locked_by matches, but the old attempt must still be rejected.
     */
    @Test
    void attemptNumberFencesOffAnOldAttemptBySameWorker() throws SQLException {
        ClaimedJob attempt1 = enqueueAndClaim("worker-a", 5);
        simulateReaper(attempt1.id());
        ClaimedJob attempt2 = claimer.claim("q", "worker-a", 1, LEASE).get(0);

        assertThat(attempt2.lockedBy()).isEqualTo(attempt1.lockedBy());
        assertThat(lifecycle.complete(attempt1)).isEqualTo(TransitionResult.NOT_OWNER);
        assertThat(lifecycle.complete(attempt2)).isEqualTo(TransitionResult.SUCCEEDED);
    }

    /** Design decision: an expired lease alone does not take ownership away. Being reclaimed does. */
    @Test
    void expiredButNotReclaimedWorkerCanStillComplete() throws Exception {
        jobQueue.enqueue(dataSource, NewJob.of("q", "t", "{}"));
        ClaimedJob job = claimer.claim("q", "worker-a", 1, Duration.ofMillis(1)).get(0);
        Thread.sleep(50); // lease is now over

        assertThat(lifecycle.complete(job)).isEqualTo(TransitionResult.SUCCEEDED);
    }

    // ---------------------------------------------------------------- complete in caller's transaction

    @Test
    void completionInCallersTransactionIsAtomicWithTheWork() throws SQLException {
        ClaimedJob job = enqueueAndClaim("worker-a", 5);
        execute("INSERT INTO invoices VALUES (1, false)");

        // First try: do the work and complete, then roll back (e.g. a crash before commit).
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            markInvoicePaid(conn, 1);
            assertThat(lifecycle.complete(conn, job)).isEqualTo(TransitionResult.SUCCEEDED);
            conn.rollback();
        }
        // Neither happened: the invoice is unpaid AND the job is still running (will be retried).
        assertThat(invoicePaid(1)).isFalse();
        assertThat(row(job.id()).state()).isEqualTo("running");

        // Second try: commit. Both happen together.
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            markInvoicePaid(conn, 1);
            assertThat(lifecycle.complete(conn, job)).isEqualTo(TransitionResult.SUCCEEDED);
            conn.commit();
        }
        assertThat(invoicePaid(1)).isTrue();
        assertThat(row(job.id()).state()).isEqualTo("succeeded");
    }

    @Test
    void completeWithAutoCommitConnectionIsRejected() throws SQLException {
        ClaimedJob job = enqueueAndClaim("worker-a", 5);
        try (Connection conn = dataSource.getConnection()) {
            assertThatIllegalArgumentException().isThrownBy(() -> lifecycle.complete(conn, job));
        }
        assertThat(row(job.id()).state()).isEqualTo("running");
    }

    // ---------------------------------------------------------------- fail

    @Test
    void failWithAttemptsLeftMakesJobAvailableAgain() throws SQLException {
        ClaimedJob job = enqueueAndClaim("worker-a", 3);

        assertThat(lifecycle.fail(job, "SMTP timeout")).isEqualTo(TransitionResult.RETRY_SCHEDULED);

        Row row = row(job.id());
        assertThat(row.state()).isEqualTo("available");
        assertThat(row.lockedBy()).isNull();
        assertThat(row.lastError()).isEqualTo("SMTP timeout");
        assertThat(row.attempts()).isEqualTo(1); // the attempt that failed still counts
        assertThat(row.finished()).isFalse();

        // And it can be claimed again, as attempt 2.
        assertThat(claimer.claim("q", "worker-b", 1, LEASE).get(0).attempts()).isEqualTo(2);
    }

    @Test
    void failOnLastAttemptMakesJobDeadAndKeepsIt() throws SQLException {
        ClaimedJob job = enqueueAndClaim("worker-a", 1); // only one attempt allowed

        assertThat(lifecycle.fail(job, "bad payload")).isEqualTo(TransitionResult.DEAD);

        Row row = row(job.id());
        assertThat(row.state()).isEqualTo("dead");
        assertThat(row.lastError()).isEqualTo("bad payload");
        assertThat(row.finished()).isTrue();
        assertThat(claimer.claim("q", "worker-b", 1, LEASE)).isEmpty(); // never claimed again
    }

    @Test
    void failByNonOwnerIsRejected() throws SQLException {
        ClaimedJob mine = enqueueAndClaim("worker-a", 5);

        assertThat(lifecycle.fail(withOwner(mine, "worker-b", mine.attempts()), "x"))
                .isEqualTo(TransitionResult.NOT_OWNER);
        assertThat(row(mine.id()).state()).isEqualTo("running");
    }

    @Test
    void cannotFailACompletedJob() throws SQLException {
        ClaimedJob job = enqueueAndClaim("worker-a", 5);
        lifecycle.complete(job);

        assertThat(lifecycle.fail(job, "too late")).isEqualTo(TransitionResult.NOT_OWNER);
        assertThat(row(job.id()).state()).isEqualTo("succeeded");
    }

    @Test
    void longErrorIsTruncatedAndNulCharactersAreRemoved() throws SQLException {
        ClaimedJob job = enqueueAndClaim("worker-a", 5);
        String huge = "a\0b" + "x".repeat(JobLifecycle.MAX_ERROR_LENGTH * 2);

        lifecycle.fail(job, huge);

        String stored = row(job.id()).lastError();
        assertThat(stored).hasSize(JobLifecycle.MAX_ERROR_LENGTH).startsWith("a b");
    }

    // ---------------------------------------------------------------- races

    /**
     * Two threads try to move the same job at the same instant: one completes, one fails. Exactly
     * one may win. Repeated many times so a race, if there were one, has many chances to show up.
     */
    @Test
    void concurrentCompleteAndFailExactlyOneWins() throws Exception {
        for (int round = 0; round < 100; round++) {
            execute("TRUNCATE foleni_jobs");
            ClaimedJob job = enqueueAndClaim("worker-a", 5);
            CountDownLatch go = new CountDownLatch(1);

            Callable<TransitionResult> completer = () -> {
                go.await();
                return lifecycle.complete(job);
            };
            Callable<TransitionResult> failer = () -> {
                go.await();
                return lifecycle.fail(job, "boom");
            };
            Future<TransitionResult> c = racePool.submit(completer);
            Future<TransitionResult> f = racePool.submit(failer);
            go.countDown();

            List<TransitionResult> results = List.of(c.get(30, TimeUnit.SECONDS), f.get(30, TimeUnit.SECONDS));

            assertThat(results).as("round " + round).containsOnlyOnce(TransitionResult.NOT_OWNER);
            String state = row(job.id()).state();
            if (results.get(0) == TransitionResult.SUCCEEDED) {
                assertThat(state).isEqualTo("succeeded");
            } else {
                assertThat(state).isEqualTo("available");
            }
        }
    }

    // ---------------------------------------------------------------- helpers

    private ClaimedJob enqueueAndClaim(String workerId, int maxAttempts) throws SQLException {
        jobQueue.enqueue(dataSource, NewJob.of("q", "t", "{}").withMaxAttempts(maxAttempts));
        return claimer.claim("q", workerId, 1, LEASE).get(0);
    }

    private static ClaimedJob withOwner(ClaimedJob job, String workerId, int attempts) {
        return new ClaimedJob(job.id(), job.queue(), job.jobType(), job.payloadJson(),
                attempts, job.maxAttempts(), workerId, job.lockedUntil());
    }

    /** Stand-in for the Milestone 4 reaper: put a running job back to available. */
    private static void simulateReaper(UUID id) throws SQLException {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "UPDATE foleni_jobs SET state = 'available', locked_by = NULL, locked_until = NULL"
                                + " WHERE id = ?")) {
            ps.setObject(1, id);
            assertThat(ps.executeUpdate()).isEqualTo(1);
        }
    }

    private static void markInvoicePaid(Connection conn, long id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("UPDATE invoices SET paid = true WHERE id = ?")) {
            ps.setLong(1, id);
            ps.executeUpdate();
        }
    }

    private static boolean invoicePaid(long id) throws SQLException {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT paid FROM invoices WHERE id = ?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }

    /** The columns these tests look at. A record keeps the helper short. */
    private record Row(String state, String lockedBy, int attempts, String lastError, boolean finished) {}

    private static Row row(UUID id) throws SQLException {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT state, locked_by, attempts, last_error, finished_at IS NOT NULL AS finished"
                                + " FROM foleni_jobs WHERE id = ?")) {
            ps.setObject(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).as("job exists").isTrue();
                return new Row(rs.getString("state"), rs.getString("locked_by"), rs.getInt("attempts"),
                        rs.getString("last_error"), rs.getBoolean("finished"));
            }
        }
    }

    private static void execute(String sql) throws SQLException {
        try (Connection conn = dataSource.getConnection(); Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }
}