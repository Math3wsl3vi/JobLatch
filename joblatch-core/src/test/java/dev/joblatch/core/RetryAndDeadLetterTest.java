package dev.joblatch.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** Backoff scheduling, attempt limits, permanent failures and the dead-letter queue. */
@Testcontainers
class RetryAndDeadLetterTest {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    private static final Duration LEASE = Duration.ofMinutes(5);

    /** Fixed 1-hour delay, no jitter: easy to assert on. */
    private static final RetryPolicy ONE_HOUR =
            new RetryPolicy(Duration.ofHours(1), Duration.ofHours(1), 1.0, 0.0);

    static DataSource dataSource;

    private final JobQueue jobQueue = new JobQueue();
    private JobClaimer claimer;
    private DeadLetterQueue deadLetters;

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
        claimer = new JobClaimer(dataSource);
        deadLetters = new DeadLetterQueue(dataSource);
    }

    // ---------------------------------------------------------------- backoff scheduling

    @Test
    void failedJobIsScheduledAfterTheBackoffDelay() throws SQLException {
        JobLifecycle lifecycle = new JobLifecycle(dataSource, ONE_HOUR);
        ClaimedJob job = enqueueAndClaim(5);
        Instant before = dbNow();

        assertThat(lifecycle.fail(job, "timeout")).isEqualTo(TransitionResult.RETRY_SCHEDULED);

        Instant runAt = runAtOf(job.id());
        assertThat(runAt).isBetween(before.plus(Duration.ofHours(1)), before.plus(Duration.ofHours(1)).plusSeconds(5));
        assertThat(lastFailedAtOf(job.id())).isNotNull();
    }

    /** Acceptance: failed jobs are not retried before their scheduled time. */
    @Test
    void failedJobIsNotClaimedBeforeItsRetryTime() throws Exception {
        RetryPolicy halfSecond = new RetryPolicy(Duration.ofMillis(500), Duration.ofMillis(500), 1.0, 0.0);
        JobLifecycle lifecycle = new JobLifecycle(dataSource, halfSecond);
        ClaimedJob job = enqueueAndClaim(5);

        lifecycle.fail(job, "timeout");

        assertThat(claimer.claim("q", "w", 1, LEASE)).as("too early").isEmpty();
        Thread.sleep(700);
        assertThat(claimer.claim("q", "w", 1, LEASE)).as("after the delay").hasSize(1);
    }

    @Test
    void delayGrowsWithEachAttempt() throws SQLException {
        // 1 min, 2 min, 4 min... with no jitter. Each failure is measured against the DB clock.
        RetryPolicy doubling = new RetryPolicy(Duration.ofMinutes(1), Duration.ofHours(1), 2.0, 0.0);
        JobLifecycle lifecycle = new JobLifecycle(dataSource, doubling);
        UUID id = jobQueue.enqueue(dataSource, NewJob.of("q", "t", "{}").withMaxAttempts(5));

        for (int attempt = 1; attempt <= 3; attempt++) {
            makeDue(id); // skip the wait so the test runs fast
            ClaimedJob job = claimer.claim("q", "w", 1, LEASE).get(0);
            assertThat(job.attempts()).isEqualTo(attempt);
            Instant before = dbNow();

            lifecycle.fail(job, "timeout");

            Duration expected = Duration.ofMinutes(1L << (attempt - 1)); // 1, 2, 4 minutes
            Duration actual = Duration.between(before, runAtOf(id));
            assertThat(actual).isBetween(expected, expected.plusSeconds(5));
        }
    }

    // ---------------------------------------------------------------- attempt limit and dead state

    /** Acceptance: maximum attempts are enforced, and the dead job is preserved for inspection. */
    @Test
    void jobDiesAfterMaxAttemptsAndIsPreserved() throws SQLException {
        JobLifecycle lifecycle = new JobLifecycle(dataSource, RetryPolicy.noDelay());
        UUID id = jobQueue.enqueue(dataSource,
                NewJob.of("q", "send-receipt", "{\"orderId\":99}").withMaxAttempts(3));

        assertThat(lifecycle.fail(claimer.claim("q", "w", 1, LEASE).get(0), "e1"))
                .isEqualTo(TransitionResult.RETRY_SCHEDULED);
        assertThat(lifecycle.fail(claimer.claim("q", "w", 1, LEASE).get(0), "e2"))
                .isEqualTo(TransitionResult.RETRY_SCHEDULED);
        assertThat(lifecycle.fail(claimer.claim("q", "w", 1, LEASE).get(0), "e3"))
                .isEqualTo(TransitionResult.DEAD);
        assertThat(claimer.claim("q", "w", 1, LEASE)).as("no 4th attempt").isEmpty();

        List<DeadJob> dead = deadLetters.list("q", 10);
        assertThat(dead).hasSize(1);
        DeadJob d = dead.get(0);
        assertThat(d.id()).isEqualTo(id);
        assertThat(d.jobType()).isEqualTo("send-receipt");
        assertThat(d.payloadJson()).contains("99");
        assertThat(d.attempts()).isEqualTo(3);
        assertThat(d.lastError()).isEqualTo("e3");
        assertThat(d.lastFailedAt()).isNotNull();
        assertThat(d.diedAt()).isNotNull();
    }

    @Test
    void permanentFailureSkipsRemainingAttempts() throws SQLException {
        JobLifecycle lifecycle = new JobLifecycle(dataSource, RetryPolicy.noDelay());
        ClaimedJob job = enqueueAndClaim(10);

        assertThat(lifecycle.failPermanently(job, "payload has no orderId")).isEqualTo(TransitionResult.DEAD);

        assertThat(stateOf(job.id())).isEqualTo("dead");
        assertThat(deadLetters.list("q", 10)).extracting(DeadJob::attempts).containsExactly(1);
    }

    @Test
    void permanentFailureStillChecksOwnership() throws SQLException {
        JobLifecycle lifecycle = new JobLifecycle(dataSource, RetryPolicy.noDelay());
        ClaimedJob job = enqueueAndClaim(10);
        lifecycle.complete(job);

        assertThat(lifecycle.failPermanently(job, "late")).isEqualTo(TransitionResult.NOT_OWNER);
        assertThat(stateOf(job.id())).isEqualTo("succeeded");
    }

    @Test
    void reaperRecordsFailureTimeToo() throws Exception {
        jobQueue.enqueue(dataSource, NewJob.of("q", "t", "{}").withMaxAttempts(1));
        ClaimedJob job = claimer.claim("q", "w", 1, Duration.ofMillis(1)).get(0);
        Thread.sleep(50);

        new LeaseManager(dataSource).reclaimExpired(10);

        DeadJob dead = deadLetters.list("q", 10).get(0);
        assertThat(dead.id()).isEqualTo(job.id());
        assertThat(dead.lastError()).contains("lease expired");
        assertThat(dead.lastFailedAt()).isNotNull();
    }

    // ---------------------------------------------------------------- dead-letter queue

    @Test
    void requeuedDeadJobRunsAgainWithFullAttempts() throws SQLException {
        JobLifecycle lifecycle = new JobLifecycle(dataSource, RetryPolicy.noDelay());
        ClaimedJob job = enqueueAndClaim(1);
        lifecycle.fail(job, "downstream outage");

        assertThat(deadLetters.requeue(job.id())).isTrue();

        assertThat(stateOf(job.id())).isEqualTo("available");
        ClaimedJob again = claimer.claim("q", "w", 1, LEASE).get(0);
        assertThat(again.id()).isEqualTo(job.id());
        assertThat(again.attempts()).isEqualTo(1); // fresh budget
        assertThat(lifecycle.complete(again)).isEqualTo(TransitionResult.SUCCEEDED);
    }

    @Test
    void requeueOnlyAffectsDeadJobs() throws SQLException {
        UUID available = jobQueue.enqueue(dataSource, NewJob.of("q", "t", "{}"));

        assertThat(deadLetters.requeue(available)).isFalse();
        assertThat(deadLetters.requeue(UUID.randomUUID())).isFalse();
    }

    @Test
    void requeueTwiceIsHarmless() throws SQLException {
        JobLifecycle lifecycle = new JobLifecycle(dataSource, RetryPolicy.noDelay());
        ClaimedJob job = enqueueAndClaim(1);
        lifecycle.fail(job, "x");

        assertThat(deadLetters.requeue(job.id())).isTrue();
        assertThat(deadLetters.requeue(job.id())).isFalse();
    }

    @Test
    void listIsPerQueueAndNewestFirst() throws Exception {
        JobLifecycle lifecycle = new JobLifecycle(dataSource, RetryPolicy.noDelay());
        UUID first = killJobIn("q", lifecycle);
        Thread.sleep(10);
        UUID second = killJobIn("q", lifecycle);
        killJobIn("other", lifecycle);

        assertThat(deadLetters.list("q", 10)).extracting(DeadJob::id).containsExactly(second, first);
        assertThat(deadLetters.list("q", 1)).extracting(DeadJob::id).containsExactly(second);
    }

    @Test
    void deadJobToStringHidesPayloadAndError() throws SQLException {
        JobLifecycle lifecycle = new JobLifecycle(dataSource, RetryPolicy.noDelay());
        jobQueue.enqueue(dataSource, NewJob.of("q", "t", "{\"card\":\"SECRET-1\"}").withMaxAttempts(1));
        lifecycle.fail(claimer.claim("q", "w", 1, LEASE).get(0), "card SECRET-2 declined");

        String text = deadLetters.list("q", 1).get(0).toString();

        assertThat(text).doesNotContain("SECRET-1").doesNotContain("SECRET-2");
    }

    @Test
    void rejectsInvalidArguments() {
        assertThatIllegalArgumentException().isThrownBy(() -> deadLetters.list("bad queue", 10));
        assertThatIllegalArgumentException().isThrownBy(() -> deadLetters.list("q", 0));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> deadLetters.list("q", DeadLetterQueue.MAX_LIST_LIMIT + 1));
    }

    // ---------------------------------------------------------------- helpers

    private ClaimedJob enqueueAndClaim(int maxAttempts) throws SQLException {
        jobQueue.enqueue(dataSource, NewJob.of("q", "t", "{}").withMaxAttempts(maxAttempts));
        return claimer.claim("q", "w", 1, LEASE).get(0);
    }

    private UUID killJobIn(String queue, JobLifecycle lifecycle) throws SQLException {
        UUID id = jobQueue.enqueue(dataSource, NewJob.of(queue, "t", "{}").withMaxAttempts(1));
        lifecycle.fail(claimer.claim(queue, "w", 1, LEASE).get(0), "x");
        return id;
    }

    /** Moves a waiting job's run_at into the past so the test does not have to sleep. */
    private static void makeDue(UUID id) throws SQLException {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "UPDATE joblatch_jobs SET run_at = now() - interval '1 second' WHERE id = ?")) {
            ps.setObject(1, id);
            ps.executeUpdate();
        }
    }

    private static Instant dbNow() throws SQLException {
        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT now()")) {
            rs.next();
            return rs.getObject(1, OffsetDateTime.class).toInstant();
        }
    }

    private static Instant runAtOf(UUID id) throws SQLException {
        return instant("SELECT run_at FROM joblatch_jobs WHERE id = ?", id);
    }

    private static Instant lastFailedAtOf(UUID id) throws SQLException {
        return instant("SELECT last_failed_at FROM joblatch_jobs WHERE id = ?", id);
    }

    private static Instant instant(String sql, UUID id) throws SQLException {
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                OffsetDateTime value = rs.getObject(1, OffsetDateTime.class);
                return value == null ? null : value.toInstant();
            }
        }
    }

    private static String stateOf(UUID id) throws SQLException {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT state FROM joblatch_jobs WHERE id = ?")) {
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
