package dev.foleni.core;

import java.time.Duration;
import java.util.Objects;

/**
 * Settings for a {@link WorkerPool}. Start from {@link #defaults(String)} and change what you need:
 *
 * <pre>{@code
 * WorkerPoolConfig config = WorkerPoolConfig.defaults("emails")
 *         .withConcurrency(8)
 *         .withJobTimeout(Duration.ofMinutes(2));
 * }</pre>
 *
 * @param queue            which queue this pool takes jobs from
 * @param concurrency      how many jobs run at the same time (one thread each), and therefore the
 *                         most jobs this pool ever holds claimed
 * @param pollInterval     the longest an idle pool waits before checking the table again. With
 *                         notifications on, this is only the safety net for missed wake-ups.
 * @param leaseDuration    how long a claim lasts without a heartbeat. Heartbeats run every
 *                         leaseDuration / 3. Choose it well above your worst pause (GC, network).
 * @param jobTimeout       interrupt and fail a job that runs longer than this, or {@code null} for
 *                         no limit
 * @param shutdownTimeout  how long {@link WorkerPool#close()} waits for running jobs before
 *                         interrupting them. Zero means interrupt straight away.
 * @param retryPolicy      how long failed jobs wait before their next attempt
 * @param useNotifications wake up immediately on LISTEN/NOTIFY when a job becomes available. Uses
 *                         one extra connection per pool. Turn off behind PgBouncer in
 *                         transaction mode, and lower pollInterval instead.
 */
public record WorkerPoolConfig(
        String queue,
        int concurrency,
        Duration pollInterval,
        Duration leaseDuration,
        Duration jobTimeout,
        Duration shutdownTimeout,
        RetryPolicy retryPolicy,
        boolean useNotifications) {

    /** Upper limit on threads per pool. Each running job also needs database connections. */
    public static final int MAX_CONCURRENCY = 1000;

    /** Heartbeats per lease period. 3 means a worker must miss two in a row to lose a job. */
    public static final int HEARTBEATS_PER_LEASE = 3;

    public static final int DEFAULT_CONCURRENCY = 4;
    /** Safety-net polling. Notifications normally wake the pool long before this. */
    public static final Duration DEFAULT_POLL_INTERVAL = Duration.ofSeconds(5);
    public static final Duration DEFAULT_LEASE_DURATION = Duration.ofSeconds(60);
    public static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);

    public WorkerPoolConfig {
        Names.requireValid("queue", queue);
        if (concurrency < 1 || concurrency > MAX_CONCURRENCY) {
            throw new IllegalArgumentException("concurrency must be between 1 and " + MAX_CONCURRENCY);
        }
        requireAtLeastOneMilli("pollInterval", pollInterval);
        Objects.requireNonNull(leaseDuration, "leaseDuration");
        if (leaseDuration.dividedBy(HEARTBEATS_PER_LEASE).toMillis() < 1) {
            throw new IllegalArgumentException(
                    "leaseDuration must be at least " + HEARTBEATS_PER_LEASE + " milliseconds");
        }
        if (jobTimeout != null) {
            requireAtLeastOneMilli("jobTimeout", jobTimeout);
        }
        Objects.requireNonNull(shutdownTimeout, "shutdownTimeout");
        if (shutdownTimeout.isNegative()) {
            throw new IllegalArgumentException("shutdownTimeout must not be negative");
        }
        Objects.requireNonNull(retryPolicy, "retryPolicy");
    }

    /**
     * Sensible defaults: 4 threads, notifications on with 5 s safety-net polling, 60 s lease, no
     * job timeout, 30 s shutdown.
     */
    public static WorkerPoolConfig defaults(String queue) {
        return new WorkerPoolConfig(queue, DEFAULT_CONCURRENCY, DEFAULT_POLL_INTERVAL,
                DEFAULT_LEASE_DURATION, null, DEFAULT_SHUTDOWN_TIMEOUT, RetryPolicy.defaults(), true);
    }

    /** How often each running job's lease is renewed. */
    public Duration heartbeatInterval() {
        return leaseDuration.dividedBy(HEARTBEATS_PER_LEASE);
    }

    public WorkerPoolConfig withConcurrency(int concurrency) {
        return new WorkerPoolConfig(queue, concurrency, pollInterval, leaseDuration, jobTimeout,
                shutdownTimeout, retryPolicy, useNotifications);
    }

    public WorkerPoolConfig withPollInterval(Duration pollInterval) {
        return new WorkerPoolConfig(queue, concurrency, pollInterval, leaseDuration, jobTimeout,
                shutdownTimeout, retryPolicy, useNotifications);
    }

    public WorkerPoolConfig withLeaseDuration(Duration leaseDuration) {
        return new WorkerPoolConfig(queue, concurrency, pollInterval, leaseDuration, jobTimeout,
                shutdownTimeout, retryPolicy, useNotifications);
    }

    /** @param jobTimeout the limit, or {@code null} for none */
    public WorkerPoolConfig withJobTimeout(Duration jobTimeout) {
        return new WorkerPoolConfig(queue, concurrency, pollInterval, leaseDuration, jobTimeout,
                shutdownTimeout, retryPolicy, useNotifications);
    }

    public WorkerPoolConfig withShutdownTimeout(Duration shutdownTimeout) {
        return new WorkerPoolConfig(queue, concurrency, pollInterval, leaseDuration, jobTimeout,
                shutdownTimeout, retryPolicy, useNotifications);
    }

    public WorkerPoolConfig withRetryPolicy(RetryPolicy retryPolicy) {
        return new WorkerPoolConfig(queue, concurrency, pollInterval, leaseDuration, jobTimeout,
                shutdownTimeout, retryPolicy, useNotifications);
    }

    public WorkerPoolConfig withNotifications(boolean useNotifications) {
        return new WorkerPoolConfig(queue, concurrency, pollInterval, leaseDuration, jobTimeout,
                shutdownTimeout, retryPolicy, useNotifications);
    }

    private static void requireAtLeastOneMilli(String field, Duration value) {
        Objects.requireNonNull(value, field);
        if (value.toMillis() < 1) {
            throw new IllegalArgumentException(field + " must be at least 1 millisecond");
        }
    }
}
