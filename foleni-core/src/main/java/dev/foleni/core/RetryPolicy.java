package dev.foleni.core;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

/**
 * How long to wait before retrying a failed job: exponential backoff, capped, with jitter.
 *
 * <pre>
 *   delay(attempt) = min(maxDelay, baseDelay * multiplier^(attempt - 1))
 *   actual         = delay * (1 - jitter * random)        random is in [0, 1)
 * </pre>
 *
 * <p>Example with the defaults (10 s base, x2, 1 h cap, jitter 0.5): after attempt 1 wait 5-10 s,
 * after attempt 2 wait 10-20 s, after attempt 3 wait 20-40 s, and so on up to 30-60 min.
 *
 * <p>Records are immutable, so one policy can be shared by any number of threads.
 *
 * @param baseDelay  delay after the first failed attempt (before jitter). Zero means "no delay".
 * @param maxDelay   the delay never grows beyond this (before jitter)
 * @param multiplier how much the delay grows per attempt. 2.0 doubles it. At least 1.0.
 * @param jitter     how much randomness: 0 = none, 0.5 = up to 50% shorter, 1 = anywhere from 0
 */
public record RetryPolicy(Duration baseDelay, Duration maxDelay, double multiplier, double jitter) {

    /** Default first delay. Long enough for a brief outage or rate limit to clear. */
    public static final Duration DEFAULT_BASE_DELAY = Duration.ofSeconds(10);

    /** Default cap. Past an hour, a human should probably look at the job instead. */
    public static final Duration DEFAULT_MAX_DELAY = Duration.ofHours(1);

    /** Default growth: double the wait each time. */
    public static final double DEFAULT_MULTIPLIER = 2.0;

    /** Default jitter: retries land between 50% and 100% of the computed delay. */
    public static final double DEFAULT_JITTER = 0.5;

    /** Validation. Runs on every construction, so an invalid policy cannot exist. */
    public RetryPolicy {
        Objects.requireNonNull(baseDelay, "baseDelay");
        Objects.requireNonNull(maxDelay, "maxDelay");
        if (baseDelay.isNegative()) {
            throw new IllegalArgumentException("baseDelay must not be negative");
        }
        if (maxDelay.compareTo(baseDelay) < 0) {
            throw new IllegalArgumentException("maxDelay must be at least baseDelay");
        }
        // "!(x >= 1.0)" instead of "x < 1.0" so that NaN (not-a-number) is rejected too:
        // every comparison with NaN is false.
        if (!(multiplier >= 1.0) || Double.isInfinite(multiplier)) {
            throw new IllegalArgumentException("multiplier must be a finite number of at least 1.0");
        }
        if (!(jitter >= 0.0 && jitter <= 1.0)) {
            throw new IllegalArgumentException("jitter must be between 0.0 and 1.0");
        }
    }

    /** The recommended policy for production. */
    public static RetryPolicy defaults() {
        return new RetryPolicy(DEFAULT_BASE_DELAY, DEFAULT_MAX_DELAY, DEFAULT_MULTIPLIER, DEFAULT_JITTER);
    }

    /** Retry immediately. Useful in tests. Not recommended in production. */
    public static RetryPolicy noDelay() {
        return new RetryPolicy(Duration.ZERO, Duration.ZERO, 1.0, 0.0);
    }

    /**
     * Delay before the next try, after attempt number {@code attempt} failed.
     *
     * @param attempt the attempt that just failed, counting from 1
     */
    public Duration delayAfter(int attempt) {
        // ThreadLocalRandom.current() must be called on the thread that uses it, so it is
        // called inside the lambda (each time), not once when the lambda is created.
        return delayAfter(attempt, () -> ThreadLocalRandom.current().nextDouble());
    }

    /**
     * Same as {@link #delayAfter(int)}, with the random number source passed in. Tests pass a
     * fixed value so the result is predictable.
     *
     * @param random returns a value in [0, 1)
     */
    Duration delayAfter(int attempt, DoubleSupplier random) {
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be at least 1, was " + attempt);
        }
        // Work in double milliseconds. For large attempts, Math.pow overflows to Infinity, which
        // is fine: Math.min below turns it back into maxDelay. With longs it would wrap around
        // to a negative number instead, a classic overflow bug.
        double exponential = baseDelay.toMillis() * Math.pow(multiplier, attempt - 1);
        double capped = Math.min(exponential, maxDelay.toMillis());
        double jittered = capped * (1.0 - jitter * random.getAsDouble());
        return Duration.ofMillis(Math.round(jittered));
    }
}
