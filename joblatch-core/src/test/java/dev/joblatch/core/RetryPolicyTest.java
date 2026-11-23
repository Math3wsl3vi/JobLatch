package dev.joblatch.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/** Unit tests: pure arithmetic, no database. */
class RetryPolicyTest {

    /** 1 s base, doubling, capped at 10 s. */
    private static final RetryPolicy NO_JITTER =
            new RetryPolicy(Duration.ofSeconds(1), Duration.ofSeconds(10), 2.0, 0.0);

    @Test
    void delaysGrowExponentiallyThenCap() {
        assertThat(NO_JITTER.delayAfter(1)).isEqualTo(Duration.ofSeconds(1));
        assertThat(NO_JITTER.delayAfter(2)).isEqualTo(Duration.ofSeconds(2));
        assertThat(NO_JITTER.delayAfter(3)).isEqualTo(Duration.ofSeconds(4));
        assertThat(NO_JITTER.delayAfter(4)).isEqualTo(Duration.ofSeconds(8));
        assertThat(NO_JITTER.delayAfter(5)).isEqualTo(Duration.ofSeconds(10)); // 16 s capped to 10
        assertThat(NO_JITTER.delayAfter(6)).isEqualTo(Duration.ofSeconds(10));
    }

    @Test
    void hugeAttemptNumbersDoNotOverflow() {
        // 2^9999 is far beyond any long. The cap must still hold, not wrap around to negative.
        assertThat(NO_JITTER.delayAfter(10_000)).isEqualTo(Duration.ofSeconds(10));
        assertThat(NO_JITTER.delayAfter(Integer.MAX_VALUE)).isEqualTo(Duration.ofSeconds(10));
    }

    @Test
    void jitterShortensTheDelayByUpToTheJitterFraction() {
        RetryPolicy halfJitter = new RetryPolicy(Duration.ofSeconds(8), Duration.ofMinutes(1), 2.0, 0.5);

        // random = 0 -> no reduction. random close to 1 -> almost the full 50% reduction.
        assertThat(halfJitter.delayAfter(1, () -> 0.0)).isEqualTo(Duration.ofSeconds(8));
        assertThat(halfJitter.delayAfter(1, () -> 0.5)).isEqualTo(Duration.ofSeconds(6));
        assertThat(halfJitter.delayAfter(1, () -> 0.999_999)).isEqualTo(Duration.ofSeconds(4));
    }

    @Test
    void realRandomDelaysStayWithinBounds() {
        RetryPolicy policy = RetryPolicy.defaults(); // 10 s base, jitter 0.5
        for (int i = 0; i < 10_000; i++) {
            Duration d = policy.delayAfter(1);
            assertThat(d).isBetween(Duration.ofSeconds(5), Duration.ofSeconds(10));
        }
    }

    @Test
    void jitterActuallySpreadsDelays() {
        RetryPolicy policy = RetryPolicy.defaults();
        long distinct = java.util.stream.IntStream.range(0, 1000)
                .mapToObj(i -> policy.delayAfter(3))
                .distinct()
                .count();
        // 1000 retries after the same attempt should not all land on the same millisecond.
        assertThat(distinct).isGreaterThan(100);
    }

    @Test
    void noDelayPolicyRetriesImmediately() {
        assertThat(RetryPolicy.noDelay().delayAfter(1)).isEqualTo(Duration.ZERO);
        assertThat(RetryPolicy.noDelay().delayAfter(50)).isEqualTo(Duration.ZERO);
    }

    @Test
    void rejectsInvalidPolicies() {
        Duration s = Duration.ofSeconds(1);
        assertThatIllegalArgumentException().isThrownBy(() -> new RetryPolicy(s.negated(), s, 2, 0));
        assertThatIllegalArgumentException().isThrownBy(() -> new RetryPolicy(s, Duration.ZERO, 2, 0));
        assertThatIllegalArgumentException().isThrownBy(() -> new RetryPolicy(s, s, 0.5, 0));
        assertThatIllegalArgumentException().isThrownBy(() -> new RetryPolicy(s, s, Double.NaN, 0));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new RetryPolicy(s, s, Double.POSITIVE_INFINITY, 0));
        assertThatIllegalArgumentException().isThrownBy(() -> new RetryPolicy(s, s, 2, -0.1));
        assertThatIllegalArgumentException().isThrownBy(() -> new RetryPolicy(s, s, 2, 1.1));
        assertThatIllegalArgumentException().isThrownBy(() -> new RetryPolicy(s, s, 2, Double.NaN));
        assertThatIllegalArgumentException().isThrownBy(() -> NO_JITTER.delayAfter(0));
    }
}
