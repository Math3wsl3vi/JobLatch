package dev.joblatch.core;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

/**
 * A ready-made {@link JobEventListener} that keeps running totals in memory. No dependencies:
 * useful for a health or status endpoint, for tests, and for applications without a metrics system.
 *
 * <pre>{@code
 * JobCounters counters = new JobCounters();
 * WorkerPool pool = new WorkerPool(dataSource, config, handlers, counters);
 * ...
 * JobCounters.Snapshot s = counters.snapshot();
 * log.info("succeeded={} retried={} dead={}", s.succeeded(), s.retryScheduled(), s.dead());
 * }</pre>
 *
 * <p>Totals cover everything this object has been told about since it was created, across all
 * pools and reapers it is attached to. They are not persisted.
 *
 * <p>Thread-safe. {@link LongAdder} keeps counting cheap even when many worker threads update the
 * same counter at once (each thread mostly writes its own cell; reads add the cells up).
 */
public final class JobCounters implements JobEventListener {

    private final LongAdder claimed = new LongAdder();
    private final LongAdder emptyClaims = new LongAdder();
    private final LongAdder succeeded = new LongAdder();
    private final LongAdder retryScheduled = new LongAdder();
    private final LongAdder dead = new LongAdder();
    private final LongAdder notOwner = new LongAdder();
    private final LongAdder notRecorded = new LongAdder();
    private final LongAdder timedOut = new LongAdder();
    private final LongAdder leasesLost = new LongAdder();
    private final LongAdder reclaimedRequeued = new LongAdder();
    private final LongAdder reclaimedDead = new LongAdder();
    private final LongAdder handlerNanosTotal = new LongAdder();
    private final LongAccumulator handlerNanosMax = new LongAccumulator(Math::max, 0);
    private final Map<Component, LongAdder> errors = new EnumMap<>(Component.class);

    public JobCounters() {
        for (Component c : Component.values()) {
            errors.put(c, new LongAdder()); // filled once here, only read afterwards: thread-safe
        }
    }

    @Override
    public void jobsClaimed(String queue, int requested, int claimedJobs, Duration duration) {
        claimed.add(claimedJobs);
        if (claimedJobs == 0) {
            emptyClaims.increment();
        }
    }

    @Override
    public void jobFinished(JobExecution execution) {
        switch (execution.outcome()) {
            case SUCCEEDED -> succeeded.increment();
            case RETRY_SCHEDULED -> retryScheduled.increment();
            case DEAD -> dead.increment();
            case NOT_OWNER -> notOwner.increment();
            case NOT_RECORDED -> notRecorded.increment();
        }
        if (execution.timedOut()) {
            timedOut.increment();
        }
        long nanos = execution.handlerDuration().toNanos();
        handlerNanosTotal.add(nanos);
        handlerNanosMax.accumulate(nanos);
    }

    @Override
    public void leaseLost(ClaimedJob job) {
        leasesLost.increment();
    }

    @Override
    public void leasesReclaimed(ReclaimResult result) {
        reclaimedRequeued.add(result.requeued());
        reclaimedDead.add(result.dead());
    }

    @Override
    public void backgroundError(Component component, String queue, Throwable error) {
        errors.get(component).increment();
    }

    /**
     * The totals at this moment.
     *
     * @param claimed            jobs claimed
     * @param emptyClaims        claims that found nothing (the queue was empty or not yet due)
     * @param succeeded          attempts that succeeded
     * @param retryScheduled     attempts that failed and will be retried
     * @param dead               attempts that made their job dead
     * @param notOwner           attempts whose outcome was rejected: another worker owned the job
     * @param notRecorded        attempts whose outcome could not be written (database problem)
     * @param timedOut           attempts interrupted by the job timeout (also counted above)
     * @param leasesLost         running jobs whose lease was found taken over
     * @param reclaimedRequeued  jobs taken back from crashed workers and made available again
     * @param reclaimedDead      jobs taken back from crashed workers on their last attempt
     * @param finished           all finished attempts (the sum of the five outcomes)
     * @param averageHandlerTime mean handler run time, or zero if nothing finished yet
     * @param maxHandlerTime     longest handler run time seen
     * @param errors             background errors by component
     */
    public record Snapshot(
            long claimed, long emptyClaims, long succeeded, long retryScheduled, long dead,
            long notOwner, long notRecorded, long timedOut, long leasesLost,
            long reclaimedRequeued, long reclaimedDead, long finished,
            Duration averageHandlerTime, Duration maxHandlerTime, Map<Component, Long> errors) {}

    /** Reads every total. Each value is exact; values read while jobs run may be a moment apart. */
    public Snapshot snapshot() {
        long s = succeeded.sum();
        long r = retryScheduled.sum();
        long d = dead.sum();
        long n = notOwner.sum();
        long u = notRecorded.sum();
        long finished = s + r + d + n + u;
        Map<Component, Long> errorCounts = new EnumMap<>(Component.class);
        errors.forEach((component, adder) -> errorCounts.put(component, adder.sum()));
        return new Snapshot(
                claimed.sum(), emptyClaims.sum(), s, r, d, n, u, timedOut.sum(), leasesLost.sum(),
                reclaimedRequeued.sum(), reclaimedDead.sum(), finished,
                finished == 0 ? Duration.ZERO : Duration.ofNanos(handlerNanosTotal.sum() / finished),
                Duration.ofNanos(handlerNanosMax.get()),
                Map.copyOf(errorCounts));
    }
}
