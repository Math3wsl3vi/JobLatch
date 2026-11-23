package dev.joblatch.core;

import java.time.Duration;

/**
 * Receives events from a {@link WorkerPool} and a {@link LeaseReaper}, so an application can turn
 * them into metrics (Micrometer, Prometheus, OpenTelemetry...) without JobLatch depending on any
 * metrics library. See {@code docs/observability.md} for a Micrometer adapter.
 *
 * <p>Every method has an empty default: implement only what you need. {@link #NONE} does nothing
 * and is used when no listener is given. {@link JobCounters} is a ready-made, dependency-free
 * implementation that keeps totals in memory.
 *
 * <h2>Rules for implementations</h2>
 *
 * <ul>
 *   <li><b>Fast.</b> Methods are called on JobLatch's own threads (the poller, worker threads, the
 *       heartbeat scheduler, the reaper). A slow listener slows job processing down.
 *   <li><b>Thread-safe.</b> Methods are called from many threads at once.
 *   <li><b>Must not throw.</b> Anything a listener throws (except {@link VirtualMachineError}) is
 *       caught, logged at WARN and ignored, so it cannot break the pool. It is still a bug in the
 *       listener.
 *   <li><b>Low cardinality.</b> Tag metrics with the queue, job type, outcome and error class.
 *       Never with job IDs, worker IDs or exception messages: one time series per job would
 *       overwhelm any metrics system, and messages can contain personal data.
 * </ul>
 *
 * <p>There is deliberately no "job enqueued" event: an enqueue inside the caller's transaction can
 * still be rolled back, so counting it would count jobs that never existed. Backlog sizes come
 * from the database (see {@code docs/sql/monitoring.sql}), which is the source of truth.
 */
public interface JobEventListener {

    /** A listener that ignores every event. */
    JobEventListener NONE = new JobEventListener() {};

    /** Which background activity an error came from. */
    enum Component {
        /** The pool's poller could not claim jobs. */
        CLAIM,
        /** A running job's lease could not be renewed. */
        HEARTBEAT,
        /** A finished job's outcome could not be written. The job will run again after its lease expires. */
        RECORD_OUTCOME,
        /** A reaper pass failed. */
        REAPER,
        /** The LISTEN connection was lost. The pool falls back to polling until it reconnects. */
        NOTIFICATION_LISTENER
    }

    /**
     * The poller ran a claim. Called for empty claims too, so {@code claimed / requested} shows how
     * full the queue was, and {@code duration} is the claim query's latency.
     *
     * @param queue     the pool's queue
     * @param requested free worker slots the poller tried to fill
     * @param claimed   jobs actually claimed, 0 to {@code requested}
     * @param duration  how long the claim took, including getting a connection
     */
    default void jobsClaimed(String queue, int requested, int claimed, Duration duration) {}

    /** A job's handler finished and the pool tried to record the outcome. One call per attempt. */
    default void jobFinished(JobExecution execution) {}

    /**
     * A heartbeat found that this worker no longer owns a running job (its lease expired and
     * another worker claimed it). The handler is interrupted; its outcome will be
     * {@link JobExecution.Outcome#NOT_OWNER}. Frequent events mean leases are too short or workers
     * are pausing (GC, CPU starvation).
     */
    default void leaseLost(ClaimedJob job) {}

    /**
     * A reaper pass took back jobs whose leases had expired: their workers crashed or stalled.
     * Called only when the pass reclaimed at least one job.
     */
    default void leasesReclaimed(ReclaimResult result) {}

    /**
     * A background activity failed and will be retried automatically. Usually a database problem.
     *
     * @param component which activity failed
     * @param queue     the queue involved, or {@code null} for the reaper (it serves every queue)
     * @param error     the failure. Use its class for metrics, not its message.
     */
    default void backgroundError(Component component, String queue, Throwable error) {}
}
