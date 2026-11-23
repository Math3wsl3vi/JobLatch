package dev.joblatch.core;

/**
 * What happened when a worker reported a job's outcome.
 *
 * <p>An {@code enum} is a fixed list of named values. Callers can {@code switch} over it, and the
 * compiler can warn them if they forget a case.
 */
public enum TransitionResult {

    /** The job is now 'succeeded'. */
    SUCCEEDED,

    /** The job failed but has attempts left. It is 'available' again and will be retried. */
    RETRY_SCHEDULED,

    /** The job failed on its last attempt. It is now 'dead' and kept for inspection. */
    DEAD,

    /**
     * Nothing was changed, because the caller no longer owns the job. Either another worker
     * claimed it after this worker's lease expired, or the job had already been completed or
     * failed. This is normal in a distributed system, not a bug: log it and move on. Never retry
     * the transition.
     */
    NOT_OWNER
}