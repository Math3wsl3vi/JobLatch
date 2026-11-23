package dev.joblatch.core;

import java.time.Duration;
import java.util.Objects;

/**
 * One finished attempt of one job, as reported to {@link JobEventListener#jobFinished}.
 *
 * @param job             the job as it was claimed (payload included: never use it as a metric tag)
 * @param outcome         what was recorded in the database
 * @param handlerDuration how long the handler ran, from start to return or throw
 * @param failure         what the handler threw, or {@code null} if it returned normally. A job
 *                        with no registered handler is reported as an
 *                        {@link IllegalStateException}. Use its class for metrics, not its message.
 * @param timedOut        true if the pool interrupted the handler because of the job timeout
 */
public record JobExecution(
        ClaimedJob job, Outcome outcome, Duration handlerDuration, Throwable failure, boolean timedOut) {

    /** What happened to the job after this attempt. */
    public enum Outcome {
        /** The job succeeded. */
        SUCCEEDED,
        /** The attempt failed and the job will be retried after a backoff delay. */
        RETRY_SCHEDULED,
        /** The attempt failed and the job is now dead (last attempt, or a permanent failure). */
        DEAD,
        /**
         * Nothing was recorded because another worker owns the job now. Its handler's side effects
         * may still have happened: this is a likely duplicate execution.
         */
        NOT_OWNER,
        /**
         * The outcome could not be written (database unavailable). The job is still 'running' and
         * will run again after its lease expires.
         */
        NOT_RECORDED
    }

    public JobExecution {
        Objects.requireNonNull(job, "job");
        Objects.requireNonNull(outcome, "outcome");
        Objects.requireNonNull(handlerDuration, "handlerDuration");
    }

    /** The job's queue: a good metric tag. */
    public String queue() {
        return job.queue();
    }

    /** The job's type: a good metric tag. */
    public String jobType() {
        return job.jobType();
    }

    /** The failure's class name, or {@code "none"}: a safe metric tag (unlike the message). */
    public String failureClass() {
        return failure == null ? "none" : failure.getClass().getName();
    }

    /** Leaves out the payload and the failure's message, which may contain personal data. */
    @Override
    public String toString() {
        return "JobExecution[jobId=" + job.id()
                + ", queue=" + job.queue()
                + ", jobType=" + job.jobType()
                + ", attempt=" + job.attempts()
                + ", outcome=" + outcome
                + ", handlerDuration=" + handlerDuration
                + ", failure=" + failureClass()
                + ", timedOut=" + timedOut + "]";
    }
}
