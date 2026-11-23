package dev.joblatch.core;

import java.time.Instant;
import java.util.UUID;

/**
 * A job that gave up, as shown to an operator.
 *
 * @param id           the job's ID
 * @param queue        its queue
 * @param jobType      its type
 * @param payloadJson  its input, kept so the job can be inspected and requeued. Never logged.
 * @param attempts     how many attempts it used
 * @param maxAttempts  its attempt limit
 * @param lastError    why the last attempt failed
 * @param lastFailedAt when the last attempt failed. Null only for jobs that died before
 *                     migration V2 added this column.
 * @param diedAt       when it became dead
 */
public record DeadJob(
        UUID id,
        String queue,
        String jobType,
        String payloadJson,
        int attempts,
        int maxAttempts,
        String lastError,
        Instant lastFailedAt,
        Instant diedAt) {

    /** Leaves out the payload and the error text, which may contain sensitive data. */
    @Override
    public String toString() {
        return "DeadJob[id=" + id
                + ", queue=" + queue
                + ", jobType=" + jobType
                + ", attempts=" + attempts + "/" + maxAttempts
                + ", diedAt=" + diedAt
                + ", payload=<redacted>, lastError=<redacted>]";
    }
}
