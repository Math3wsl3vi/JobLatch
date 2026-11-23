package dev.joblatch.core;

import java.time.Instant;
import java.util.UUID;

/**
 * A job a worker has claimed and now owns until {@code lockedUntil}.
 *
 * <p>Ownership is recorded in the database row (state = 'running', locked_by, locked_until), not
 * in an open transaction. The worker uses {@code id} together with {@code lockedBy} to prove it
 * still owns the job when it later completes or fails it (Milestone 3).
 *
 * @param id          the job's ID
 * @param queue       the queue it was claimed from
 * @param jobType     selects which handler runs it
 * @param payloadJson the job's input. Never logged.
 * @param attempts    which attempt this is, counting from 1
 * @param maxAttempts the attempt limit
 * @param lockedBy    the worker that owns it (the one that claimed it)
 * @param lockedUntil when ownership expires unless renewed, on the database clock
 */
public record ClaimedJob(
        UUID id,
        String queue,
        String jobType,
        String payloadJson,
        int attempts,
        int maxAttempts,
        String lockedBy,
        Instant lockedUntil) {

    /** Same reason as {@link NewJob#toString()}: the generated version would print the payload. */
    @Override
    public String toString() {
        return "ClaimedJob[id=" + id
                + ", queue=" + queue
                + ", jobType=" + jobType
                + ", attempts=" + attempts + "/" + maxAttempts
                + ", lockedBy=" + lockedBy
                + ", lockedUntil=" + lockedUntil
                + ", payload=<redacted>]";
    }
}