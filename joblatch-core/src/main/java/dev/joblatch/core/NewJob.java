package dev.joblatch.core;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * A job that has not been saved yet: what the application wants to run.
 *
 * <p>This is a Java {@code record}: a small immutable class whose fields are declared in the
 * header below. Java writes the constructor, getters ({@code queue()}, {@code payloadJson()}, ...),
 * {@code equals} and {@code hashCode} for us.
 *
 * <p>Every {@code NewJob} that exists is valid, because the constructor checks every field and
 * throws {@link IllegalArgumentException} otherwise. Invalid jobs are rejected before any SQL
 * runs, with a clear message.
 *
 * @param queue       queue name, 1-128 chars of {@code A-Z a-z 0-9 _ . : -}
 * @param jobType     job type name, same rules as {@code queue}
 * @param payloadJson the job's input as a JSON string. Postgres checks that it is valid JSON.
 *                    Never logged.
 * @param runAt       earliest time the job may start, or {@code null} for "now" on the
 *                    database's clock
 * @param priority    higher runs first
 * @param maxAttempts how many times the job may be tried in total, at least 1
 */
public record NewJob(
        String queue,
        String jobType,
        String payloadJson,
        Instant runAt,
        int priority,
        int maxAttempts) {



    /** Default number of attempts. Retries arrive in Milestone 5. */
    public static final int DEFAULT_MAX_ATTEMPTS = 5;

    /** Default priority: neither boosted nor lowered. */
    public static final int DEFAULT_PRIORITY = 0;

    /**
     * Largest accepted payload: 1 MiB of UTF-8. A queue is not a file store. Big blobs bloat the
     * table, slow every claim, and should live elsewhere with only their ID in the payload.
     */
    public static final int MAX_PAYLOAD_BYTES = 1024 * 1024;

    /**
     * The "compact constructor". It runs before the fields are assigned, so it is the place to
     * validate. If any check throws, the object is never created.
     */
    public NewJob {
        Names.requireValid("queue", queue);
        Names.requireValid("jobType", jobType);

        if (payloadJson == null || payloadJson.isBlank()) {
            throw new IllegalArgumentException("payloadJson must not be null or blank");
        }
        // Count bytes, not characters: "é" is 1 char but 2 bytes in UTF-8.
        int payloadBytes = payloadJson.getBytes(StandardCharsets.UTF_8).length;
        if (payloadBytes > MAX_PAYLOAD_BYTES) {
            // Report the size, never the content.
            throw new IllegalArgumentException(
                    "payloadJson is " + payloadBytes + " bytes, limit is " + MAX_PAYLOAD_BYTES);
        }

        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1, was " + maxAttempts);
        }
        // runAt may be null (= now). priority may be any int.
    }

    /** Convenience factory: run as soon as possible, default priority and attempts. */
    public static NewJob of(String queue, String jobType, String payloadJson) {
        return new NewJob(queue, jobType, payloadJson, null, DEFAULT_PRIORITY, DEFAULT_MAX_ATTEMPTS);
    }

    /** Returns a copy that must not start before {@code runAt}. The original is unchanged. */
    public NewJob withRunAt(Instant runAt) {
        return new NewJob(queue, jobType, payloadJson, runAt, priority, maxAttempts);
    }

    /** Returns a copy with a different priority. */
    public NewJob withPriority(int priority) {
        return new NewJob(queue, jobType, payloadJson, runAt, priority, maxAttempts);
    }

    /** Returns a copy with a different attempt limit. */
    public NewJob withMaxAttempts(int maxAttempts) {
        return new NewJob(queue, jobType, payloadJson, runAt, priority, maxAttempts);
    }

    /**
     * Records generate a {@code toString()} that prints every field, including the payload. If
     * anyone logs a {@code NewJob}, for example {@code log.info("job {}", job)}, that would leak
     * the payload into the logs. So we override it and leave the payload out on purpose.
     */
    @Override
    public String toString() {
        return "NewJob[queue=" + queue
                + ", jobType=" + jobType
                + ", runAt=" + runAt
                + ", priority=" + priority
                + ", maxAttempts=" + maxAttempts
                + ", payload=<redacted>]";
    }

}