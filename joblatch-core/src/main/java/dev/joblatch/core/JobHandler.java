package dev.joblatch.core;

/**
 * Application code that runs one job. Register one per job type with a {@link WorkerPool}.
 *
 * <p>The pool decides the outcome from how this method ends:
 *
 * <ul>
 *   <li>returns normally: the job succeeded.
 *   <li>throws {@link PermanentJobFailureException}: retrying cannot help, so the job goes dead.
 *   <li>throws anything else: the job is retried later, with backoff, until it runs out of
 *       attempts.
 * </ul>
 *
 * <p><b>Write handlers to be idempotent.</b> Delivery is at-least-once: the same job can run more
 * than once (a crash after the work but before the completion was recorded, an expired lease, a
 * timeout). Running it twice must not do the work twice, for example by using the job ID as an
 * idempotency key with your payment provider.
 *
 * <p><b>Respond to interruption.</b> The pool interrupts the handler's thread when the job times
 * out, when another worker has taken the job over, or when shutdown runs out of patience. Blocking
 * calls such as {@code Thread.sleep} then throw {@link InterruptedException}. Let it propagate, or
 * check {@code Thread.currentThread().isInterrupted()} in long loops.
 *
 * <p>Exception messages are stored in the job's {@code last_error} column for operators. Do not
 * put secrets or personal data in them.
 *
 * <p>{@code @FunctionalInterface} means a lambda can implement it:
 * {@code job -> mailer.send(job.payloadJson())}.
 */
@FunctionalInterface
public interface JobHandler {

    /**
     * Runs the job. Called on a worker thread. Many jobs run at once, so the handler must be
     * thread-safe.
     */
    void handle(ClaimedJob job) throws Exception;
}
