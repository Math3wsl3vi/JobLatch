package dev.joblatch.core;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs jobs from one queue: claims them, runs their handlers on a fixed number of threads,
 * heartbeats them while they run, and records how each one ended.
 *
 * <pre>{@code
 * WorkerPool pool = new WorkerPool(dataSource, WorkerPoolConfig.defaults("emails"),
 *         Map.of("send-receipt", job -> mailer.send(job.payloadJson())));
 * pool.start();
 * // Stop cleanly on Ctrl+C / SIGTERM (e.g. from Kubernetes):
 * Runtime.getRuntime().addShutdownHook(new Thread(pool::close));
 * }</pre>
 *
 * <h2>Backpressure</h2>
 *
 * <p>The pool never claims more jobs than it has free threads. A {@link Semaphore} holds one permit
 * per thread. The poller takes permits before claiming, and each job gives its permit back when
 * its outcome has been recorded. Waiting work stays in the database, which is durable and visible
 * to other workers, instead of piling up in memory.
 *
 * <h2>Threads</h2>
 *
 * <ul>
 *   <li>one poller: waits for free slots, claims, hands jobs to workers
 *   <li>{@code concurrency} workers: run handlers and record outcomes
 *   <li>one scheduler: heartbeats and timeouts for running jobs
 *   <li>one notification listener (unless turned off): wakes the poller when a job arrives
 * </ul>
 *
 * <p>Run a {@link LeaseReaper} alongside, so jobs from crashed processes are recovered.
 */
public final class WorkerPool implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WorkerPool.class);

    /** After shutdownNow(), how long to wait for interrupted handlers to finish recording. */
    private static final Duration FORCED_STOP_GRACE = Duration.ofSeconds(5);

    /**
     * Shortest idle wait. Stops a busy loop when a job is due but every due row is momentarily
     * locked by other workers (the claim skips them and comes back empty).
     */
    private static final Duration MIN_IDLE_WAIT = Duration.ofMillis(10);

    private final WorkerPoolConfig config;
    /** Never null; already wrapped so a throwing listener cannot break this pool. */
    private final JobEventListener events;
    private final Map<String, JobHandler> handlers;
    private final JobClaimer claimer;
    private final JobLifecycle lifecycle;
    private final LeaseManager leases;
    private final String workerId;
    private final int maxClaimBatch;

    /** One permit per worker thread. Taken before claiming, given back after the outcome. */
    private final Semaphore freeSlots;
    /** Jobs handed to a worker and not yet finished. Separate from freeSlots, see activeJobs(). */
    private final AtomicInteger activeJobs = new AtomicInteger();
    private final ThreadPoolExecutor workers;
    private final ScheduledThreadPoolExecutor scheduler;

    /**
     * The idle poller waits on this. A notification (or anything else) calls wake(), which releases
     * a permit and ends the wait early. Permits beyond the first are drained after waking, so a
     * burst of notifications causes one extra claim pass, not hundreds.
     */
    private final Semaphore wakeUp = new Semaphore(0);
    /** Null when notifications are turned off. */
    private final JobNotificationListener listener;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    /** volatile: written by close() on one thread, read by the poller on another. */
    private volatile boolean stopping = false;
    private volatile Thread poller;

    /**
     * @param dataSource where connections come from. Use a connection pool (e.g. HikariCP) with
     *     room for roughly concurrency + 2 connections.
     * @param config     pool settings
     * @param handlers   job type → handler. Jobs of other types are failed (and retried), not lost.
     */
    public WorkerPool(DataSource dataSource, WorkerPoolConfig config, Map<String, JobHandler> handlers) {
        this(dataSource, config, handlers, JobEventListener.NONE);
    }

    /**
     * Same, reporting what happens to {@code events}: for metrics, see {@link JobEventListener}.
     *
     * @param events receives claim, outcome, lease and error events. Use {@link JobCounters} for
     *     in-memory totals, or an adapter to your metrics library.
     */
    public WorkerPool(DataSource dataSource, WorkerPoolConfig config, Map<String, JobHandler> handlers,
            JobEventListener events) {
        Objects.requireNonNull(dataSource, "dataSource");
        this.config = Objects.requireNonNull(config, "config");
        this.events = SafeEventListener.wrap(events);
        // Map.copyOf makes an unmodifiable copy (later changes by the caller do not affect us) and
        // rejects null keys and values.
        this.handlers = Map.copyOf(Objects.requireNonNull(handlers, "handlers"));
        this.handlers.keySet().forEach(type -> Names.requireValid("job type", type));

        this.claimer = new JobClaimer(dataSource);
        this.lifecycle = new JobLifecycle(dataSource, config.retryPolicy());
        this.leases = new LeaseManager(dataSource);

        // Unique per pool instance: process ID plus random characters. Ownership checks rely on it.
        this.workerId = "w-" + ProcessHandle.current().pid() + "-" + UUID.randomUUID().toString().substring(0, 8);
        this.maxClaimBatch = Math.min(config.concurrency(), JobClaimer.MAX_BATCH_SIZE);

        int n = config.concurrency();
        this.freeSlots = new Semaphore(n);
        // A fixed set of n threads with a queue of capacity n. The semaphore already guarantees at
        // most n jobs in flight, so the queue can never overflow. The bound is a second safety net:
        // a bug would show up as a RejectedExecutionException, not as unbounded memory growth.
        this.workers = new ThreadPoolExecutor(n, n, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(n), namedThreads("joblatch-worker-" + config.queue()));
        this.scheduler = new ScheduledThreadPoolExecutor(1, namedThreads("joblatch-heartbeat-" + config.queue()));
        // Cancelled heartbeat/timeout tasks are removed at once instead of lingering until their
        // due time. Without this, a long timeout on many short jobs leaks memory.
        this.scheduler.setRemoveOnCancelPolicy(true);

        this.listener = config.useNotifications()
                ? new JobNotificationListener(dataSource, config.queue(), this::wake,
                        e -> this.events.backgroundError(JobEventListener.Component.NOTIFICATION_LISTENER, config.queue(), e))
                : null;
    }

    /**
     * Starts claiming and running jobs.
     *
     * @throws IllegalStateException if already started, or already closed
     */
    public void start() {
        if (closed.get()) {
            throw new IllegalStateException("WorkerPool is closed");
        }
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("WorkerPool already started");
        }
        Thread t = new Thread(this::pollLoop, "joblatch-poller-" + config.queue());
        t.setDaemon(true);
        poller = t;
        t.start();
        if (listener != null) {
            listener.start();
        }
        log.info("Worker pool {} started on queue {} with {} thread(s)", workerId, config.queue(), config.concurrency());
    }

    /** This pool's worker ID, as stored in {@code locked_by}. */
    public String workerId() {
        return workerId;
    }

    /** The queue this pool takes jobs from. Useful as a metric tag. */
    public String queue() {
        return config.queue();
    }

    /** The number of worker threads: the most jobs this pool runs at once. For utilization gauges. */
    public int concurrency() {
        return config.concurrency();
    }

    /**
     * How many jobs this pool is running right now.
     *
     * <p>Counted separately rather than derived from the semaphore: the poller also holds permits
     * briefly while a claim is in flight, even when the claim comes back empty.
     */
    public int activeJobs() {
        return activeJobs.get();
    }

    // ------------------------------------------------------------------ poller

    /**
     * The poller thread. Never throws: every failure is logged and followed by a pause, so a
     * database outage slows the pool down instead of killing it.
     */
    private void pollLoop() {
        while (!stopping) {
            int slots;
            try {
                slots = acquireSlots();
            } catch (InterruptedException e) {
                break; // close() interrupts the poller to stop it
            }
            if (stopping) {
                freeSlots.release(slots);
                break;
            }

            List<ClaimedJob> jobs;
            long claimStart = System.nanoTime();
            try {
                jobs = claimer.claim(config.queue(), workerId, slots, config.leaseDuration());
            } catch (SQLException e) {
                freeSlots.release(slots);
                log.warn("Claim failed ({}, SQLState {}); retrying in {} ms",
                        e.getClass().getSimpleName(), e.getSQLState(), config.pollInterval().toMillis());
                events.backgroundError(JobEventListener.Component.CLAIM, config.queue(), e);
                if (!pause(config.pollInterval())) {
                    break;
                }
                continue;
            } catch (RuntimeException e) {
                freeSlots.release(slots);
                log.error("Claim failed unexpectedly ({})", e.getClass().getName());
                events.backgroundError(JobEventListener.Component.CLAIM, config.queue(), e);
                if (!pause(config.pollInterval())) {
                    break;
                }
                continue;
            }

            events.jobsClaimed(config.queue(), slots, jobs.size(), Duration.ofNanos(System.nanoTime() - claimStart));

            // Give back the permits we reserved but could not fill.
            freeSlots.release(slots - jobs.size());
            // Submit every claimed job, even if close() started meanwhile: they are already ours
            // in the database, and the workers still accept tasks until the poller has stopped.
            for (ClaimedJob job : jobs) {
                submit(job);
            }
            if (jobs.isEmpty() && !pause(idleWait())) {
                break;
            }
        }
        log.debug("Poller for {} stopped", workerId);
    }

    /**
     * Blocks until at least one thread is free, then also takes any other free permits without
     * waiting, up to the batch limit. Claiming several at once saves round trips when a backlog
     * builds up.
     */
    private int acquireSlots() throws InterruptedException {
        freeSlots.acquire();
        int slots = 1;
        while (slots < maxClaimBatch && freeSlots.tryAcquire()) {
            slots++;
        }
        return slots;
    }

    /**
     * How long to wait after a claim found nothing: until the next scheduled job is due, but never
     * longer than the poll interval (the safety net) and never shorter than MIN_IDLE_WAIT.
     */
    private Duration idleWait() {
        Duration wait = config.pollInterval();
        try {
            Optional<Duration> nextDue = claimer.timeUntilNextDue(config.queue());
            if (nextDue.isPresent() && nextDue.get().compareTo(wait) < 0) {
                wait = nextDue.get();
            }
        } catch (SQLException | RuntimeException e) {
            // Not worth a warning of its own: the next claim will report a real outage.
            log.debug("Could not look up next due job ({})", e.getClass().getSimpleName());
        }
        return wait.compareTo(MIN_IDLE_WAIT) < 0 ? MIN_IDLE_WAIT : wait;
    }

    /**
     * Waits up to {@code wait}, or less if {@link #wake()} is called. Returns false if interrupted
     * (meaning: stop).
     */
    private boolean pause(Duration wait) {
        try {
            wakeUp.tryAcquire(wait.toMillis(), TimeUnit.MILLISECONDS);
            wakeUp.drainPermits(); // collapse a burst of wake-ups into this one
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }

    /** Ends an idle wait early. Called by the notification listener. Cheap and thread-safe. */
    private void wake() {
        wakeUp.release();
    }

    private void submit(ClaimedJob job) {
        activeJobs.incrementAndGet();
        try {
            workers.execute(() -> runJob(job));
        } catch (RejectedExecutionException e) {
            // Should be impossible (see the constructor). If it happens anyway, the job is not
            // lost: it stays 'running' in the database until its lease expires and is reclaimed.
            activeJobs.decrementAndGet();
            freeSlots.release();
            log.error("Job {} could not be scheduled; it will be recovered when its lease expires", job.id());
        }
    }

    // ------------------------------------------------------------------ running one job

    /** Runs on a worker thread: handler, then outcome. Always gives the permit back. */
    private void runJob(ClaimedJob job) {
        RunningJob running = new RunningJob(Thread.currentThread());
        long heartbeatMs = config.heartbeatInterval().toMillis();
        ScheduledFuture<?> heartbeat = scheduler.scheduleAtFixedRate(
                () -> heartbeat(job, running), heartbeatMs, heartbeatMs, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> timeout = config.jobTimeout() == null
                ? null
                : scheduler.schedule(() -> running.interrupt(RunningJob.TIMED_OUT),
                        config.jobTimeout().toMillis(), TimeUnit.MILLISECONDS);

        Throwable failure = null;
        long handlerStart = System.nanoTime();
        try {
            JobHandler handler = handlers.get(job.jobType());
            if (handler == null) {
                failure = new IllegalStateException("no handler registered for job type " + job.jobType());
            } else {
                handler.handle(job);
            }
        } catch (Throwable t) {
            // Throwable, not just Exception: an Error from one handler (AssertionError,
            // NoClassDefFoundError...) must be recorded as that job's failure, not leave it
            // 'running' until its lease expires.
            failure = t;
        } finally {
            heartbeat.cancel(false);
            if (timeout != null) {
                timeout.cancel(false);
            }
        }
        Duration handlerDuration = Duration.ofNanos(System.nanoTime() - handlerStart);

        // From here on, no heartbeat or timeout can interrupt this thread for this job.
        boolean interruptedByShutdown = running.finish();
        try {
            boolean timedOut = RunningJob.TIMED_OUT.equals(running.reason());
            JobExecution.Outcome outcome = recordOutcome(job, failure, running.reason());
            events.jobFinished(new JobExecution(job, outcome, handlerDuration, failure, timedOut));
        } finally {
            activeJobs.decrementAndGet();
            freeSlots.release();
            if (interruptedByShutdown) {
                Thread.currentThread().interrupt(); // hand the shutdown signal back to the pool
            }
        }

        if (failure instanceof VirtualMachineError vmError) {
            // Out of memory, stack overflow: the JVM itself is in trouble. The failure is already
            // recorded. Rethrow so this thread dies, and the executor starts a fresh one.
            throw vmError;
        }
    }

    /** Writes the outcome and returns what was recorded. Never throws. */
    private JobExecution.Outcome recordOutcome(ClaimedJob job, Throwable failure, String interruptReason) {
        try {
            TransitionResult result;
            if (failure == null) {
                result = lifecycle.complete(job);
            } else if (failure instanceof PermanentJobFailureException) {
                result = lifecycle.failPermanently(job, describe(failure));
            } else if (RunningJob.TIMED_OUT.equals(interruptReason)) {
                result = lifecycle.fail(job, "timed out after " + config.jobTimeout().toMillis() + " ms");
            } else {
                result = lifecycle.fail(job, describe(failure));
            }
            if (failure != null) {
                // Class name only: exception messages can contain payload data. The full message
                // is in the database's last_error column.
                log.warn("Job {} ({}) failed with {}: {}", job.id(), job.jobType(),
                        failure.getClass().getName(), result);
            }
            return switch (result) {
                case SUCCEEDED -> JobExecution.Outcome.SUCCEEDED;
                case RETRY_SCHEDULED -> JobExecution.Outcome.RETRY_SCHEDULED;
                case DEAD -> JobExecution.Outcome.DEAD;
                case NOT_OWNER -> JobExecution.Outcome.NOT_OWNER;
            };
        } catch (SQLException e) {
            // The database did not record the outcome. The job is still 'running' and will be
            // reclaimed when its lease expires, then run again: at-least-once, not lost.
            log.warn("Could not record outcome of job {} ({}, SQLState {}); it will be retried after its lease expires",
                    job.id(), e.getClass().getSimpleName(), e.getSQLState());
            events.backgroundError(JobEventListener.Component.RECORD_OUTCOME, config.queue(), e);
            return JobExecution.Outcome.NOT_RECORDED;
        } catch (RuntimeException e) {
            log.error("Could not record outcome of job {} ({})", job.id(), e.getClass().getName());
            events.backgroundError(JobEventListener.Component.RECORD_OUTCOME, config.queue(), e);
            return JobExecution.Outcome.NOT_RECORDED;
        }
    }

    /** "java.io.IOException: connection reset". Truncated and cleaned by JobLifecycle. */
    private static String describe(Throwable failure) {
        String message = failure.getMessage();
        return message == null ? failure.getClass().getName() : failure.getClass().getName() + ": " + message;
    }

    /** Runs on the scheduler thread. Must never throw (a throwing task is silently cancelled). */
    private void heartbeat(ClaimedJob job, RunningJob running) {
        if (running.isInterrupted()) {
            return; // already told to stop; nothing left to protect
        }
        try {
            if (leases.renew(job, config.leaseDuration()).isEmpty()) {
                log.warn("Lost ownership of job {}; interrupting its handler", job.id());
                running.interrupt(RunningJob.OWNERSHIP_LOST);
                events.leaseLost(job);
            }
        } catch (SQLException e) {
            // Keep the job running. If renewals keep failing, the lease expires and another worker
            // may take over. The fencing token then rejects this worker's outcome.
            log.warn("Heartbeat for job {} failed ({}, SQLState {})",
                    job.id(), e.getClass().getSimpleName(), e.getSQLState());
            events.backgroundError(JobEventListener.Component.HEARTBEAT, config.queue(), e);
        } catch (RuntimeException e) {
            log.error("Heartbeat for job {} failed unexpectedly ({})", job.id(), e.getClass().getName());
            events.backgroundError(JobEventListener.Component.HEARTBEAT, config.queue(), e);
        }
    }

    // ------------------------------------------------------------------ shutdown

    /**
     * Stops the pool: no new claims, running jobs get {@code shutdownTimeout} to finish, then they
     * are interrupted. Jobs that still do not stop are left alone. Their leases expire and another
     * worker recovers them.
     *
     * <p>Safe to call more than once, and from a shutdown hook. Do not call it from inside a
     * handler: it would wait for itself.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        stopping = true;
        try {
            if (listener != null) {
                listener.close(); // no more wake-ups needed
            }
            Thread t = poller;
            if (t != null) {
                t.interrupt();
                t.join(Math.max(1, config.shutdownTimeout().toMillis()));
            }

            workers.shutdown(); // running jobs continue, no new ones are accepted
            if (!workers.awaitTermination(config.shutdownTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                log.warn("Jobs still running after {} ms; interrupting them", config.shutdownTimeout().toMillis());
                workers.shutdownNow();
                if (!workers.awaitTermination(FORCED_STOP_GRACE.toMillis(), TimeUnit.MILLISECONDS)) {
                    log.error("{} job(s) ignored the interrupt; they will be recovered when their leases expire",
                            workers.getActiveCount());
                }
            }
        } catch (InterruptedException e) {
            // Our caller wants us to hurry: stop everything now and keep the interrupt flag set.
            workers.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            // Stopped last, so leases keep being renewed while jobs drain.
            scheduler.shutdownNow();
            log.info("Worker pool {} stopped", workerId);
        }
    }

    // ------------------------------------------------------------------ helpers

    private static ThreadFactory namedThreads(String prefix) {
        AtomicInteger counter = new AtomicInteger();
        return task -> {
            Thread t = new Thread(task, prefix + "-" + counter.incrementAndGet());
            // Daemon: the JVM can exit even if close() was never called. Jobs cut off that way
            // are recovered through lease expiry.
            t.setDaemon(true);
            return t;
        };
    }

    /**
     * Makes sure a heartbeat or timeout interrupts the handler only while THIS job is running.
     *
     * <p>Worker threads are reused. Without this, a timeout firing a moment after job A finished
     * could interrupt job B on the same thread. Both methods are {@code synchronized} on this
     * object, so "is the job still running?" and "interrupt" happen as one step.
     */
    private static final class RunningJob {
        static final String TIMED_OUT = "timed out";
        static final String OWNERSHIP_LOST = "ownership lost";

        private final Thread thread;
        private boolean finished;        // guarded by this
        private String interruptReason;  // guarded by this

        RunningJob(Thread thread) {
            this.thread = thread;
        }

        synchronized void interrupt(String reason) {
            if (!finished && interruptReason == null) {
                interruptReason = reason;
                thread.interrupt();
            }
        }

        synchronized boolean isInterrupted() {
            return interruptReason != null;
        }

        synchronized String reason() {
            return interruptReason;
        }

        /**
         * Called by the worker thread when the handler has returned. After this, interrupt() does
         * nothing. Clears the thread's interrupt flag so recording the outcome is not disturbed
         * (a pooled DataSource may refuse to hand out connections to an interrupted thread).
         *
         * @return true if the flag was set by someone else (the pool's shutdownNow), so the caller
         *     can restore it afterwards
         */
        synchronized boolean finish() {
            finished = true;
            boolean flagWasSet = Thread.interrupted(); // reads AND clears the current thread's flag
            return flagWasSet && interruptReason == null;
        }
    }
}
