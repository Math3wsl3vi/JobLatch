package dev.joblatch.core;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background thread that calls {@link LeaseManager#reclaimExpired} on a fixed schedule.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * try (LeaseReaper reaper = new LeaseReaper(leaseManager, Duration.ofSeconds(10), 100)) {
 *     reaper.start();
 *     ... run the application ...
 * } // close() stops the thread
 * }</pre>
 *
 * <p>Run one in every worker process. Several reapers at once are safe (SKIP LOCKED gives each a
 * different set of rows), and having one per process means recovery keeps working as long as any
 * process is alive.
 *
 * <p>{@code AutoCloseable} means it can be used in try-with-resources, like a Connection.
 */
public final class LeaseReaper implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LeaseReaper.class);

    /**
     * If a pass reclaims a full batch, there are probably more expired jobs waiting, so the reaper
     * goes again straight away. This caps how many batches one tick may run, so a huge backlog
     * cannot keep the reaper (and a database connection) busy without a break.
     */
    static final int MAX_BATCHES_PER_TICK = 100;

    /** How long close() waits for a running pass to finish before interrupting it. */
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(10);

    private final LeaseManager leaseManager;
    /** Never null; already wrapped so a throwing listener cannot stop the reaper. */
    private final JobEventListener events;
    private final Duration interval;
    private final int batchSize;
    private final ScheduledExecutorService executor;
    private final AtomicBoolean started = new AtomicBoolean(false);

    /**
     * @param leaseManager does the actual reclaiming
     * @param interval     pause between the end of one pass and the start of the next
     * @param batchSize    jobs per transaction, between 1 and {@link LeaseManager#MAX_RECLAIM_BATCH}
     */
    public LeaseReaper(LeaseManager leaseManager, Duration interval, int batchSize) {
        this(leaseManager, interval, batchSize, JobEventListener.NONE);
    }

    /**
     * Same, reporting reclaimed leases and failed passes to {@code events}.
     *
     * @param events receives {@link JobEventListener#leasesReclaimed} and
     *     {@link JobEventListener#backgroundError} with {@link JobEventListener.Component#REAPER}
     */
    public LeaseReaper(LeaseManager leaseManager, Duration interval, int batchSize, JobEventListener events) {
        this.leaseManager = Objects.requireNonNull(leaseManager, "leaseManager");
        this.events = SafeEventListener.wrap(events);
        this.interval = Objects.requireNonNull(interval, "interval");
        if (interval.toMillis() < 1) {
            throw new IllegalArgumentException("interval must be at least 1 millisecond");
        }
        if (batchSize < 1 || batchSize > LeaseManager.MAX_RECLAIM_BATCH) {
            throw new IllegalArgumentException(
                    "batchSize must be between 1 and " + LeaseManager.MAX_RECLAIM_BATCH);
        }
        this.batchSize = batchSize;

        // One named daemon thread. "Daemon" means it will not keep the JVM alive on its own if
        // the application forgets to call close().
        this.executor = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread t = new Thread(task, "joblatch-lease-reaper");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Starts reaping. The first pass runs immediately.
     *
     * @throws IllegalStateException if already started
     */
    public void start() {
        // compareAndSet: atomically "if false, set to true". Only one caller can win, even if two
        // threads call start() at the same moment.
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("LeaseReaper already started");
        }
        // scheduleWithFixedDelay waits `interval` AFTER each pass finishes, so passes never overlap,
        // even if one is slow.
        executor.scheduleWithFixedDelay(this::runOnce, 0, interval.toMillis(), TimeUnit.MILLISECONDS);
        log.info("Lease reaper started (interval {} ms, batch {})", interval.toMillis(), batchSize);
    }

    /**
     * One tick: reclaim batches until a batch comes back less than full, or the cap is reached.
     *
     * <p>This method must NEVER throw. If a task scheduled with a ScheduledExecutorService throws,
     * the executor silently cancels every future run. The reaper would stop forever with no error
     * anywhere. So every exception is caught here, logged, and the next tick tries again.
     */
    private void runOnce() {
        try {
            for (int batch = 0; batch < MAX_BATCHES_PER_TICK; batch++) {
                if (Thread.currentThread().isInterrupted()) {
                    return; // close() asked us to stop
                }
                ReclaimResult result = leaseManager.reclaimExpired(batchSize);
                if (result.total() > 0) {
                    events.leasesReclaimed(result);
                }
                if (result.total() < batchSize) {
                    return; // nothing more waiting
                }
            }
        } catch (SQLException e) {
            // Database down, network blip, failover... Log only the class and SQLState: the
            // message of a database error can quote row data.
            log.warn("Lease reaper pass failed ({}, SQLState {}); retrying in {} ms",
                    e.getClass().getSimpleName(), e.getSQLState(), interval.toMillis());
            events.backgroundError(JobEventListener.Component.REAPER, null, e);
        } catch (RuntimeException e) {
            // A bug, not an outage. Still keep the reaper alive: stopping recovery is worse.
            log.error("Lease reaper pass failed unexpectedly ({}); retrying in {} ms",
                    e.getClass().getName(), interval.toMillis());
            events.backgroundError(JobEventListener.Component.REAPER, null, e);
        }
    }

    /**
     * Stops the reaper. Waits up to 10 seconds for a running pass to finish, then interrupts it.
     * Safe to call more than once, and safe to call if {@link #start()} was never called.
     */
    @Override
    public void close() {
        executor.shutdown(); // no new passes. A pass already running may finish.
        try {
            if (!executor.awaitTermination(CLOSE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                executor.shutdownNow(); // interrupt the running pass
            }
        } catch (InterruptedException e) {
            // Someone interrupted US while we were waiting. Stop the reaper now, and restore the
            // interrupt flag so our caller can still see that it was interrupted. Swallowing an
            // interrupt is a classic Java bug: the caller loses its "please stop" signal.
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /** For tests: true once close() has fully stopped the background thread. */
    boolean isTerminated() {
        return executor.isTerminated();
    }
}