package dev.joblatch.core;

import java.time.Duration;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps the application's {@link JobEventListener} so that an exception from it can never break a
 * JobLatch thread. Package-private: JobLatch uses it internally around every listener it is given.
 *
 * <p>Why this matters: listeners run inside the poller loop, worker threads, scheduled heartbeats
 * and the reaper. An exception escaping a scheduled task silently cancels all its future runs, and
 * one escaping the poller would stop all claiming. A metrics bug must never stop job processing.
 */
final class SafeEventListener implements JobEventListener {

    private static final Logger log = LoggerFactory.getLogger(SafeEventListener.class);

    private final JobEventListener delegate;

    private SafeEventListener(JobEventListener delegate) {
        this.delegate = delegate;
    }

    /** Wraps {@code listener}, unless it is {@link #NONE} or already wrapped. */
    static JobEventListener wrap(JobEventListener listener) {
        Objects.requireNonNull(listener, "listener");
        if (listener == NONE || listener instanceof SafeEventListener) {
            return listener;
        }
        return new SafeEventListener(listener);
    }

    @Override
    public void jobsClaimed(String queue, int requested, int claimed, Duration duration) {
        try {
            delegate.jobsClaimed(queue, requested, claimed, duration);
        } catch (Throwable e) {
            report("jobsClaimed", e);
        }
    }

    @Override
    public void jobFinished(JobExecution execution) {
        try {
            delegate.jobFinished(execution);
        } catch (Throwable e) {
            report("jobFinished", e);
        }
    }

    @Override
    public void leaseLost(ClaimedJob job) {
        try {
            delegate.leaseLost(job);
        } catch (Throwable e) {
            report("leaseLost", e);
        }
    }

    @Override
    public void leasesReclaimed(ReclaimResult result) {
        try {
            delegate.leasesReclaimed(result);
        } catch (Throwable e) {
            report("leasesReclaimed", e);
        }
    }

    @Override
    public void backgroundError(Component component, String queue, Throwable error) {
        try {
            delegate.backgroundError(component, queue, error);
        } catch (Throwable e) {
            report("backgroundError", e);
        }
    }

    /**
     * Throwable, not just RuntimeException: an AssertionError or NoClassDefFoundError from a metrics
     * listener must not kill a JobLatch thread either. Only VirtualMachineError (out of memory,
     * stack overflow) is rethrown: then the JVM itself is in trouble.
     */
    private void report(String method, Throwable e) {
        if (e instanceof VirtualMachineError vmError) {
            throw vmError;
        }
        // Class names only: the listener's exception message is outside our control.
        log.warn("JobEventListener {}.{} threw {}; event ignored",
                delegate.getClass().getName(), method, e.getClass().getName());
    }
}
