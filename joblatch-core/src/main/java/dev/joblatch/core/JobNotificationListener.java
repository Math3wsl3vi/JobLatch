package dev.joblatch.core;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds one database connection that LISTENs for "a job became available" notifications, and calls
 * {@code onWake} when one arrives for its queue.
 *
 * <p><b>Notifications are hints, never the source of truth.</b> Postgres does not store them: if
 * this connection is down when one is sent, it is gone. So:
 *
 * <ul>
 *   <li>after every (re)connect, {@code onWake} is called once, because anything could have
 *       arrived while we were not listening.
 *   <li>the {@link WorkerPool} keeps polling on its normal interval anyway.
 * </ul>
 *
 * <p>The connection is checked every {@link #HEALTH_CHECK_INTERVAL}. A network that dies silently
 * (no error, just no packets) would otherwise leave us "listening" to nothing forever.
 *
 * <p>Needs a real session: does not work through PgBouncer in transaction-pooling mode. Disable
 * notifications in {@link WorkerPoolConfig} in that case.
 */
public final class JobNotificationListener implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(JobNotificationListener.class);

    /** The channel the V3 trigger sends on. */
    static final String CHANNEL = "joblatch_jobs";

    /** A fixed SQL string: LISTEN cannot take a ? parameter, and nothing here comes from input. */
    private static final String LISTEN_SQL = "LISTEN joblatch_jobs";

    /** How long one wait for notifications lasts. Also how quickly close() is noticed. */
    private static final Duration RECEIVE_TIMEOUT = Duration.ofMillis(500);

    /** How often the connection is proven alive with a round trip. */
    static final Duration HEALTH_CHECK_INTERVAL = Duration.ofSeconds(10);

    /** Seconds isValid() may take before the connection is considered dead. */
    private static final int HEALTH_CHECK_TIMEOUT_SECONDS = 5;

    /** Reconnect delays: start small, double on each failure, never above the max. */
    private static final Duration MIN_RECONNECT_DELAY = Duration.ofMillis(100);
    private static final Duration MAX_RECONNECT_DELAY = Duration.ofSeconds(30);

    private final DataSource dataSource;
    private final String queue;
    private final Runnable onWake;
    private final AtomicBoolean started = new AtomicBoolean(false);

    private volatile boolean stopping = false;
    private volatile boolean listening = false;
    private volatile int backendPid = -1;
    private volatile Thread thread;

    /**
     * @param dataSource where the listening connection comes from. The listener keeps it for as
     *     long as it runs.
     * @param queue      only notifications for this queue trigger {@code onWake}
     * @param onWake     called on the listener's thread. Must be quick and must not throw.
     */
    public JobNotificationListener(DataSource dataSource, String queue, Runnable onWake) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        Names.requireValid("queue", queue);
        this.queue = queue;
        this.onWake = Objects.requireNonNull(onWake, "onWake");
    }

    /** Starts listening on a background thread. */
    public void start() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("JobNotificationListener already started");
        }
        Thread t = new Thread(this::run, "joblatch-listener-" + queue);
        t.setDaemon(true);
        thread = t;
        t.start();
    }

    /** True while a LISTEN connection is open and healthy. */
    public boolean isListening() {
        return listening;
    }

    /** For tests: the server process ID of the current listening connection, or -1. */
    int backendPid() {
        return backendPid;
    }

    /** Outer loop: connect, listen until something breaks, wait, reconnect. Never throws. */
    private void run() {
        Duration reconnectDelay = MIN_RECONNECT_DELAY;
        while (!stopping) {
            try {
                listenUntilBroken();
                reconnectDelay = MIN_RECONNECT_DELAY; // a clean exit means we are stopping
            } catch (SQLException e) {
                log.warn("Notification connection for queue {} lost ({}, SQLState {}); reconnecting in {} ms",
                        queue, e.getClass().getSimpleName(), e.getSQLState(), reconnectDelay.toMillis());
            } catch (RuntimeException e) {
                log.error("Notification listener for queue {} failed unexpectedly ({}); reconnecting in {} ms",
                        queue, e.getClass().getName(), reconnectDelay.toMillis());
            } finally {
                listening = false;
                backendPid = -1;
            }
            if (stopping || !sleep(reconnectDelay)) {
                break;
            }
            reconnectDelay = min(reconnectDelay.multipliedBy(2), MAX_RECONNECT_DELAY);
        }
        log.debug("Notification listener for queue {} stopped", queue);
    }

    /** Inner loop: one connection's lifetime. Returns normally only when stopping. */
    private void listenUntilBroken() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            // Each statement commits on its own. LISTEN only takes effect once committed.
            conn.setAutoCommit(true);
            // unwrap: get the Postgres-specific interface behind the generic JDBC Connection.
            // Works through pools like HikariCP, which wrap the real connection.
            PGConnection pg = conn.unwrap(PGConnection.class);
            try (Statement st = conn.createStatement()) {
                st.execute(LISTEN_SQL);
            }
            backendPid = pg.getBackendPID();
            listening = true;
            log.info("Listening for new jobs on queue {}", queue);

            // We may have missed notifications while disconnected: check the table now.
            wake();

            long nextHealthCheck = System.nanoTime() + HEALTH_CHECK_INTERVAL.toNanos();
            while (!stopping) {
                // Blocks until a notification arrives or the timeout passes. Throws if the
                // connection is closed or broken.
                PGNotification[] notifications = pg.getNotifications((int) RECEIVE_TIMEOUT.toMillis());
                if (notifications != null && containsOurQueue(notifications)) {
                    wake(); // once per batch is enough: one claim pass picks up everything due
                }
                if (System.nanoTime() - nextHealthCheck >= 0) {
                    if (!conn.isValid(HEALTH_CHECK_TIMEOUT_SECONDS)) {
                        throw new SQLException("notification connection failed its health check", "08006");
                    }
                    nextHealthCheck = System.nanoTime() + HEALTH_CHECK_INTERVAL.toNanos();
                }
            }
        }
    }

    private boolean containsOurQueue(PGNotification[] notifications) {
        for (PGNotification n : notifications) {
            if (CHANNEL.equals(n.getName()) && queue.equals(n.getParameter())) {
                return true;
            }
        }
        return false;
    }

    private void wake() {
        try {
            onWake.run();
        } catch (RuntimeException e) {
            // A misbehaving callback must not take the listener down.
            log.error("onWake callback for queue {} threw {}", queue, e.getClass().getName());
        }
    }

    private boolean sleep(Duration d) {
        try {
            Thread.sleep(d.toMillis());
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }

    private static Duration min(Duration a, Duration b) {
        return a.compareTo(b) <= 0 ? a : b;
    }

    /**
     * Stops listening and closes the connection. Returns within about {@link #RECEIVE_TIMEOUT}.
     * Safe to call more than once.
     */
    @Override
    public void close() {
        stopping = true;
        Thread t = thread;
        if (t == null) {
            return;
        }
        // Wakes it if it is sleeping between reconnects. A thread blocked in a socket read does
        // not react to interrupts, but getNotifications returns within RECEIVE_TIMEOUT anyway.
        t.interrupt();
        try {
            t.join(RECEIVE_TIMEOUT.multipliedBy(4).toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
