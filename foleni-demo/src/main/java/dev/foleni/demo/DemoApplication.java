package dev.foleni.demo;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.foleni.core.ClaimedJob;
import dev.foleni.core.FoleniMigrations;
import dev.foleni.core.JobQueue;
import dev.foleni.core.LeaseManager;
import dev.foleni.core.LeaseReaper;
import dev.foleni.core.NewJob;
import dev.foleni.core.PermanentJobFailureException;
import dev.foleni.core.WorkerPool;
import dev.foleni.core.WorkerPoolConfig;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A complete, runnable example of using Foleni in an application. It places a few orders and sends
 * a receipt for each one in the background.
 *
 * <p>What it shows, in the order it happens:
 *
 * <ol>
 *   <li><b>Configuration from the environment.</b> {@code FOLENI_DB_URL}, {@code FOLENI_DB_USER},
 *       {@code FOLENI_DB_PASSWORD}. Never hard-code credentials.
 *   <li><b>Transactional enqueue.</b> Each order and its "send receipt" job are written in ONE
 *       transaction: both exist, or neither does. One order is rolled back on purpose to show it.
 *   <li><b>A scheduled job</b> that must not run before its time.
 *   <li><b>An idempotent handler.</b> Delivery is at-least-once, so the handler records "receipt
 *       sent" with a conditional UPDATE: running it twice cannot send two receipts.
 *   <li><b>A permanent failure</b> that goes straight to the dead-letter state.
 *   <li><b>A worker pool and a lease reaper</b>, stopped cleanly by a shutdown hook.
 * </ol>
 *
 * <p>The demo exits by itself once every job has finished.
 */
public final class DemoApplication {

    private static final Logger log = LoggerFactory.getLogger(DemoApplication.class);

    private static final String QUEUE = "emails";
    private static final String SEND_RECEIPT = "send-receipt";
    private static final int ORDERS = 5;
    private static final Duration SCHEDULED_DELAY = Duration.ofSeconds(3);
    private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration PROGRESS_INTERVAL = Duration.ofMillis(500);

    private DemoApplication() {}

    public static void main(String[] args) throws Exception {
        HikariDataSource dataSource = connectionPool();
        // For the demo, one role does everything. In production run migrations at deploy time with
        // the owner role and give the application only foleni_runtime (docs/security.md).
        FoleniMigrations.migrate(dataSource);
        createOrdersTable(dataSource);

        JobQueue jobQueue = new JobQueue();
        long firstOrder = placeOrder(dataSource, jobQueue, "a@example.invalid", null);
        for (int i = 2; i <= ORDERS; i++) {
            placeOrder(dataSource, jobQueue, "customer" + i + "@example.invalid", null);
        }
        placeOrderThenRollBack(dataSource, jobQueue);
        placeOrder(dataSource, jobQueue, "later@example.invalid", Instant.now().plus(SCHEDULED_DELAY));
        jobQueue.enqueue(dataSource, NewJob.of(QUEUE, SEND_RECEIPT, "{\"orderId\": -1}").withMaxAttempts(3));
        log.info("Placed {} orders (+1 rolled back, +1 scheduled in {} s, +1 with a bad order ID)",
                ORDERS, SCHEDULED_DELAY.toSeconds());

        WorkerPool pool = new WorkerPool(dataSource,
                WorkerPoolConfig.defaults(QUEUE).withConcurrency(2),
                Map.of(SEND_RECEIPT, job -> sendReceipt(dataSource, job)));
        LeaseReaper reaper = new LeaseReaper(new LeaseManager(dataSource), Duration.ofSeconds(10), 100);

        // SIGTERM / Ctrl+C: stop claiming, let running jobs finish, then release connections.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            pool.close();
            reaper.close();
            dataSource.close();
        }, "demo-shutdown"));

        reaper.start();
        pool.start();

        waitUntilQueueIsDone(dataSource);
        // Run the first order's handler a second time by hand: the idempotency guard makes it a no-op.
        log.info("Re-running the handler for order {}, as an at-least-once redelivery would",
                firstOrder);
        markReceiptSent(dataSource, firstOrder);
        printSummary(dataSource);
        System.exit(0); // runs the shutdown hook
    }

    // ---------------------------------------------------------------- application code

    /**
     * Inserts an order AND enqueues its receipt job in one transaction. The job joins the caller's
     * transaction: Foleni never commits or rolls back a connection it was given.
     */
    private static long placeOrder(DataSource ds, JobQueue jobQueue, String email, Instant runAt)
            throws SQLException {
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try {
                long orderId = insertOrder(conn, email);
                NewJob job = NewJob.of(QUEUE, SEND_RECEIPT, "{\"orderId\": " + orderId + "}");
                jobQueue.enqueue(conn, runAt == null ? job : job.withRunAt(runAt));
                conn.commit();
                return orderId;
            } catch (SQLException | RuntimeException e) {
                conn.rollback();
                throw e;
            }
        }
    }

    /** Same as placeOrder, but the transaction is rolled back: neither the order nor its job exists. */
    private static void placeOrderThenRollBack(DataSource ds, JobQueue jobQueue) throws SQLException {
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            long orderId = insertOrder(conn, "cancelled@example.invalid");
            jobQueue.enqueue(conn, NewJob.of(QUEUE, SEND_RECEIPT, "{\"orderId\": " + orderId + "}"));
            conn.rollback(); // e.g. payment declined
            log.info("Order {} rolled back: its receipt job was never visible to any worker", orderId);
        }
    }

    /**
     * The job handler. Idempotent: it "sends" the receipt only if the order does not have one yet,
     * and records it in the same statement. A second delivery of the same job finds
     * receipt_sent_at already set and does nothing.
     */
    private static void sendReceipt(DataSource ds, ClaimedJob job) throws SQLException {
        long orderId = orderIdFromPayload(ds, job.payloadJson());
        if (orderId <= 0) {
            // Retrying cannot fix a bad order ID: go straight to the dead-letter state.
            throw new PermanentJobFailureException("payload has no valid orderId");
        }
        if (markReceiptSent(ds, orderId)) {
            log.info("Sent receipt for order {} (job {}, attempt {})", orderId, job.id(), job.attempts());
        }
    }

    /** True if this call recorded the receipt, false if it had already been sent. */
    private static boolean markReceiptSent(DataSource ds, long orderId) throws SQLException {
        try (Connection conn = ds.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "UPDATE demo_orders SET receipt_sent_at = now()"
                                + " WHERE id = ? AND receipt_sent_at IS NULL")) {
            ps.setLong(1, orderId);
            boolean sentNow = ps.executeUpdate() == 1;
            if (!sentNow) {
                log.info("Receipt for order {} was already sent: skipped (idempotency)", orderId);
            }
            return sentNow;
        }
    }

    /** Lets Postgres parse the JSON, so the demo needs no JSON library. */
    private static long orderIdFromPayload(DataSource ds, String payloadJson) throws SQLException {
        try (Connection conn = ds.getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT (?::jsonb ->> 'orderId')::bigint")) {
            ps.setString(1, payloadJson);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                long id = rs.getLong(1);
                return rs.wasNull() ? -1 : id;
            }
        }
    }

    // ---------------------------------------------------------------- plumbing

    private static HikariDataSource connectionPool() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(requireEnv("FOLENI_DB_URL"));
        config.setUsername(requireEnv("FOLENI_DB_USER"));
        config.setPassword(requireEnv("FOLENI_DB_PASSWORD"));
        // Pool concurrency (2) + listener + poller + heartbeats + reaper + this thread, with room.
        config.setMaximumPoolSize(10);
        config.setPoolName("demo");
        return new HikariDataSource(config);
    }

    private static void createOrdersTable(DataSource ds) throws SQLException {
        try (Connection conn = ds.getConnection(); Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS demo_orders ("
                    + " id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,"
                    + " email text NOT NULL,"
                    + " receipt_sent_at timestamptz)");
        }
    }

    private static long insertOrder(Connection conn, String email) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO demo_orders (email) VALUES (?) RETURNING id")) {
            ps.setString(1, email);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private static void waitUntilQueueIsDone(DataSource ds) throws Exception {
        long deadline = System.nanoTime() + WAIT_TIMEOUT.toNanos();
        while (countJobs(ds, "state IN ('available', 'running')") > 0) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("jobs did not finish within " + WAIT_TIMEOUT);
            }
            Thread.sleep(PROGRESS_INTERVAL.toMillis());
        }
    }

    private static void printSummary(DataSource ds) throws SQLException {
        log.info("Done. Jobs succeeded: {}, dead: {}. Orders with a receipt: {}.",
                countJobs(ds, "state = 'succeeded'"), countJobs(ds, "state = 'dead'"),
                count(ds, "SELECT count(*) FROM demo_orders WHERE receipt_sent_at IS NOT NULL"));
    }

    /** {@code where} is always a fixed string from this file, never input. */
    private static long countJobs(DataSource ds, String where) throws SQLException {
        return count(ds, "SELECT count(*) FROM foleni_jobs WHERE queue = '" + QUEUE + "' AND " + where);
    }

    private static long count(DataSource ds, String sql) throws SQLException {
        try (Connection conn = ds.getConnection();
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static String requireEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            // Name the variable, never print a value: it might be the password.
            throw new IllegalStateException("Missing environment variable " + name + " (see README: Quick start)");
        }
        return value;
    }
}
