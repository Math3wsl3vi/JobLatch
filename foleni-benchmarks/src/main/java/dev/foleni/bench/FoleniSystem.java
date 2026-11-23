package dev.foleni.bench;

import dev.foleni.core.FoleniMigrations;
import dev.foleni.core.JobQueue;
import dev.foleni.core.NewJob;
import dev.foleni.core.WorkerPool;
import dev.foleni.core.WorkerPoolConfig;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.function.LongConsumer;
import javax.sql.DataSource;

/** Foleni, either with LISTEN/NOTIFY (the default) or with plain polling and the triggers off. */
final class FoleniSystem implements QueueSystem {

    static final String QUEUE = "bench";
    static final String JOB_TYPE = "noop";

    private final DataSource dataSource;
    private final boolean notifications;
    private final Duration pollInterval;
    private final JobQueue jobQueue = new JobQueue();
    private WorkerPool pool;

    /**
     * @param notifications true: NOTIFY triggers on, pool listens (Foleni's default).
     *                      false: triggers disabled, pool only polls.
     * @param pollInterval  the pool's polling interval
     */
    FoleniSystem(DataSource dataSource, boolean notifications, Duration pollInterval) {
        this.dataSource = dataSource;
        this.notifications = notifications;
        this.pollInterval = pollInterval;
    }

    @Override
    public String name() {
        return notifications ? "foleni" : "foleni-poll";
    }

    @Override
    public String settings() {
        return notifications
                ? "LISTEN/NOTIFY on, safety poll " + pollInterval.toMillis() + " ms, lease 60 s"
                : "NOTIFY triggers disabled, poll every " + pollInterval.toMillis() + " ms, lease 60 s";
    }

    @Override
    public void createSchema() throws Exception {
        FoleniMigrations.migrate(dataSource);
        if (!notifications) {
            // No NOTIFY at all: also removes its cost from enqueue, which is what we want to see.
            execute("ALTER TABLE foleni_jobs DISABLE TRIGGER USER");
        }
    }

    @Override
    public void enqueue(long enqueuedAtNanos) throws Exception {
        jobQueue.enqueue(dataSource, NewJob.of(QUEUE, JOB_TYPE, "{\"t\":" + enqueuedAtNanos + "}"));
    }

    @Override
    public void startWorkers(int threads, LongConsumer onRun) {
        WorkerPoolConfig config = WorkerPoolConfig.defaults(QUEUE)
                .withConcurrency(threads)
                .withPollInterval(pollInterval)
                .withNotifications(notifications);
        pool = new WorkerPool(dataSource, config, Map.of(JOB_TYPE, job -> onRun.accept(readT(job.payloadJson()))));
        pool.start();
    }

    @Override
    public void stopWorkers() {
        if (pool != null) {
            pool.close();
            pool = null;
        }
    }

    @Override
    public long pending() throws Exception {
        return count("SELECT count(*) FROM foleni_jobs WHERE state <> 'succeeded'");
    }

    /**
     * Unfinished = available or running. Each half is answered by a partial index (claim index,
     * lease index) without touching finished rows, which Foleni keeps.
     */
    @Override
    public boolean anyPending() throws Exception {
        return count("SELECT (EXISTS (SELECT 1 FROM foleni_jobs WHERE state = 'available')"
                + " OR EXISTS (SELECT 1 FROM foleni_jobs WHERE state = 'running'))::int") > 0;
    }

    @Override
    public void clear() throws Exception {
        execute("TRUNCATE foleni_jobs");
    }

    @Override
    public void close() {
        stopWorkers();
    }

    /** Reads the number out of {"t": 123}. Postgres's jsonb output adds a space after the colon. */
    private static long readT(String payload) {
        int start = payload.indexOf(':') + 1;
        int end = payload.indexOf('}', start);
        return Long.parseLong(payload.substring(start, end).trim());
    }

    private void execute(String sql) throws Exception {
        try (Connection conn = dataSource.getConnection(); Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    private long count(String sql) throws Exception {
        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
