package dev.foleni.bench;

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.function.LongConsumer;
import javax.sql.DataSource;

/**
 * db-scheduler, configured for PostgreSQL with its "lock and fetch" polling strategy (SELECT ...
 * FOR UPDATE SKIP LOCKED), the closest match to how Foleni claims jobs.
 *
 * <p>Differences that remain, and are reported with the results:
 *
 * <ul>
 *   <li>db-scheduler has no LISTEN/NOTIFY: idle workers find new jobs by polling.
 *   <li>It DELETEs a one-time task when it completes. Foleni UPDATEs the row to 'succeeded' and
 *       keeps it.
 *   <li>Task data is serialized with Java serialization into a bytea column. Foleni stores JSON.
 *   <li>It compares execution_time with the JVM's clock. Foleni uses the database's clock.
 * </ul>
 */
final class DbSchedulerSystem implements QueueSystem {

    private static final String TASK_NAME = "noop";
    /** Fetch when at least half the threads are free, and up to one job per thread. */
    private static final double LOWER_LIMIT_FRACTION_OF_THREADS = 0.5;
    private static final double UPPER_LIMIT_FRACTION_OF_THREADS = 1.0;

    private final DataSource dataSource;
    private final Duration pollInterval;
    private final OneTimeTask<Long> task;
    private final SchedulerClient client;
    /** Set by startWorkers. volatile: read on db-scheduler's threads. */
    private volatile LongConsumer onRun = t -> { };
    private Scheduler scheduler;

    DbSchedulerSystem(DataSource dataSource, Duration pollInterval) {
        this.dataSource = dataSource;
        this.pollInterval = pollInterval;
        this.task = Tasks.oneTime(TASK_NAME, Long.class)
                .execute((instance, context) -> onRun.accept(instance.getData()));
        this.client = SchedulerClient.Builder.create(dataSource, task).build();
    }

    @Override
    public String name() {
        return "db-scheduler";
    }

    @Override
    public String settings() {
        return "lock-and-fetch (SKIP LOCKED), poll every " + pollInterval.toMillis() + " ms, fetch "
                + LOWER_LIMIT_FRACTION_OF_THREADS + "-" + UPPER_LIMIT_FRACTION_OF_THREADS + " x threads";
    }

    @Override
    public void createSchema() throws Exception {
        try (InputStream in = DbSchedulerSystem.class.getResourceAsStream("/db-scheduler/postgresql_tables.sql");
                Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement()) {
            st.execute(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    @Override
    public void enqueue(long enqueuedAtNanos) {
        // db-scheduler 17 asks callers to say what happens if the task instance already exists.
        // Our IDs are random UUIDs, so it never does: DO_NOTHING is the plain insert.
        client.schedule(task.instance(UUID.randomUUID().toString(), enqueuedAtNanos), Instant.now(),
                SchedulerClient.ScheduleOptions.WHEN_EXISTS_DO_NOTHING);
    }

    @Override
    public void startWorkers(int threads, LongConsumer onRun) {
        this.onRun = onRun;
        scheduler = Scheduler.create(dataSource, task)
                .threads(threads)
                .pollingInterval(pollInterval)
                .pollUsingLockAndFetch(LOWER_LIMIT_FRACTION_OF_THREADS, UPPER_LIMIT_FRACTION_OF_THREADS)
                .shutdownMaxWait(Duration.ofSeconds(10))
                .build();
        scheduler.start();
    }

    @Override
    public void stopWorkers() {
        if (scheduler != null) {
            scheduler.stop();
            scheduler = null;
        }
    }

    @Override
    public long pending() throws Exception {
        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT count(*) FROM scheduled_tasks")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    /** db-scheduler deletes finished tasks, so any row at all means unfinished work. */
    @Override
    public boolean anyPending() throws Exception {
        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT EXISTS (SELECT 1 FROM scheduled_tasks)")) {
            rs.next();
            return rs.getBoolean(1);
        }
    }

    @Override
    public void clear() throws Exception {
        try (Connection conn = dataSource.getConnection(); Statement st = conn.createStatement()) {
            st.execute("TRUNCATE scheduled_tasks");
        }
    }

    @Override
    public void close() {
        stopWorkers();
    }
}
