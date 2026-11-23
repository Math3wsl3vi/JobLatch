package dev.foleni.bench;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.foleni.core.ClaimedJob;
import dev.foleni.core.JobClaimer;
import dev.foleni.core.JobLifecycle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.Function;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Runs every benchmark phase against each job system, each on its own fresh PostgreSQL container,
 * and prints Markdown tables. Raw per-repetition numbers go to {@code target/benchmark-raw.csv}.
 *
 * <p>Sizes can be changed with system properties, e.g.
 * {@code -Dbench.jobs=20000 -Dbench.reps=5}. Defaults keep a full run around 10 minutes.
 */
public final class BenchmarkMain {

    // ---- workload (overridable with -Dbench.*) -------------------------------------------------
    private static final int JOBS = Integer.getInteger("bench.jobs", 10_000);
    private static final int REPS = Integer.getInteger("bench.reps", 3);
    private static final int PRODUCERS = Integer.getInteger("bench.producers", 8);
    private static final int[] THREAD_LEVELS = {1, 4, 16, 32};
    private static final int WARMUP_JOBS = 2_000;
    private static final int LATENCY_RATE_PER_SECOND = 200;
    private static final Duration LATENCY_DURATION = Duration.ofSeconds(15);
    private static final int LATENCY_THREADS = 16;
    private static final int CLAIM_BATCH = 100;

    // ---- environment ------------------------------------------------------------------------------
    private static final String POSTGRES_IMAGE = "postgres:18-alpine";
    /** How many of the most expensive SQL statements to report per system. */
    private static final int TOP_STATEMENTS = 6;
    /** Enough for 32 workers + producers + housekeeping, the same for every system. */
    private static final int CONNECTION_POOL_SIZE = 64;
    private static final Duration POLL = Duration.ofMillis(100);
    private static final Duration DRAIN_TIMEOUT = Duration.ofMinutes(5);
    private static final Duration PENDING_CHECK = Duration.ofMillis(20);

    private BenchmarkMain() {}

    /** Builds one system against a fresh database. */
    private record Candidate(String label, Function<HikariDataSource, QueueSystem> factory) {}

    public static void main(String[] args) throws Exception {
        List<Candidate> candidates = List.of(
                new Candidate("foleni", ds -> new FoleniSystem(ds, true, Duration.ofSeconds(5))),
                new Candidate("foleni-poll", ds -> new FoleniSystem(ds, false, POLL)),
                new Candidate("db-scheduler", ds -> new DbSchedulerSystem(ds, POLL)));

        StringBuilder report = new StringBuilder();
        List<String> raw = new ArrayList<>();
        raw.add("system,rep,phase,threads,ops_per_sec,db_cpu_s,db_mem_mib,wal_mib,worker_cpu_s,worker_heap_mib");

        report.append(environment());
        for (Candidate c : candidates) {
            report.append(runAll(c, raw));
        }

        Path out = Path.of("target");
        Files.createDirectories(out);
        Files.writeString(out.resolve("benchmark-results.md"), report.toString());
        Files.write(out.resolve("benchmark-raw.csv"), raw);
        System.out.println(report);
        System.out.println("Written to target/benchmark-results.md and target/benchmark-raw.csv");
        // Libraries can leave non-daemon background threads behind (connection pools, container
        // clean-up). This is a command-line program that has finished: exit explicitly.
        System.exit(0);
    }

    /** All phases for one system, on its own database. */
    private static String runAll(Candidate candidate, List<String> raw) throws Exception {
        System.out.println(">>> " + candidate.label());
        // Testcontainers starts Postgres with fsync=off by default (fast unit tests). A benchmark of a
        // DURABLE queue must pay for durable commits, so fsync is switched back on explicitly.
        // pg_stat_statements records where the database spends its time, per SQL statement.
        try (PostgreSQLContainer postgres = new PostgreSQLContainer(POSTGRES_IMAGE)
                .withCommand("postgres", "-c", "fsync=on", "-c", "shared_preload_libraries=pg_stat_statements")) {
            postgres.start();
            try (HikariDataSource ds = dataSource(postgres); QueueSystem system = candidate.factory().apply(ds)) {
                system.createSchema();
                execute(ds, "CREATE EXTENSION pg_stat_statements");
                Resources resources = new Resources(postgres, ds);
                StringBuilder sb = new StringBuilder();
                sb.append("\n## ").append(system.name()).append("\n\n");
                sb.append("Settings: ").append(system.settings()).append("\n\n");

                // Warm-up: let the JIT compile the hot paths. Not reported.
                enqueueAll(system, WARMUP_JOBS);
                drain(system, 4);
                system.clear();

                // Enqueue + drain, several repetitions per thread level. Statement stats cover exactly this.
                execute(ds, "SELECT pg_stat_statements_reset()");
                double[] enqueueRates = new double[REPS * THREAD_LEVELS.length];
                int e = 0;
                sb.append("| Worker threads | Drain jobs/s (median) | range | DB CPU s | DB mem MiB | WAL MiB | Worker CPU s | Worker peak heap MiB |\n");
                sb.append("|---:|---:|---|---:|---:|---:|---:|---:|\n");
                for (int threads : THREAD_LEVELS) {
                    double[] rates = new double[REPS];
                    Resources.Usage lastUsage = null;
                    for (int rep = 0; rep < REPS; rep++) {
                        system.clear();
                        double enqueueRate = enqueueAll(system, JOBS);
                        enqueueRates[e++] = enqueueRate;
                        raw.add(csv(system.name(), rep, "enqueue", PRODUCERS, enqueueRate, null));

                        resources.start();
                        double drainRate = drain(system, threads);
                        Resources.Usage usage = resources.stop();
                        rates[rep] = drainRate;
                        lastUsage = usage;
                        raw.add(csv(system.name(), rep, "drain", threads, drainRate, usage));
                        System.out.printf(Locale.ROOT, "  %s threads=%d rep=%d enqueue=%.0f/s drain=%.0f/s%n",
                                system.name(), threads, rep, enqueueRate, drainRate);
                    }
                    sb.append(String.format(Locale.ROOT, "| %d | %.0f | %.0f-%.0f | %.1f | %.0f | %.1f | %.1f | %.0f |%n",
                            threads, median(rates), min(rates), max(rates),
                            lastUsage.dbCpuSeconds(), lastUsage.dbMemoryMib(), lastUsage.walMib(),
                            lastUsage.workerCpuSeconds(), lastUsage.workerPeakHeapMib()));
                }
                sb.append(topStatements(ds));
                sb.append(String.format(Locale.ROOT,
                        "%nEnqueue (%d producer threads, 1 transaction per job): median %.0f jobs/s, range %.0f-%.0f%n",
                        PRODUCERS, median(enqueueRates), min(enqueueRates), max(enqueueRates)));

                // Foleni only: claim and complete on their own (db-scheduler has no public API for this).
                if (system instanceof FoleniSystem) {
                    sb.append(claimAndCompleteFoleni(system, ds));
                }

                // Latency under a steady load.
                system.clear();
                sb.append(latency(system));
                return sb.toString();
            }
        }
    }

    // ---------------------------------------------------------------- phases

    /** Enqueues {@code count} jobs with PRODUCERS threads. Returns jobs per second. */
    private static double enqueueAll(QueueSystem system, int count) throws Exception {
        AtomicInteger next = new AtomicInteger();
        ExecutorService producers = Executors.newFixedThreadPool(PRODUCERS);
        try {
            long start = System.nanoTime();
            List<Future<?>> futures = new ArrayList<>();
            for (int p = 0; p < PRODUCERS; p++) {
                futures.add(producers.submit(() -> {
                    while (next.getAndIncrement() < count) {
                        system.enqueue(System.nanoTime());
                    }
                    return null;
                }));
            }
            for (Future<?> f : futures) {
                f.get(); // rethrows any producer failure
            }
            return count / seconds(System.nanoTime() - start);
        } finally {
            producers.shutdownNow();
        }
    }

    /**
     * Starts workers on a preloaded queue and times until the DATABASE says every job is finished,
     * i.e. claim + run + completion all committed. Returns jobs per second.
     */
    private static double drain(QueueSystem system, int threads) throws Exception {
        long total = system.pending();
        long start = System.nanoTime();
        system.startWorkers(threads, t -> { });
        try {
            long deadline = start + DRAIN_TIMEOUT.toNanos();
            while (system.anyPending()) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException(system.name() + " did not drain within " + DRAIN_TIMEOUT);
                }
                Thread.sleep(PENDING_CHECK.toMillis());
            }
            return total / seconds(System.nanoTime() - start);
        } finally {
            system.stopWorkers();
        }
    }

    /** Foleni's claim and completion rates measured separately, with PRODUCERS threads each. */
    private static String claimAndCompleteFoleni(QueueSystem system, HikariDataSource ds) throws Exception {
        system.clear();
        enqueueAll(system, JOBS);
        JobClaimer claimer = new JobClaimer(ds);
        JobLifecycle lifecycle = new JobLifecycle(ds);
        ConcurrentLinkedQueue<ClaimedJob> claimed = new ConcurrentLinkedQueue<>();
        ExecutorService pool = Executors.newFixedThreadPool(PRODUCERS);
        try {
            long start = System.nanoTime();
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < PRODUCERS; i++) {
                futures.add(pool.submit(() -> {
                    List<ClaimedJob> batch;
                    while (!(batch = claimer.claim(FoleniSystem.QUEUE, "bench-" + Thread.currentThread().threadId(),
                            CLAIM_BATCH, Duration.ofMinutes(10))).isEmpty()) {
                        claimed.addAll(batch);
                    }
                    return null;
                }));
            }
            for (Future<?> f : futures) {
                f.get();
            }
            double claimRate = claimed.size() / seconds(System.nanoTime() - start);

            start = System.nanoTime();
            futures.clear();
            int completedCount = claimed.size();
            for (int i = 0; i < PRODUCERS; i++) {
                futures.add(pool.submit(() -> {
                    ClaimedJob job;
                    while ((job = claimed.poll()) != null) {
                        lifecycle.complete(job);
                    }
                    return null;
                }));
            }
            for (Future<?> f : futures) {
                f.get();
            }
            double completeRate = completedCount / seconds(System.nanoTime() - start);
            return String.format(Locale.ROOT,
                    "Claim only (%d threads, batches of %d): %.0f jobs/s. Complete only (%d threads, 1 per transaction): %.0f jobs/s%n",
                    PRODUCERS, CLAIM_BATCH, claimRate, PRODUCERS, completeRate);
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * Workers already running and idle. Jobs arrive at a steady rate. Latency = from just before the
     * enqueue call to the moment the handler starts (enqueue commit + wake-up or poll + claim).
     */
    private static String latency(QueueSystem system) throws Exception {
        int total = (int) (LATENCY_RATE_PER_SECOND * LATENCY_DURATION.toSeconds());
        AtomicLongArray latencies = new AtomicLongArray(total);
        AtomicInteger recorded = new AtomicInteger();
        system.startWorkers(LATENCY_THREADS, enqueuedAt -> {
            int i = recorded.getAndIncrement();
            if (i < total) {
                latencies.set(i, System.nanoTime() - enqueuedAt);
            }
        });
        Thread.sleep(1000); // let workers reach their idle state

        ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor();
        ExecutorService producers = Executors.newFixedThreadPool(PRODUCERS);
        AtomicInteger sent = new AtomicInteger();
        long periodNanos = TimeUnit.SECONDS.toNanos(1) / LATENCY_RATE_PER_SECOND;
        try {
            ticker.scheduleAtFixedRate(() -> {
                if (sent.getAndIncrement() < total) {
                    producers.submit(() -> {
                        system.enqueue(System.nanoTime());
                        return null;
                    });
                }
            }, 0, periodNanos, TimeUnit.NANOSECONDS);

            long deadline = System.nanoTime() + LATENCY_DURATION.toNanos() + DRAIN_TIMEOUT.toNanos();
            while (recorded.get() < total) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException(system.name() + ": latency run did not finish");
                }
                Thread.sleep(PENDING_CHECK.toMillis());
            }
        } finally {
            ticker.shutdownNow();
            producers.shutdownNow();
            system.stopWorkers();
        }

        long[] values = new long[total];
        for (int i = 0; i < total; i++) {
            values[i] = latencies.get(i);
        }
        Arrays.sort(values);
        return String.format(Locale.ROOT,
                "%nLatency (%d jobs/s for %d s, %d worker threads, enqueue call to handler start): "
                        + "p50 %.1f ms, p95 %.1f ms, p99 %.1f ms, max %.1f ms%n",
                LATENCY_RATE_PER_SECOND, LATENCY_DURATION.toSeconds(), LATENCY_THREADS,
                ms(percentile(values, 50)), ms(percentile(values, 95)), ms(percentile(values, 99)),
                ms(values[values.length - 1]));
    }

    // ---------------------------------------------------------------- helpers

    /** The statements that used the most database time during the enqueue + drain phases. */
    private static String topStatements(HikariDataSource ds) throws Exception {
        StringBuilder sb = new StringBuilder(String.format(Locale.ROOT,
                "%nTop %d statements by database time (all enqueue + drain runs):%n%n"
                        + "| Calls | Total ms | Mean ms | Statement |%n|---:|---:|---:|---|%n", TOP_STATEMENTS));
        try (Connection conn = ds.getConnection();
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT calls, total_exec_time, mean_exec_time,"
                                + " left(regexp_replace(query, '\\s+', ' ', 'g'), 90) AS q"
                                + " FROM pg_stat_statements"
                                + " WHERE query NOT ILIKE '%pg_stat_statements%'"
                                + " ORDER BY total_exec_time DESC LIMIT " + TOP_STATEMENTS)) {
            while (rs.next()) {
                sb.append(String.format(Locale.ROOT, "| %d | %.0f | %.3f | `%s` |%n",
                        rs.getLong(1), rs.getDouble(2), rs.getDouble(3), rs.getString(4).replace("|", "\\|")));
            }
        }
        return sb.toString();
    }

    private static void execute(HikariDataSource ds, String sql) throws Exception {
        try (Connection conn = ds.getConnection(); Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    private static HikariDataSource dataSource(PostgreSQLContainer postgres) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(postgres.getJdbcUrl());
        config.setUsername(postgres.getUsername());
        config.setPassword(postgres.getPassword());
        config.setMaximumPoolSize(CONNECTION_POOL_SIZE);
        return new HikariDataSource(config);
    }

    private static String environment() {
        Runtime rt = Runtime.getRuntime();
        return String.format(Locale.ROOT,
                "# Benchmark results%n%nJava %s (%s), %d processors visible to the JVM, max heap %d MiB. "
                        + "PostgreSQL image %s with default settings and fsync=on, in Docker. "
                        + "%d jobs per run, %d repetitions, HikariCP pool size %d for every system.%n",
                System.getProperty("java.version"), System.getProperty("java.vm.name"),
                rt.availableProcessors(), rt.maxMemory() / (1024 * 1024), POSTGRES_IMAGE,
                JOBS, REPS, CONNECTION_POOL_SIZE);
    }

    private static String csv(String system, int rep, String phase, int threads, double rate, Resources.Usage u) {
        return String.format(Locale.ROOT, "%s,%d,%s,%d,%.1f,%s,%s,%s,%s,%s", system, rep, phase, threads, rate,
                u == null ? "" : String.format(Locale.ROOT, "%.2f", u.dbCpuSeconds()),
                u == null ? "" : String.format(Locale.ROOT, "%.0f", u.dbMemoryMib()),
                u == null ? "" : String.format(Locale.ROOT, "%.2f", u.walMib()),
                u == null ? "" : String.format(Locale.ROOT, "%.2f", u.workerCpuSeconds()),
                u == null ? "" : String.format(Locale.ROOT, "%.0f", u.workerPeakHeapMib()));
    }

    /** Nearest-rank percentile of a sorted array. */
    private static long percentile(long[] sorted, int p) {
        int rank = (int) Math.ceil(p / 100.0 * sorted.length);
        return sorted[Math.max(0, Math.min(sorted.length - 1, rank - 1))];
    }

    private static double median(double[] values) {
        double[] copy = values.clone();
        Arrays.sort(copy);
        int mid = copy.length / 2;
        return copy.length % 2 == 1 ? copy[mid] : (copy[mid - 1] + copy[mid]) / 2;
    }

    private static double min(double[] values) {
        return Arrays.stream(values).min().orElse(Double.NaN);
    }

    private static double max(double[] values) {
        return Arrays.stream(values).max().orElse(Double.NaN);
    }

    private static double seconds(long nanos) {
        return nanos / 1_000_000_000.0;
    }

    private static double ms(long nanos) {
        return nanos / 1_000_000.0;
    }
}
