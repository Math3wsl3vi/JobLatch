package dev.foleni.bench;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import javax.sql.DataSource;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Measures CPU and memory for one benchmark phase: the database container and this JVM.
 *
 * <p>Database numbers come from the container's own cgroup files (Linux's per-container
 * accounting), so they are exact totals, not samples:
 *
 * <ul>
 *   <li>{@code cpu.stat} → usage_usec: CPU time used by everything in the container
 *   <li>{@code memory.current}: memory charged to the container right now, including Postgres's
 *       shared buffers and file cache
 * </ul>
 *
 * <p>WAL bytes: how much the database wrote to its write-ahead log during the phase, read from
 * {@code pg_current_wal_lsn()}. Every committed change goes through the WAL first, so this is
 * the most direct measure of how much each system writes per job.
 *
 * <p>Worker numbers are this JVM's process CPU time and its peak heap during the phase. They
 * include the benchmark harness itself (producer threads, the "pending" query), which is small but
 * not zero.
 */
final class Resources {

    private static final double MICROS_PER_SECOND = 1_000_000.0;
    private static final double NANOS_PER_SECOND = 1_000_000_000.0;
    private static final double BYTES_PER_MIB = 1024.0 * 1024.0;

    private final PostgreSQLContainer postgres;
    private final com.sun.management.OperatingSystemMXBean os =
            (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    private final DataSource dataSource;
    private String walStart;
    private long dbCpuStartMicros;
    private long jvmCpuStartNanos;

    /** What one phase used. -1 where the value could not be read. */
    record Usage(double dbCpuSeconds, double dbMemoryMib, double walMib, double workerCpuSeconds,
            double workerPeakHeapMib) {}

    Resources(PostgreSQLContainer postgres, DataSource dataSource) {
        this.postgres = postgres;
        this.dataSource = dataSource;
    }

    void start() throws Exception {
        walStart = currentWalLsn();
        dbCpuStartMicros = dbCpuMicros();
        jvmCpuStartNanos = os.getProcessCpuTime();
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == MemoryType.HEAP) {
                pool.resetPeakUsage();
            }
        }
    }

    Usage stop() throws Exception {
        double walMib = walBytesSince(walStart) / BYTES_PER_MIB;
        long dbEnd = dbCpuMicros();
        double dbCpu = dbEnd < 0 || dbCpuStartMicros < 0 ? -1 : (dbEnd - dbCpuStartMicros) / MICROS_PER_SECOND;
        double jvmCpu = (os.getProcessCpuTime() - jvmCpuStartNanos) / NANOS_PER_SECOND;
        long peakHeap = 0;
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == MemoryType.HEAP && pool.getPeakUsage() != null) {
                peakHeap += pool.getPeakUsage().getUsed();
            }
        }
        long dbMem = readCgroupNumber("/sys/fs/cgroup/memory.current", null);
        return new Usage(dbCpu, dbMem < 0 ? -1 : dbMem / BYTES_PER_MIB, walMib, jvmCpu,
                peakHeap / BYTES_PER_MIB);
    }

    private String currentWalLsn() throws Exception {
        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT pg_current_wal_lsn()::text")) {
            rs.next();
            return rs.getString(1);
        }
    }

    private long walBytesSince(String startLsn) throws Exception {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), ?::pg_lsn)::bigint")) {
            ps.setString(1, startLsn);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private long dbCpuMicros() {
        return readCgroupNumber("/sys/fs/cgroup/cpu.stat", "usage_usec");
    }

    /**
     * Reads a number from a cgroup v2 file inside the container. With {@code key}, the file has
     * "key value" lines. Returns -1 if unavailable (for example on a cgroup v1 host).
     */
    private long readCgroupNumber(String file, String key) {
        try {
            ExecResult result = postgres.execInContainer("cat", file);
            if (result.getExitCode() != 0) {
                return -1;
            }
            for (String line : result.getStdout().split("\n")) {
                String[] parts = line.trim().split("\\s+");
                if (key == null && parts.length == 1) {
                    return Long.parseLong(parts[0]);
                }
                if (key != null && parts.length == 2 && parts[0].equals(key)) {
                    return Long.parseLong(parts[1]);
                }
            }
            return -1;
        } catch (Exception e) {
            return -1;
        }
    }
}
