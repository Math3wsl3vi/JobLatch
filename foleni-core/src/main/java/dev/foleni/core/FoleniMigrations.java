package dev.foleni.core;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates and upgrades Foleni's tables.
 *
 * <p>Run this once at deploy time with a database role that is allowed to create tables. The
 * application itself should then run with a role that can only read and write rows (least
 * privilege). It cannot drop or alter tables, even if it is compromised.
 *
 * <p>It is safe to call repeatedly, and safe to call from several processes at once.
 */
public final class FoleniMigrations {

    private static final Logger log = LoggerFactory.getLogger(FoleniMigrations.class);

    /** One versioned SQL file shipped inside the jar. */
    private record Migration(int version, String description, String resource) {}

    /**
     * Every migration, in order. New ones are only ever appended. A migration that has shipped is
     * never edited, because databases out there have already run the old version.
     */
    private static final List<Migration> MIGRATIONS = List.of(
            new Migration(1, "create jobs table", "/db/foleni/V1__create_jobs.sql"),
            new Migration(2, "failure metadata", "/db/foleni/V2__failure_metadata.sql"),
            new Migration(3, "notify and schedule", "/db/foleni/V3__notify_and_schedule.sql"));

    /**
     * An arbitrary but fixed number used as the name of a Postgres "advisory lock". It just has to
     * be the same in every process and unlikely to clash with locks the application uses.
     */
    private static final long MIGRATION_LOCK_KEY = 0x466F6C656E69L; // the letters "Foleni" in hex

    private FoleniMigrations() {} // static methods only, no instances

    /**
     * Applies every migration that has not been applied yet, all in one transaction.
     *
     * @throws SQLException if any migration fails. Nothing is applied in that case.
     */
    public static void migrate(DataSource dataSource) throws SQLException {
        Objects.requireNonNull(dataSource, "dataSource");

        // We opened this connection, so we own its transaction (same rule as JobQueue).
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                // Two servers starting at once must not both create the table. An advisory lock
                // is a named lock with no table attached. The "xact" variant is released
                // automatically at commit or rollback, so it can never be leaked.
                // The second process waits here, then sees version 1 as already applied.
                try (PreparedStatement lock = conn.prepareStatement("SELECT pg_advisory_xact_lock(?)")) {
                    lock.setLong(1, MIGRATION_LOCK_KEY);
                    lock.execute();
                }

                try (Statement st = conn.createStatement()) {
                    st.execute("""
                            CREATE TABLE IF NOT EXISTS foleni_schema_history (
                                version     integer     PRIMARY KEY,
                                description text        NOT NULL,
                                applied_at  timestamptz NOT NULL DEFAULT now()
                            )
                            """);
                }

                Set<Integer> applied = appliedVersions(conn);
                for (Migration m : MIGRATIONS) {
                    if (applied.contains(m.version())) {
                        continue;
                    }
                    log.info("Applying Foleni migration V{} ({})", m.version(), m.description());
                    try (Statement st = conn.createStatement()) {
                        // A plain Statement is fine here: the SQL comes from our own jar file,
                        // never from user input. User input always goes through PreparedStatement.
                        st.execute(readResource(m.resource()));
                    }
                    try (PreparedStatement record = conn.prepareStatement(
                            "INSERT INTO foleni_schema_history (version, description) VALUES (?, ?)")) {
                        record.setInt(1, m.version());
                        record.setString(2, m.description());
                        record.executeUpdate();
                    }
                }

                // Postgres can run CREATE TABLE inside a transaction (many databases cannot).
                // So either every pending migration lands, or none do.
                conn.commit();
            } catch (SQLException | RuntimeException e) {
                try {
                    conn.rollback();
                } catch (SQLException rollbackFailure) {
                    e.addSuppressed(rollbackFailure);
                }
                throw e;
            }
        }
    }

    private static Set<Integer> appliedVersions(Connection conn) throws SQLException {
        Set<Integer> versions = new HashSet<>();
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT version FROM foleni_schema_history")) {
            while (rs.next()) {
                versions.add(rs.getInt(1));
            }
        }
        return versions;
    }

    private static String readResource(String path) {
        try (InputStream in = FoleniMigrations.class.getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("Migration file missing from the jar: " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not read migration " + path, e);
        }
    }
}