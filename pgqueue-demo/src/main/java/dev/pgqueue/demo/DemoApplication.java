package dev.pgqueue.demo;

/**
 * Demo entry point. Intentionally empty for now.
 *
 * <p>When implemented, connection settings must come from environment variables
 * (for example {@code PGQUEUE_JDBC_URL}, {@code PGQUEUE_DB_USER}, {@code PGQUEUE_DB_PASSWORD}),
 * never from values committed to the repository.
 */
public final class DemoApplication {

    private DemoApplication() {
    }

    public static void main(String[] args) {
        // TODO: wire up pgqueue-core once the queue API exists.
    }
}
