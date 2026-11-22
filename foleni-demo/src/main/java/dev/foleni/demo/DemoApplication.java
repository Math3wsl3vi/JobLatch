package dev.foleni.demo;

/**
 * Demo entry point. Intentionally empty for now.
 *
 * <p>When implemented, connection settings must come from environment variables
 * (for example {@code FOLENI_JDBC_URL}, {@code FOLENI_DB_USER}, {@code FOLENI_DB_PASSWORD}),
 * never from values committed to the repository.
 */
public final class DemoApplication {

    private DemoApplication() {
    }

    public static void main(String[] args) {
        // TODO: wire up foleni-core once the queue API exists.
    }
}
