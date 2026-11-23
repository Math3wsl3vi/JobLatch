package dev.joblatch.bench;

import java.util.function.LongConsumer;

/**
 * One job system under test, reduced to the operations every benchmark phase needs. Each
 * implementation uses only the library's public API, configured as its documentation recommends
 * for PostgreSQL.
 *
 * <p>Every job carries one number: the {@code System.nanoTime()} taken just before it was
 * enqueued. The handler passes it to {@code onRun}, which is how latency is measured.
 */
interface QueueSystem extends AutoCloseable {

    /** Short name used in result tables. */
    String name();

    /** The settings that matter for fairness, printed with the results. */
    String settings();

    /** Creates the library's tables in an empty database. */
    void createSchema() throws Exception;

    /** Enqueues one job, in its own transaction. Called from many threads at once. */
    void enqueue(long enqueuedAtNanos) throws Exception;

    /** Starts processing with {@code threads} worker threads. */
    void startWorkers(int threads, LongConsumer onRun) throws Exception;

    /** Stops processing and waits for running jobs. */
    void stopWorkers() throws Exception;

    /** Jobs that have not finished yet, according to the database. */
    long pending() throws Exception;

    /**
     * Whether any job is unfinished. Called every few milliseconds while a drain is timed, so it
     * must be cheap and equally cheap for every system: an EXISTS answered from an index, not a
     * count over the whole table.
     */
    boolean anyPending() throws Exception;

    /** Removes all jobs, so the next repetition starts from an empty table. */
    void clear() throws Exception;

    /**
     * Stops workers if running. Narrows AutoCloseable's {@code close() throws Exception}: no
     * implementation throws, and callers should not have to handle what cannot happen.
     */
    @Override
    void close();
}
