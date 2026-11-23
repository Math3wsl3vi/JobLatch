package dev.joblatch.core;

/**
 * What one reaper pass did with expired leases.
 *
 * @param requeued jobs put back to 'available' because their worker stopped heartbeating
 * @param dead     jobs moved to 'dead' because the expired attempt was their last one
 */
public record ReclaimResult(int requeued, int dead) {

    /** Total number of jobs taken back from workers. */
    public int total() {
        return requeued + dead;
    }
}