-- =============================================================================
-- V3: wake-up notifications and the "next due job" index
-- =============================================================================

-- Sends a notification on channel 'foleni_jobs' whose payload is the queue name.
--
-- pg_notify is transactional: the notification is delivered only when the
-- transaction commits, and dropped if it rolls back. Identical notifications in
-- one transaction are merged, so a bulk insert of 10,000 jobs into one queue
-- sends a single wake-up.
--
-- RETURN NULL: for an AFTER trigger the return value is ignored.
CREATE OR REPLACE FUNCTION foleni_notify_available() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('foleni_jobs', NEW.queue);
    RETURN NULL;
END
$$;

-- New jobs.
CREATE TRIGGER foleni_jobs_notify_insert
    AFTER INSERT ON foleni_jobs
    FOR EACH ROW
    WHEN (NEW.state = 'available')
    EXECUTE FUNCTION foleni_notify_available();

-- Jobs that become available again: retry after fail(), reaper, requeue().
-- "UPDATE OF state" plus the WHEN clause keeps heartbeats and other updates
-- from firing it.
CREATE TRIGGER foleni_jobs_notify_update
    AFTER UPDATE OF state ON foleni_jobs
    FOR EACH ROW
    WHEN (NEW.state = 'available' AND OLD.state <> 'available')
    EXECUTE FUNCTION foleni_notify_available();

-- An idle worker asks "when is the next job in my queue due?":
--   SELECT min(run_at) ... WHERE queue = ? AND state = 'available'
-- With this index that is one step down a B-tree, even with millions of jobs
-- scheduled for the future.
CREATE INDEX IF NOT EXISTS foleni_jobs_next_due_idx
    ON foleni_jobs (queue, run_at)
    WHERE state = 'available';
