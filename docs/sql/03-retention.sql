-- =============================================================================
-- Foleni: delete old finished jobs, in small batches.
-- =============================================================================
-- Run as a role holding foleni_maintenance, on a schedule (e.g. hourly cron).
-- Repeat until it deletes 0 rows. Small batches keep each transaction short, so
-- workers are never blocked behind a long DELETE.
--
-- Keeps dead jobs: they are deleted only after someone has looked at them, by
-- changing 'succeeded' to 'dead' below deliberately.
--
-- Verified by RolesAndPrivilegesTest in foleni-core.
-- =============================================================================
DELETE FROM foleni_jobs
WHERE id IN (
    SELECT id
    FROM foleni_jobs
    WHERE state = 'succeeded'
      AND finished_at < now() - interval '7 days'
    LIMIT 1000
);
