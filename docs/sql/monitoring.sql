-- =============================================================================
-- JobLatch: monitoring queries. Each is read-only and safe to run at any time.
-- Verified by RolesAndPrivilegesTest in joblatch-core (run as joblatch_runtime).
-- =============================================================================

-- 1. Jobs per queue and state. Alert if 'available' keeps growing (workers
--    cannot keep up) or 'dead' grows (jobs need a person).
SELECT queue, state, count(*) AS jobs
FROM joblatch_jobs
GROUP BY queue, state
ORDER BY queue, state;

-- 2. How late is the oldest job that is due but not yet claimed? This is the
--    best single health signal: it grows when workers are down or overloaded.
SELECT queue, max(now() - run_at) AS oldest_due_wait
FROM joblatch_jobs
WHERE state = 'available' AND run_at <= now()
GROUP BY queue;

-- 3. Running jobs whose lease has already expired. Normally 0 or close to it:
--    the reaper takes them back within its interval. A growing number means no
--    reaper is running.
SELECT queue, count(*) AS expired_leases, min(locked_until) AS expired_since
FROM joblatch_jobs
WHERE state = 'running' AND locked_until < now()
GROUP BY queue;

-- 4. The most recent dead jobs, with the reason. Payloads are left out on
--    purpose: they can contain personal data.
SELECT id, queue, job_type, attempts, finished_at, left(last_error, 200) AS last_error
FROM joblatch_jobs
WHERE state = 'dead'
ORDER BY finished_at DESC
LIMIT 20;

-- 5. Jobs retried the most: candidates for a bug or a broken dependency.
SELECT queue, job_type, count(*) AS jobs, max(attempts) AS max_attempts_used
FROM joblatch_jobs
WHERE attempts > 1
GROUP BY queue, job_type
ORDER BY jobs DESC
LIMIT 20;
