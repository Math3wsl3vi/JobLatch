-- =============================================================================
-- V1: the jobs table
-- =============================================================================
-- One row = one job. The row moves through states over its life:
--
--   available --(worker claims it)--> running --(handler succeeds)--> succeeded
--       ^                                |
--       |                                +--(handler fails, attempts left)--> available (run_at pushed into the future)
--       |                                |
--       +--(lease expires, worker died)--+--(handler fails, no attempts left)--> dead
--
-- Milestone 1 only ever INSERTs rows in state 'available'. The other states and
-- the lease columns are defined now so the table's rules are complete from day
-- one, and later milestones only add behaviour, not new columns.
--
-- Note: avoid semicolons inside comments in this file. The migration runner
-- sends the whole file to Postgres in one go, and the JDBC driver splits
-- statements on semicolons.
-- =============================================================================

CREATE TABLE foleni_jobs (
    -- uuidv7() is built into Postgres 18. A v7 UUID starts with a timestamp, so
    -- new IDs are always "bigger" than old ones. That keeps inserts into the
    -- primary-key index at the right-hand edge (fast), unlike random v4 UUIDs.
    -- IDs are also unguessable and safe to show in logs.
    id              uuid        PRIMARY KEY DEFAULT uuidv7(),

    -- Which queue the job belongs to (e.g. 'emails'). Workers pull from queues.
    queue           text        NOT NULL,

    -- What kind of job this is (e.g. 'send-receipt'). Used to pick the handler.
    job_type        text        NOT NULL,

    -- The job's input data. jsonb means Postgres checks it is valid JSON and
    -- stores it in a parsed binary form. NEVER log this column: it can hold
    -- personal data.
    payload         jsonb       NOT NULL,

    -- Lifecycle state. Stored as text plus a CHECK constraint (below) instead of
    -- a Postgres ENUM type, because enum values cannot be removed later and are
    -- awkward to change in migrations. A CHECK can simply be replaced.
    state           text        NOT NULL DEFAULT 'available',

    -- Higher number = claimed first. Ties are broken by run_at.
    priority        integer     NOT NULL DEFAULT 0,

    -- How many times a worker has started this job, and the limit.
    attempts        integer     NOT NULL DEFAULT 0,
    max_attempts    integer     NOT NULL DEFAULT 5,

    -- The job must not start before this time. Defaults to "now" (the
    -- database's clock, not the application's).
    -- timestamptz stores an absolute instant, so time zones can never confuse it.
    run_at          timestamptz NOT NULL DEFAULT now(),

    -- Lease columns (used from Milestone 2/4). While a worker owns a job:
    --   locked_by    = which worker owns it
    --   locked_until = when that ownership expires unless renewed by a heartbeat
    locked_by       text,
    locked_until    timestamptz,

    -- Short, sanitised description of the most recent failure (Milestone 5).
    last_error      text,

    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),

    -- Set when the job reaches a final state (succeeded or dead).
    finished_at     timestamptz,

    -- -------------------------------------------------------------------------
    -- Constraints: the database itself refuses impossible rows. Java validates
    -- first and gives nicer errors, but these are the last line of defence
    -- against bugs, manual SQL, and future code we have not written yet.
    -- -------------------------------------------------------------------------

    -- Names are 1-128 chars of a safe alphabet. This keeps them safe to put in
    -- logs and metric labels.
    CONSTRAINT foleni_jobs_queue_format
        CHECK (queue ~ '^[A-Za-z0-9_.:-]{1,128}$'),
    CONSTRAINT foleni_jobs_job_type_format
        CHECK (job_type ~ '^[A-Za-z0-9_.:-]{1,128}$'),

    CONSTRAINT foleni_jobs_state_valid
        CHECK (state IN ('available', 'running', 'succeeded', 'dead')),

    CONSTRAINT foleni_jobs_attempts_valid
        CHECK (attempts >= 0 AND max_attempts >= 1 AND attempts <= max_attempts),

    -- A job has an owner if and only if it is running.
    -- Stops "running with no owner" (would never be recovered) and
    -- "available but still owned" (two workers could think they own it).
    CONSTRAINT foleni_jobs_lease_matches_state
        CHECK (
            (state = 'running'  AND locked_by IS NOT NULL AND locked_until IS NOT NULL)
         OR (state <> 'running' AND locked_by IS NULL     AND locked_until IS NULL)
        ),

    -- finished_at is set if and only if the job is in a final state.
    CONSTRAINT foleni_jobs_finished_matches_state
        CHECK ((state IN ('succeeded', 'dead')) = (finished_at IS NOT NULL))
);

-- -----------------------------------------------------------------------------
-- Indexes
-- -----------------------------------------------------------------------------

-- The claim index (used from Milestone 2). Workers will ask:
--   "give me the highest-priority, oldest due job in queue X that is available"
-- It is a PARTIAL index (the WHERE clause): only 'available' rows are in it.
-- Finished jobs pile up over time, but this index stays small because they
-- are not in it.
CREATE INDEX foleni_jobs_claim_idx
    ON foleni_jobs (queue, priority DESC, run_at)
    WHERE state = 'available';

-- The reaper index (used from Milestone 4). The reaper asks:
--   "which running jobs have a lease that has already expired?"
CREATE INDEX foleni_jobs_lease_expiry_idx
    ON foleni_jobs (locked_until)
    WHERE state = 'running';