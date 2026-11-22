-- =============================================================================
-- V2: failure metadata and the dead-letter index
-- =============================================================================
-- Never edit V1: it has already run on existing databases. Changes go in new
-- files, applied in order by FoleniMigrations.
-- =============================================================================

-- When the most recent failure happened (handler failure or expired lease).
-- Adding a nullable column with no default is instant in Postgres, even on a
-- huge table: only the table's description changes, no rows are rewritten.
ALTER TABLE foleni_jobs
    ADD COLUMN last_failed_at timestamptz;

-- Operators list dead jobs per queue, newest first. Partial index again: only
-- dead rows are in it, so it stays small.
--
-- Operational note: CREATE INDEX blocks writes to the table while it builds.
-- On a very large existing table, build it beforehand by hand with
-- CREATE INDEX CONCURRENTLY (which cannot run inside a transaction, so it
-- cannot live in this file) using the same name. IF NOT EXISTS then skips it.
CREATE INDEX IF NOT EXISTS foleni_jobs_dead_idx
    ON foleni_jobs (queue, finished_at DESC)
    WHERE state = 'dead';
