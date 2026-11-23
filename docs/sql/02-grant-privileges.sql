-- =============================================================================
-- JobLatch: least-privilege roles. Step 2 of 2.
-- =============================================================================
-- Run as joblatch_owner AFTER the migrations, and again after any JobLatch upgrade
-- whose release notes say it adds a table:
--
--   psql -v ON_ERROR_STOP=1 -U joblatch_owner -d <database> -f 02-grant-privileges.sql
--
-- Verified by RolesAndPrivilegesTest in joblatch-core.
-- =============================================================================

-- Enqueue (INSERT), claim / complete / fail / heartbeat / reap / requeue
-- (UPDATE), and every read (SELECT). No DELETE, no TRUNCATE, no DDL.
GRANT SELECT, INSERT, UPDATE ON joblatch_jobs TO joblatch_runtime;

-- Retention only: find and delete old finished jobs. Cannot create or change jobs.
GRANT SELECT, DELETE ON joblatch_jobs TO joblatch_maintenance;

-- joblatch_schema_history gets no grants: only the owner reads or writes it.
