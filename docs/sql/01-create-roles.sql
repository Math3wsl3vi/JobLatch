-- =============================================================================
-- JobLatch: least-privilege roles. Step 1 of 2.
-- =============================================================================
-- Run ONCE per database, BEFORE the first migration, as a superuser (or a role
-- with CREATEROLE that owns the database):
--
--   psql -v ON_ERROR_STOP=1 -v owner_password="$JOBLATCH_OWNER_PASSWORD" \
--        -d <database> -f 01-create-roles.sql
--
-- The password comes from an environment variable, never from this file.
-- Verified by RolesAndPrivilegesTest in joblatch-core.
-- =============================================================================

-- Owns JobLatch's tables. Used ONLY to run JobLatchMigrations at deploy time.
-- Never give its credentials to a running application.
CREATE ROLE joblatch_owner LOGIN PASSWORD :'owner_password';

-- PostgreSQL 15 and later no longer let every role create objects in the public
-- schema. The owner needs that to create JobLatch's tables.
GRANT USAGE, CREATE ON SCHEMA public TO joblatch_owner;

-- What a running application needs, as a group role (NOLOGIN: nobody logs in
-- as it). Grant it to the application's own login role, the one that also
-- writes the business tables, so transactional enqueue works:
--
--   GRANT joblatch_runtime TO my_app;
CREATE ROLE joblatch_runtime NOLOGIN;
GRANT USAGE ON SCHEMA public TO joblatch_runtime;

-- For a scheduled retention job that deletes old finished jobs. Also a group role.
CREATE ROLE joblatch_maintenance NOLOGIN;
GRANT USAGE ON SCHEMA public TO joblatch_maintenance;
