-- =============================================================================
-- Foleni: least-privilege roles. Step 1 of 2.
-- =============================================================================
-- Run ONCE per database, BEFORE the first migration, as a superuser (or a role
-- with CREATEROLE that owns the database):
--
--   psql -v ON_ERROR_STOP=1 -v owner_password="$FOLENI_OWNER_PASSWORD" \
--        -d <database> -f 01-create-roles.sql
--
-- The password comes from an environment variable, never from this file.
-- Verified by RolesAndPrivilegesTest in foleni-core.
-- =============================================================================

-- Owns Foleni's tables. Used ONLY to run FoleniMigrations at deploy time.
-- Never give its credentials to a running application.
CREATE ROLE foleni_owner LOGIN PASSWORD :'owner_password';

-- PostgreSQL 15 and later no longer let every role create objects in the public
-- schema. The owner needs that to create Foleni's tables.
GRANT USAGE, CREATE ON SCHEMA public TO foleni_owner;

-- What a running application needs, as a group role (NOLOGIN: nobody logs in
-- as it). Grant it to the application's own login role, the one that also
-- writes the business tables, so transactional enqueue works:
--
--   GRANT foleni_runtime TO my_app;
CREATE ROLE foleni_runtime NOLOGIN;
GRANT USAGE ON SCHEMA public TO foleni_runtime;

-- For a scheduled retention job that deletes old finished jobs. Also a group role.
CREATE ROLE foleni_maintenance NOLOGIN;
GRANT USAGE ON SCHEMA public TO foleni_maintenance;
