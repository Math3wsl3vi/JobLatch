# Security guide

This guide covers database privileges, credentials and sensitive data. It describes what Foleni
does; it is not a compliance assessment. If your jobs carry regulated data (health, payment or
personal data under GDPR), involve your security and compliance teams in how you use it.

## Least-privilege database roles

Three roles, each with only what it needs. The scripts are in [`docs/sql/`](sql/) and are applied
exactly as written below by `RolesAndPrivilegesTest`, which also proves each role is limited.

| Role | Logs in? | Can | Cannot |
|---|---|---|---|
| `foleni_owner` | yes (deploy time only) | create and alter Foleni's tables; run migrations | — (keep its credentials out of the running application) |
| `foleni_runtime` | no: granted to your app's login role | `SELECT`, `INSERT`, `UPDATE` on `foleni_jobs`; LISTEN | `DELETE`, `TRUNCATE`, any DDL, read `foleni_schema_history`, run migrations |
| `foleni_maintenance` | no: granted to a retention job's login role | `SELECT`, `DELETE` on `foleni_jobs` | `INSERT`, `UPDATE`, any DDL |

### Setting them up

```bash
# 1. As a superuser, once per database. The password comes from the environment.
psql -v ON_ERROR_STOP=1 -v owner_password="$FOLENI_OWNER_PASSWORD" -d mydb -f docs/sql/01-create-roles.sql

# 2. Deploy step: migrations as the owner (in Java)
#    FoleniMigrations.migrate(ownerDataSource);

# 3. As foleni_owner, after migrations
psql -v ON_ERROR_STOP=1 -U foleni_owner -d mydb -f docs/sql/02-grant-privileges.sql
```

```sql
-- 4. Your application's login role (the one that also writes your business tables)
GRANT foleni_runtime TO my_app;
-- A separate login role for the retention job
CREATE ROLE retention_job LOGIN PASSWORD '...' IN ROLE foleni_maintenance;
```

Transactional enqueue needs **one** connection that can write both your tables and `foleni_jobs`,
which is why `foleni_runtime` is a group role granted to your application's login role rather than a
separate login.

Notes:

- The NOTIFY trigger runs with the privileges of the role that changed the row. It only calls
  `pg_notify`, which needs no privilege. If your database revokes `EXECUTE` on functions from
  `PUBLIC`, grant `EXECUTE ON FUNCTION foleni_notify_available()` to `foleni_runtime`.
- Future migrations that add tables will say so in the changelog; re-run step 3 after them.

## Credentials

- Pass database credentials through environment variables or a secret manager, never in code,
  configuration committed to git, or command-line arguments (other users on the machine can read
  those with `ps`). The demo and the crash-test worker read `FOLENI_DB_URL`, `FOLENI_DB_USER` and
  `FOLENI_DB_PASSWORD`.
- `.env` is git-ignored; only `.env.example` (placeholder values) is committed.
- Foleni never logs connection URLs, user names or passwords.
- Use TLS between application and database outside a single trusted host: add `sslmode=verify-full`
  (and the CA certificate) to the JDBC URL.

## Sensitive data in jobs

Payloads and error messages are stored in plain text in `foleni_jobs`:

| Data | Stored in | Logged by Foleni? | Guidance |
|---|---|---|---|
| Payload | `payload` (jsonb) | **never** | Put **identifiers** in payloads, not personal data: `{"orderId": 42}`, not the customer's address. The handler loads what it needs. |
| Error text | `last_error` | **never** (only the exception class) | Comes from your exceptions' messages. Keep secrets and personal data out of them. |
| Queue, job type, worker ID, job ID | columns | yes | Restricted to `A-Z a-z 0-9 _ . : -` and 128 characters, so they are safe in logs. |

`NewJob`, `ClaimedJob` and `DeadJob` override `toString()` to leave the payload (and, for `DeadJob`,
the error) out, so logging one of them by accident does not leak it.

### Errors from the database

`JobQueue.enqueue` replaces the database's error text with its own message (queue, type, SQLState),
because Postgres error details can quote the failed row, payload included. Other operations pass the
driver's `SQLException` through unchanged. If your application logs SQL exceptions, set the JDBC
option `logServerErrorDetail=false` so the driver leaves row data out of them:

```
jdbc:postgresql://db:5432/mydb?logServerErrorDetail=false
```

### Retention and deletion

Finished jobs (and their payloads) stay in the table until deleted. Run the retention job
([operations.md](operations.md#retention)) with a period that matches your data-retention policy.
Remember that deleted rows still exist in backups and WAL archives until those expire.

## Input validation

- Names (`queue`, `jobType`, worker IDs) are validated in Java and again by database `CHECK`
  constraints.
- Payloads must be valid JSON (checked by Postgres) and at most 1 MiB.
- In `foleni-core`, every value reaches SQL as a `PreparedStatement` parameter. The only SQL not
  sent that way is fixed text: `LISTEN foleni_jobs` and the migration files shipped in the jar.

## Reporting a vulnerability

Please do not open a public issue. Use GitHub's private vulnerability reporting on the repository
(Security → Report a vulnerability). Maintainers: it must be enabled under Settings → Code security
(see the release checklist).
