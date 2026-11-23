# Database schema

Foleni creates two tables, one trigger function, two triggers, and five indexes on `foleni_jobs`. The migrations that
create them ship inside the `foleni-core` jar under `db/foleni/` and are applied by
`FoleniMigrations.migrate(DataSource)`.

| Migration | Adds |
|---|---|
| `V1__create_jobs.sql` | `foleni_jobs`, its constraints, the claim index and the lease index |
| `V2__failure_metadata.sql` | `last_failed_at` column, dead-letter index |
| `V3__notify_and_schedule.sql` | NOTIFY trigger function and triggers, next-due index |

Rules for migrations (see also [release.md](release.md)): a migration that has shipped is **never
edited**; changes go in a new file with the next number; each migration must be safe to apply to a
live table.

## `foleni_jobs`

One row per job, for its whole life.

| Column | Type | Null | Default | Meaning |
|---|---|---|---|---|
| `id` | `uuid` | no | `uuidv7()` | Primary key. UUIDv7 starts with a timestamp, so new keys go to the end of the index. Safe to show in logs. |
| `queue` | `text` | no | | Which queue the job belongs to. |
| `job_type` | `text` | no | | Selects the handler. |
| `payload` | `jsonb` | no | | The job's input. Validated as JSON by Postgres. **May contain personal data: never log it.** |
| `state` | `text` | no | `'available'` | `available`, `running`, `succeeded` or `dead`. |
| `priority` | `integer` | no | `0` | Higher is claimed first. |
| `attempts` | `integer` | no | `0` | Claims so far. Also the fencing token. |
| `max_attempts` | `integer` | no | `5` | Attempt limit. |
| `run_at` | `timestamptz` | no | `now()` | Not claimed before this time (database clock). Pushed forward by retries. |
| `locked_by` | `text` | yes | | Owning worker. Set if and only if `running`. |
| `locked_until` | `timestamptz` | yes | | Lease expiry. Set if and only if `running`. |
| `last_error` | `text` | yes | | Most recent failure, up to 2,000 characters. Written by the application's exception messages. |
| `last_failed_at` | `timestamptz` | yes | | When the most recent failure happened (V2). |
| `created_at` | `timestamptz` | no | `now()` | |
| `updated_at` | `timestamptz` | no | `now()` | Last state change, heartbeat or retry. |
| `finished_at` | `timestamptz` | yes | | Set if and only if `succeeded` or `dead`. |

### Constraints

| Name | Rule | Why |
|---|---|---|
| `foleni_jobs_queue_format` | `queue ~ '^[A-Za-z0-9_.:-]{1,128}$'` | Names are safe in logs and metric labels, even when written by raw SQL. |
| `foleni_jobs_job_type_format` | same for `job_type` | |
| `foleni_jobs_state_valid` | `state IN (...)` | `text` + `CHECK` instead of an `ENUM`: a `CHECK` can be replaced in a migration; enum values cannot be removed. |
| `foleni_jobs_attempts_valid` | `attempts >= 0 AND max_attempts >= 1 AND attempts <= max_attempts` | |
| `foleni_jobs_lease_matches_state` | owner and lease set exactly when `running` | No unowned running job (never recovered), no owned available job (two owners). |
| `foleni_jobs_finished_matches_state` | `finished_at` set exactly when `succeeded` or `dead` | |

**Performance note:** Postgres evaluates every `CHECK` on every `INSERT` and `UPDATE`. The two regex
checks measured at about 45% of an insert's execution time ([benchmarks.md](benchmarks.md)).
Replacing them with length checks is a possible future change; Java validates the same rule first.

### Indexes

All four secondary indexes are **partial**: they contain only the rows their query needs, so they
stay small no matter how many finished jobs the table keeps.

| Index | Columns | Rows | Serves |
|---|---|---|---|
| `foleni_jobs_pkey` | `id` | all | lookups by ID: complete, fail, heartbeat |
| `foleni_jobs_claim_idx` | `queue, priority DESC, run_at` | `state = 'available'` | the claim query, in claim order |
| `foleni_jobs_lease_expiry_idx` | `locked_until` | `state = 'running'` | the reaper |
| `foleni_jobs_dead_idx` | `queue, finished_at DESC` | `state = 'dead'` | `DeadLetterQueue.list` |
| `foleni_jobs_next_due_idx` | `queue, run_at` | `state = 'available'` | "when is the next job due?" for idle pools |

There is no index for `succeeded` rows by age. The retention query (`docs/sql/03-retention.sql`)
scans for them; on a very large table, add one, for example
`CREATE INDEX CONCURRENTLY ... ON foleni_jobs (finished_at) WHERE state = 'succeeded'`.

### Trigger

`foleni_notify_available()` calls `pg_notify('foleni_jobs', NEW.queue)`. It fires:

- `AFTER INSERT` for rows inserted as `available`
- `AFTER UPDATE OF state` when a row becomes `available` from another state (retry, reaper,
  requeue). Heartbeats and other updates do not fire it.

Notifications are sent only on commit and merged per transaction, so a bulk insert of 10,000 jobs
into one queue sends one notification. Disable with `ALTER TABLE foleni_jobs DISABLE TRIGGER USER`
if you run without notifications (see [configuration.md](configuration.md)).

## `foleni_schema_history`

| Column | Type | Meaning |
|---|---|---|
| `version` | `integer` PK | Migration number applied |
| `description` | `text` | |
| `applied_at` | `timestamptz` | |

`FoleniMigrations` takes a Postgres advisory lock before reading it, so several processes can start
at once and only one applies each migration. All pending migrations run in one transaction.

## Which columns may your SQL rely on?

Operators query `foleni_jobs` directly (see `docs/sql/monitoring.sql`). Within a major version,
Foleni will not remove or rename these columns or change their meaning: `id`, `queue`, `job_type`,
`state` and its four values, `priority`, `attempts`, `max_attempts`, `run_at`, `locked_until`,
`last_error`, `last_failed_at`, `created_at`, `finished_at`. Writing to the table directly, other than
the documented retention `DELETE` and the requeue done through `DeadLetterQueue`, is not supported.
