# Operations guide

Running JobLatch in production: deploying, sizing, monitoring and maintenance. For incidents see
[failure-recovery.md](failure-recovery.md); for roles and data handling see [security.md](security.md).

## Requirements

- PostgreSQL 18 or later (the schema uses the built-in `uuidv7()`).
- Java 21 or later.
- A connection pool such as HikariCP. JobLatch opens and returns connections constantly; without a
  pool, every claim and completion pays for a new TCP connection and login.
- For LISTEN/NOTIFY: direct connections or PgBouncer in **session** mode. In transaction mode, turn
  notifications off ([configuration.md](configuration.md#turning-notifications-off)).

## Deploying

1. **Roles, once per database.** Apply `docs/sql/01-create-roles.sql` ([security.md](security.md)).
2. **Migrations, every deploy, before the application starts.** Run `JobLatchMigrations.migrate` with
   the `joblatch_owner` credentials (a deploy step, init container or one-off job). It is idempotent and
   safe to run from several places at once.
3. **Grants, after migrations that add tables.** Apply `docs/sql/02-grant-privileges.sql` as
   `joblatch_owner`. Release notes say when this is needed.
4. **Application**, with a login role that holds `joblatch_runtime`. In every worker process start one
   `WorkerPool` per queue **and one `LeaseReaper`**, and close them on shutdown.

### Shutdown and Kubernetes

Call `pool.close()` and `reaper.close()` from a shutdown hook or your framework's lifecycle callback.
On SIGTERM the pool stops claiming, waits up to `shutdownTimeout` for running jobs, interrupts the
rest, and waits a further 5 s for them to record their outcome.

Kubernetes sends SIGTERM, waits `terminationGracePeriodSeconds` (default 30), then sends SIGKILL. Set
it **above** `shutdownTimeout + 5 s + your other shutdown work`. With the default 30 s
`shutdownTimeout`, use at least 45 s. If SIGKILL arrives first nothing is lost: the jobs that were
running are recovered after their leases expire, and may run again.

### Rolling deploys

New and old versions can run side by side. A job whose type the old version does not know is
failed with "no handler registered" and retried with backoff, so the new version picks it up. If
the rollout takes longer than a job's retries last, give such jobs a higher `maxAttempts`.

## Sizing

| Resource | Rule of thumb |
|---|---|
| Worker threads per pool | Match the work: for I/O-bound handlers, more threads than cores is normal. Each thread can hold a database connection. |
| Connections | See [configuration.md](configuration.md#connection-pool-sizing). Sum over all processes must fit `max_connections`. |
| Queues | Separate queues for work with different urgency or failure behaviour, so a slow job type cannot starve a fast one. Each queue needs its own pool. |
| Throughput | Measured on a laptop, JobLatch drained roughly 1,500 jobs/s with one thread and 8,000–10,000 jobs/s with 16–32 threads, with no-op handlers ([benchmarks.md](benchmarks.md)). Real handlers will be the limit long before that. Measure your own. |

## Monitoring

Two complementary sources. **Metrics from the workers** (throughput, outcomes, handler latency,
errors, utilization) come from a `JobEventListener`: see [observability.md](observability.md) for a
Micrometer adapter and alert rules. **The state of the queue** (backlog, waiting time, stuck leases,
dead jobs, across every process) comes from the database, below.

Every query is in [`docs/sql/monitoring.sql`](sql/monitoring.sql), runs as the read/write runtime
role, and is verified by the test suite.

| Signal | Query | Alert when |
|---|---|---|
| Backlog per queue and state | #1 | `available` keeps growing; `dead` grows at all |
| **Oldest due job's wait** | #2 | above your latency budget. The best single health signal: it rises when workers are down, too few, or stuck. |
| Expired leases not yet reclaimed | #3 | non-zero for longer than a lease plus a reaper interval: **no reaper is running** |
| Recent dead jobs, with reasons | #4 | (investigation) |
| Most-retried job types | #5 | (investigation): a broken dependency or a bug |

Also watch the logs: `WARN` lines from `LeaseManager` ("Reclaimed N job(s) with expired leases")
mean workers are crashing or stalling; repeated `WARN` "Claim failed" or "Heartbeat … failed" mean
database trouble.

## Maintenance

### Retention

JobLatch never deletes jobs. Finished jobs stay as rows: useful for inspection, but the table grows.
Schedule [`docs/sql/03-retention.sql`](sql/03-retention.sql) (for example hourly), run as a role
holding `joblatch_maintenance`, repeated until it deletes 0 rows. It deletes `succeeded` jobs older than
7 days in batches of 1,000, so it never blocks workers for long. Dead jobs are kept until someone
deals with them.

### VACUUM and bloat

Every job is written several times (insert, claim, completion), so `joblatch_jobs` produces dead row
versions quickly. Autovacuum handles this by default. On busy systems consider per-table settings so
it runs more often, for example:

```sql
ALTER TABLE joblatch_jobs SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_analyze_scale_factor = 0.02);
```

Long-running transactions anywhere in the database stop VACUUM from cleaning up; keep them short.

### Dead jobs

List them with `DeadLetterQueue.list(queue, limit)` or monitoring query #4. Fix the cause, then
`DeadLetterQueue.requeue(id)`: the job becomes available with a fresh set of attempts. Delete dead
jobs you will never retry with a `DELETE … WHERE state = 'dead' AND id = …` as `joblatch_maintenance`.

### Upgrading JobLatch

Read the changelog. Run the new version's migrations before (or while) starting the new
application version: migrations are designed to be compatible with the previous release's code
([release.md](release.md#semantic-versioning)). Never downgrade the schema: migrations are not
reversible.

### Backups

`joblatch_jobs` is ordinary data: it is in your normal database backups. After restoring an old
backup, jobs that had already succeeded may be `available` or `running` again and will run again.
Idempotent handlers make that safe.
