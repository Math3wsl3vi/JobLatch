# Changelog

All notable changes to JobLatch are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org) as described in [docs/release.md](docs/release.md).

## [Unreleased]

First release candidate: 0.1.0.

### Added

- Metrics without a metrics dependency: `JobEventListener` (claims, finished attempts, lost and
  reclaimed leases, background errors), `JobExecution`, and `JobCounters` for in-memory totals.
  `WorkerPool` and `LeaseReaper` take a listener in new constructors; a throwing listener cannot
  break them. `WorkerPool.queue()` and `concurrency()` for utilization gauges. A tested Micrometer
  adapter in `docs/observability.md`.
- Transactional enqueue on the caller's connection (`JobQueue.enqueue(Connection, NewJob)`) and
  standalone enqueue (`JobQueue.enqueue(DataSource, NewJob)`), with input validation and payload-free
  error messages.
- Job claiming with `SELECT … FOR UPDATE SKIP LOCKED`, in priority and due-time order
  (`JobClaimer`).
- Completion and failure guarded by a fencing token, so stale workers cannot overwrite a newer
  owner's state (`JobLifecycle`, `TransitionResult`).
- Leases, heartbeats and a background reaper for jobs of crashed workers (`LeaseManager`,
  `LeaseReaper`).
- Retries with exponential backoff and jitter (`RetryPolicy`), permanent failures
  (`PermanentJobFailureException`, `failPermanently`) and a dead-letter state with listing and
  requeueing (`DeadLetterQueue`, `DeadJob`).
- A worker pool with backpressure, per-job heartbeats, job timeouts and graceful shutdown
  (`WorkerPool`, `WorkerPoolConfig`, `JobHandler`).
- LISTEN/NOTIFY wake-ups with polling as the fallback, automatic reconnection and health checks
  (`JobNotificationListener`); scheduled jobs that run on time on the database's clock.
- Database migrations V1–V3 (`JobLatchMigrations`).
- Least-privilege role scripts, monitoring and retention SQL (`docs/sql/`), verified by tests.
- Multi-process crash tests, benchmarks against db-scheduler, a runnable demo and Docker Compose
  setup for local development.
- Documentation: architecture, API, configuration, schema, operations, observability, failure
  recovery, security, benchmarks and releasing.

### Tables

- Adds `joblatch_jobs` and `joblatch_schema_history`. Run `docs/sql/02-grant-privileges.sql` after
  migrating.
