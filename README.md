# JobLatch

[![Build](https://github.com/Math3wsl3vi/joblatch/actions/workflows/build.yml/badge.svg?branch=prod)](https://github.com/Math3wsl3vi/joblatch/actions/workflows/build.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Background jobs for Java that commit in the same transaction as your data. PostgreSQL only: no
Redis, no RabbitMQ, no second system to run.

> **Status: 0.1.0-SNAPSHOT, not yet on Maven Central.** The design is complete and tested (122
> automated tests, including multi-process crash tests), but the API may still change before 1.0.
> See [Versioning](docs/release.md#semantic-versioning).

## Why

An application saves an order and then needs to send a receipt. If it writes to the database and
then publishes a job to a separate broker, a crash between the two steps either loses the job or
sends a receipt for an order that was rolled back.

JobLatch stores jobs in the same PostgreSQL database as your data. A job is enqueued **inside your
own transaction**, so the job and the business write commit together or not at all. There is no
second system to run, monitor or back up.

## Guarantees

- **At-least-once delivery.** A committed job runs until it succeeds or uses up its attempts. It
  may run more than once, so **handlers must be idempotent** (see below).
- **Transactional enqueue.** A job becomes visible to workers only when the transaction that
  created it commits. Rollback removes it.
- **No double ownership.** Claiming uses `SELECT … FOR UPDATE SKIP LOCKED`. Every state change
  carries a fencing token, so a worker that lost a job cannot overwrite the new owner's result.
- **Crash recovery.** A job held by a worker that dies is taken back when its lease expires. Tested
  by killing real worker processes with `kill -9`.
- **Retries with backoff** (exponential, capped, with jitter) and a **dead-letter state** that keeps
  failed jobs for inspection and requeueing.
- **Scheduled jobs** never run before their time, measured on the database's clock.
- **Metrics without a metrics dependency.** Workers report claims, outcomes, handler latency, lost
  leases and errors to a listener: plug in Micrometer or Prometheus, or use the built-in counters
  ([observability](docs/observability.md)).

**Not provided:** exactly-once execution, ordering across workers, recurring (cron) jobs.

## Quick start

```java
DataSource dataSource = ...;                       // a pooled DataSource, e.g. HikariCP
JobLatchMigrations.migrate(dataSource);            // once, at deploy time

// 1. Enqueue in the same transaction as your business write
try (Connection conn = dataSource.getConnection()) {
    conn.setAutoCommit(false);
    long orderId = insertOrder(conn, order);
    new JobQueue().enqueue(conn, NewJob.of("emails", "send-receipt", "{\"orderId\":" + orderId + "}"));
    conn.commit();                                 // order and job, together
}

// 2. Run workers (in the same or another process)
WorkerPool pool = new WorkerPool(dataSource, WorkerPoolConfig.defaults("emails"),
        Map.of("send-receipt", job -> mailer.sendReceipt(job.payloadJson())));
LeaseReaper reaper = new LeaseReaper(new LeaseManager(dataSource), Duration.ofSeconds(10), 100);
reaper.start();
pool.start();
Runtime.getRuntime().addShutdownHook(new Thread(() -> { pool.close(); reaper.close(); }));
```

A handler that returns normally succeeds. One that throws is retried later. One that throws
`PermanentJobFailureException` goes straight to the dead-letter state.

### Idempotent handlers

At-least-once means a handler can run twice: for example, when a worker crashes after sending the
email but before recording that the job succeeded. Make the second run harmless:

```sql
-- Only the first run changes anything. A redelivery finds receipt_sent_at already set.
UPDATE orders SET receipt_sent_at = now() WHERE id = ? AND receipt_sent_at IS NULL
```

For external calls, pass the job ID as the idempotency key (most payment and email APIs accept one).

### Run the demo

Needs Java 21 and Docker.

```bash
cp .env.example .env                 # then edit the passwords in .env
docker compose up -d                 # local PostgreSQL on localhost:5432
set -a; source .env; set +a          # export JOBLATCH_DB_URL, JOBLATCH_DB_USER, JOBLATCH_DB_PASSWORD
./mvnw -pl joblatch-demo -am install -DskipTests
./mvnw -pl joblatch-demo exec:exec
```

The demo ([source](joblatch-demo/src/main/java/dev/joblatch/demo/DemoApplication.java)) places orders
with transactional enqueue, rolls one back, schedules one for later, sends a bad payload to the
dead-letter state and shows the idempotency guard skipping a redelivery.

## Documentation

| Document | Read it to… |
|---|---|
| [Architecture](docs/architecture.md) | understand how claiming, leases, retries and notifications fit together |
| [API guide](docs/api.md) | use every public class, with examples |
| [Configuration](docs/configuration.md) | look up every setting, default and limit |
| [Database schema](docs/schema.md) | know every table, column, constraint and index, and why |
| [Operations](docs/operations.md) | deploy, size, monitor and maintain JobLatch in production |
| [Observability](docs/observability.md) | get metrics out of workers (Micrometer, Prometheus, or built-in counters) |
| [Failure recovery](docs/failure-recovery.md) | know what happens, and what to do, when something breaks |
| [Security](docs/security.md) | set up least-privilege database roles and handle sensitive data |
| [Benchmarks](docs/benchmarks.md) | see measured performance against db-scheduler, and its limits |
| [Releasing](docs/release.md) | publish to Maven Central; versioning policy |
| [Changelog](CHANGELOG.md) | see what changed |

## Performance, in one paragraph

Measured on one laptop against db-scheduler ([details and caveats](docs/benchmarks.md)): with
LISTEN/NOTIFY, JobLatch picks up a new job in about **3 ms at the median, versus about 50 ms** for
100 ms polling. db-scheduler drains a backlog somewhat faster under load (by ~10% to ~2× across two
runs) and uses less database CPU per job. NOTIFY roughly halves JobLatch's enqueue throughput; it can
be turned off. These are queue-overhead numbers with no-op handlers, not production capacity.

## Modules

| Module | Published | Purpose |
|---|---|---|
| `joblatch-core` | yes | The library. Plain Java and JDBC; depends only on the PostgreSQL driver and the SLF4J API. |
| `joblatch-demo` | no | The runnable example above. |
| `joblatch-crash-test` | no | Starts real worker processes and kills them with SIGKILL to prove recovery. |
| `joblatch-benchmarks` | no | Benchmarks against db-scheduler. |
| `joblatch-spring-boot-starter` | no | Placeholder. Not implemented. |

## Roadmap

- [x] 1. Schema and transactional enqueue
- [x] 2. Claiming with `SKIP LOCKED`, tested under concurrent workers
- [x] 3. Complete and fail, guarded by a fencing token
- [x] 4. Leases, heartbeats, and a reaper for expired leases
- [x] 5. Retries with backoff and jitter; dead-letter state
- [x] 6. Worker pool with backpressure and graceful shutdown
- [x] 7. Scheduled jobs; `LISTEN`/`NOTIFY` with polling as fallback
- [x] 8. Crash test: workers killed at random, every job processed
- [x] 9. Benchmarks against an established Java job library
- [x] 10. Documentation and Maven Central publishing setup (first release pending, see [docs/release.md](docs/release.md))

## Building

Requires Java 21 and Docker (tests start PostgreSQL with Testcontainers).

```bash
./mvnw verify          # all modules, ~1.5 minutes
```

## Licence

Apache License 2.0. See [LICENSE](LICENSE).
