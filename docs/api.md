# API guide

Everything public lives in the package `dev.joblatch.core`. The full Javadoc is generated with
`./mvnw -pl joblatch-core javadoc:javadoc` (output in `joblatch-core/target/reports/apidocs`) and is
published with every release as `joblatch-core-<version>-javadoc.jar`.

Most applications need only the first five classes below. The rest exist for tests, tools and
custom workers.

## Dependency

Not yet on Maven Central. Once released:

```xml
<dependency>
    <groupId>io.github.math3wsl3vi</groupId>
    <artifactId>joblatch-core</artifactId>
    <version>0.1.0</version>
</dependency>
```

Requires Java 21+ and PostgreSQL 18+ (the schema uses the built-in `uuidv7()`). Brings in the
PostgreSQL JDBC driver and the SLF4J API; choose your own logging backend and connection pool.

## Every application

### `JobLatchMigrations`: create and upgrade the tables

```java
JobLatchMigrations.migrate(dataSource);
```

Applies all pending migrations in one transaction. Safe to call repeatedly and from several
processes at once (an advisory lock serializes them). Run it at deploy time with the role that owns
the tables; see [security.md](security.md).

### `JobQueue` and `NewJob`: submit jobs

```java
JobQueue queue = new JobQueue();   // stateless, thread-safe, share one

// Inside YOUR transaction (the usual case). Auto-commit must be off.
queue.enqueue(connection, NewJob.of("emails", "send-receipt", "{\"orderId\": 42}"));

// Standalone: its own transaction, committed before the call returns.
UUID id = queue.enqueue(dataSource, NewJob.of("reports", "nightly", "{}"));

// Options. NewJob is immutable: each with... returns a copy.
NewJob job = NewJob.of("emails", "send-receipt", payloadJson)
        .withRunAt(Instant.now().plus(Duration.ofMinutes(10)))   // scheduled
        .withPriority(10)                                         // higher first
        .withMaxAttempts(8);                                      // default 5
```

Rules enforced before any SQL runs (`IllegalArgumentException`):

- `queue` and `jobType`: 1–128 characters of `A-Z a-z 0-9 _ . : -`
- `payloadJson`: not blank, at most 1 MiB of UTF-8. Must be valid JSON (Postgres rejects it otherwise,
  with `SQLException` SQLState `22P02`).
- `maxAttempts` ≥ 1

`enqueue(Connection, …)` never commits, rolls back, closes or reconfigures your connection. If it
throws, Postgres has aborted your transaction: roll back. Error messages never contain the payload.

### `WorkerPool`, `WorkerPoolConfig`, `JobHandler`: run jobs

```java
WorkerPoolConfig config = WorkerPoolConfig.defaults("emails")
        .withConcurrency(8)
        .withJobTimeout(Duration.ofMinutes(2));

WorkerPool pool = new WorkerPool(dataSource, config, Map.of(
        "send-receipt", job -> mailer.sendReceipt(job.id(), job.payloadJson()),
        "refund",       job -> payments.refund(job.id(), job.payloadJson())));
pool.start();
...
pool.close();   // graceful: see configuration.md, shutdownTimeout
```

A `JobHandler` decides the outcome by how it ends:

| Handler | Outcome |
|---|---|
| returns normally | `succeeded` |
| throws `PermanentJobFailureException` | `dead` immediately |
| throws anything else, including `Error` | retried after a backoff delay; `dead` after the last attempt |
| (no handler for the job's type) | treated as a retryable failure: a newer deployment may handle it |

Handlers must be **thread-safe** (several run at once), **idempotent** (a job can run more than
once), and should **let `InterruptedException` propagate**: the pool interrupts handlers on timeout,
on lost ownership and at the end of shutdown. Exception messages are stored in `last_error` for
operators, so keep secrets and personal data out of them.

`pool.workerId()` is the value stored in `locked_by`. `pool.activeJobs()` is the number of jobs
running right now. One pool serves one queue; create one pool per queue.

### `LeaseReaper` and `LeaseManager`: recover jobs from crashed workers

```java
LeaseReaper reaper = new LeaseReaper(new LeaseManager(dataSource), Duration.ofSeconds(10), 100);
reaper.start();
...
reaper.close();
```

Run one in **every** worker process. Without any reaper, jobs of crashed workers stay `running`
forever. Several reapers at once are safe.

### `JobEventListener`, `JobExecution`, `JobCounters`: metrics

```java
JobCounters counters = new JobCounters();                        // or your Micrometer adapter
WorkerPool pool = new WorkerPool(dataSource, config, handlers, counters);
LeaseReaper reaper = new LeaseReaper(new LeaseManager(dataSource), Duration.ofSeconds(10), 100, counters);
...
JobCounters.Snapshot s = counters.snapshot();                    // s.succeeded(), s.dead(), ...
```

A `JobEventListener` is told about claims, every finished attempt (`JobExecution`: outcome, handler
duration, failure, timeout), lost leases, reclaimed leases and background errors. Implement only
the methods you need; implementations must be fast, thread-safe and must not throw (anything thrown
is caught and logged). `pool.queue()`, `pool.concurrency()` and `pool.activeJobs()` feed
utilization gauges. Full event list, a Micrometer adapter and alerting advice:
[observability.md](observability.md).

### `DeadLetterQueue` and `DeadJob`: inspect and requeue failed jobs

```java
DeadLetterQueue dlq = new DeadLetterQueue(dataSource);
for (DeadJob dead : dlq.list("emails", 50)) {            // newest first, up to 1,000
    System.out.println(dead.id() + " " + dead.attempts() + " " + dead.lastError());
}
boolean requeued = dlq.requeue(deadJobId);                // back to available, attempts reset to 0
```

`requeue` changes only jobs that are `dead`, so calling it twice is harmless. `DeadJob.toString()`
leaves out the payload and the error text.

## Lower-level building blocks

`WorkerPool` is built from these. Use them directly for a custom worker loop or in tests.

| Class | Main methods | Notes |
|---|---|---|
| `JobClaimer` | `claim(queue, workerId, batchSize, leaseDuration)` → `List<ClaimedJob>`; `timeUntilNextDue(queue)` | Batch size 1–1,000. Returned jobs are already committed as owned. |
| `ClaimedJob` | `id()`, `queue()`, `jobType()`, `payloadJson()`, `attempts()`, `maxAttempts()`, `lockedBy()`, `lockedUntil()` | `toString()` leaves out the payload. |
| `JobLifecycle` | `complete(job)`, `complete(connection, job)`, `fail(job, error)`, `failPermanently(job, error)` | Each returns a `TransitionResult`. `complete(connection, job)` joins your transaction (see below). |
| `TransitionResult` | `SUCCEEDED`, `RETRY_SCHEDULED`, `DEAD`, `NOT_OWNER` | `NOT_OWNER`: you no longer own the job; nothing changed. Log it and move on. |
| `LeaseManager` | `renew(job, leaseDuration)` → `Optional<Instant>`; `reclaimExpired(maxJobs)` → `ReclaimResult` | Empty from `renew`: you lost the job. |
| `RetryPolicy` | `defaults()`, `noDelay()`, `new RetryPolicy(base, max, multiplier, jitter)`, `delayAfter(attempt)` | See [configuration.md](configuration.md). |
| `JobNotificationListener` | `start()`, `isListening()`, `close()` | One LISTEN connection; calls a `Runnable` when a job arrives in its queue, and optionally a callback when the connection is lost. |

### Exactly-once for database work: `complete(connection, job)`

If a job's work is a write to the same database, do the work and the completion in one transaction.
Either both commit or neither does, so a crash can never leave "work done, job still running":

```java
try (Connection conn = dataSource.getConnection()) {
    conn.setAutoCommit(false);
    markInvoicePaid(conn, invoiceId);
    if (lifecycle.complete(conn, job) == TransitionResult.NOT_OWNER) {
        conn.rollback();          // someone else owns the job now and will do the work
        return;
    }
    conn.commit();
}
```

This currently requires your own worker loop (`JobClaimer` + `JobLifecycle`): a `WorkerPool`
handler's job is completed by the pool after the handler returns, so calling `complete(connection,
job)` inside a handler leads to a harmless `NOT_OWNER` log line from the pool.

## Thread safety

Every class above is thread-safe, except that `WorkerPool`, `LeaseReaper` and
`JobNotificationListener` must be started once and closed once (both are checked). Records
(`NewJob`, `ClaimedJob`, `DeadJob`, `RetryPolicy`, `WorkerPoolConfig`, `ReclaimResult`,
`JobExecution`) are immutable. Your `JobEventListener` must be thread-safe; `JobCounters` is.

## Errors

Database failures surface as `SQLException` with a meaningful SQLState (`08xxx`: connection problem,
`22P02`: invalid JSON, `23514`: a `CHECK` constraint failed, `42501`: missing privilege, `42P01`:
table missing, so migrations have not run). Invalid arguments throw `IllegalArgumentException`;
`null` where a value is required throws `NullPointerException`.

JobLatch's own log messages never contain payloads or credentials: they carry job IDs, queue and type
names, and SQLState codes. `JobQueue.enqueue` also strips the database's error text from the
exceptions it throws, because a failed insert's message can quote the row. Other operations pass
the driver's `SQLException` through unchanged; if your application logs those, set the JDBC driver
option `logServerErrorDetail=false` (see [security.md](security.md)).
