# Configuration reference

Foleni has no configuration files and reads no environment variables. Everything is set in code.
The values below are the defaults in `foleni-core`; the constants named are public so your code can
refer to them.

## `WorkerPoolConfig`

Start from `WorkerPoolConfig.defaults(queue)` and change what you need with the `with…` methods.

| Setting | Default | Allowed | Effect |
|---|---|---|---|
| `queue` | (required) | 1–128 chars of `A-Z a-z 0-9 _ . : -` | The queue this pool takes jobs from. |
| `concurrency` | `4` (`DEFAULT_CONCURRENCY`) | 1–1,000 (`MAX_CONCURRENCY`) | Worker threads, and therefore the most jobs this pool holds claimed at once. |
| `pollInterval` | 5 s (`DEFAULT_POLL_INTERVAL`) | ≥ 1 ms | Longest an idle pool waits before checking the table. With notifications on, only a safety net. **With notifications off, this is your pickup latency: lower it** (e.g. 100–500 ms). |
| `leaseDuration` | 60 s (`DEFAULT_LEASE_DURATION`) | ≥ 3 ms | How long a claim lasts without a heartbeat. Heartbeats run every `leaseDuration / 3`. |
| `jobTimeout` | none (`null`) | ≥ 1 ms or `null` | Interrupt and fail (retryable) a job that runs longer. |
| `shutdownTimeout` | 30 s (`DEFAULT_SHUTDOWN_TIMEOUT`) | ≥ 0 | How long `close()` waits for running jobs before interrupting them. Interrupted jobs get a further 5 s to record their outcome. |
| `retryPolicy` | `RetryPolicy.defaults()` | any `RetryPolicy` | Delay before retrying a failed job. |
| `useNotifications` | `true` | `true` / `false` | Wake up on LISTEN/NOTIFY. Holds one extra connection per pool. |

### Choosing `leaseDuration`

- **Longer than your worst pause.** A garbage-collection pause or network stall longer than about
  two heartbeat intervals (2/3 of the lease) can cost the worker its job, which then runs twice.
- **Short enough for recovery.** After a crash, jobs wait up to one lease before the reaper can take
  them back, plus up to one reaper interval.
- 30–120 s suits most applications. It does not limit how long a job may run: heartbeats extend it.

### Turning notifications off

Needed behind **PgBouncer in transaction-pooling mode** (LISTEN needs a stable session), and
worthwhile when enqueue throughput matters more than pickup latency (NOTIFY roughly halved enqueue
throughput in the [benchmarks](benchmarks.md)):

```java
WorkerPoolConfig.defaults("emails").withNotifications(false).withPollInterval(Duration.ofMillis(200));
```

and, to remove the cost from enqueue as well, disable the triggers once (as the table owner):

```sql
ALTER TABLE foleni_jobs DISABLE TRIGGER foleni_jobs_notify_insert;
ALTER TABLE foleni_jobs DISABLE TRIGGER foleni_jobs_notify_update;
```

Re-enable them with `ENABLE TRIGGER` before turning notifications back on in any pool.

## `RetryPolicy`

```
delay(attempt) = min(maxDelay, baseDelay × multiplier^(attempt − 1)) × (1 − jitter × random[0,1))
```

| Setting | Default | Allowed |
|---|---|---|
| `baseDelay` | 10 s | ≥ 0 |
| `maxDelay` | 1 h | ≥ `baseDelay` |
| `multiplier` | 2.0 | finite, ≥ 1.0 |
| `jitter` | 0.5 | 0.0–1.0 |

With the defaults, retries wait 5–10 s, 10–20 s, 20–40 s, … up to 30–60 min. `RetryPolicy.noDelay()`
retries immediately: for tests. Jobs reclaimed after a crash are retried immediately regardless.

## `NewJob`

| Setting | Default | Allowed |
|---|---|---|
| `queue`, `jobType` | (required) | 1–128 chars of `A-Z a-z 0-9 _ . : -` |
| `payloadJson` | (required) | valid JSON, ≤ 1 MiB UTF-8 (`MAX_PAYLOAD_BYTES`) |
| `runAt` | now (database clock) | any `Instant` |
| `priority` | `0` (`DEFAULT_PRIORITY`) | any `int`; higher first |
| `maxAttempts` | `5` (`DEFAULT_MAX_ATTEMPTS`) | ≥ 1 |

Keep payloads small: IDs, not documents. Large payloads slow every claim and bloat the table.

## `LeaseReaper`

```java
new LeaseReaper(new LeaseManager(dataSource), interval, batchSize)
```

| Setting | Suggested | Allowed | Effect |
|---|---|---|---|
| `interval` | 10–30 s, or about `leaseDuration / 3` | ≥ 1 ms | Pause between passes. Adds to recovery time after a crash. |
| `batchSize` | 100 | 1–1,000 (`LeaseManager.MAX_RECLAIM_BATCH`) | Jobs per transaction. A pass repeats full batches up to 100 times. |

## Other limits

| Limit | Value | Where |
|---|---|---|
| Claim batch | 1,000 | `JobClaimer.MAX_BATCH_SIZE` |
| Stored error text | 2,000 characters | `JobLifecycle.MAX_ERROR_LENGTH` |
| Dead-letter listing | 1,000 per call | `DeadLetterQueue.MAX_LIST_LIMIT` |
| Listener health check | every 10 s, 5 s timeout | `JobNotificationListener` |
| Listener reconnect delay | 100 ms doubling to 30 s | `JobNotificationListener` |

## Connection pool sizing

Foleni borrows connections from the `DataSource` you give it, briefly, for each operation, except
the notification listener, which keeps one for as long as it runs. Per worker process, plan for:

```
pool size ≥ Σ over pools (concurrency + 1 listener + 2 for poller and heartbeats) + 1 for the reaper
            + whatever your handlers and the rest of your application hold at the same time
```

For one pool with `concurrency` 8 whose handlers each use one connection: 8 (handlers) + 3 + 1 ≈ 12,
plus your application's own needs. The pool must also fit within PostgreSQL's `max_connections`
across all processes.

## Logging

Foleni logs through the SLF4J API and ships no backend. Loggers are named after the classes
(`dev.foleni.core.WorkerPool`, …).

| Level | Examples |
|---|---|
| `ERROR` | A background loop failed unexpectedly (it keeps running). A job ignored the interrupt at shutdown. |
| `WARN` | Leases reclaimed (a worker crashed or stalled). A job went dead. A claim, heartbeat or reaper pass failed (database problem). A handler failed. |
| `INFO` | Pool, reaper and listener started/stopped. Migrations applied. A worker no longer owns a job. A dead job requeued. |
| `DEBUG` | Every enqueue, claim, completion and retry. |

Log lines contain job IDs, queue names, job types, worker IDs and SQLStates, never payloads or
credentials.
