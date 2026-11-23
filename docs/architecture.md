# Architecture

How JobLatch works, and why it is built this way. For the exact columns and indexes see
[schema.md](schema.md); for the classes see [api.md](api.md).

## The big picture

```
 your application                    PostgreSQL                         worker process(es)
┌───────────────────┐   INSERT in   ┌───────────────────────────┐   claim / complete /  ┌──────────────────┐
│ business write +  │ ─────────────▶│ joblatch_jobs               │◀───────────────────── │ WorkerPool       │
│ JobQueue.enqueue  │  YOUR tx      │  available → running →    │   fail / heartbeat    │  poller          │
└───────────────────┘               │  succeeded | dead         │                       │  N worker threads│
                                    │                           │──── NOTIFY ─────────▶ │  heartbeats      │
                                    │ trigger: pg_notify on     │   (wake-up hint)      │  listener        │
                                    │ "job became available"    │                       ├──────────────────┤
                                    └───────────────────────────┘◀── reclaim expired ── │ LeaseReaper      │
                                                                                         └──────────────────┘
```

Everything that matters is in **one table**, `joblatch_jobs`. There is no in-memory queue that can be
lost: a job is either a committed row or it does not exist.

## Job states

```
           enqueue (commit)
                │
                ▼
          ┌───────────┐   claim (SKIP LOCKED)   ┌─────────┐   handler returns   ┌───────────┐
   ┌────▶ │ available │ ──────────────────────▶ │ running │ ──────────────────▶ │ succeeded │
   │      └───────────┘                          └─────────┘                     └───────────┘
   │        ▲     ▲                               │  │  │
   │        │     └── fail, attempts left ────────┘  │  │ fail on last attempt,
   │        │         (run_at = now + backoff)       │  │ or permanent failure   ┌──────┐
   │        └──── lease expired, attempts left ──────┘  └──────────────────────▶ │ dead │
   │              (reaper; immediately)                  lease expired on last   └──────┘
   │                                                     attempt (reaper)            │
   └────────────────────────────── requeue (operator) ────────────────────────────────┘
```

`CHECK` constraints make impossible combinations unrepresentable: a running job always has an owner
and a lease; no other state has either; only `succeeded` and `dead` have `finished_at`.

## The five mechanisms

### 1. Transactional enqueue

`JobQueue.enqueue(Connection, NewJob)` runs one `INSERT` on **the caller's connection** and never
commits, rolls back or closes it. The job is part of the caller's transaction. This is the
transactional outbox pattern with the job table as the outbox.

`enqueue(DataSource, NewJob)` is the standalone variant: one `INSERT` in auto-commit mode.

### 2. Claiming with `FOR UPDATE SKIP LOCKED`

One statement picks the best due jobs (`priority DESC, run_at`), locks them, and marks them
`running` with an owner and a lease:

```sql
WITH candidates AS (
    SELECT id FROM joblatch_jobs
    WHERE queue = ? AND state = 'available' AND run_at <= now() AND attempts < max_attempts
    ORDER BY priority DESC, run_at
    LIMIT ?
    FOR UPDATE SKIP LOCKED
)
UPDATE joblatch_jobs SET state = 'running', locked_by = ?, locked_until = now() + ?, attempts = attempts + 1
FROM candidates WHERE joblatch_jobs.id = candidates.id
RETURNING ...
```

`SKIP LOCKED` lets concurrent workers take *different* rows instead of queueing behind the same one.
Under `READ COMMITTED`, `FOR UPDATE` re-checks the `WHERE` clause against the latest committed row
version, so a row claimed a moment ago by someone else drops out. The statement runs in auto-commit
mode: it is its own transaction, and the row locks last only milliseconds.

**Why not hold the transaction open while the job runs?** Each running job would pin a database
connection for its whole duration, long transactions block VACUUM, and a hung handler would hold
its lock forever. Instead, ownership is recorded *in the row* and protected by leases.

### 3. Leases, heartbeats and the reaper

A claim gives the worker ownership until `locked_until`. While the handler runs, the pool renews the
lease every `leaseDuration / 3` (a heartbeat). If the worker dies, nobody renews it; once it expires,
any process's `LeaseReaper` takes the job back:

- attempts left: back to `available`, immediately
- last attempt used: `dead`

Attempts are counted **at claim time**, so a job that crashes its worker every time cannot loop
forever: it ends up `dead`. Several reapers can run at once safely; `SKIP LOCKED` gives each a
different set of rows.

### 4. Fencing: protection against stale workers

Every state change by a worker is one conditional `UPDATE`:

```sql
... WHERE id = ? AND state = 'running' AND locked_by = ? AND attempts = ?
```

`attempts` is the **fencing token**: it increases on every claim, so it identifies one ownership
period exactly. A worker whose job was reclaimed (or that is the same worker on a newer attempt)
updates 0 rows and gets `TransitionResult.NOT_OWNER`. The check and the write are one statement,
so there is no window for a race.

Fencing protects the job's **state**, not the handler's **side effects**: a stale worker may already
have sent the email. That is what idempotent handlers are for.

### 5. Wake-ups: LISTEN/NOTIFY, with polling as the safety net

A trigger calls `pg_notify('joblatch_jobs', <queue>)` whenever a job becomes available. Each
`WorkerPool` holds one `LISTEN` connection and wakes its poller on a notification for its queue.

Notifications are **hints**: Postgres does not store them, so a listener that was disconnected
misses them. Three safety nets: the listener wakes the pool after every (re)connect; the pool still
polls every `pollInterval`; and a health check every 10 s replaces a connection that died silently.

When the claim finds nothing, the pool asks the database how long until the next scheduled job is
due and sleeps exactly that long (or `pollInterval`, if shorter). That is how scheduled jobs run on
time without frequent polling.

## Retries

A failed attempt is rescheduled at `now() + delay`:

```
delay(attempt) = min(maxDelay, baseDelay × multiplier^(attempt − 1)) × (1 − jitter × random)
```

Java computes the length; the database computes the time, so all workers use one clock. Defaults:
10 s, doubling, capped at 1 hour, up to 50% jitter. Jobs taken back by the reaper are retried
immediately: the expired lease already was the delay.

## Backpressure in the worker pool

The pool never claims more jobs than it has free threads. A semaphore with one permit per thread is
taken before claiming and released after the outcome is recorded. Unstarted work stays in the
database, where it is durable and visible to other workers; memory use is bounded by
`concurrency`, not by the backlog.

```
poller:   take 1 permit (wait if all threads busy), take any other free permits, claim that many
worker:   run handler → complete / fail / failPermanently → release permit
scheduler: per running job, a heartbeat every lease/3 and an optional timeout
listener: LISTEN connection; wakes the poller
```

## Graceful shutdown

`WorkerPool.close()`: stop the listener and the poller (no new claims) → let running jobs finish for
up to `shutdownTimeout` (heartbeats keep running, so leases stay valid) → interrupt what is left →
anything that ignores the interrupt is abandoned, and its lease expires for the reaper. Nothing is
lost; it is only delayed.

## Design decisions and their trade-offs

| Decision | Benefit | Cost |
|---|---|---|
| Jobs in the application's database | Transactional enqueue, one system to run | Queue load competes with application queries |
| Ownership in the row, not an open transaction | Short transactions, no pinned connections | Needs leases, heartbeats, a reaper and fencing |
| Attempts counted at claim time | Crash loops end in `dead` | An interrupted attempt (crash, shutdown) uses up an attempt |
| Finished jobs kept (`succeeded` row) | Inspectable history | Table grows; needs a retention job |
| LISTEN/NOTIFY on by default | ~3 ms pickup instead of half a poll interval | One connection per pool; commit-time cost halves enqueue throughput |
| Database clock for every time comparison | No clock-skew bugs between workers | One more reason the database must be healthy |
| Regex `CHECK` constraints on names | Database rejects bad names even from raw SQL | ~45% of an insert's execution time (measured) |

## What happens when…

| Situation | Result |
|---|---|
| The application rolls back after enqueueing | The job never existed. No worker sees it, no notification is sent. |
| A worker process is killed mid-job | Its jobs stay `running` until their leases expire, then the reaper requeues them. The handler may run again. |
| A worker pauses longer than its lease (GC, network) | The job may be reclaimed and run elsewhere. The paused worker's late outcome is rejected (`NOT_OWNER`). |
| PostgreSQL is unavailable | Enqueue throws. Workers, heartbeats, the reaper and the listener log and retry; nothing is lost. Jobs whose outcome could not be recorded are retried after their lease expires. |
| A handler throws `Error` (not `Exception`) | Recorded as that job's failure; the pool continues. `VirtualMachineError` is rethrown after recording, and its thread is replaced. |
| No reaper runs anywhere | Jobs of crashed workers stay `running` forever. **Every deployment needs one.** |

See [failure-recovery.md](failure-recovery.md) for what to do in each case.
