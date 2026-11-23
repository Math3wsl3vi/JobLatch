# Failure recovery guide

What Foleni does by itself when something breaks, what it cannot do, and what an operator should
do. Every "Foleni does" below is covered by an automated test; the test is named where it helps.

## The one rule behind all of it

**A job is never lost; it can run more than once.** Every failure below ends in one of three ways:
the job finishes, it waits for a retry, or it sits in `dead` with its reason. Recovery after a crash
re-runs work, which is why handlers must be idempotent.

## Worker process crashes (SIGKILL, OOM kill, power loss)

**Foleni does:** nothing runs in the dead process. Its jobs stay `running` until their leases expire
(at most `leaseDuration`), then any process's `LeaseReaper` returns them to `available` (or `dead`,
if that was their last attempt) with `last_error = 'lease expired: worker … stopped heartbeating'`.
Another worker runs them again. Proven by killing real worker processes 12 times during 3,000 jobs
(`CrashRecoveryTest`).

**Not handled:** side effects the handler already performed are not undone. They happen again
unless the handler is idempotent (`killAfterSideEffectCausesDuplicateExecutionThatIdempotencyAbsorbs`).

**You do:** restart the process. Check monitoring query #3: if expired leases stay unreclaimed, no
reaper is running.

## Worker pauses longer than its lease (GC pause, network stall, laptop sleep)

**Foleni does:** the reaper may hand the job to another worker. When the paused worker resumes, its
heartbeat reports lost ownership and the pool interrupts its handler. Its late complete or fail is
rejected (`NOT_OWNER`) by the fencing token.

**Not handled:** both workers may perform the side effect.

**You do:** if `WARN … Lost ownership` appears regularly, raise `leaseDuration` or find the pauses
(GC logs).

## Graceful shutdown (SIGTERM, deploy)

**Foleni does:** stops claiming, lets running jobs finish for up to `shutdownTimeout`, interrupts the
rest (they are failed and retried later), and abandons handlers that ignore interrupts. Their leases
expire and they are recovered. No job is left without an outcome (`sigtermDrainsRunningJobsWithoutDuplicates`).

**You do:** give the platform's grace period more time than `shutdownTimeout` + 5 s.

## PostgreSQL unavailable (restart, failover, network partition)

**Foleni does:**

| Component | Behaviour while the database is down | After it returns |
|---|---|---|
| `JobQueue.enqueue` | throws `SQLException` (SQLState `08xxx`) | works |
| Worker pool poller | logs `WARN Claim failed`, waits `pollInterval`, retries | claims again |
| Running handler | keeps running; its heartbeats fail and are logged | if the lease expired meanwhile, the job may be reclaimed and its outcome rejected |
| Recording an outcome | fails; logged as `WARN Could not record outcome` | the job is still `running`; it is retried after its lease expires |
| Lease reaper | logs `WARN … pass failed`, retries next interval | reclaims as usual |
| Notification listener | reconnects with backoff (100 ms doubling to 30 s) | wakes the pool once, in case it missed anything |

Proven with simulated outages (connections refused) for the pool and the reaper (`WorkerPoolTest`,
`LeaseTest`), and with the listener's connection killed by the server
(`NotificationAndSchedulingTest`).

**Not handled:** the application's own enqueue calls fail while the database is down. Because the
job is in the same transaction as your business write, that write fails too: nothing is half-done.

**You do:** nothing for Foleni. Expect some jobs to run twice after a long outage.

## A handler keeps failing

**Foleni does:** retries with exponential backoff and jitter until `maxAttempts`, then moves the job
to `dead` with its last error. Retries are spread out, so a broken dependency is not hammered.

**You do:** find the reason with monitoring query #4 or `DeadLetterQueue.list`. Fix the cause,
then requeue: `DeadLetterQueue.requeue(id)` gives the job a fresh set of attempts. For a bug that
can never succeed (bad payload), throw `PermanentJobFailureException` so the job skips its remaining
attempts.

## A handler hangs

**Foleni does:** with `jobTimeout` set, interrupts the handler after the timeout and fails the job
("timed out after … ms"); it is retried. Without `jobTimeout`, heartbeats keep the lease alive and the
job holds its thread until shutdown.

**Not handled:** Java cannot kill a thread. A handler that ignores interrupts keeps its thread busy
(and the pool one thread smaller) until the process restarts.

**You do:** set `jobTimeout` for any queue whose handlers call external services. Use timeouts on
every network call inside handlers.

## A job crashes the worker every time it runs ("poison pill")

**Foleni does:** each crash uses an attempt (counted at claim time). After `maxAttempts` crashes the
reaper moves it to `dead` instead of returning it to the queue, so it stops taking workers down.

**You do:** inspect it in the dead-letter list; fix the handler before requeueing.

## No reaper is running

**Foleni does:** nothing can recover jobs of crashed workers. They stay `running` forever. (Checked
during development: with the worker's reaper removed, the crash test's duplicate-execution scenario
never finishes.)

**You do:** run a `LeaseReaper` in every worker process. Alert on monitoring query #3.

## Jobs were deleted, edited or inserted by hand

**Foleni does:** `CHECK` constraints reject impossible rows (an owner without `running`, attempts
above the limit, an unknown state, invalid names). The runtime role cannot delete or alter the table.

**You do:** don't. Use `DeadLetterQueue` for requeueing and the documented retention query for
deletion.

## Restoring a backup

Jobs return to the state they had at backup time. Jobs that succeeded after the backup may run
again; `running` jobs are reclaimed after their (long expired) leases. Idempotent handlers make
this safe.
