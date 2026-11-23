# Benchmarks: Foleni vs db-scheduler

Measured, not estimated. Everything below can be reproduced with the commands at the end. Read the
**Limitations** section before quoting any number.

## What is compared

| System | Version | Licence | Configuration |
|---|---|---|---|
| **foleni** | 0.1.0-SNAPSHOT | Apache 2.0 | Defaults: LISTEN/NOTIFY on, 5 s safety-net polling, 60 s lease |
| **foleni-poll** | 0.1.0-SNAPSHOT | Apache 2.0 | NOTIFY triggers disabled, 100 ms polling. Like-for-like with db-scheduler. |
| **db-scheduler** | 17.0.0 | Apache 2.0 | `pollUsingLockAndFetch(0.5, 1.0)` (SELECT … FOR UPDATE SKIP LOCKED), 100 ms polling |

JobRunr is not included yet. Its open-source edition is LGPL-3.0, and adding it is waiting on a
licence review.

db-scheduler 17.0.0 was the newest release that had been public for at least two weeks when these
benchmarks were written. Its PostgreSQL table comes unchanged from the project's v17.0.0 tag.

## Method

All three systems run the same program (`foleni-benchmarks`, `BenchmarkMain`), one after another,
**each against its own fresh PostgreSQL container**: same image (`postgres:18-alpine`), default
settings except two: **`fsync=on`** (Testcontainers turns it off by default, which would remove the
cost of durable commits, the main cost a durable queue has) and the `pg_stat_statements` extension,
which records database time per SQL statement. Every system gets the same HikariCP pool size (64) and the same thread counts. The
benchmark runs in its own JVM with a fixed 1 GiB heap.

Each system first processes 2,000 warm-up jobs (not reported), so the JIT compiler has optimised the
hot paths. Every measurement is then repeated 3 times; tables show the **median and the range**.
The raw numbers for every repetition are written to `target/benchmark-raw.csv`.

The job handler does nothing. These benchmarks measure **queue overhead**: what the library and the
database cost per job. Real handlers that send emails or call APIs will be far slower than any
number here.

| Phase | Measured | How |
|---|---|---|
| Enqueue | jobs/s | 10,000 jobs from 8 producer threads, one transaction per job, no workers running |
| Drain | jobs/s at 1, 4, 16, 32 worker threads | Queue preloaded with 10,000 jobs. Timer starts when workers start, stops when **the database** reports every job finished (claim + run + completion committed). |
| Claim only / complete only | jobs/s, **Foleni only** | 8 threads calling `JobClaimer.claim` (batches of 100), then 8 threads calling `JobLifecycle.complete`. db-scheduler has no public API for these steps, so there is nothing to compare. |
| Latency | p50 / p95 / p99 / max | Workers running and idle (16 threads). Jobs arrive at 200 per second for 15 s (3,000 jobs). Measured from just before the enqueue call to the moment the handler starts. |
| DB CPU, DB memory | seconds, MiB | Read from the Postgres container's own cgroup counters (`cpu.stat`, `memory.current`): exact totals for the phase, not samples. Memory includes Postgres's shared buffers and file cache. Shown for the **last** of the 3 repetitions. |
| WAL | MiB | Write-ahead log written during the drain (`pg_current_wal_lsn()` before and after): how much each system writes per job. |
| Statement profile | ms per statement | `pg_stat_statements` over all enqueue and drain runs: where the database time goes. |
| Worker CPU, heap | seconds, MiB | The benchmark JVM's process CPU time and peak heap during the drain. Includes the harness itself (the "pending" query every 20 ms). |

## Differences between the systems that affect the numbers

These are design differences, not configuration mistakes. They are part of what is being measured.

1. **Wake-up.** db-scheduler always polls. Foleni can be woken by LISTEN/NOTIFY. That shows up
   mostly in latency.
2. **Completion.** db-scheduler **deletes** a finished one-time task. Foleni **updates** the row to
   `succeeded` and keeps it (for inspection, until a retention job removes it). An update leaves a
   dead row version behind for VACUUM, and so does a delete. Foleni's table grows; db-scheduler's
   does not.
3. **Payload.** db-scheduler stores task data with Java serialization in a `bytea` column. Foleni
   stores JSON in `jsonb`, which Postgres parses and validates on insert.
4. **Clock.** db-scheduler compares `execution_time` with the JVM's clock. Foleni uses the
   database's clock (`now()`).
5. **Triggers.** With notifications on, every Foleni insert fires a trigger that calls
   `pg_notify`. The `foleni-poll` configuration disables the triggers, which shows their cost in
   the enqueue numbers.

## Results

Machine: Apple M4 (10 cores, 16 GiB), macOS 26.2, Java 21.0.11. PostgreSQL 18 in Docker Desktop's
Linux VM (4 CPUs, 3.8 GiB). 10,000 jobs per run, 3 repetitions, `fsync=on`. Run on 2026-11-23.

### Drain throughput (claim + run + complete), jobs/s, median (range)

| Worker threads | foleni | foleni-poll | db-scheduler |
|---:|---:|---:|---:|
| 1 | 1,539 (925–1,626) | 1,286 (787–1,421) | 1,573 (1,544–1,736) |
| 4 | 1,809 (1,614–2,109) | 3,145 (2,706–3,600) | 3,666 (3,543–4,795) |
| 16 | 7,141 (2,836–7,275) | 6,421 (6,392–7,000) | 9,767 (9,066–11,286) |
| 32 | 5,523 (4,338–6,340) | 8,454 (6,677–8,550) | 12,429 (10,878–14,972) |

### Database cost per 10,000-job drain (last repetition)

| Worker threads | foleni DB CPU s / WAL MiB | foleni-poll | db-scheduler |
|---:|---|---|---|
| 1 | 4.0 / 7.6 | 2.7 / 7.6 | 1.8 / 6.4 |
| 4 | 4.1 / 7.1 | 2.8 / 7.1 | 1.5 / 6.3 |
| 16 | 2.0 / 6.7 | 1.9 / 6.6 | 0.9 / 6.0 |
| 32 | 2.3 / 6.6 | 1.9 / 6.5 | 1.1 / 6.0 |

Worker JVM CPU was 0.4–1.1 s per drain for every system, so the **database**, not the worker, is
where the systems differ.

### Enqueue, jobs/s (8 producer threads, one transaction per job), median (range)

| foleni | foleni-poll | db-scheduler |
|---:|---:|---:|
| 4,729 (246–7,211) | 10,513 (6,044–13,874) | 8,591 (6,802–10,638) |

### Latency, enqueue call to handler start (200 jobs/s for 15 s, 16 idle workers)

| | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|
| **foleni** (NOTIFY) | **2.7 ms** | **4.4 ms** | 24.9 ms | 109.6 ms |
| foleni-poll (100 ms poll) | 49.5 ms | 101.4 ms | 107.1 ms | 139.6 ms |
| db-scheduler (100 ms poll) | 51.5 ms | 101.9 ms | 116.9 ms | 424.9 ms |

### Where the database time goes (mean execution time per statement)

| Statement | foleni | foleni-poll | db-scheduler |
|---|---:|---:|---:|
| Insert a job | 0.207 ms | 0.079 ms | 0.015 ms |
| Finish a job (Foleni: UPDATE to succeeded. db-scheduler: DELETE) | 0.122 ms | 0.106 ms | 0.013 ms |
| Claim (SKIP LOCKED CTE) | 0.140 ms | 0.141 ms | 0.069 ms |

Foleni only, single run each: claim-only 40,556 jobs/s (foleni) and 79,910 (foleni-poll) with
batches of 100. Complete-only 5,982 and 3,985 jobs/s.

## What the numbers say

1. **Latency: Foleni with LISTEN/NOTIFY is ~19× faster at the median** (2.7 ms vs ~50 ms). Both
   polling systems sit at about half their 100 ms poll interval, as expected.
2. **Throughput under load: db-scheduler is faster.** At 16–32 threads it drained 1.4–2.3× more
   jobs per second than Foleni, while using about half the database CPU per job.
3. **NOTIFY has a real cost.** The trigger adds ~0.13 ms of database time per insert and halved
   enqueue throughput (4,729 vs 10,513 jobs/s). This is the commit-time NOTIFY cost predicted in
   Milestone 7, now measured.
4. **Without NOTIFY, Foleni enqueues faster than db-scheduler** (10,513 vs 8,591 jobs/s), even
   though each of its inserts costs more database time. Insert execution time is only part of an
   enqueue: round trips and client-side work count too. Where db-scheduler spends the difference
   was not investigated. (Its profile also shows one single-row `select` per job, 120,000 calls;
   which step issues it was not checked.)
5. **Foleni's per-statement database cost is several times db-scheduler's.** Measured causes:
   - The regex `CHECK` constraints on `queue` and `job_type` are about **45% of an insert's
     execution time** (isolated experiment, 50,000 inserts: 0.053 ms with them, 0.029 ms without,
     +25% throughput). Postgres re-checks every `CHECK` on every `UPDATE` too, so claims and
     completions pay for them as well.
   - Foleni keeps finished jobs (`UPDATE` to `succeeded`, a full new row version) where
     db-scheduler `DELETE`s them. WAL volume is only ~10–20% higher, so this is **not** the main
     cause, contrary to the first hypothesis during this work.
   - Other contributors (three partial indexes, `jsonb` parsing, UUIDv7 keys) are **not yet
     separately measured**.
6. **Results are noisy.** Ranges are wide, and some medians are out of order (foleni at 4 threads
   below foleni-poll; foleni at 32 threads below 16). One enqueue repetition fell to 246 jobs/s.
   Likely causes on default settings: checkpoints (every 5 minutes) and autovacuum during runs.
   **Not verified.** Treat differences under ~30% as inconclusive.

## Corrections made while building this benchmark

Kept here because they change how to read any earlier number.

1. **Harness bug, fixed.** The "is the drain finished?" check counted Foleni's unfinished rows with
   a full table scan 50 times a second, because Foleni keeps finished rows. It now uses an `EXISTS`
   query answered from an index, for every system.
2. **Foleni change, kept.** Single-statement operations (claim, complete, fail, renew, reclaim,
   `enqueue(DataSource)`) used an explicit `BEGIN … COMMIT`. In auto-commit mode a single statement
   is already its own atomic transaction, so the explicit transaction added a database round trip
   and no safety. Removing it raised drain throughput by up to ~90% in the fsync=off runs. All 113
   core tests and the 3 crash tests still pass.
3. **Invalid runs, discarded.** Testcontainers starts PostgreSQL with `fsync=off`. All runs before
   it was switched back on are discarded; none of their numbers appear above.
4. **db-scheduler API updated after the run.** The results above were produced with
   `SchedulerClient.schedule(TaskInstance, Instant)`, which db-scheduler 17.0.0 deprecates. The
   harness now calls `schedule(TaskInstance, Instant, ScheduleOptions.WHEN_EXISTS_DO_NOTHING)`. With
   random task IDs both insert the same row, and a smoke run shows the same SQL statements, but the
   full benchmark has not been rerun since. db-scheduler's enqueue numbers may differ slightly.

## Recommended next steps

- **Replace the regex `CHECK` constraints** with cheap length checks (Java already validates the
  character set). This is a trade-off: the database stops guarding the name format against raw SQL
  writes. Needs a V4 migration.
- **Measure, then decide,** on the other per-statement costs: partial indexes, `jsonb` vs `text`
  payloads.
- **Batch completions**, as db-scheduler batches its picking. Completing several jobs in one
  `UPDATE … WHERE id = ANY(?)` would cut round trips at high concurrency. Ownership checks must stay
  per job.
- **Rerun on a tuned, dedicated PostgreSQL server** before quoting absolute numbers.

## Limitations

- **One machine, laptop-class.** Apple M4 (10 cores, 16 GiB) running macOS 26.2. PostgreSQL runs in
  Docker Desktop's Linux VM, limited to **4 CPUs and 3.8 GiB**. The benchmark JVM runs on the host
  and can use all 10 cores. The database is the bottleneck well before the JVM, so results at 16
  and 32 threads mostly show how each system behaves when **Postgres is saturated on 4 cores**.
- **Default PostgreSQL settings.** No tuning of `shared_buffers`, `max_wal_size`, `synchronous_commit`
  and so on. Production numbers on a tuned server with fast disks will be higher, for every system.
- **Same host for database and workers.** No network latency between them. Real deployments add a
  network round trip to every claim, completion and heartbeat.
- **No-op handlers.** Measures queue overhead only.
- **Short runs.** 10,000 jobs per run. Long-running effects (table and index bloat over hours,
  autovacuum, Foleni's growing `succeeded` rows) are **not** measured.
- **Three repetitions.** Enough to see when numbers are noisy (look at the ranges), not enough for
  statistical claims.
- **A comparison, not a verdict.** db-scheduler is a mature scheduler with features Foleni does not
  have (recurring tasks, a dashboard ecosystem). These numbers say nothing about features.

## Reproduce

Needs Docker. From the repository root:

```bash
./mvnw -pl foleni-benchmarks -am install -DskipTests
./mvnw -pl foleni-benchmarks exec:exec                       # ~10 minutes
./mvnw -pl foleni-benchmarks exec:exec -Dbench.jobs=20000 -Dbench.reps=5   # bigger run
```

Results: `foleni-benchmarks/target/benchmark-results.md` and `benchmark-raw.csv`. Every run
produces slightly different numbers; compare ranges, not single values.
