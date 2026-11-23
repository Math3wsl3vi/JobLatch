# Observability

JobLatch reports what its workers do through a listener interface, `JobEventListener`, with no
dependency on any metrics library. Plug in Micrometer, Prometheus or OpenTelemetry with a small
adapter, or use the built-in `JobCounters` for in-memory totals.

Two sources of truth, used together:

| Source | Answers | Where |
|---|---|---|
| **Events** (this page) | What *this process* did: throughput, outcomes, handler latency, errors, utilization | `JobEventListener` |
| **The database** | What is *waiting*: backlog, oldest due job, dead jobs, stuck leases, across all processes | [`docs/sql/monitoring.sql`](sql/monitoring.sql), [operations.md](operations.md#monitoring) |

There is no "job enqueued" event on purpose: an enqueue inside the caller's transaction can still be
rolled back, so counting it would count jobs that never existed. Backlog numbers come from the
database.

## Attaching a listener

```java
JobEventListener events = ...;   // JobCounters, your Micrometer adapter, ...
WorkerPool pool = new WorkerPool(dataSource, config, handlers, events);
LeaseReaper reaper = new LeaseReaper(new LeaseManager(dataSource), Duration.ofSeconds(10), 100, events);
```

The same listener can serve several pools and a reaper. Without one, nothing is reported
(`JobEventListener.NONE`).

## Events

| Method | When | Useful for |
|---|---|---|
| `jobsClaimed(queue, requested, claimed, duration)` | every claim the poller runs, empty ones included | claim latency; jobs claimed per second; how often the queue is empty |
| `jobFinished(JobExecution)` | once per attempt, after the outcome was written (or failed to be) | throughput and latency per queue, type and outcome; failure classes; timeouts |
| `leaseLost(ClaimedJob)` | a heartbeat found another worker owns a running job | leases too short, or workers pausing (GC, CPU starvation) |
| `leasesReclaimed(ReclaimResult)` | a reaper pass took back expired jobs (only when > 0) | worker crashes |
| `backgroundError(component, queue, error)` | a background activity failed and will retry | database trouble, by component |

`JobExecution.outcome()` is one of:

| Outcome | Meaning |
|---|---|
| `SUCCEEDED` | done |
| `RETRY_SCHEDULED` | failed, will run again after a backoff delay |
| `DEAD` | failed on its last attempt, or permanently |
| `NOT_OWNER` | another worker owns the job now; nothing was recorded. A likely **duplicate execution**. |
| `NOT_RECORDED` | the outcome could not be written (database down); the job will run again after its lease expires |

`JobExecution` also carries `handlerDuration()`, `failure()` (what the handler threw, or `null`),
`failureClass()` (a safe tag value) and `timedOut()`.

`backgroundError` components: `CLAIM`, `HEARTBEAT`, `RECORD_OUTCOME`, `REAPER` (queue is `null`),
`NOTIFICATION_LISTENER` (the pool falls back to polling until it reconnects).

## Rules for listener implementations

- **Fast.** Methods run on JobLatch's own threads: the poller, worker threads, the heartbeat
  scheduler, the reaper. A slow listener slows processing down.
- **Thread-safe.** Many threads call it at once.
- **Must not throw.** JobLatch catches anything a listener throws (except `VirtualMachineError`),
  logs it at WARN, and carries on, so a metrics bug cannot stop job processing. That is tested: a
  listener that throws on every event, including an `Error`, does not stop a pool from finishing
  its jobs.
- **Low cardinality, no personal data.** Tag with queue, job type, outcome, component and exception
  *class*. Never with job IDs, worker IDs, payloads or exception *messages*: one time series per job
  overwhelms a metrics system, and messages and payloads can contain personal data.

## Built in: `JobCounters`

Thread-safe running totals in memory, no dependencies. Good for a status endpoint, tests and the
demo:

```java
JobCounters counters = new JobCounters();
WorkerPool pool = new WorkerPool(dataSource, config, handlers, counters);
...
JobCounters.Snapshot s = counters.snapshot();
// s.claimed(), s.emptyClaims(), s.succeeded(), s.retryScheduled(), s.dead(), s.notOwner(),
// s.notRecorded(), s.timedOut(), s.leasesLost(), s.reclaimedRequeued(), s.reclaimedDead(),
// s.finished(), s.averageHandlerTime(), s.maxHandlerTime(), s.errors()  // Map<Component, Long>
```

Totals live as long as the object and are not persisted. The demo (`joblatch-demo`) prints them at
the end of its run.

## Micrometer

JobLatch does not depend on Micrometer; copy this adapter into your application. It was compiled and
run against Micrometer 1.17.2.

```java
import dev.joblatch.core.ClaimedJob;
import dev.joblatch.core.JobEventListener;
import dev.joblatch.core.JobExecution;
import dev.joblatch.core.ReclaimResult;
import dev.joblatch.core.WorkerPool;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.Locale;

/** JobLatch events as Micrometer metrics. Copy into your application and adjust names to taste. */
public final class MicrometerJobEventListener implements JobEventListener {

    private final MeterRegistry registry;

    public MicrometerJobEventListener(MeterRegistry registry) {
        this.registry = registry;
    }

    /** Pool utilization: gauges are sampled by the registry, not pushed by events. */
    public static void bindPool(MeterRegistry registry, WorkerPool pool) {
        Gauge.builder("joblatch.pool.active", pool, WorkerPool::activeJobs)
                .tag("queue", pool.queue())
                .description("Jobs running right now")
                .register(registry);
        Gauge.builder("joblatch.pool.capacity", pool, WorkerPool::concurrency)
                .tag("queue", pool.queue())
                .description("Worker threads")
                .register(registry);
    }

    @Override
    public void jobsClaimed(String queue, int requested, int claimed, Duration duration) {
        registry.timer("joblatch.claim", "queue", queue).record(duration);
        registry.counter("joblatch.jobs.claimed", "queue", queue).increment(claimed);
    }

    @Override
    public void jobFinished(JobExecution execution) {
        // One timer per outcome: its count is "jobs finished with this outcome", its total time
        // and max come for free. Tags: low-cardinality values only, never IDs or messages.
        Timer.builder("joblatch.job.duration")
                .tag("queue", execution.queue())
                .tag("type", execution.jobType())
                .tag("outcome", execution.outcome().name().toLowerCase(Locale.ROOT))
                .register(registry)
                .record(execution.handlerDuration());
        if (execution.failure() != null) {
            registry.counter("joblatch.job.failures",
                    "queue", execution.queue(),
                    "type", execution.jobType(),
                    "exception", execution.failure().getClass().getSimpleName(),
                    "timed_out", Boolean.toString(execution.timedOut()))
                    .increment();
        }
    }

    @Override
    public void leaseLost(ClaimedJob job) {
        registry.counter("joblatch.leases.lost", "queue", job.queue()).increment();
    }

    @Override
    public void leasesReclaimed(ReclaimResult result) {
        registry.counter("joblatch.leases.reclaimed", "result", "requeued").increment(result.requeued());
        registry.counter("joblatch.leases.reclaimed", "result", "dead").increment(result.dead());
    }

    @Override
    public void backgroundError(Component component, String queue, Throwable error) {
        registry.counter("joblatch.background.errors",
                "component", component.name().toLowerCase(Locale.ROOT),
                "queue", queue == null ? "all" : queue)
                .increment();
    }
}
```

Wiring it up:

```java
MicrometerJobEventListener events = new MicrometerJobEventListener(meterRegistry);
WorkerPool pool = new WorkerPool(dataSource, config, handlers, events);
MicrometerJobEventListener.bindPool(meterRegistry, pool);
LeaseReaper reaper = new LeaseReaper(new LeaseManager(dataSource), Duration.ofSeconds(10), 100, events);
```

It produces these meters. The Prometheus names in brackets were checked by scraping Micrometer's
Prometheus registry (1.17.2):

| Meter | Type | Tags |
|---|---|---|
| `joblatch.job.duration` (`joblatch_job_duration_seconds`) | timer | `queue`, `type`, `outcome` |
| `joblatch.job.failures` (`joblatch_job_failures_total`) | counter | `queue`, `type`, `exception`, `timed_out` |
| `joblatch.jobs.claimed` (`joblatch_jobs_claimed_total`) | counter | `queue` |
| `joblatch.claim` (`joblatch_claim_seconds`) | timer | `queue` |
| `joblatch.leases.lost` (`joblatch_leases_lost_total`) | counter | `queue` |
| `joblatch.leases.reclaimed` (`joblatch_leases_reclaimed_total`) | counter | `result` = `requeued` / `dead` |
| `joblatch.background.errors` (`joblatch_background_errors_total`) | counter | `component`, `queue` |
| `joblatch.pool.active` (`joblatch_pool_active`), `joblatch.pool.capacity` (`joblatch_pool_capacity`) | gauges | `queue` |

A timer's count is "jobs finished with this outcome", so there is no separate counter per outcome.
In Prometheus a timer appears as a summary (`…_seconds_count`, `…_seconds_sum`) plus a
`…_seconds_max` gauge. For percentiles (p95 handler time), add `.publishPercentileHistogram()` to the
`Timer.builder(...)` call: it adds histogram buckets, so use it only where you need them.

For **OpenTelemetry**, either use Micrometer's OpenTelemetry bridge or write the same adapter
against the OpenTelemetry metrics API: one counter or histogram per row above.

## What to alert on

| Alert | Metric | Why |
|---|---|---|
| Jobs dying | rate of `joblatch.job.duration{outcome="dead"}` above zero | jobs need a person (see [failure-recovery.md](failure-recovery.md)) |
| Retries spiking | rate of `outcome="retry_scheduled"` far above normal | a dependency is failing |
| Duplicate risk | rate of `outcome="not_owner"` or `joblatch.leases.lost` above zero | leases too short or workers pausing; handlers ran twice |
| Workers crashing | `joblatch.leases.reclaimed` increasing | processes are dying mid-job |
| Database trouble | `joblatch.background.errors` increasing, by `component` | claims, heartbeats or outcome writes failing |
| Saturated pool | `joblatch.pool.active / joblatch.pool.capacity` near 1 for long periods | add threads or processes, **and** check the oldest-due-job query |
| Falling behind | monitoring query #2 (oldest due wait) above your budget | the best single signal; comes from the database, not events |
