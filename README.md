# Foleni

Durable background jobs for Java, on Postgres alone.

*Foleni* is Swahili for "queue".

> **Status: early development.** Not ready for production use. Nothing is published to Maven Central yet. The roadmap below shows what works today.

## Why

An application saves an order and then needs to send a receipt. If it writes to the database and then publishes a job to a separate broker, a crash between the two steps either loses the job or creates one for an order that was rolled back.

Foleni stores jobs in the same Postgres database as your data. A job is enqueued inside your own transaction, so the job and the business write commit together or not at all. There is no second system to run.

## Guarantees

These are the design goals. Each one is only claimed once its tests pass (see the roadmap).

- **At-least-once delivery.** A job that was committed will be run. It may run more than once, so handlers must be idempotent.
- **Transactional enqueue.** A job is visible to workers only if the transaction that created it commits.
- **Crash safety.** A job held by a worker that dies is returned to the queue when its lease expires.

Not provided:

- Exactly-once execution.
- Strict ordering across multiple workers.

## How it works

- **Claiming:** workers claim jobs with `FOR UPDATE SKIP LOCKED`, so many workers can pull from one table without blocking each other or taking the same job.
- **Leases:** a claimed job is leased for a limited time. A healthy worker extends the lease with a heartbeat.
- **Retries:** a failed job is retried with exponential backoff and jitter.
- **Dead letters:** a job that exhausts its attempts is moved to a dead-letter state with its last error.

## Roadmap

- [ ] 1. Schema and transactional enqueue
- [ ] 2. Claiming with `SKIP LOCKED`, tested under concurrent workers
- [ ] 3. Complete and fail, with completion guarded by lease owner
- [ ] 4. Leases, heartbeats, and a reaper for expired leases
- [ ] 5. Retries with backoff and jitter; dead-letter state
- [ ] 6. Worker pool with backpressure and graceful shutdown
- [ ] 7. Scheduled jobs; `LISTEN`/`NOTIFY` with polling as fallback
- [ ] 8. Crash test: workers killed at random, every job processed
- [ ] 9. Benchmarks against an established Java job library
- [ ] 10. Publish to Maven Central

## Design decisions

Written as each milestone lands.

## Failure modes

Written as each milestone lands.

## Benchmarks

None yet. Results will be published with the method, hardware, and Postgres configuration used.

## Modules

| Module | Purpose |
|---|---|
| `foleni-core` | The queue. Plain Java and JDBC, no framework dependencies. |
| `foleni-spring-boot-starter` | Auto-configuration for Spring Boot applications. Planned. |
| `foleni-demo` | Example application, also used by the crash test. |

## Building

Requires Java 21 and Docker (tests use Testcontainers).

```bash
./mvnw verify
```

## Licence

Apache License 2.0.