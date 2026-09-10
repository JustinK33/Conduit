# 0002. Kafka is transport, not the queue

Status: accepted.
Date: 2025-07 (`77b8508`, corrected in `a8cb35f`).

## Context

Given [0001](0001-postgres-as-the-source-of-truth.md), Postgres already holds every job and `ClaimNextJob` can already hand work to a worker.
So Postgres alone is a complete queue, and the honest question is what Kafka adds.

It adds one thing: latency.
A Postgres-polling loop finds a new job somewhere between zero and one poll interval after it is enqueued.
Shortening the interval to hide that cost means every idle instance hammers the database with claim queries that return nothing.
The reconciler already backs off from 1s to 15s when the queue is empty for exactly that reason, which makes the worst-case pickup delay 15 seconds on an idle system.

Kafka removes that tradeoff. It is a push, so a worker learns about a job when the job exists rather than when it next looks.

The first version of the enqueue path published to Kafka synchronously, inside the HTTP request, before returning the job ID.
That put a broker round trip on the hot path of every enqueue.

## Decision

Kafka is a wake-up signal and nothing else.

Enqueue writes to Postgres, and that write is the commit.
The publish then happens in a goroutine (`internal/service/job_service.go`), and a publish failure is logged at warn level.
The caller already has their job ID and the job is already durable.

Consumers are a Kafka consumer group, so partition assignment distributes work across instances for free.
Offsets are marked only after the pool accepts the job (`internal/queue/kafka.go`), which makes delivery at-least-once.
A job arriving twice is fine, because the claim in Postgres decides who actually runs it.

Jobs with a future `scheduled_at` are not published at all (`shouldPublishImmediately`), because Kafka has no delayed delivery.
Those are the reconciler's job. See [0004](0004-leases-and-a-reconciler-instead-of-kafka-redelivery.md).

## Consequences

Good:

- Enqueue latency is a Postgres insert, measured in the README as API p50. Moving the publish off the hot path was the single largest latency win in the project's history (`77b8508`: p50 roughly 2ms down to 0.82ms on the hardware of the day).
- Dispatch latency is sub-second under normal operation instead of bounded by a poll interval.
- Kafka can be down and the system still accepts and eventually runs work. The README's measured results include a run with the broker killed, and the interesting number there is what dispatch latency degrades to: `RECONCILER_BATCH_SIZE` jobs per `RECONCILER_INTERVAL`, which with the defaults is a hard ceiling of 100 jobs per second no matter how many workers are free.

Costs, stated plainly:

- A broker to operate, for latency only. If a 15-second worst-case pickup delay is acceptable for the workload, Kafka is pure operational overhead and deleting it would make this a two-dependency system with no loss of correctness.
- Kafka is best-effort at runtime but **mandatory at startup**. `queue.NewKafkaClient` returns an error if the brokers are unreachable, and `run()` propagates it, so the process exits. The design says Kafka is optional; the boot sequence does not. That is a real inconsistency and it is not fixed.
- Two dispatch paths means two code paths into the worker. A Kafka-delivered job arrives `PENDING` and is claimed by the worker; a reconciler-delivered job is already `RUNNING` when it arrives. `jobWorker.Run` branches on `job.State` to handle both, and that branch is load-bearing.
- The async publish is one goroutine per enqueue with no bound. Under a burst large enough to make Kafka slow, that is unbounded goroutine growth. A bounded producer or a proper outbox is the correct fix and is listed as unbuilt in `docs/ROADMAP.md`.
