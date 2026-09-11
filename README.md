# Conduit

[![ci](https://github.com/JustinK33/Conduit/actions/workflows/ci.yml/badge.svg)](https://github.com/JustinK33/Conduit/actions/workflows/ci.yml) [![cd](https://github.com/JustinK33/Conduit/actions/workflows/cd.yml/badge.svg)](https://github.com/JustinK33/Conduit/actions/workflows/cd.yml)

Exactly-once execution is what every job queue's marketing implies and none of them can deliver: a worker can always die between doing the work and recording that it did.
Conduit ships at-least-once and says so out loud.
Postgres is the only place a job's fate is written, and a fencing token bounds the window where a duplicate is still possible.

## What it does

You POST a job, Conduit runs it, and it keeps running it across process crashes, broker restarts, and handler failures without anyone watching.

Your code runs it one of two ways.
Either a worker you write in any language claims jobs over HTTP and reports the outcome back, which is the pull API in [docs/WORKERS.md](docs/WORKERS.md), or you use one of the two handlers that ship in-process: `webhook` for outbound HTTP delivery and `sql.etl` for Postgres-to-Postgres pipelines defined in JSON.
A remote worker gets the same lease, the same fencing token, and the same retry policy as an in-process one, because both paths write through the same code.

The design decision everything else follows from is that Kafka holds no authority.
A job is durable the moment the Postgres insert commits, and the Kafka publish happens afterwards in a goroutine, so the HTTP response doesn't wait on a broker round trip.
That would normally be a data-loss bug, since a dropped publish means nothing ever consumes the job.
It isn't one here, because the reconciler polls Postgres directly with `FOR UPDATE SKIP LOCKED` and feeds the same worker pool.
Kafka is the fast path and the database is the floor.

Crash recovery works the same way rather than being a separate mechanism.
A worker that claims a job takes a lease with an expiry and renews it while running, so a process that dies mid-job leaves a `RUNNING` row with a stale `lease_expires_at`.
`RequeueExpiredRunning` finds those and moves them back to `PENDING` with `last_error` set to say why.
Nothing has to notice the crash.
Every state change goes through `CanTransition` and carries the claim's `lease_token` in its `WHERE` clause, so a worker whose lease already expired writes zero rows instead of overwriting a job someone else now owns.

## Architecture

```mermaid
flowchart TD
    client["HTTP client"] -->|"POST /api/jobs"| api["internal/api<br/>Gin, request id, error envelope"]
    api --> svc["internal/service<br/>JobService"]
    svc -->|"CreateJob, PENDING, synchronous"| pg[("Postgres<br/>jobs, source of truth")]
    svc -.->|"Publish to topic jobs, in a goroutine"| kafka["Kafka<br/>transport only"]
    kafka -->|"consumer group, filtered by queue"| pool["internal/worker<br/>semaphore-bounded pool"]
    pg -->|"ClaimNextJob, FOR UPDATE SKIP LOCKED"| recon["internal/reconciler<br/>polls, requeues expired leases"]
    recon --> pool
    pool --> jw["jobWorker<br/>breaker, Redlock, lease renewal"]
    jw -->|"SET NX on job:exec:id"| redis[("Redis x3<br/>Redlock quorum")]
    jw -->|"dispatch by task.name"| handlers["internal/webhook<br/>internal/etl"]
    jw -->|"Complete / Fail, fenced on lease_token"| svc
    remote["your worker<br/>any language, any host"] -->|"POST /api/jobs/claim"| api
    api -->|"job + lease_token"| remote
    remote -->|"heartbeat, complete, fail"| api
    jw --> prom["Prometheus /metrics"]
```

An enqueue writes the job to Postgres as `PENDING` and returns; the publish to Kafka is fire-and-forget behind it.
A consumer hands the message to the worker pool, which is bounded by a semaphore and a buffered channel, and `jobWorker` does the four things that have to happen in order: ask the circuit breaker, take the Redlock so two instances consuming the same topic can't run one job twice, execute the registered handler under `Task.Timeout`, then report the outcome back through `JobService`.
A remote worker enters at the same place from the other side: it claims a job, which is one `FOR UPDATE SKIP LOCKED` statement that hands back the row and its `lease_token`, and reports the outcome with that token.
Both paths end in the same two compare-and-swap statements, so a job's fate is written the same way whoever ran it.

A retryable failure computes a backoff delay and moves the job `RUNNING -> PENDING` with a future `scheduled_at` in one statement; an unknown task name goes straight to `DEAD` rather than looping.
The reconciler runs on its own ticker doing the two jobs Kafka can't: pulling `PENDING` work the broker never delivered, and rescuing `RUNNING` rows whose lease ran out.

[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) has the full diagram, the state machine, and a per-package breakdown.
[docs/decisions/](docs/decisions/) has the six decisions that shaped it, each with its costs written out.

## Quick start

```bash
cp .env.example .env
make up
```

That brings up the app, Postgres, Kafka, three Redis nodes, Prometheus, and Grafana, gated on health checks so the app doesn't start before its dependencies.

```bash
make enqueue url=https://example.com/webhook   # returns a job id
make status id=<that-id>
make list state=DEAD
make ready                                     # per-dependency readiness
```

`/live` always returns 200 and is what the Kubernetes liveness probe uses. `/ready` pings Postgres and the Redis quorum and returns 503 with per-dependency detail, which is what gates traffic.

`make enqueue-elt` runs the SQL pipeline example, and needs the demo tables from `migrations/002_create_elt_demo.sql`.

### Run your own worker

[`loadtest/worker.sh`](loadtest/worker.sh) is a complete worker in about 40 lines of shell.
Set `WORKER_QUEUES` on the server first, or the server's own pool competes for the jobs your worker is meant to run:

```bash
make worker queues=remote     # claims, executes, reports; Ctrl-C to stop
```

[docs/WORKERS.md](docs/WORKERS.md) is the protocol: the four endpoints, the lease contract, and what to do when you lose one.

Set `API_KEYS` in `.env` (`openssl rand -hex 32`) to stop the API being open, then pass the same key to the Make targets as `API_KEY=...`.
Conduit does not speak TLS, so anything beyond a laptop needs a reverse proxy in front: [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).

### Measure it

```bash
make bench      # microbenchmarks, no infra, about 55 seconds
make measure    # every number in the next section, about 12 minutes
```

`make measure` starts a webhook sink behind the `loadtest` Compose profile and runs [`loadtest/measure.sh`](loadtest/measure.sh), which recreates the `app` container between runs to change its configuration.
It needs `k6`, `jq`, and `python3`, and it will disrupt anything else you have running on that stack.
Individual sections run on their own: `loadtest/measure.sh drain`, `./loadtest/measure.sh crash`.

## Measured results

All numbers below come from `make measure` and `make bench` on an Apple M4, 10 cores, 16 GB RAM, macOS 26.3.1, with every service in Docker Compose on the same host.
That means the load generator, the broker, three Redis nodes, Postgres, and the app all contend for the same 10 cores, so treat these as relative and reproducible rather than as a capacity number for real hardware.

### Intake

k6, 100 VUs, 10 s ramp / 30 s hold / 10 s ramp-down, `WORKER_CONCURRENCY=8`.

| | |
| --- | --- |
| Accepted | 4,684 req/s, 234,279 jobs |
| API latency | p50 1.49 ms, p95 8.03 ms, p99 18.19 ms |
| Failed requests | 0 |
| Peak queue depth | 234,263 `PENDING` |

This is a re-measurement after the pull API landed, with `API_KEYS` unset so the auth middleware is the pass-through.
The previous run was 4,864 req/s at p50 1.39 ms, so the four new endpoints and the queue filter cost nothing outside run-to-run variance.

This measures intake only, and the peak queue depth is the tell: the API accepts jobs about 90x faster than the system executes them.
The default k6 task name is `k6-load-test`, which has no registered handler, so every one of those jobs is designed to fail on execution.
Worse, they fail through the *global* circuit breaker, which then opens and starves the real work, so an intake run and an execution run cannot be the same run.

### Execution

2,000 real `webhook` jobs against a local sink, timed from the first enqueue until all 2,000 reach `COMPLETED` or `DEAD`.
Latency is end-to-end wall clock out of Postgres (`updated_at - created_at`), not time spent inside the worker.

| Config | jobs/s | e2e p50 | e2e p99 |
| --- | --- | --- | --- |
| Defaults: concurrency 8, queue 256, reconciler batch 100 | 39.5 - 78.7 (5 runs) | 18.5 s | 26.8 s |
| `WORKER_QUEUE_SIZE=4096` | 195.3 | 1.69 s | - |
| `RECONCILER_BATCH_SIZE=2000` | 133.4 | 0.22 s | 14.0 s |
| All of the above, concurrency 32 | 530.5 | 1.86 s | 3.10 s |

The default configuration is the bottleneck, not the architecture.
Mean time inside a worker is 5.76 ms (`conduit_service_job_duration_seconds_sum / _count` over the 2,000-job run), so eight workers should manage roughly 1,400 jobs/s.
They manage 55.
A burst larger than `WORKER_QUEUE_SIZE=256` gets refused by the pool, falls off the Kafka fast path, and lands on the reconciler, whose `RECONCILER_BATCH_SIZE=100` claims per one-second tick becomes the real ceiling.
Raising the pool's buffer is the single biggest win because it keeps work on the fast path at all.

Run-to-run variance on the default config is about +/- 40% (39.6, 56.3, 54.0, 78.7, 39.5 jobs/s), which is why that row is a range.
Anything in this table under a 2x difference is noise.

### What Kafka buys

20 jobs enqueued one second apart onto an otherwise idle queue, so each one is dispatched with nothing ahead of it.

| Dispatch path | p50 | p95 | max |
| --- | --- | --- | --- |
| Kafka consumer | 14.9 ms | 1,537.5 ms | 2,568.6 ms |
| Reconciler only (broker killed) | 3,796.3 ms | 13,159.8 ms | 14,201.4 ms |

Roughly 255x on median dispatch latency, and that is Kafka's entire contribution.
The reconciler-only numbers are not a bug: an idle reconciler backs off to `RECONCILER_IDLE_INTERVAL=15s`, and a 13.2 s p95 is exactly what a 15 s poll looks like.
Throughput is unaffected either way, because at volume both paths converge on the same worker pool.

One honest gap: Kafka is best-effort at runtime but mandatory at boot.
`queue.NewKafkaClient` returns an error if the broker is unreachable and `run()` exits, so the design tolerates losing Kafka but not starting without it.

### What Redlock buys

Same 2,000-job drain, three-node Redlock quorum versus a single Redis node.

| Config | jobs/s (per run) |
| --- | --- |
| `redis:6379,redis-2:6379,redis-3:6379` | 39.6, 56.3, 54.0, 78.7, 39.5 |
| `redis:6379` | 67.7, 54.2, 54.0, 65.5 |

No measurable difference, and the ranges overlap completely.
The quorum is not free (N acquire plus N release round trips per job, roughly 200 ms on the contended path), it is just far cheaper than the dispatch ceiling above it.
It also isn't what makes execution safe: [ADR 0003](docs/decisions/0003-redlock-over-postgres-advisory-locks.md) explains why the `lease_token` fencing check in `UpdateJob` is the actual correctness mechanism and Redlock is defence against duplicated *effort*, like an outbound webhook no fencing token can undo.

### Crash recovery

20 jobs held in `RUNNING` against a sink that sleeps 8 seconds, `RECONCILER_RUNNING_LEASE=15s`, then `docker compose kill -s KILL app` and an immediate restart.

| | Run 1 | Run 2 | Run 3 |
| --- | --- | --- | --- |
| All 20 requeued to `PENDING` | 16.34 s | 23.65 s | 16.39 s |
| All 20 terminal | 936.98 s | 37.91 s | 31.37 s |

Requeue time is bounded below by the lease, which is the design working: nothing can requeue a job until its lease is provably dead, so a 15 s lease costs at least 15 s.
The default `RECONCILER_RUNNING_LEASE` is 5 minutes, so a real crash recovers in minutes, not seconds.

That 936.98 s outlier is not explainable from the code and did not reproduce in two subsequent runs, so it is reported rather than averaged away.
What the logs *did* explain: after a `SIGKILL`, the dead process's `job:exec:<id>` Redlock keys survive with their full 30 s TTL, so the restarted process burns 300 lock-acquire failures and up to 16 attempts per job churning claim-and-release until they expire.
Redlock's TTL is hardcoded and never renewed, which means a crash costs the lock TTL on top of the lease.

### Microbenchmarks

`make bench`, in-process, no infra. These bound the hot paths, they do not predict system throughput.

| | |
| --- | --- |
| `CanTransition` | 1.36 ns/op, 0 allocs |
| Circuit breaker, closed | 3.77 ns/op |
| Circuit breaker, 32 goroutines | 128.8 ns/op |
| Backoff with jitter | 6.87 ns/op |
| Pool dispatch | 733 ns/op, 1,363,948 jobs/s, 2 allocs |
| Pool submit, contended | 3,395 ns/op, 294,517 jobs/s |
| Cron parse | 211 - 303 ns/op, 12 - 14 allocs |

The pool can move 1.4 million jobs/s and the system drains 530.
Nothing in Go is the bottleneck here; the network round trips and the dispatch configuration are.

## What this is not

Conduit is not Temporal.
There is no durable execution, no replay of a function from an event history, no signals or timers or child workflows, and no DAG.
A job is one call to one handler, and if you need step three to depend on step two you need something else.
It is not Sidekiq or Asynq either.
Your code can run a job now, but it runs in *your* process behind an HTTP claim, not inside a library you imported: there is no decorator, no autodiscovery, no serialised closure, and no shared type between your worker and the queue.
[ADR 0006](docs/decisions/0006-a-pull-api-instead-of-a-worker-sdk.md) is why that trade was made, and [ADR 0005](docs/decisions/0005-webhook-as-the-execution-model.md) is what it replaced.
There is no UI, and there are no dependencies between jobs.

Celery is the closest comparison, and the one worth drawing carefully, because the scope is nearly the same: one task, retries with backoff, delayed execution, a cron ticker, workers fed by a broker.
Three things differ.
The broker is not the queue here: in Celery the message *is* the job and the result backend is a side table, so losing RabbitMQ loses queued work, whereas killing Kafka in the measurements above cost 255x on dispatch latency and zero jobs.
Delivery semantics are not a flag: Celery acks on receipt by default and silently drops a task whose worker dies, `acks_late=True` upgrades that to at-least-once with a visibility timeout, and neither mode has any equivalent of the fencing token, so two Celery workers can both finish the same task and both report success.
And defining a task costs more here.
`@app.task` on any function is the entire reason people reach for Celery, and the equivalent in Conduit is a process that polls `POST /api/jobs/claim` and reports back, which is more code than a decorator and buys you a worker that can be written in any language and does not have to trust the queue's runtime.
Celery is a library you import. Conduit is a service you claim from.

The stack is the honest problem.
Measured against its own numbers, Postgres alone with `FOR UPDATE SKIP LOCKED` would serve this entire design: it is the source of truth, the claim mechanism, the scheduler, and the recovery path already.
Kafka buys a 255x improvement in median dispatch latency and nothing else, which is a real win if you care about the difference between 15 ms and 3.8 s, and a broker to operate if you don't.
Redis buys cross-instance mutual exclusion that the fencing token already covers for correctness, and measurably nothing for throughput; it earns its place only because a duplicated *side effect* is not something a fencing token can undo.
Below the throughput where dispatch latency matters, the three-dependency stack is not worth it, and the version of Conduit that dropped Kafka and Redis would be smaller, cheaper, and almost as good.
I built all three because I wanted to know exactly what each one cost, and now the numbers above say so.

## What building this taught me

**`sarama.OffsetNewest` silently discards every job enqueued before the consumer group finishes joining.** The Kafka consumer was configured with `OffsetNewest`, which reads as a reasonable default and actually means "skip everything published before this group's rebalance completed". Measuring a 2,000-job drain with the reconciler disabled found 1,332 jobs stranded `PENDING` at `attempt=0` with not one log line, and `kafka-consumer-groups.sh --describe` confirmed it: group joined, all three partitions assigned, `LOG-END-OFFSET` 600, `CURRENT-OFFSET` empty, consuming nothing, permanently. This happens on every deploy and every rebalance, not just at first boot. The reason it was never a data-loss incident is the reconciler, so the architecture's central claim held up under a bug that would otherwise have been catastrophic, which is the most useful thing measurement told me all project.

**Turning on `Consumer.Return.Errors` without draining `Errors()` makes a dead consumer look exactly like an idle one.** The line `sc.Consumer.Return.Errors = true` had been there from the start; `grep -rn "Errors()" --include=*.go .` returned nothing. Sarama pushes consume and rebalance failures onto a channel nobody was reading, which is why the bug above produced total silence instead of an error. A metric would not have caught it either: "zero jobs dispatched via Kafka" is indistinguishable from "no jobs to dispatch" unless something is watching the transport itself.

**"Leave the offset uncommitted so Kafka redelivers it" is not how Kafka works.** When the worker pool was full, the handler returned an error to skip `MarkMessage`, and the log line said the job was left uncommitted for redelivery. Sarama commits the *highest* marked offset per partition, so the next message that succeeds commits straight past the unmarked one and it is never redelivered. With the reconciler off, that stranded 1,115 of 2,000 jobs. The recovery path was always Postgres; the code just claimed otherwise in a log message, which is worse than saying nothing.

**A `NULL` column scanned into a `string` broke every job without an idempotency key, with CI green.** `scanJob` read `idempotency_key` and `lease_token` into `string`, which fails on `NULL`, and every enqueue path in the load test sets an idempotency key. So the test suite and the k6 run both passed while the plain `POST /api/jobs` that the README documents returned a scan error. The fix is four pointers and a nil-to-empty flatten in one place; the lesson is that the happy path in your load generator is not the happy path in your docs.

**The default configuration, not the architecture, caps throughput at 55 jobs/s.** Mean execution time is 5.76 ms, eight workers is theoretically 1,400 jobs/s, and the measured rate was 55. `WORKER_QUEUE_SIZE=256` was the culprit: any burst past the buffer is refused by the pool, drops off the Kafka fast path, and inherits the reconciler's `RECONCILER_BATCH_SIZE=100`-per-tick ceiling. Raising just that one value took it to 195 jobs/s and all three knobs together to 530. I had reasoned about the fast path and the fallback path as alternatives, when in practice the defaults routed nearly all traffic down the slow one.

**A global circuit breaker plus a reconciler that re-claims immediately is a hot loop, not backpressure.** The first attempt at measuring throughput used the no-handler k6 task, which fails every job, which opens the one circuit breaker shared by all task types, which makes `jobWorker` release the claim, which the reconciler re-claims at 100/s, forever. 225,660 jobs `PENDING`, 41 terminal, CPU pinned. Two separate problems, both of which look fine in isolation: the breaker should be per task type, and `releaseClaim` should push `scheduled_at` out rather than making the job immediately eligible again.

**A retry engine nobody calls still passes its unit tests.** `internal/retry` computed exponential backoff correctly and had tests to prove it, and the worker never applied the result. Jobs entered `FAILED` and stopped there permanently. In the same pass I found `Attempt` was never incremented, the `PENDING -> RUNNING` transition wasn't happening at all, and `Task.Timeout` was parsed but never turned into a `context.WithTimeout`, so a hung handler hung forever. Every one of those is a component that worked in isolation and was not wired to anything. Testing the retry engine was never the same thing as testing that jobs retry.

**Deleting a state was the bug fix.** Retrying a job used to be two writes, `RUNNING -> FAILED` then `FAILED -> PENDING`, because `CanTransition` forbids `RUNNING -> PENDING` directly. A crash between them left a row in `FAILED` holding a live `lease_token`, and `RequeueExpiredRunning` only ever scans `RUNNING`, so that job was unrecoverable by any mechanism in the system. The fix was one statement that goes `RUNNING -> PENDING(scheduled_at)` or `RUNNING -> DEAD` on a `CASE`, which makes `FAILED` unreachable: nothing writes it now, and a retrying job is `PENDING` with `attempt > 0` and `last_error` set. The state was only ever observable for the milliseconds between two writes, and that window existing at all was the whole defect. The enum value stays for wire compatibility.

**A column nothing reads is not a routing key.** `task_queue` had existed since the first migration, defaulted to `'default'`, and appeared in zero `WHERE` clauses, so nothing noticed that `CreateJob` always wrote the field explicitly and a job enqueued without a queue landed as `''` rather than `'default'`. The moment claims started filtering on it, that was a routing bug: a worker claiming `default` could not see those rows. The same latent gap ran deeper - the Kafka consumer submitted every message to the local pool regardless of queue, so the server would have eaten a remote worker's jobs and dead-lettered them for having no handler, and the filter on the reconciler alone would not have caught it. A default that only exists in the schema is not a default.

**Adding jitter to a capped backoff uncaps it.** The engine clamped the delay to `MaxDelay` and then added a random jitter on top, which is the natural order to write and puts the result above the cap. It needs clamping again after the jitter. A backoff ceiling that the jitter can exceed is not a ceiling, and the test that would have caught it had to assert on the maximum over many runs rather than on one.

**A distributed lock is not a fencing token.** Redlock stops two workers starting the same job at once, and does nothing about a worker whose lease expired mid-execution writing `COMPLETED` over a job the reconciler already requeued and handed to someone else. That needed `lease_token` in the `UPDATE ... WHERE` clause so the stale write matches zero rows and returns `ErrInvalidTransition`. Everything the README claims about at-least-once rests on that one predicate, not on the three Redis nodes.

**Moving work off the hot path is worth measuring, and worth admitting the shortcut in.** The Kafka publish used to sit inline in `Enqueue`, so every HTTP request waited on a broker round trip. Firing it in a goroutine after the Postgres commit is what gets the 1.49 ms p50 and 4,684 req/s in the intake table above. The honest version of that story is that `go func()` around a `SyncProducer` is not what you'd ship at scale: it is one unbounded goroutine per enqueue, and Sarama's `AsyncProducer` is the real answer, which in turn requires draining an error channel in a background loop or you leak. I took the simpler one deliberately, and the reconciler is what makes it safe rather than lucky.

**A benchmark with no entry point can be broken for months.** Five `_bench_test.go` files existed and the Makefile had no target that ran them, so nothing did. When I added `make bench` it hung. `BenchmarkBreakerRecordSuccess` rebuilt the circuit breaker inside the loop behind `StopTimer`/`StartTimer`, which meant the measured time per iteration barely grew, so Go kept raising `b.N` looking for a stable measurement and never found one. `make bench` finishes in about 55 seconds now. Unrunnable code rots exactly as fast as unwritten code, and looks better on a file listing.

**`wg.Add` racing `wg.Wait` is invisible without `-race`.** `Pool.Stop` closed the jobs channel and waited, while the dispatcher goroutine was still calling `wg.Add(1)` per dispatch. Under normal `go test` it passed every time. The fix is to put the dispatcher itself in the WaitGroup so nothing can `Add` after `Wait` starts. This is the class of bug where "it works on my machine" and "it works" have no relationship to each other, and the race detector in CI is the only reason I know about it.

**CI waiting for something that already died will wait for the full six hours.** The load-test job polled for server readiness in an unbounded loop, so a container that crashed at startup produced a job that sat there until GitHub's default timeout killed it. It now waits on `docker compose --wait` for infrastructure health, caps the readiness poll at 90 seconds, checks the process is still alive between polls, and carries a 15 minute job timeout. Every wait in automation needs a deadline and a liveness check, not one or the other.

**Offset pagination is a scan you pay for later.** `GET /api/jobs` uses keyset pagination over `(created_at DESC, id DESC)` behind an opaque base64 cursor, rather than `LIMIT/OFFSET`. It costs a slightly awkward API and buys stability under concurrent inserts, where an offset query silently skips or repeats rows as the table shifts underneath the pages.

## Documentation

- [docs/decisions/](docs/decisions/) is six decision records with a column for the uncomfortable part of each: Postgres as the source of truth, Kafka as transport only, Redlock over advisory locks, leases instead of Kafka redelivery, the webhook execution model, and workers pulling over HTTP instead of importing an SDK.
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) is the reference: system diagram, job state machine, and a package-by-package breakdown.
- [docs/WORKERS.md](docs/WORKERS.md) is how you write a worker: the four endpoints, the lease and heartbeat contract, what to do when you lose a lease, and the two wire-format quirks a non-Go client hits.
- [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) is the reverse proxy recipe, in Caddy and nginx, and why one is not optional: Conduit has no TLS and four endpoints that are deliberately unauthenticated.
- [PROJECT.md](PROJECT.md) has the API surface with curl examples and response shapes, plus the config table.
- [docs/FEATURES.md](docs/FEATURES.md) goes deep on four pieces: the async publish and its tradeoff, Redlock, the Kubernetes manifests, and the CI/CD pipeline.
- [docs/use-cases/sql-elt.md](docs/use-cases/sql-elt.md) walks a real pipeline config, with [examples/daily_revenue_pipeline.json](examples/daily_revenue_pipeline.json) as the input.
- [docs/ROADMAP.md](docs/ROADMAP.md) is the ordered list of what stands between this and someone else being able to use it. Phase 1 is the pull API above; next is not needing Kafka and Redis to run it at all.
- [migrations/](migrations/) is the schema, applied by Postgres on first boot.

## Tech stack

| Layer | What it uses |
| --- | --- |
| Language | Go 1.23 |
| HTTP | Gin, with request-id, structured-logging, metrics, and panic-recovery middleware |
| State | PostgreSQL via `pgx/v5`, pool tuned and exposed as env vars |
| Transport | Kafka via IBM `sarama`, snappy compression, 5 ms flush |
| Locking | Redis via `go-redis/v9`, 3-node Redlock quorum |
| Logs | `zerolog` |
| Metrics | `prometheus/client_golang`, scraped by Prometheus, Grafana alongside |
| Deploy | Docker Compose for local, Kubernetes manifests under `deploy/k8s` with an HPA |
| Checks | `go vet`, `go test -race`, five benchmark suites, k6 load test in CI |

Six direct requires.

## Testing

```bash
make vet
make test
make test-race
make bench
```

That is what CI runs before it boots a live stack and drives it with k6.
The store has one integration test gated behind an env var, because the `NULL`-scan bug above needed a real Postgres to reproduce:

```bash
POSTGRES_HOST_PORT=5434 make up
POSTGRES_TEST_DSN='postgres://conduit:conduit@localhost:5434/conduit?sslmode=disable' go test ./internal/store
```

Every Makefile target is commented inline, and `make down` wipes the volumes for a clean start.
