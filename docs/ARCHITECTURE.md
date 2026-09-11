# Conduit Architecture

Reliable data workflow runtime in Go.
Jobs come in over HTTP, land in Postgres (durable), get fanned out through Kafka (transport), and are executed either by the bounded in-process worker pool or by a worker of your own that claims them over HTTP.
Redis Redlock prevents duplicate execution when multiple instances run against the same topic.
Built-in handlers include `webhook` delivery and `sql.etl` pipelines for Postgres-backed ELT workflows.
See [WORKERS.md](WORKERS.md) for the pull protocol and [DEPLOYMENT.md](DEPLOYMENT.md) for what has to sit in front of the server.

---

## System Diagram

```
                        ┌────────────────────────────┐
                        │         HTTP Client         │
                        └─────────────┬──────────────┘
                                      │ POST /api/jobs
                                      ▼
                        ┌────────────────────────────┐
                        │         Gin Router          │
                        │      (api/handler.go)       │
                        │  validates task.name ≠ ""   │
                        └─────────────┬──────────────┘
                                      │ JobService.Enqueue
                                      ▼
                   ┌──────────────────────────────────────┐
                   │             JobService                │
                   │       (service/job_service.go)        │
                   │                                       │
                   │  1. Generate UUID job ID              │
                   │  2. Persist job as PENDING  (sync)    │
                   │  3. Publish to Kafka topic  (async)   │
                   └───────┬──────────────────┬───────────┘
                           │ CreateJob         │ Publish (goroutine)
                           ▼                   ▼
              ┌──────────────────┐   ┌───────────────────────┐
              │  PostgresStore   │   │     KafkaClient        │
              │  (store/store)   │   │   (queue/kafka.go)     │
              │                  │   │                        │
              │  FOR UPDATE      │   │  Sarama SyncProducer   │
              │  SKIP LOCKED     │   │  at-least-once         │
              └──────────────────┘   └──────────┬────────────┘
                  source of truth               │ topic: "jobs"
                                                ▼
                                     ┌──────────────────────┐
                                     │     Kafka Broker      │
                                     │   (durable log)       │
                                     └──────────┬────────────┘
                                                │ consumer group
                                                ▼
                                     ┌──────────────────────┐
                                     │   kafkaJobHandler     │
                                     │  (cmd/server/main)    │
                                     │  Unmarshal → Submit   │
                                     └──────────┬────────────┘
                                                │
                                                ▼
                        ┌───────────────────────────────────────┐
                        │             Worker Pool                │
                        │          (worker/pool.go)              │
                        │                                        │
                        │  semaphore-bounded goroutines          │
                        │  buffered jobs channel                 │
                        └────────────────┬──────────────────────┘
                                         │ Run(ctx, job)
                                         ▼
                        ┌───────────────────────────────────────┐
                        │             jobWorker                  │
                        │          (cmd/server/main)             │
                        │                                        │
                        │  1. CircuitBreaker.Allow()?            │
                        │  2. Redlock.Acquire("job:exec:<id>")   │
                        │  3. execute(ctx, job)  ← handler hook  │
                        │  4. RecordSuccess / RecordFailure      │
                        │  5. JobService.Complete / Fail         │
                        │     (fenced on lease_token)            │
                        └───────────────────────────────────────┘

Parallel path - a worker of your own:

                        ┌───────────────────────────────────────┐
                        │        your worker process             │
                        │      (any language, any host)          │
                        │                                        │
                        │  POST /api/jobs/claim  → job + token   │
                        │  run it wherever you like              │
                        │  POST /api/jobs/:id/heartbeat          │
                        │  POST /api/jobs/:id/complete | /fail   │
                        └───────────────────────────────────────┘

                        Same JobService methods, same two
                        compare-and-swap statements, same retry
                        policy as the in-process path above.

Parallel path - Reconciler:

                        ┌───────────────────────────────────────┐
                        │            Reconciler                  │
                        │     (reconciler/reconciler.go)         │
                        │                                        │
                        │  adaptive tick loop                    │
                        │  claims due PENDING jobs               │
                        │  recovers expired RUNNING leases       │
                        │  submits work to Worker Pool           │
                        └───────────────────────────────────────┘
```

---

## Job State Machine

```
  PENDING ──► RUNNING ──► COMPLETED  (terminal)
     ▲            │
     └────────────┤  retry: one statement, PENDING with a future scheduled_at
                  │
                  └──► DEAD  (terminal, reachable from any state)
```

`CanTransition` in `store/store.go` guards every `UpdateJob` call. Illegal
transitions return `ErrInvalidTransition` - no silent state corruption.

`FAILED` is an unreachable state. Nothing writes it, so `GET /api/jobs?state=FAILED`
is always empty and a retrying job is `PENDING` with `attempt > 0` and `last_error`
set. It used to be the midpoint of a two-write retry, `RUNNING -> FAILED -> PENDING`,
which existed only because `CanTransition` forbids `RUNNING -> PENDING`. A crash in
that window left a row in `FAILED` holding a live lease token, which nothing
recovered, because `RequeueExpiredRunning` only scans `RUNNING`. `FailClaimedJob`
now does the whole move in one statement, with a `CASE` choosing `PENDING` or
`DEAD`. The enum value is kept for wire compatibility.

---

## Packages

### `cmd/server`
Main wiring. Stands up the HTTP server, Kafka consumer, and worker pool, then
blocks on SIGTERM/SIGINT. Implements `jobWorker` (the `worker.JobRunner` that
runs the circuit breaker / Redlock / execute chain) and `kafkaJobHandler` (the
consumer bridge that feeds the pool). Drains in-flight work before exit.

Both entry points into the in-process pool respect `CONDUIT_WORKER_QUEUES`: the reconciler
passes it to `ClaimNextJob`, and `kafkaJobHandler` filters on `task.queue` before
submitting, because the topic carries every job regardless of queue. Without both,
the server would win jobs meant for a remote worker and dead-letter them for having
no registered handler. Empty means every queue, which is the right default for a
single-node install and the wrong one the moment remote workers exist.

### `internal/etl`
Built-in SQL ELT task executor.
The `sql.etl` handler reads a pipeline spec from `task.payload`, validates target identifiers, rejects write-oriented extraction SQL, and executes `INSERT INTO target SELECT ...` against Postgres.
This turns the queue into a small data workflow runtime for operational analytics.

### `internal/api`
Nine Gin job endpoints, registered in `RegisterRoutes`. The whole group sits behind
`APIKeyAuth(h.APIKeys)`, which is a pass-through when `CONDUIT_API_KEYS` is unset.

| Method | Path | Notes |
|--------|------|-------|
| `POST` | `/api/jobs` | Requires `task.name`; returns the assigned job ID. Optional `idempotency_key` and `scheduled_at`. |
| `GET`  | `/api/jobs` | Cursor-paginated list. Query params: `state` (PENDING / RUNNING / COMPLETED / DEAD), `limit`, `cursor`. |
| `GET`  | `/api/jobs/by-idempotency-key/:key` | Look up the job a given idempotency key produced. |
| `GET`  | `/api/jobs/:id` | Reads directly from Postgres. Never includes `lease_token`. |
| `POST` | `/api/jobs/:id/cancel` | Transitions job to DEAD. |
| `POST` | `/api/jobs/claim` | Claims the next due job in the named queues. `200` with `{job, lease_token, lease_expires_at}`, or `204` when nothing is due. |
| `POST` | `/api/jobs/:id/heartbeat` | Extends the lease. Requires the token. |
| `POST` | `/api/jobs/:id/complete` | Marks COMPLETED and merges `metadata`. Requires the token. |
| `POST` | `/api/jobs/:id/fail` | Applies the retry policy, or dead-letters when `retry` is false. Requires the token. |

The last four are the pull protocol (`internal/api/worker.go`), documented for
worker authors in [WORKERS.md](WORKERS.md). Their request bodies are narrow DTOs
rather than a `models.Job`, deliberately: `UpdateJob` is a full-row overwrite whose
fencing predicate is disabled by an empty token, so nothing on a client-input path
is allowed to reach it. All three token-bearing endpoints return `409 lease_lost`
when the compare-and-swap matches zero rows.

Operational routes live on the root router in `cmd/server/main.go`, outside this
group and therefore outside auth, so that probes and the Prometheus scrape work
without a credential. That is why a reverse proxy has to gate them; see
[DEPLOYMENT.md](DEPLOYMENT.md).

| Method | Path | Notes |
|--------|------|-------|
| `GET` | `/metrics` | Prometheus scrape endpoint. |
| `GET` | `/live` | Liveness - process is up, no dependency checks. |
| `GET` | `/ready` | Readiness - probes Postgres and the Redis nodes. |
| `GET` | `/health` | Combined health summary. |

Metrics middleware counts requests by method / path / status.

### `internal/service`
`JobService` is the glue between the HTTP layer and the store + Kafka. It writes
to Postgres first (that's the commit), then publishes to Kafka in a goroutine.
Kafka being down doesn't fail the caller.
The job sits PENDING until the reconciler claims it or a reconnected consumer picks it up.

It also owns the outcome of every job, whoever ran it: `Claim`, `Heartbeat`,
`Complete`, and `Fail` are called by both the HTTP handlers and `jobWorker`. `Fail`
is where the retry decision lives, which is why it takes an id and a token rather
than a job: one entry point means the remote path and the in-process path cannot
drift into two retry policies. It costs one extra `SELECT`, on the failure path
only. Lease durations requested by a client are clamped to
`CONDUIT_RECONCILER_RUNNING_LEASE`, so a worker cannot park a job for a week.

### `internal/store`
`PostgresStore` with a pgx pool. The interesting bit is `ClaimNextJob`:
`SELECT ... FOR UPDATE SKIP LOCKED` lets multiple instances poll without
stepping on each other - each grabs a distinct row or moves on immediately.
No queue manager needed. It mints the `lease_token` in the same statement that
flips the row to `RUNNING`, and takes an optional queue filter where empty means
any queue.

`CompleteClaimedJob` and `FailClaimedJob` are the two writes that end a job. Both
are single statements fenced on `state = 'RUNNING' AND lease_token = $n`, and both
return `ErrLeaseLost` when that matches zero rows, which is how a worker learns its
lease expired and the job now belongs to someone else. `FailClaimedJob` chooses
`PENDING` with a future `scheduled_at` or `DEAD` on a `CASE` over whether a next-run
time was passed, so a retry is one round trip and has no intermediate state to crash
in.

### `internal/queue`
Thin Sarama wrapper. Exposes a `Publisher` interface so `JobService` doesn't
import the concrete type (easier to mock in tests). Consumer marks offsets only
after successful dispatch - at-least-once delivery.

### `internal/circuitbreaker`
Three-state FSM: Closed → Open → Half-Open → Closed. `jobWorker` calls
`Allow()` before every execution. When the circuit is open it returns false
immediately rather than piling goroutines into a broken downstream. One probe
is allowed per `OpenTimeout` to test recovery.

### `internal/lock`
Redlock over 3 independent Redis nodes. Lock key is `job:exec:<id>`. Quorum
is 2/3; if we can't get it, another instance already has the job. Release is
a Lua script - checks the token atomically before deleting so a slow worker
can't steal an expired lock it no longer owns.

### `internal/retry`
Exponential backoff with proportional jitter. The cap is applied twice - once
before jitter, once after - so `MaxDelay` is actually a ceiling regardless of
the jitter factor. `ErrNoRetry` skips remaining attempts for permanent failures.

### `internal/scheduler`
Tick-based cron runner with a built-in 5-field parser. No external dependency.
It handles cron-style scheduled callbacks only.

### `internal/reconciler`
Adaptive Postgres-backed recovery loop.
It claims due PENDING jobs, submits them to the worker pool, and recovers expired RUNNING leases.
When the queue is idle, it backs off to a slower polling interval to reduce steady-state database load.

### `internal/metrics`
Prometheus counters and histograms registered at startup:

| Metric | Type |
|--------|------|
| `jobs_enqueued_total` | Counter |
| `jobs_started_total` | Counter |
| `jobs_completed_total` | Counter |
| `jobs_failed_total` | Counter |
| `jobs_cancelled_total` | Counter |
| `worker_in_flight` | Gauge |
| `job_duration_seconds` | Histogram |
| `http_requests_total` | CounterVec (method/path/status) |

### `internal/logger`
zerolog setup. `WithComponent` adds a `component` field so you can filter by
subsystem in any structured log aggregator without grep-ing free-form text.

### `pkg/models`
Shared types (`Job`, `Task`, `JobState`) and config structs. Everything that
crosses a package boundary lives here so import cycles stay impossible.

---

## Design Decisions Worth Explaining

The five decisions that shaped the system live in [docs/decisions](decisions/),
one record each, with the consequences and the costs written out:

- [0001](decisions/0001-postgres-as-the-source-of-truth.md) Postgres is the source of truth, not Redis and not Kafka.
- [0002](decisions/0002-kafka-as-transport-not-as-the-queue.md) Kafka is transport, not the queue.
- [0003](decisions/0003-redlock-over-postgres-advisory-locks.md) Redlock over Postgres advisory locks, and why the fencing token matters more.
- [0004](decisions/0004-leases-and-a-reconciler-instead-of-kafka-redelivery.md) Leases and a reconciler, not Kafka redelivery.
- [0005](decisions/0005-webhook-as-the-execution-model.md) A webhook is the execution model, so Conduit is not a library.
- [0006](decisions/0006-a-pull-api-instead-of-a-worker-sdk.md) Workers pull over HTTP rather than importing an SDK, which is what amended 0005.

Two smaller choices that do not warrant their own record:

### Why a semaphore channel over `sync.WaitGroup` for the pool?
`WaitGroup` lets you wait for work to finish but doesn't bound how much work
starts. The channel acts as both a rate limiter (acquire before starting) and a
drain mechanism (close + wait). Panic recovery in the dispatch loop means one
bad job handler can't bring down the entire pool.

### Why no external cron dependency in the scheduler?
The 5-field parser is ~100 lines and covers every pattern we need. Adding a
library dependency for something this self-contained would be overkill, and
keeping it in-tree means we control the `Next()` behavior for tests.

---

## Stack

| | Technology |
|--|------------|
| HTTP | [Gin](https://github.com/gin-gonic/gin) |
| Broker | [Kafka](https://kafka.apache.org/) via [Sarama](https://github.com/IBM/sarama) |
| Database | [PostgreSQL 16](https://www.postgresql.org/) via [pgx v5](https://github.com/jackc/pgx) |
| Cache / lock | [Redis 7](https://redis.io/) × 3 via [go-redis v9](https://github.com/redis/go-redis) |
| Metrics | [Prometheus](https://prometheus.io/) + [Grafana](https://grafana.com/) |
| Logging | [zerolog](https://github.com/rs/zerolog) |
| Infra | Docker Compose (local), Kubernetes (`deploy/k8s/`) |
| CI | GitHub Actions - test to build to k6 load test |

---

## Running Locally

```bash
# Spin up everything (Kafka, 3× Redis, Postgres, Prometheus, Grafana, app):
docker compose up --build

# First run: apply the schema
docker exec -i conduit-postgres-1 psql -U conduit -d conduit < migrations/001_create_jobs.sql
```

Infrastructure only (run the server outside Docker):

```bash
docker compose up -d kafka redis redis-2 redis-3 postgres
docker exec -i conduit-postgres-1 psql -U conduit -d conduit < migrations/001_create_jobs.sql
go run ./cmd/server
```

Tests (no infra needed):

```bash
go test ./...                            # or: make test
go test -race ./...                      # or: make test-race
go test -bench=. -benchmem -run=^$ ./... # or: make bench
go vet ./...                             # or: make vet
```

Endpoints:

| | |
|--|--|
| `POST localhost:8080/api/jobs` | Enqueue |
| `GET  localhost:8080/api/jobs` | List (filter with `?state=`, `?limit=`, `?cursor=`) |
| `GET  localhost:8080/api/jobs/by-idempotency-key/:key` | Look up by idempotency key |
| `GET  localhost:8080/api/jobs/:id` | Status |
| `POST localhost:8080/api/jobs/:id/cancel` | Cancel |
| `POST localhost:8080/api/jobs/claim` | Claim the next due job (`204` when idle) |
| `POST localhost:8080/api/jobs/:id/heartbeat` | Extend the lease |
| `POST localhost:8080/api/jobs/:id/complete` | Report success |
| `POST localhost:8080/api/jobs/:id/fail` | Report failure |
| `GET  localhost:8080/live` `/ready` `/health` | Probes |
| `GET  localhost:8080/metrics` | Prometheus |
| `GET  localhost:9090` | Prometheus UI |
| `GET  localhost:3000` | Grafana (admin / admin) |

---

## Repo Layout

```
Conduit/
├── cmd/server/             # main + jobWorker + kafkaJobHandler
├── internal/
│   ├── api/                # HTTP handlers, pull protocol, API-key auth
│   ├── circuitbreaker/     # CB state machine
│   ├── config/             # env-var loading
│   ├── lock/               # Redlock
│   ├── logger/             # zerolog setup
│   ├── metrics/            # Prometheus collectors
│   ├── queue/              # Kafka client
│   ├── reconciler/         # Postgres-backed due-job and lease recovery
│   ├── retry/              # backoff engine
│   ├── scheduler/          # cron scheduler
│   ├── service/            # JobService
│   └── store/              # Postgres store
├── pkg/models/             # shared types
├── migrations/             # SQL
├── deploy/
│   ├── k8s/                # Deployment, Service, HPA, ConfigMap, Secret template
│   └── prometheus/
├── loadtest/               # k6 script, measurement harness, reference worker
└── .github/workflows/      # CI/CD
```
