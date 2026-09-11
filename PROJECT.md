# Conduit

A production-grade **distributed task queue** written in Go - built from scratch to demonstrate real-world backend engineering across the full distributed-systems stack.

---

## What It Does

Conduit is an HTTP-driven job queue service that accepts work from callers, durably persists it in PostgreSQL, publishes it to a Kafka topic, and executes it either through its own bounded worker pool or through workers you write that claim jobs over HTTP - all with automatic retries, distributed locking, circuit-breaking, and Prometheus observability.

```
Client
  │  POST /api/jobs
  ▼
API Handler (Gin)
  │  Enqueue(job)
  ▼
Job Service ──────────────► Kafka topic "conduit.jobs"
  │  CreateJob                   │
  ▼                              │ Consume (filtered by queue)
PostgreSQL                  Worker Pool ──► JobRunner
  │  PENDING → RUNNING               │
  │  RUNNING → COMPLETED             │ Retry Engine
  ▼  RUNNING → PENDING / DEAD        ▼
 State Machine               Circuit Breaker
                                    │
 Scheduler ──── cron entries        ▼
 (recurring jobs)           Redis Distributed Lock
                                    │
 Prometheus /metrics         observability

Your worker, anywhere
  │  POST /api/jobs/claim        → job + lease_token
  │  run it in your own process
  ▼  POST /api/jobs/:id/complete | /fail   (token fences the write)
Job Service - same methods, same retry policy
```

---

## Key Features

| Feature | Details |
|---|---|
| **Async job dispatch** | HTTP POST enqueues a job; returns the ID immediately |
| **Kafka transport** | Durable publish with per-message headers for tracing |
| **PostgreSQL state machine** | `PENDING → RUNNING → COMPLETED / DEAD` with `FOR UPDATE SKIP LOCKED` claim; a retry is one statement back to `PENDING` |
| **Worker pull API** | Claim, heartbeat, complete, fail over HTTP, so a worker can be any language on any host. `lease_token` is both the fencing token and the per-claim credential. |
| **API-key auth** | Bearer tokens on `/api/jobs`, constant-time compared, multiple keys for rotation |
| **Redlock distributed locking** | Multi-node quorum lock for exclusive resource coordination |
| **Exponential backoff** | Configurable base, multiplier, cap, and jitter |
| **Circuit breaker** | Closed / Open / Half-Open state machine protecting downstream calls |
| **Cron scheduler** | 5-field cron expressions with `*/n`, ranges, and lists - zero external dependencies |
| **Bounded worker pool** | Semaphore-controlled concurrency with graceful shutdown |
| **Prometheus metrics** | Counters, gauges, histograms exposed on `/metrics` |
| **Structured logging** | zerolog JSON logs with service/env/component fields |
| **Graceful shutdown** | SIGTERM drains in-flight jobs before exit |
| **Fully tested** | All packages tested with `-race`; the store integration test is gated behind `POSTGRES_TEST_DSN` |

---

## Architecture

### 12 packages, ~1 600 lines of production Go

```
cmd/server/          ← bootstrap, wiring, graceful shutdown
internal/
  api/               ← Gin handlers: enqueue, read, cancel, the worker pull protocol, API-key auth
  config/            ← env-var config loading + validation
  circuitbreaker/    ← Closed / Open / Half-Open state machine
  lock/              ← Redlock algorithm over multiple Redis nodes
  logger/            ← zerolog setup (JSON + pretty modes)
  metrics/           ← Prometheus counter/gauge/histogram registry
  queue/             ← Sarama Kafka producer + consumer group
  retry/             ← exponential backoff + jitter engine
  scheduler/         ← cron scheduler with built-in 5-field parser
  service/           ← Queue adapter: bridges API ↔ Kafka + Postgres; owns the retry decision
  store/             ← pgx v5 CRUD + state machine + `FOR UPDATE SKIP LOCKED`
  worker/            ← goroutine pool with semaphore and WaitGroup
pkg/models/          ← shared domain types (Job, Task, JobState, Config)
migrations/          ← SQL schema (001_create_jobs.sql)
loadtest/            ← k6 script, measurement harness, worker.sh reference worker
deploy/prometheus/   ← prometheus.yml scrape config
```

---

## Technology Stack

| Layer | Technology |
|---|---|
| Language | Go 1.23 |
| HTTP | Gin (`github.com/gin-gonic/gin`) |
| Message queue | Apache Kafka via IBM Sarama (`github.com/IBM/sarama`) |
| Database | PostgreSQL 16 via pgx v5 (`github.com/jackc/pgx/v5`) |
| Caching / Locking | Redis 7 via go-redis v9 (`github.com/redis/go-redis/v9`) |
| Observability | Prometheus (`github.com/prometheus/client_golang`) |
| Logging | zerolog (`github.com/rs/zerolog`) |
| Containerisation | Docker Compose (Kafka, Postgres, 3× Redis, Prometheus, Grafana) |
| Orchestration | Kubernetes - Deployment (3 replicas), Service, HPA (min 3 / max 10) |
| CI/CD | GitHub Actions - vet → unit tests → race detector → Docker build → k6 load test |

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/jobs` | Enqueue a new job; returns `{"id": "..."}` |
| `GET` | `/api/jobs` | List jobs; filter with `?state=`, page with `?limit=` and `?cursor=` |
| `GET` | `/api/jobs/by-idempotency-key/:key` | Fetch the job created for a given idempotency key |
| `GET` | `/api/jobs/:id` | Fetch job state and timestamps |
| `POST` | `/api/jobs/:id/cancel` | Cancel a job (transitions to `DEAD`) |
| `POST` | `/api/jobs/claim` | Claim the next due job; `200` with `{job, lease_token, lease_expires_at}` or `204` when nothing is due |
| `POST` | `/api/jobs/:id/heartbeat` | Extend the lease on a claimed job; returns `{lease_expires_at}` |
| `POST` | `/api/jobs/:id/complete` | Report success; merges `metadata` into the job |
| `POST` | `/api/jobs/:id/fail` | Report failure; returns `{state, attempt, scheduled_at}` |
| `GET` | `/metrics` | Prometheus scrape endpoint |
| `GET` | `/live` | Liveness probe (process only) |
| `GET` | `/ready` | Readiness probe (checks Postgres and Redis) |
| `GET` | `/health` | Combined health summary |

`state` must be one of `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `DEAD`, though `FAILED` is unreachable: nothing writes it, and a retrying job is `PENDING` with `attempt > 0`.
The enqueue body accepts `idempotency_key` and `scheduled_at` alongside `task`.

Everything under `/api/jobs` sits behind `CONDUIT_API_KEYS` when it is set, as `Authorization: Bearer <key>`.
The four claim-through-fail endpoints are the worker pull protocol; [docs/WORKERS.md](docs/WORKERS.md) is the contract, including what a worker does when a lease is lost.
The probe and scrape routes are deliberately outside auth, which is why a deployment needs a proxy to gate them: [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).

### Example

```bash
# Enqueue
curl -X POST http://localhost:8080/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{"task":{"name":"send-email","queue":"default","max_retries":3}}'
# → {"id":"4a7b1c2d-..."}

# Check status
curl http://localhost:8080/api/jobs/4a7b1c2d-...
# → {"ID":"4a7b1c2d-...","State":"COMPLETED",...}

# Cancel
curl -X POST http://localhost:8080/api/jobs/4a7b1c2d-.../cancel
# → {"status":"cancelled"}

# Claim as a worker, run it yourself, then report the outcome
curl -X POST http://localhost:8080/api/jobs/claim \
  -H 'Content-Type: application/json' \
  -d '{"queues":["remote"],"lease_seconds":60}'
# → {"job":{...},"lease_token":"9f2c...","lease_expires_at":"..."}  (or 204)

curl -X POST http://localhost:8080/api/jobs/4a7b1c2d-.../complete \
  -H 'Content-Type: application/json' \
  -d '{"lease_token":"9f2c...","metadata":{"worker":"gpu-3"}}'
# → {"state":"COMPLETED"}
```

`loadtest/worker.sh` is the same loop as a runnable script: `make worker queues=remote`.

---

## Metrics

All metrics are prefixed `conduit_service_*` by default (configurable via `CONDUIT_METRICS_NAMESPACE` / `CONDUIT_METRICS_SUBSYSTEM`).

| Metric | Type | Description |
|---|---|---|
| `jobs_enqueued_total` | Counter | Jobs accepted via the API |
| `jobs_started_total` | Counter | Jobs claimed by a worker |
| `jobs_completed_total` | Counter | Jobs finished successfully |
| `jobs_failed_total` | Counter | Jobs that errored |
| `jobs_cancelled_total` | Counter | Jobs cancelled by callers |
| `worker_in_flight` | Gauge | Concurrently executing jobs |
| `job_duration_seconds` | Histogram | End-to-end execution latency |
| `http_requests_total` | CounterVec | Requests by method / path / status |

---

## Running Locally

```bash
# Infrastructure only, so the server can run outside Docker.
# `make up` instead brings up the app container too, and Postgres applies
# migrations/ automatically on first boot either way.
docker compose up -d kafka redis redis-2 redis-3 postgres

# Build and run
make build
./bin/conduit

# In another terminal, enqueue a test job. `webhook` and `sql.etl` are the only
# registered handlers; any other name goes straight to DEAD.
curl -X POST http://localhost:8080/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{"task":{"name":"webhook","metadata":{"url":"https://example.com/hook"}}}'

# Metrics
curl http://localhost:8080/metrics | grep conduit

# Tear down
make down
```

### Environment Variables

Every variable Conduit reads is prefixed `CONDUIT_`, and an unprefixed name is ignored rather than half-honoured.
`API_KEYS` and `POSTGRES_DSN` are why: both are generic enough to already mean something else in a shared environment, and a silent collision on the key list is one that hands out a credential.
`.env.example` is the full list; the table below is the part you are most likely to change.

| Variable | Default | Description |
|---|---|---|
| `CONDUIT_POSTGRES_DSN` | `postgres://postgres:postgres@localhost:5432/conduit?sslmode=disable` | PostgreSQL connection string |
| `CONDUIT_KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker list |
| `CONDUIT_KAFKA_TOPIC` | `conduit.jobs` | Topic for job messages |
| `CONDUIT_REDIS_ADDRESSES` | `localhost:6379,localhost:6380,localhost:6381` | Comma-separated Redis addresses |
| `CONDUIT_HTTP_ADDRESS` | `:8080` | Server listen address. Set it to `127.0.0.1:8080` behind a local proxy. |
| `CONDUIT_API_KEYS` | empty | Comma-separated bearer tokens for `/api/jobs`, 16 characters minimum. Empty means the API is open. |
| `CONDUIT_TRUSTED_PROXIES` | empty | CIDRs whose `X-Forwarded-For` is believed. Empty trusts nothing and uses the peer address. |
| `CONDUIT_WORKER_QUEUES` | empty | Queues the in-process pool claims from. Empty means all of them, which competes with remote workers. |
| `CONDUIT_WORKER_CONCURRENCY` | `8` | Max parallel job executions |
| `CONDUIT_LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `CONDUIT_LOG_PRETTY` | `false` | Human-readable console output |

---

## Running Tests

```bash
# Unit tests (no infrastructure required)
make test

# Unit tests with race detector
make test-race

# Microbenchmarks
make bench
```

---

## Project Statistics

| Metric | Value |
|---|---|
| Go packages | 13 |
| Source lines (production) | ~1 600 |
| Test coverage | all packages (`-race` clean) |
| Infrastructure components | Kafka, PostgreSQL, 3× Redis, Prometheus, Grafana |
| Kubernetes manifests | Deployment, Service, ConfigMap, HPA |
| API endpoints | 9 job routes plus 4 operational |
| Prometheus metrics | 8 |
| Cron scheduler | built-in (zero external deps) |

---

## Performance

Every measured number lives in one place: the [Measured results](README.md#measured-results) section of the README, reproducible with `make measure`.

The tuning that drove the intake numbers:
- Made Kafka publish non-blocking (goroutine after Postgres write) - removed the Kafka round-trip from the HTTP hot path
- pgx pool: MaxConns 25 → 50, added MaxConnLifetime / MaxConnIdleTime / HealthCheckPeriod
- Kafka producer: snappy compression, 5ms flush frequency, 1MiB flush threshold, 256-deep channel buffer

`CONDUIT_WORKER_QUEUE_SIZE` is the knob that dominates execution throughput, not `CONDUIT_WORKER_CONCURRENCY`; the README explains why.
