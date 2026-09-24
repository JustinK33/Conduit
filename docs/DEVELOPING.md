# Developing

Working on Conduit itself, or reproducing [the measurements](BENCHMARKS.md).

## From a clone

```bash
cp .env.example .env
make up
```

That brings up Postgres, one migration run, and the app, gated on health checks so the app doesn't start before its schema exists.
Nothing else is required.

Kafka, Redis, Prometheus, and Grafana are behind compose profiles.
`make up-full` starts all of them and points the app at them, which is what reproducing the measurements needs.

```bash
make enqueue url=https://example.com/webhook   # returns a job id
make status id=<that-id>
make list state=DEAD
make ready                                     # per-dependency readiness
```

`/live` always returns 200 and is what the Kubernetes liveness probe uses. `/ready` pings Postgres, plus the Redis quorum when Redlock is on, and returns 503 with per-dependency detail, which is what gates traffic.

`make enqueue-elt` runs the SQL pipeline example, and needs the demo tables from `migrations/002_create_elt_demo.sql`.

Every Makefile target is commented inline, and `make down` wipes the volumes for a clean start.

## Make it recurring

```bash
curl -X POST localhost:8080/api/schedules -H 'content-type: application/json' \
  -d '{"name":"nightly-revenue","cron":"0 3 * * *","task":{"name":"sql.etl","timeout":"10m","payload":{"source_table":"raw.orders","target_table":"analytics.daily_revenue"}}}'

curl localhost:8080/api/schedules     # next_run_at, last_run_at, last_job_id
```

Cron runs in UTC, missed fires are skipped rather than replayed, and running ten replicas still produces one job per fire instant.
[SCHEDULES.md](SCHEDULES.md) is the contract: supported syntax, the reserved `sched:` idempotency prefix, and how the dedup works.

## Operate it

```bash
curl -X POST localhost:8080/api/jobs/$ID/requeue   # a DEAD job back to PENDING
```

Completed jobs are pruned after seven days by default (`CONDUIT_RETENTION_COMPLETED=0` keeps them forever), and `conduit_server_jobs_backlog{state}` answers whether the queue is keeping up.
The job counters carry a `task` label bounded to the registered handlers plus `other`.
[DEPLOYMENT.md](DEPLOYMENT.md) has the PromQL and the retention knobs.

## Run your own worker

[`loadtest/worker.sh`](../loadtest/worker.sh) is a complete worker in about 40 lines of shell.
Set `CONDUIT_WORKER_QUEUES` on the server first, or the server's own pool competes for the jobs your worker is meant to run:

```bash
make worker queues=remote     # claims, executes, reports; Ctrl-C to stop
```

[WORKERS.md](WORKERS.md) is the protocol: the four endpoints, the lease contract, and what to do when you lose one.

Set `CONDUIT_API_KEYS` in `.env` (`openssl rand -hex 32`) to stop the API being open, then pass the same key to the Make targets as `API_KEY=...`.
Conduit does not speak TLS, so anything beyond a laptop needs a reverse proxy in front: [DEPLOYMENT.md](DEPLOYMENT.md).

## Testing

```bash
make vet
make test
make test-race
make bench
```

That is what CI runs before it boots a live stack and drives it with k6.
The store has one integration test gated behind an env var, because a `NULL`-scan bug in `scanJob` needed a real Postgres to reproduce:

```bash
POSTGRES_HOST_PORT=5434 make up
POSTGRES_TEST_DSN='postgres://conduit:conduit@localhost:5434/conduit?sslmode=disable' go test ./internal/store
```

## Tech stack

| Layer | What it uses |
| --- | --- |
| Language | Go 1.23 |
| HTTP | Gin, with request-id, structured-logging, metrics, and panic-recovery middleware |
| State | PostgreSQL via `pgx/v5`, pool tuned and exposed as env vars |
| Transport | Postgres `LISTEN`/`NOTIFY` by default; Kafka via IBM `sarama` (snappy, 5 ms flush) with `CONDUIT_TRANSPORT=kafka` |
| Locking | `pg_try_advisory_lock` by default; 3-node Redis Redlock via `go-redis/v9` with `CONDUIT_LOCK=redlock` |
| Logs | `zerolog` |
| Metrics | `prometheus/client_golang`, scraped by Prometheus, Grafana alongside |
| Deploy | `ghcr.io/justink33/conduit`, `amd64` and `arm64`, published only after the full suite and the k6 load test pass. Docker Compose for local, Kubernetes manifests under `deploy/k8s` with an HPA |
| Checks | `go vet`, `go test -race`, five benchmark suites, k6 load test in CI |

Six direct requires.
