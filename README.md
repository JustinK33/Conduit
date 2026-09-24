# Conduit

[![ci](https://github.com/JustinK33/Conduit/actions/workflows/ci.yml/badge.svg)](https://github.com/JustinK33/Conduit/actions/workflows/ci.yml) [![license](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

A durable job queue where Postgres is the only place a job's fate is written.
You POST a job, Conduit runs it, and it keeps running it across process crashes, broker restarts, and handler failures without anyone watching.

Exactly-once execution is what every job queue's marketing implies and none of them can deliver: a worker can always die between doing the work and recording that it did.
Conduit ships at-least-once, says so out loud, and bounds the window where a duplicate is still possible with a fencing token.

A job is one-off or recurring, and your code runs it either as a worker you write in any language that claims over HTTP, or through one of the two handlers that ship in-process: `webhook` and `sql.etl`.
The default install is two containers, because both the transport and the distributed lock are optional.

> **~600 jobs/s end to end on the default settings, worst e2e p95 1.27 s**, on one Apple M4 where the load generator, Postgres, and the app all share 10 cores.
> That is the floor for the untuned configuration; tuning past it measured as noise.
> [docs/BENCHMARKS.md](docs/BENCHMARKS.md) has every number, the hardware caveat, and how to reproduce it.

## Architecture

```mermaid
flowchart TD
    client["HTTP client"] -->|"POST /api/jobs"| api["internal/api<br/>Gin, request id, error envelope"]
    api --> svc["internal/service<br/>JobService"]
    svc -->|"CreateJob, PENDING, synchronous"| pg[("Postgres<br/>jobs, source of truth")]
    svc -.->|"wake-up, after the commit"| transport["transport<br/>pg_notify (default) or Kafka"]
    transport -->|"wake the reconciler / consume by queue"| pool["internal/worker<br/>semaphore-bounded pool"]
    pg -->|"ClaimNextJob, FOR UPDATE SKIP LOCKED"| recon["internal/reconciler<br/>polls, requeues expired leases"]
    recon --> pool
    pool --> jw["jobWorker<br/>breaker, exec lock, lease renewal"]
    jw -->|"job:exec:id"| locker[("pg_try_advisory_lock (default)<br/>or Redis x3 Redlock")]
    jw -->|"dispatch by task.name"| handlers["internal/webhook<br/>internal/etl"]
    jw -->|"Complete / Fail, fenced on lease_token"| svc
    remote["your worker<br/>any language, any host"] -->|"POST /api/jobs/claim"| api
    api -->|"job + lease_token"| remote
    remote -->|"heartbeat, complete, fail"| api
    jw --> prom["Prometheus /metrics"]
```

The transport holds no authority.
A job is durable the moment the Postgres insert commits and the wake-up happens after it, so a dropped wake-up costs latency and never work: the reconciler polls Postgres directly with `FOR UPDATE SKIP LOCKED` and feeds the same worker pool.
The transport is the fast path and the database is the floor.
[docs/DESIGN.md](docs/DESIGN.md) is the rest of that argument, including what Conduit deliberately is not.

## Quick start

No clone. The image carries its own migrations, so the whole install is a Postgres, one migration run, and the server.

```bash
docker network create conduit

docker run -d --name conduit-pg --network conduit \
  -e POSTGRES_USER=conduit -e POSTGRES_PASSWORD=conduit -e POSTGRES_DB=conduit \
  postgres:16-alpine

DSN='postgres://conduit:conduit@conduit-pg:5432/conduit?sslmode=disable'

docker run --rm --network conduit -e CONDUIT_POSTGRES_DSN="$DSN" \
  ghcr.io/justink33/conduit:v0.3.1 migrate

docker run -d --name conduit --network conduit -p 8080:8080 \
  -e CONDUIT_POSTGRES_DSN="$DSN" \
  ghcr.io/justink33/conduit:v0.3.1
```

```bash
curl -X POST localhost:8080/api/jobs -H 'content-type: application/json' \
  -d '{"task":{"queue":"default","name":"webhook","metadata":{"url":"https://your-endpoint.example/hook","method":"POST"},"timeout":"15s"}}'
```

`amd64` and `arm64` images are published, so that runs natively on an Apple Silicon or Graviton machine rather than under emulation.
The tag is a pin: `:v0.3.1` never moves, `:latest` follows the newest release, and `:main` follows the tip of this branch.
The v0.3 line changes defaults only and takes a v0.2.0 client unmodified; v0.2.0 changed the wire format, so a client written against v0.1.0 needs [docs/UPGRADING.md](docs/UPGRADING.md).

That quickstart is a local trial and not a deployment: the database password is `conduit`, there is no TLS, and `CONDUIT_API_KEYS` is unset so anything that can reach port 8080 can enqueue work.
[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) is the checklist for the real thing.

Working on Conduit itself, scheduling recurring work, or running your own worker: [docs/DEVELOPING.md](docs/DEVELOPING.md) is `make up` and everything after it.

## Documentation

- [docs/DESIGN.md](docs/DESIGN.md) is why it is shaped this way: the transport with no authority, the path a job takes, what Conduit is not (Temporal, Sidekiq, and Celery drawn carefully), and three things building it taught me.
- [docs/BENCHMARKS.md](docs/BENCHMARKS.md) is every measured number: intake, execution, what Kafka buys, what Redlock buys, crash recovery, microbenchmarks.
- [docs/decisions/](docs/decisions/) is seven decision records with a column for the uncomfortable part of each: Postgres as the source of truth, Kafka as transport only, Redlock over advisory locks, leases instead of Kafka redelivery, the webhook execution model, workers pulling over HTTP instead of importing an SDK, and a strict wire contract that breaks every older client on purpose.
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) is the reference: system diagram, job state machine, and a package-by-package breakdown.
- [docs/SCHEDULES.md](docs/SCHEDULES.md) is recurring work: the cron syntax and the one place it deviates from Vixie cron, why everything is UTC, why a missed fire is skipped rather than replayed, and how ten replicas racing the same fire produce one job.
- [docs/WORKERS.md](docs/WORKERS.md) is how you write a worker: the four endpoints, the lease and heartbeat contract, what to do when you lose a lease, and the exact request and response shapes.
- [docs/DEVELOPING.md](docs/DEVELOPING.md) is the clone workflow: `make up`, the Make targets, testing, and the tech stack.
- [docs/UPGRADING.md](docs/UPGRADING.md) is what to change per release. v0.2.0 broke the wire format, including the outbound webhook envelope, so a v0.1.0 client and any endpoint receiving Conduit webhooks both need a change.
- [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) is the reverse proxy recipe, in Caddy and nginx, and why one is not optional: Conduit has no TLS and four endpoints that are deliberately unauthenticated.
- [PROJECT.md](PROJECT.md) has the API surface with curl examples and response shapes, plus the config table.
- [docs/FEATURES.md](docs/FEATURES.md) goes deep on four pieces: the async publish and its tradeoff, Redlock, the Kubernetes manifests, and the CI/CD pipeline.
- [docs/use-cases/sql-elt.md](docs/use-cases/sql-elt.md) walks a real pipeline config, with [examples/daily_revenue_pipeline.json](examples/daily_revenue_pipeline.json) as the input.
- [docs/ROADMAP.md](docs/ROADMAP.md) is the ordered list of what stands between this and someone else being able to use it. Phase 1 is the pull API, phase 3 is the Postgres-only default, phase 4 is the defaults and the per-task breaker, phase 6 is the pinned image in the quick start, phase 7 is the wire contract; next is multi-tenancy and operability.
- [migrations/](migrations/) is the schema, applied by `conduit migrate` (`make migrate`, and automatically on every `make up`).

## License

[Apache-2.0](LICENSE).
