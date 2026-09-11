# Roadmap

This is the ordered list of what stands between Conduit and someone other than its author being able to use it.
It replaces the old `IMPROVEMENT_PLAN.md`, whose phases 1 to 3 are largely built and are recorded under [Already done](#already-done) below.

Phases are ordered by how hard they block adoption, not by how interesting they are.
Each has a "done when" so it is unambiguous whether it shipped.

## Phase 1 - A pull-based worker API (done)

Shipped. The protocol is [WORKERS.md](WORKERS.md), the reference worker is `loadtest/worker.sh`, and the deployment it assumes is [DEPLOYMENT.md](DEPLOYMENT.md).
Verified against a live stack with `CONDUIT_API_KEYS` set: a job enqueued to queue `remote` was claimed, executed, and completed by a shell script in a separate process; a second one failed with `retry: true`, went back to `PENDING` in one write, and reached `DEAD` when its attempt budget ran out, never appearing in `FAILED`.
A claim with no key returns 401, a stale token returns 409 on heartbeat, complete, and fail, and `GET /api/jobs/:id` on a RUNNING job carries no lease token.

The original write-up follows, since the reasoning is still what the design is for.

**The problem.** The only way to run your own code is to add an entry to a `map[string]JobHandler` in `cmd/server/main.go` and recompile the binary.
`docs/decisions/0005-webhook-as-the-execution-model.md` states this as a deliberate limitation, and it is the single fact that disqualifies Conduit for almost everything someone would otherwise reach for Celery or Sidekiq for.
Webhook execution covers the case where your work is already an HTTP endpoint, and nothing else.

**The change.** A worker process, in any language, claims a job over HTTP, executes it wherever it lives, and reports the outcome back.
The lease token Conduit already mints at claim time becomes the per-claim credential, so a worker that pauses past its lease and comes back finds its writes rejected rather than silently double-applied.

```
POST /api/jobs/claim              -> 200 {job, lease_token, lease_expires_at} | 204 no work
POST /api/jobs/:id/heartbeat      -> 200 {lease_expires_at}
POST /api/jobs/:id/complete       -> 200 {state}
POST /api/jobs/:id/fail           -> 200 {state, attempt, scheduled_at}
```

`Task.Queue` becomes the routing key it was always meant to be.
It has existed since the first migration, defaults to `default`, and until now appeared in zero `WHERE` clauses.
A remote worker claims from the queues it names, so it never wins a `webhook` job it has no code for.

Exposing a claim endpoint means exposing the ability to take work, so the minimum viable auth ships in the same phase: a shared bearer token from `CONDUIT_API_KEYS`, covering `/api/jobs` only, leaving `/live`, `/ready`, `/health`, and `/metrics` open for probes and scrapes.
Unset means auth is disabled, which is wrong for production and is logged as a warning at boot.

The retry decision moves out of the in-process worker and into the service layer, so the HTTP `fail` endpoint and the built-in executors produce byte-identical state transitions instead of two implementations that drift.
That also fixes a real bug: today's retry is two sequential updates, `RUNNING -> FAILED` then `FAILED -> PENDING`, and a crash between them leaves a job stuck in `FAILED` forever, because the reconciler only ever scans for `RUNNING`.

**Done when** a worker written in a language that is not Go, running in a process that is not the server, claims a job, heartbeats it, completes it, and the job reaches `COMPLETED`.
And when a claim without a key returns 401, a complete with a stale token returns 409, and `GET /api/jobs/:id` no longer leaks the lease token to whoever asks.

## Phase 2 - More than one tenant

Phase 1's API key is all-or-nothing: holding it means you can claim, cancel, and read every job in the instance.
Two people cannot share a deployment.

Needs per-key identity, a `tenant_id` column on `jobs`, and every read and claim scoped to it.
Keys stop being a config list and become rows, which means a key store, hashed at rest, and some way to issue and revoke them.

**Done when** two API keys on one instance cannot see or claim each other's jobs, proven by a test that tries.

## Phase 3 - Kafka and Redis become optional (done)

Shipped. `docker compose up` is Postgres, one migration run, and the app; a job enqueued onto that stack reached `COMPLETED` in 96 ms.
The dispatch table in the README has the Postgres row, and it is the fastest row in the table: p50 6.3 ms against Kafka's 14.9 ms and p95 8.9 ms against Kafka's 1,537.5 ms, reproduced across two runs.

The original write-up follows, since the reasoning is still what the design is for.

**The problem.** Conduit required Kafka, Postgres, and three Redis nodes to start.
That is five services to deploy for a queue, and the measurements in the README say Kafka buys dispatch latency only, while the three-node Redlock quorum has no measurable throughput cost over a single node and no correctness role at all given Postgres is the source of truth.
Kafka was mandatory at boot despite being explicitly best-effort at runtime, which is the worst of both.

**The change.** `CONDUIT_TRANSPORT=postgres|kafka`, defaulting to `postgres`, which sends wake-ups over `LISTEN`/`NOTIFY`.
A notification carries a job id and no authority: the reconciler's `FOR UPDATE SKIP LOCKED` claim is still the only thing that decides who executes, so all a notification does is cut the reconciler's sleep short.
That is why it can be dropped for free, and why it needs no queue filter and has no payload ceiling to design around.

`CONDUIT_LOCK=none|advisory|redlock`, defaulting to `advisory`, which uses `pg_try_advisory_lock` on a hashed job id.
An unrecognised value for either is a boot error rather than a fallback to the default: reading `CONDUIT_LOCK=redlok` as `none` would turn a typo into a missing guard.

`conduit migrate` came forward from phase 6, because the premise of this phase is false without it.
Migrations used to be applied by mounting `migrations/` into the Postgres entrypoint, which runs exactly once, on first boot, on an empty volume, so phase 1's migration 003 reached no existing deployment at all.
There is now exactly one mechanism: a `migrate` service that compose runs before the app on every `up`, recording each file in `schema_migrations` under an advisory lock so concurrent instances serialise.

Kafka, Redis, Prometheus, and Grafana moved behind compose profiles.
Compose rejects a `depends_on` that points at a profiled service, which is what turned "the app should tolerate them being absent" from a nice-to-have into a requirement the config layer enforces.

**Done when** `docker compose up postgres app` is a working Conduit and the dispatch-latency table has a row for the Postgres transport.

## Phase 4 - The default configuration is the good one

Out of the box Conduit drains about 55 jobs/s.
Tuned, the same hardware and the same batch does roughly 5x better, and the only differences are `CONDUIT_WORKER_QUEUE_SIZE` and `CONDUIT_RECONCILER_BATCH_SIZE`.
Whatever an evaluator measures in the first ten minutes is the number they remember, and right now that number is the bad one.

Also in this phase: the circuit breaker is a single process-global instance, so one dead endpoint opens it for every task type at once, and `ReleaseClaim` returns a job to `PENDING` without setting `scheduled_at`, so a job the pool keeps refusing spins in a claim-release loop as fast as the reconciler can tick.

**Done when** the untuned drain number is within 20% of the tuned one, and a task type whose endpoint is dead does not stop unrelated task types.

## Phase 5 - Operability under real load

- Queue priority, so an urgent queue is not stuck behind a batch job.
- Long-poll on claim, replacing poll-and-204, once claim QPS makes the polling overhead worth removing.
- Task-name filters on claim, finer-grained than queue routing.
- Manual retry and dead-letter requeue endpoints, so a `DEAD` job is recoverable without a SQL prompt.
- List responses that omit payloads by default.
- Grafana dashboards, or at minimum documented panels, for queue depth, latency, failures, retries, and worker saturation.
- Labels on the job metrics, which currently have none, so per-queue and per-task breakdowns are possible at all.

**Done when** running Conduit for a week does not require opening `psql`.

## Phase 6 - Something an adopter can actually pin

There are no tags, no releases, and no upgrade story.

- Tagged releases and a published container image an adopter can pin.
- A `needs: ci` gate on `cd.yml`, which currently publishes on any push to main even when CI is red.
- `deploy/k8s/deployment.yaml`'s `image: conduit:latest` changed to the real GHCR path.
- The `server` / `worker` split that the `migrate` subcommand made natural. `migrate` itself landed in phase 3, because the Postgres-only quickstart is not true without it.
- A `docker run` quickstart that does not require cloning the repo.
- One thin client library, in one language, so the API has at least one reference consumer.
- A Kubernetes `Secret` template, since `deployment.yaml` references `conduit-secrets` and the repo contains no example of it.

**Done when** the install instructions are a version number and not a git clone.

## Documentation hygiene

Tracked here because it keeps recurring, not because it is a phase.

- `PROJECT.md`'s environment table lists defaults that no longer match `internal/config/config.go`, and its statistics table says 5 API endpoints while the table above it lists 9.
- `docs/FEATURES.md` documents probe paths, a ConfigMap, and a CI wait loop that have all since changed.
- `CONDUIT_METRICS_SUBSYSTEM` defaults to `server` in code, is `service` in `.env.example`, and the README quotes `conduit_service_*`, so the metric prefix depends on whether you copied the example file. Pick one name.

## Already done

Carried forward from `IMPROVEMENT_PLAN.md` so the history is not lost.
Each of these has an ADR in [`docs/decisions/`](decisions/) explaining what it cost.

- Postgres is the source of truth, with a reconciler that claims due `PENDING` jobs. The transport is a wake-up path, not the queue, which is what made it optional in phase 3.
- Durable leases with fencing tokens on `RUNNING` jobs, so a crashed worker's jobs return to `PENDING` once the lease expires.
- `SELECT ... FOR UPDATE SKIP LOCKED` claims, so concurrent workers do not double-claim.
- Server-owned lifecycle fields: an enqueue caller cannot set `state`, `attempt`, or the lease columns.
- Explicit `idempotency_key` support, enforced by a unique partial index rather than by a read-then-write check.
- Webhook execution as the first task mechanism, adoptable without recompiling Conduit.
- Future-scheduled jobs get no wake-up at enqueue time.
- Indexes for the list and state-filtered views.

Two items from the old plan are deliberately not carried forward.
"Decide whether Go SDK handlers or container execution should be the second execution mechanism" is answered by phase 1, which picks a third option that needs neither.
"Choose whether the first deployment target is Compose or Kubernetes" is answered by phase 3 and phase 6: Compose first, because the point is that the quickstart should be small.
