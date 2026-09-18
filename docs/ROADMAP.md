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

## Phase 4 - The default configuration is the good one (done)

Shipped as one default change, one breaker per task type, and a delay on release.
Untuned now measures 606, 298, and 277 jobs/s against 419, 305, and 249 for the fully tuned configuration, so the untuned number is no longer the bad one.

**The premise was measured on the wrong stack, and the measurement had to come first.**
This phase was written around "the defaults drain about 55 jobs/s, tuned does 9.6x better (530.5)".
Both of those came from `loadtest/measure.sh`'s `drain` and `drain-tuned`, which route through `base_app`, which pins `CONDUIT_TRANSPORT=kafka` and `CONDUIT_LOCK=redlock` - and phase 3 made the defaults Postgres and advisory locks.
On the real default stack the untuned defaults were already doing about 260-320 jobs/s before anything changed.
`drain-postgres` and `drain-postgres-tuned` exist now, built on the `postgres_app` helper, and they are what the numbers above are.

**The transport decides which knob matters, so the README's own tuning advice was wrong for the default.**
On Kafka a burst past `CONDUIT_WORKER_QUEUE_SIZE` fills the pool buffer and spills onto the reconciler, which is why the queue size dominated there.
On `LISTEN`/`NOTIFY` the reconciler is the only claim path, so `CONDUIT_RECONCILER_BATCH_SIZE` is the only knob with a measurable effect: queue size and concurrency measured as noise, alone and together, and 32 workers with 50 Postgres connections bought nothing over 8 and 10.
So exactly one default changed, `BatchSize` 100 to 500, and it is a tail-latency argument more than a throughput one: across six back-to-back pairs the throughput margins were mostly inside the noise band, but the worst e2e p95 was 4.45 s at 100 against 1.68 s at 500.

**The claim-release loop was one bug, not the two it was written down as.**
The breaker was a single process-global instance, so one dead endpoint opened it for every task type at once, and `ReleaseClaim` returned a job to `PENDING` without touching `scheduled_at`, so the reconciler re-claimed it on the very next tick.
The diagnosis this used to carry was also wrong: the loop is not the reconciler being refused by a full pool.
`internal/reconciler/reconciler.go` submits with `SubmitBlocking`, which blocks rather than rejecting, so that path self-throttles and a large `BatchSize` is a claim budget rather than a pathology.
The release was the only unthrottled edge, which also said what the delay should be, so `ReleaseClaim` now takes a `retryAfter`: the breaker's own `OpenTimeout` for a circuit-open release, `lockContentionBackoff` for a lock-contention one, and one reconciler interval for the shutdown case.
The breakers are built eagerly from the handler table with one shared breaker for unregistered names, which is bounded by construction and needs no mutex; a lazily-populated map keyed on `job.Task.Name` would grow without limit on caller input.

Verified end to end: six `webhook` jobs against a dead endpoint trip their own breaker while a `sql.etl` job enqueued behind them still reaches `COMPLETED`, and the job that met the open circuit logged exactly one release rather than one per tick.

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

## Phase 6 - Something an adopter can actually pin (done)

Shipped. The README's install is four `docker run` commands against `ghcr.io/justink33/conduit:v0.1.0` and no clone, verified end to end from a directory outside the repository: three migrations applied out of the image, `/ready` green, and a job to `COMPLETED` in 18 ms.

Two things turned up that were not on the original list and mattered more than most of what was.

**There was no `LICENSE`.** The repo had been public for months with `licenseInfo: null`, which means default copyright: no legal right to use, modify, or redistribute it.
Every other item in this phase was about making Conduit easy to adopt, while the thing actually forbidding adoption was a missing file.
It is Apache-2.0 now, chosen over MIT for the explicit patent grant, which is what a company's legal review looks for in infrastructure.

**The image was `amd64` only.** `Dockerfile` hardcoded `GOARCH=amd64`, so the one command this phase exists to make work would have run under emulation on any Apple Silicon or Graviton machine.
`ARG TARGETARCH` plus QEMU and buildx publishes both architectures.
Worth noting the failure mode the hardcoded value would have caused once buildx was introduced: an `arm64` image containing an `amd64` binary, which fails at exec time rather than at build time.

Publishing also moved out of `cd.yml` and into `ci.yml` as a `push-image` job with `needs: load-test`.
`needs:` cannot cross workflow files, so as two workflows they ran in parallel and a red suite still shipped an image; the plan's "a `needs: ci` gate on `cd.yml`" was not expressible as written.
`:latest` now means the newest release rather than the tip of `main`, which is the only reading that makes it safe to put in a quickstart.

Deferred deliberately, both with reasons rather than silently:

- **The `server` / `worker` split.** `CONDUIT_RECONCILER_ENABLED=false` already yields an API-only instance and the pull API already yields remote workers, so a `worker` subcommand that only flips defaults is an abstraction over an environment variable. Revisit when someone needs to scale the pool separately from the API.
- **A thin client library.** Phase 1's own reasoning argues against it: `loadtest/worker.sh` is a shell script on purpose, because a protocol that needs a client library to be usable is the wrong protocol. `WORKERS.md` plus that script is the reference consumer.

The Kubernetes `Secret` template landed in phase 1 as `deploy/k8s/secret.example.yaml`.

The original write-up follows, since the reasoning is still what the design is for.

**The problem.** There are no tags, no releases, and no upgrade story.

- Tagged releases and a published container image an adopter can pin.
- A `needs: ci` gate on `cd.yml`, which currently publishes on any push to main even when CI is red.
- `deploy/k8s/deployment.yaml`'s `image: conduit:latest` changed to the real GHCR path.
- The `server` / `worker` split that the `migrate` subcommand made natural. `migrate` itself landed in phase 3, because the Postgres-only quickstart is not true without it.
- A `docker run` quickstart that does not require cloning the repo.
- One thin client library, in one language, so the API has at least one reference consumer.
- A Kubernetes `Secret` template, since `deployment.yaml` references `conduit-secrets` and the repo contains no example of it.

**Done when** the install instructions are a version number and not a git clone.

## Phase 7 - A wire contract worth committing to (done)

Shipped in v0.2.0, and it is a breaking change.
The reasoning is in [0007-a-strict-wire-contract.md](decisions/0007-a-strict-wire-contract.md).

**The problem.** Commit `dc0844a` fixed a `queue` field sitting at the wrong nesting level in the README quickstart.
The fix was one line.
The reason it survived to reach the most-copied command in the project is that `EnqueueRequest` has no top-level `queue` field and gin discarded fields it did not recognise, so a mis-routed job returned 201 and looked exactly like a correctly routed one.

Two more fields serialised the way Go serialises them rather than the way anyone would design them, and `WORKERS.md` had a section apologising for both.

**The change.** Three things, all in the request and response shape:

- Unknown fields return 400 naming the field. `binding.EnableDecoderDisallowUnknownFields` is package-level in gin, so one line in `RegisterRoutes` covers the enqueue handler and all four pull-protocol handlers. It does not descend into `map[string]string`, so `metadata` stays free-form, which is the right split: metadata keys are the caller's vocabulary, Conduit's field names are not.
- `task.timeout` is a duration string. `"15s"` rather than `15000000000`. A bare number is rejected rather than guessed at, because `15` could plausibly mean either unit and the two differ by a factor of a billion.
- `task.payload` is inline JSON rather than base64, in both the enqueue body and the webhook executor's outbound envelope, so the field means one thing in both directions.

Neither field changed on disk.
`task_timeout_ns` is still `BIGINT` and `task_payload` is still `BYTEA`; this was a serialisation fix, so there is no migration.

Two things worth recording because they were not obvious going in.

**`json.RawMessage` is the wrong type for `payload`.** It is the stdlib answer and it marshals its bytes verbatim, so a row written before v0.2.0 holding non-JSON bytes would turn `GET /api/jobs/:id` into a 500. `models.Payload` falls back to base64 when `json.Valid` says no, which keeps old rows readable instead of poisoning a response body.

**Two tests were built on the behaviour being removed.** `TestEnqueueJobIgnoresServerOwnedFields` asserted that a caller-supplied `id`, `state`, and `attempt` were silently dropped, which was its entire premise. It is now `TestEnqueueJobRejectsUnknownFields`, one case per field, and the guarantee is stronger: the caller finds out. That a passing test had to be inverted is the clearest evidence the old behaviour was a decision nobody had made deliberately.

**Done when** the pre-`dc0844a` body returns 400 naming `queue` instead of 201, and no command in the repo pipes anything through `base64` to enqueue a job.

## Documentation hygiene

Tracked here because it keeps recurring, not because it is a phase.
Currently clear: every default in `PROJECT.md`'s environment table matches `internal/config/config.go`, the endpoint counts match `internal/api/handler.go`, `docs/FEATURES.md`'s probe paths and ConfigMap match `deploy/k8s/`, and `CONDUIT_METRICS_SUBSYSTEM` is `server` in the code, in `.env.example`, in the ConfigMap, and in every metric name the docs quote.
The pattern to watch for is a doc that quotes a default or a path rather than pointing at the file that owns it.

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
