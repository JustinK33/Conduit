# 0006. Workers pull over HTTP, so there is still no SDK

Status: accepted.
Date: 2026-09.
Amends [0005](0005-webhook-as-the-execution-model.md), which said a job cannot run in your process. It now can, just not by importing anything.

## Context

[0005](0005-webhook-as-the-execution-model.md) chose webhook execution and named its own cost: there is no `conduit.Register("send-email", fn)`, and adding a task type means editing a `map[string]TaskHandler` in `cmd/server/main.go` and redeploying the queue.
That is the coupling webhooks were supposed to remove, moved rather than removed.

Webhooks only fit work that is already an HTTP endpoint and already fast.
Anything else fits badly.
A job that runs for twenty minutes has to hold an HTTP connection open for twenty minutes, or return 202 and lose the outcome.
A job that needs a GPU, a large local dataset, or a binary that is awkward to put behind a listener does not fit at all.
And because the server initiates the connection, every worker has to be reachable from the queue's network, which is backwards for anything running on a laptop, in a CI runner, or behind NAT.

The obvious next step is the one Sidekiq, Celery, and Asynq all took: ship a client library, let people register functions, make the worker process their process.
Doing that means picking a language, owning a serialisation contract for user code, and versioning a library against a server.
The three of those are most of the maintenance surface of this kind of tool.

There is a third option that neither webhooks nor an SDK cover, and Faktory is the proof it works: invert the direction.
The worker connects to the queue, asks for work, and reports back.
The protocol is the product, and a client library becomes optional rather than load-bearing.

## Decision

Workers pull. Four endpoints, all on the existing `/api/jobs` group.

```
POST /api/jobs/claim          -> 200 {job, lease_token, lease_expires_at} | 204
POST /api/jobs/:id/heartbeat  -> 200 {lease_expires_at}
POST /api/jobs/:id/complete   -> 200 {state}
POST /api/jobs/:id/fail       -> 200 {state, attempt, scheduled_at}
```

The design work was mostly already done, in three places.

**The lease token becomes the credential.** [0004](0004-leases-and-a-reconciler-instead-of-kafka-redelivery.md) already mints a random token at claim time and fences every write on `state = 'RUNNING' AND lease_token = $n`.
That is exactly the property a remote worker protocol needs.
A worker that stalls past its lease, gets requeued by the reconciler, and then wakes up and posts a result finds its write matched zero rows.
No new mechanism, and the thing that makes it safe is the thing that was already there for crash recovery.

**`Task.Queue` becomes the routing key.** It has existed since `migrations/001_create_jobs.sql`, defaults to `default`, and until this change appeared in zero `WHERE` clauses.
A worker claims from the queues it names, so a remote worker never wins a `webhook` job it has no code for, and the built-in handlers keep serving `default` untouched.

**The retry decision moves into the service layer.** `jobWorker` owned it, so an HTTP `fail` endpoint would have been a second implementation of the same policy, guaranteed to drift.
Moving it made a latent bug visible and fixed it: retry was two sequential updates, `RUNNING -> FAILED` then `FAILED -> PENDING`, because `CanTransition` forbids `RUNNING -> PENDING` directly.
A crash between the two left a row in `FAILED` with a live lease token, and `RequeueExpiredRunning` only ever scans for `RUNNING`, so nothing recovered it.
It is now one statement.

Two things had to change because a claim endpoint changes the threat model.

`UpdateJob` is a full-row overwrite whose `WHERE` clause ends in `$14 = '' OR lease_token = $14`, meaning an empty token disables fencing entirely.
That is fine for trusted in-process callers and unacceptable on a path that takes client input, so the new endpoints do not use it.
They use narrow compare-and-swap methods that take an id, a token, and an outcome, and can express nothing else.

`Job.LeaseToken` was serialised by `GET /api/jobs/:id`.
Harmless while nothing accepted a token as input; a hijack primitive the moment `complete` exists.
It is now `json:"-"` and appears only in the claim response.

Auth ships in the same change, because an unauthenticated claim endpoint is a way for anyone to take work and drop it.
`CONDUIT_API_KEYS` is a shared bearer token covering `/api/jobs` only.
It mounts on the route group rather than the root router, deliberately: `/live`, `/ready`, `/health`, and `/metrics` are registered on the root and would otherwise start returning 401 to Kubernetes probes and Prometheus.

## Consequences

Good:

- A worker can be written in any language, run anywhere with outbound HTTPS, and needs no library. The protocol is four `POST`s and the reference implementation is a shell script, `loadtest/worker.sh`, which is the honest test of whether the API is simple enough.
- Long jobs stop being a problem. A worker heartbeats instead of holding a connection open, and the lease is the only clock that matters.
- Workers behind NAT work, because the queue is never the one connecting.
- The queue still has no plugin system, no user code in its address space, and no serialisation contract for handler arguments. All three costs of an SDK stay unpaid.
- Retry policy stays server-side. A worker reports success, transient failure, or permanent failure, and the backoff is not its business.

Costs, stated plainly:

- **Correctness now depends on clients behaving.** A worker that claims and vanishes costs a full lease duration before the job is retried, five minutes by default. Webhook execution had a timeout the server controlled; pull execution does not. The lease is the only bound, which makes [0004](0004-leases-and-a-reconciler-instead-of-kafka-redelivery.md)'s five-minute default matter far more than it did.
- **Claim is a poll.** There is no long-poll and no `LISTEN`/`NOTIFY`, so an idle worker wakes up, asks, gets a 204, and sleeps. That costs a query per poll per worker and adds up with worker count long before it adds up with job count. Long-polling is the fix and is not built.
- **One shared key, no identity.** Holding `CONDUIT_API_KEYS` means being able to claim, cancel, and read every job in the instance. There is no per-worker identity, so two teams cannot share a deployment, and a leaked key cannot be rotated for one caller without rotating it for all of them. This is phase 2 in `docs/ROADMAP.md` and it is the largest thing still missing.
- **No worker identity in the database either.** The `jobs` table has no `worker_id` or `claimed_by` column, so "which worker is running this" is not answerable, and neither is "this host is wedged, requeue everything it holds."
- **`Task.Queue` changes meaning.** It was an inert label that was persisted and read back and routed nothing. It now decides who can claim a job. Anything already setting a non-default queue value gets different behaviour, and the reason that is acceptable rather than a breaking change is that the field routed nothing at all before, so nothing could have depended on it.
- **Two execution models to keep in step.** The built-in handlers and remote workers now have to produce identical state transitions. Sharing the service layer is what makes that true, and it is a property that has to be actively maintained rather than one the types enforce.
- **The wire format leaks Go.** `Task.Timeout` is a `time.Duration` with no custom marshaller, so it serialises as an integer count of nanoseconds, and `Task.Payload` is `[]byte`, so it serialises as base64. Both are perfectly usable and neither is what a Python or Rust client author would expect. Fixing it means breaking the format, so it waits for a version prefix on the API.
