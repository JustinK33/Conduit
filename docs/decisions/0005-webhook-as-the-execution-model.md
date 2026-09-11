# 0005. A webhook is the execution model, so Conduit is not a library

Status: accepted, amended by [0006](0006-a-pull-api-instead-of-a-worker-sdk.md).
Date: 2025-07 (`a8cb35f` for `webhook`, `ad5f143` for `sql.etl`).

Webhook execution is still supported and still the default.
What changed is that the cost named at the bottom of this record, "a job cannot run in your process," no longer holds: workers can now pull jobs over HTTP and execute them wherever they run.
The conclusion this record reached is intact, though. There is still no SDK to import.

## Context

Something has to actually run the job.
Sidekiq, Celery, and Asynq all answer this the same way: you write a function in your application, register it under a name, and the queue's worker process is your application process.
That is the ergonomic answer and it is why those tools are pleasant to use.

It is also the answer that forces the queue to be a library in one language, coupled to your deploy, sharing your memory limits, and redeployed whenever a handler changes.

Conduit went the other way, and the deciding factor was that the queue already had an HTTP API.
If enqueueing is `POST /api/jobs`, then executing can be a `POST` too, and the queue never needs to know anything about the language the work is written in.

## Decision

`Task.Name` selects a handler from a registry in `cmd/server/main.go`, and there are two:

- `webhook` (`internal/webhook/executor.go`) reads `metadata.url` and `metadata.method`, POSTs the job's payload, and treats a non-2xx as a failure. 4xx returns `retry.ErrNoRetry`, because retrying a client error just burns attempts.
- `sql.etl` (`internal/etl/executor.go`) runs an extract query and inserts the result into a target table, for the case where the work is already SQL and a network hop would be silly.

An unregistered `Task.Name` fails with `retry.ErrNoRetry` and the job goes straight to `DEAD` rather than looping.

Because a job body is now a URL the server will fetch, the webhook executor is an SSRF sink, and `validateURL` is not optional.
It rejects non-HTTP schemes, URLs with userinfo, the literal hostname `localhost`, and any resolved address that is loopback, private, link-local, unspecified, or multicast unless `WEBHOOK_ALLOW_PRIVATE_NETWORKS` is explicitly set.
Redirects are re-validated, since validating only the first hop is the same as not validating.

## Consequences

Good:

- Handlers are language-agnostic and deploy on their own schedule. A Python service and a Go service can both be job targets with no client library on either side.
- The queue has no plugin system, no serialisation contract for user code, and no way for a handler to take the queue process down. A handler that hangs costs a worker slot and its own timeout, nothing more.
- `sql.etl` shows the escape hatch works: a first-party handler is just another entry in the registry.

Costs, stated plainly:

- **A job cannot run in your process.** There is no `conduit.Register("send-email", fn)`. If what you wanted was Sidekiq, this is the wrong tool, and that belongs in the README rather than buried here.
- Every job now costs an HTTP round trip on top of the queue's own overhead, and the target has to be reachable from the queue's network.
- The SSRF guard is real code with real edge cases, and it exists purely because of this decision. It also makes local development awkward: pointing a job at a service on your own machine requires `WEBHOOK_ALLOW_PRIVATE_NETWORKS=true`, which is why the load-test sink in `docker-compose.yml` sits behind a profile and the measurement harness sets that flag.
- Handler registration is a compile-time map in `main.go`. Adding a task type means editing and redeploying the queue, which is exactly the coupling this decision was supposed to avoid, just moved. A config-driven registry would fix it and is not built.
