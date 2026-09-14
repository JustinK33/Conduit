# 0007. Unknown fields are an error, and the wire format stops leaking Go

Status: accepted.
Date: 2026-09.
Resolves the last cost listed in [0006](0006-a-pull-api-instead-of-a-worker-sdk.md), which said fixing the format means breaking it, so it waits.

## Context

Commit `dc0844a` moved a `queue` field from the top level of an enqueue body into `task`, where it belongs.
One line.
It had been wrong in the README, which is the most-copied text in the project, and nobody noticed because the request returned 201.

`EnqueueRequest` has no top-level `queue` field, and gin's JSON binding discards fields it does not recognise.
So the field was accepted, dropped, and the job went to `default`.
A worker that named a specific queue never received it, and the enqueuer's only signal was a success.

That is the failure mode worth naming: not a wrong answer, an answer indistinguishable from the right one.
A typo in a field name behaves identically. So does a field from a newer version of the docs than the server is running.

[0006](0006-a-pull-api-instead-of-a-worker-sdk.md) separately recorded that `Task.Timeout` serialises as an integer count of nanoseconds and `Task.Payload` as base64, because one is a `time.Duration` and the other a `[]byte`, and Go's `encoding/json` has opinions about both.
`docs/WORKERS.md` carried a section explaining both to client authors, which is a documented apology rather than a fix.
The in-repo evidence that it mattered was `make enqueue-elt`, which shelled out to `base64` to send a JSON file as a JSON field.

## Decision

Three changes, shipped together as v0.2.0, because they break the same thing once instead of three times.

**Unknown fields return 400 naming the field.**
`binding.EnableDecoderDisallowUnknownFields = true`, set in `RegisterRoutes`.
It is a package-level `var` in gin, so one assignment covers the enqueue handler and all four pull-protocol handlers, and there is no per-route way to forget it.

It deliberately does not apply to `metadata`, and cannot: the strict decoder does not descend into a `map[string]string`.
That is the correct line. Conduit's field names are Conduit's contract; metadata keys are the caller's vocabulary and Conduit has no opinion about them.

**`task.timeout` is a duration string.**
`"15s"`, `"1m30s"`, using `time.ParseDuration` syntax, via a `models.Duration` wrapper.
A bare number is rejected rather than interpreted.
Accepting the old integer form alongside the new string was the obvious kindness and was refused: `15` reads as fifteen seconds to a human and fifteen nanoseconds to the old format, the two differ by a factor of a billion, and nothing in the value distinguishes them.
A 400 is cheaper than a job that times out immediately for reasons nobody can see.

**`task.payload` is inline JSON.**
A `models.Payload` wrapper over `[]byte`, in both the enqueue body and the webhook executor's outbound envelope.
Changing only the inbound side was considered and rejected: the same field name meaning base64 in one direction and JSON in the other is exactly the sort of thing this record exists to remove.

`json.RawMessage` is the obvious stdlib choice for this and is wrong here.
It marshals its bytes verbatim, and `task_payload` is `BYTEA`, which has accepted arbitrary bytes since `migrations/001_create_jobs.sql`.
A single pre-v0.2.0 row holding non-JSON bytes would put invalid JSON in a response body and turn `GET /api/jobs/:id` into a 500.
`models.Payload.MarshalJSON` checks `json.Valid` and falls back to base64, so old rows stay readable and no response is ever malformed.

Neither field changed on disk.
Nanoseconds in a `BIGINT` and bytes in a `BYTEA` are fine storage representations, and this was a serialisation decision, so there is no migration.

## Consequences

Good:

- A mis-routed job is now a 400 that names the field, at the moment the mistake is made, instead of a success followed by a queue that stays empty.
- A client written against a newer version of `WORKERS.md` than the server is running fails loudly rather than half-working.
- `task.timeout` and `task.payload` are writable by hand in a `curl` command, which is how most people meet this API. `make enqueue-elt` no longer pipes a JSON file through `base64` to put it in a JSON field.
- `WORKERS.md` lost the section apologising for the format.

Costs, stated plainly:

- **Every v0.1.0 client breaks, including the ones in this repository.** Three forms of the same body are now rejected: a top-level `queue`, an integer `timeout`, and a base64 `payload`. That is the point, but it means the version pin in `README.md` and `deploy/k8s/deployment.yaml` is load-bearing in a way it was not before.
- **Every endpoint receiving Conduit webhooks breaks.** `payload` in the outbound envelope changed from a base64 string to inline JSON. A receiver that base64-decodes it now fails on JSON. This is the change most likely to break something already running, and it breaks silently on the receiver's side rather than loudly on Conduit's.
- **A stale client sending a base64 payload gets a 201, not a 400.** Verified against a live stack: `"payload":"aGVsbG8="` is a valid JSON string, so it is accepted and stored verbatim as a string. Conduit does not interpret payloads and so cannot tell that one apart from a caller who genuinely meant to send that string. `timeout` and unknown fields both fail loudly; this one does not, and it is the sharp edge of the upgrade.
- **Strictness makes `Task.ID` and `Task.RetryCount` more visible without fixing them.** Both are still caller-settable, so a request can name them and be accepted. They are real fields, so the decoder has no complaint. Tightening what an enqueuer owns belongs with phase 2.
- **The base64 fallback in `Payload.MarshalJSON` is a branch that exists only for history.** It is dead the day no pre-v0.2.0 row survives, and nothing will tell us when that day arrives. It carries a `ponytail:` comment saying so.
- **Two passing tests had to be inverted.** `TestEnqueueJobIgnoresServerOwnedFields` asserted a caller-supplied `id`, `state`, and `attempt` were silently dropped. That was its whole premise. That a green test encoded the behaviour being removed is the clearest evidence the old leniency was never a decision anyone made on purpose.
