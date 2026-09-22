# Upgrading

## v0.3.1 to v0.4.0

No wire change and no breaking change to any existing request or response.
A v0.2.0 client still works unmodified.
Four things to do, and the first one deletes data.

**1. Retention is on by default and it deletes completed jobs.**

`CONDUIT_RETENTION_COMPLETED` defaults to `168h`, so seven days after upgrading, completed jobs older than a week start disappearing.
Set `CONDUIT_RETENTION_COMPLETED=0` before you roll out if you want the old behaviour of keeping everything, or if you are shipping job history somewhere that reads it out of Postgres.

Dead-lettered jobs are **not** deleted: `CONDUIT_RETENTION_DEAD` defaults to `0`, which means keep forever, because a `DEAD` job is the one somebody wants to read.

The sweep runs on the reconciler, so an instance with `CONDUIT_RECONCILER_ENABLED=false` prunes nothing.
A deployment where every instance is API-only never prunes at all.
[DEPLOYMENT.md](DEPLOYMENT.md#retention) has all four knobs.

**2. Run the migrations.**

`migrations/004_create_schedules.sql` and `migrations/005_retention_indexes.sql`.
Both are additive.
Compose and the Kubernetes init container run `conduit migrate` for you; by hand it is `./bin/conduit migrate` or `docker run --rm ghcr.io/justink33/conduit:v0.4.0 migrate`.

The server does not check for the schedules table at boot, so an unmigrated database fails on the first `/api/schedules` call rather than at startup.

**3. PromQL over the job counters needs an aggregation now.**

The five job counters carry a `task` label, and `job_duration_seconds` is a histogram with one too.
A query that was `rate(conduit_server_jobs_failed_total[5m])` now returns one series per task instead of one series, so anything that graphed or alerted on it needs `sum(...)`:

```promql
# before
rate(conduit_server_jobs_failed_total[5m])
# after
sum(rate(conduit_server_jobs_failed_total[5m]))
# or, which is the reason for the label
sum by (task) (rate(conduit_server_jobs_failed_total[5m]))
```

The label is bounded to the task names with a registered handler, plus `other`.
A task name Conduit has no handler for reports as `other` rather than as itself, because the label is fed by caller input.

`worker_in_flight` is unchanged and still unlabelled.
The new `conduit_server_jobs_backlog{state}` is database-wide and sampled by every instance, so aggregate it with `max by (state)` and never `sum`.

**4. `CONDUIT_SCHEDULER_MAX_CONCURRENT_RUNS` means something different.**

It used to bound goroutines in a scheduler that fired nothing.
It is now the per-tick fire budget: how many due schedules one tick will enqueue.
The default moved from `2` to `5`, and `CONDUIT_SCHEDULER_TICK_INTERVAL` from `1m` to `30s`.
If you set either explicitly, re-read [SCHEDULES.md](SCHEDULES.md#what-due-means) before keeping your value.

Nothing was firing before this release, so there is no behaviour to preserve: `internal/scheduler` was constructed, started, and reachable by nothing.
Recurring work now means `POST /api/schedules`.

Rolling back to v0.3.1 works: both migrations are additive, and old code ignores the `schedules` table.
Jobs already enqueued by a schedule are ordinary jobs and finish normally.

## v0.2.0 to v0.3.1

Nothing to change. No wire change, no database migration, and a v0.2.0 client works unmodified.
Three behaviour changes worth knowing about, all of them in the direction of doing less damage:

**`CONDUIT_RECONCILER_BATCH_SIZE` now defaults to 500 rather than 100.**
It is a per-tick claim budget, not a buffer, and the reconciler submits with `SubmitBlocking`, so a larger budget costs no memory and stops on a full worker pool by itself.
If you set it explicitly, your value still wins.
Raising it is what stops a burst from queueing behind a 100-per-second claim ceiling; the README's Execution table has the measurements.

**Circuit breakers are per task name.**
There used to be one for the whole process, so five failures against one dead endpoint opened the circuit for every other task type too.
Unregistered task names share a single breaker, since they fail on every attempt regardless.
If you were relying on one failing task type stopping everything, that is no longer what happens.

**A released claim is now deferred rather than immediately eligible.**
When Conduit hands a claimed job back without trying it - the circuit is open, or another instance holds the execution lock - it sets `scheduled_at` into the future instead of leaving the job due now.
The delay is the breaker's `OpenTimeout` (30 s) for a circuit-open release and 5 s for lock contention.
So a job blocked by an open circuit sits in `PENDING` with `last_error = 'circuit open'` for up to 30 seconds instead of being re-claimed every second.
That is the intended behaviour, not a stall.

A release also gives the attempt back, since `attempt` is incremented at claim time and a released job never ran.
v0.3.0 did not, so on that release a job that met an open circuit twice reached `max_retries` having executed once and died for a reason that was not its own.
Pin `v0.3.1` or later, not `v0.3.0`.

## v0.1.0 to v0.2.0

v0.2.0 changes the wire format. It breaks every v0.1.0 client and every endpoint already receiving Conduit webhooks.
[ADR 0007](decisions/0007-a-strict-wire-contract.md) is why. This page is what to change.

There is no database migration. `task_timeout_ns` is still `BIGINT` and `task_payload` is still `BYTEA`; only the JSON changed.
Existing rows keep working, and `GET /api/jobs/:id` on a job enqueued under v0.1.0 still returns 200.

Read the first section even if you never call the API. It is the only change that breaks in your process rather than at Conduit's door.

### 1. Webhook receivers: `payload` is no longer base64

If anything you run receives Conduit's outbound webhook, this is the change to make first.
`payload` in the request envelope used to be a base64 string. It is now the JSON you enqueued, inline.

Before:

```json
{"job_id":"...","task_name":"webhook","attempt":1,"payload":"eyJvcmRlcl9pZCI6MTIzNH0="}
```

After:

```json
{"job_id":"...","task_name":"webhook","attempt":1,"payload":{"order_id":1234}}
```

So a receiver that did `json.loads(base64.b64decode(body["payload"]))` now does `body["payload"]`.

This one fails on your side rather than on Conduit's, which means Conduit returns a 2xx and your handler raises.
Depending on what your endpoint does with an exception, that can look like a retry storm rather than like a format change.
Update receivers before you deploy the new server.

### 2. `task.timeout` is a duration string

Before: `"timeout": 15000000000`, an integer count of nanoseconds.
After: `"timeout": "15s"`, using [Go's duration syntax](https://pkg.go.dev/time#ParseDuration).

`"90s"`, `"1m30s"`, `"2h"`, and `"500ms"` are all accepted. A bare number is a 400:

```
{"error":{"code":"invalid_request","message":"timeout must be a duration string like \"15s\", got 15000000000"}}
```

The old form is rejected rather than accepted alongside the new one, deliberately.
`15` reads as fifteen seconds to a person and as fifteen nanoseconds to the old format, the two differ by a factor of a billion, and nothing in the value says which was meant.

### 3. `task.payload` is inline JSON

Before: `"payload": "eyJoZWxsbyI6IndvcmxkIn0="`.
After: `"payload": {"hello":"world"}`.

Any valid JSON value works: an object, an array, a string, a number, `null`.
Conduit stores it byte for byte and does not interpret it.
Omit the field entirely rather than sending `""` if there is no payload.

On the way back out, `GET /api/jobs/:id` returns it the same way, so a reader stops decoding too.

### 4. Unknown fields are a 400

A body carrying a field Conduit does not define now returns 400 naming that field, instead of 201 with the field silently discarded.

The common case is `queue` at the top level when it belongs inside `task`:

```jsonc
// rejected: job would have gone to "default" and looked like a success
{"queue":"remote","task":{"name":"webhook"}}

// correct
{"task":{"queue":"remote","name":"webhook"}}
```

This also catches a misspelled field name and a field from a newer version of [WORKERS.md](WORKERS.md) than your server is running.
It applies to every endpoint, including the four pull-protocol ones.

`metadata` is exempt and always will be: its keys are your vocabulary, not Conduit's, so anything goes inside it.

### The one change that does not fail loudly

A v0.1.0 client sending a base64 payload gets a **201**, not a 400.

```
"payload":"aGVsbG8="   ->  201, stored as the string "aGVsbG8="
```

A base64 string is valid JSON, and Conduit does not interpret payloads, so it cannot tell that request apart from a caller who genuinely meant to send that string.
`timeout` and unknown fields both fail at the door. This one does not.

So grep your enqueue call sites for `base64` rather than relying on the API to tell you.
The tell in production is a worker receiving a string where it expected an object.

### Checking you are done

Point a client at a v0.2.0 server and confirm all four:

```bash
# 1. the corrected body is accepted
curl -X POST localhost:8080/api/jobs -H 'content-type: application/json' \
  -d '{"task":{"queue":"default","name":"webhook","metadata":{"url":"https://your-endpoint.example/hook"},"timeout":"15s"}}'
# -> 201 {"id":"..."}

# 2. the old timeout form is refused
curl -X POST localhost:8080/api/jobs -H 'content-type: application/json' \
  -d '{"task":{"name":"webhook","timeout":15000000000}}'
# -> 400, message names the duration string

# 3. a top-level queue is refused
curl -X POST localhost:8080/api/jobs -H 'content-type: application/json' \
  -d '{"queue":"remote","task":{"name":"webhook"}}'
# -> 400, message names "queue"

# 4. payload comes back as JSON, not base64
curl localhost:8080/api/jobs/<id> | jq '.task.payload, .task.timeout'
```

If step 2 or step 3 returns a 201, you are still talking to v0.1.0.
