# Writing a worker

A worker is any process, in any language, that can make four HTTP calls.
It claims a job, runs it wherever it lives, and reports the outcome.
Conduit never calls out to your worker, so a worker can sit behind NAT, on a laptop, or in a different cloud.

`loadtest/worker.sh` is a complete working worker in about 40 lines of shell.
Read that first if you would rather see it than read about it.

## The loop

```
POST /api/jobs/claim              -> 200 {job, lease_token, lease_expires_at} | 204 nothing due
POST /api/jobs/:id/heartbeat      -> 200 {lease_expires_at}
POST /api/jobs/:id/complete       -> 200 {state}
POST /api/jobs/:id/fail           -> 200 {state, attempt, scheduled_at}
```

Claim, run, report.
Heartbeat only if the work outlasts the lease.

## Claim

```http
POST /api/jobs/claim
Authorization: Bearer <api key>
Content-Type: application/json

{"queues": ["render", "thumbnail"], "lease_seconds": 60}
```

Both fields are optional, and an entirely empty body is a valid claim.

`queues` is the routing filter, and you almost always want it.
Omitting it claims from every queue, including queues whose jobs your worker has no code for, and a job you claim and cannot run is a job nobody else can run either.

`lease_seconds` is a request, not a grant.
The server clamps it to `RECONCILER_RUNNING_LEASE` (default 5 minutes) and uses that as the default when you omit it.
Trust `lease_expires_at` in the response, not the number you asked for.

`200` returns the job:

```json
{
  "job": {
    "id": "0d1f3d8e-...",
    "state": "RUNNING",
    "attempt": 1,
    "task": {
      "id": "",
      "name": "render.thumbnail",
      "payload": "eyJ1cmwiOiJodHRwczovLy4uLiJ9",
      "retry_count": 0,
      "max_retries": 3,
      "timeout": 30000000000,
      "queue": "render"
    },
    "metadata": {"source": "api"},
    "created_at": "2026-09-10T12:00:00Z",
    "updated_at": "2026-09-10T12:00:03Z"
  },
  "lease_token": "9f2c...",
  "lease_expires_at": "2026-09-10T12:05:03Z"
}
```

`204 No Content` with an empty body means nothing was due.
Sleep and poll again.
There is no long-poll yet, so an idle worker costs one indexed query per poll; a second or two between polls is a reasonable default and queue latency is bounded by that interval.

The claim is atomic across every worker: it is a `SELECT ... FOR UPDATE SKIP LOCKED` that flips the row to `RUNNING` in the same statement.
Two workers polling at the same instant get two different jobs, never the same one twice.

## The lease token

`lease_token` is the whole security and correctness model of the protocol, so it is worth being precise about.

It is minted fresh on every claim and it is the only thing that proves you still own the job.
Every one of `heartbeat`, `complete`, and `fail` requires it, and each of those is a single compare-and-swap in Postgres fenced on `state = 'RUNNING' AND lease_token = <your token>`.
If the row no longer matches, the write affects zero rows and you get `409 lease_lost`.

That is what makes a stalled worker safe.
If your process hangs, gets `SIGSTOP`ped, or loses the network for longer than the lease, the reconciler requeues the job and it is claimed by someone else with a new token.
When your process wakes up and reports its result, the write is rejected instead of silently overwriting the outcome of the worker that actually finished the work.

The token appears exactly once in the whole API surface: beside the job in the claim response.
`GET /api/jobs/:id` does not include it, so reading a job does not let you hijack it.

Treat it as a credential.
Do not log it, and do not persist it anywhere the next run of your worker could pick up a stale one.

## Heartbeat

If a job might run longer than `lease_expires_at`, extend the lease while you work:

```http
POST /api/jobs/0d1f3d8e-.../heartbeat
{"lease_token": "9f2c...", "lease_seconds": 60}
```

Send one every `lease/2` and you tolerate a single lost heartbeat without losing the job.
The response carries the new `lease_expires_at`.

A worker whose jobs are all short does not need to heartbeat at all.
`loadtest/worker.sh` does not, deliberately.

## Reporting the outcome

There are exactly three ways a job can end, and all three are reachable from a worker.

**It worked.**

```http
POST /api/jobs/0d1f3d8e-.../complete
{"lease_token": "9f2c...", "metadata": {"worker": "gpu-3", "output_url": "s3://..."}}
```

`metadata` is optional and merges into the job's metadata, which is how you return a result.
The job becomes `COMPLETED` and is never claimed again.

**It failed and might work later.**

```http
POST /api/jobs/0d1f3d8e-.../fail
{"lease_token": "9f2c...", "error": "upstream returned 503", "retry": true}
```

`retry: true` defers to the server's retry policy.
If the attempt budget is spent the job still goes to `DEAD`; otherwise it goes back to `PENDING` with a future `scheduled_at` computed from the exponential backoff config.
The response tells you which happened, so you do not have to guess:

```json
{"state": "PENDING", "attempt": 2, "scheduled_at": "2026-09-10T12:05:11Z"}
```

**It failed and will never work.**

```http
POST /api/jobs/0d1f3d8e-.../fail
{"lease_token": "9f2c...", "error": "payload is not valid JSON", "retry": false}
```

`retry: false` is binding: the job goes straight to `DEAD` whatever the policy would have allowed.
Use it for anything a retry cannot fix, which is most bad input.

`retry` defaults to `false`, because a worker that does not ask for a retry does not get one.
Be explicit.

**The fourth way, which is not a report.**
If you crash or hang and never report anything, the lease expires and the reconciler requeues the job.
That is the safety net, not a strategy: it costs a full lease duration of latency, and the retry has no backoff, so an unhandled crash loop hammers whatever the job talks to.
Catch your errors and report them.

## What to do when you get 409 lease_lost

```json
{"error": {"code": "lease_lost", "message": "the lease on this job is no longer held; ..."}}
```

Stop working on that job and claim another one.

Do not retry the report.
It will never succeed, because someone else owns the job now and may have already finished it.
If your job has external side effects, this is the point at which to notice that Conduit gives you at-least-once delivery, so your handler needs to be idempotent: the same job id can legitimately be executed twice if the first attempt stalled past its lease.

`409 invalid_state` is a different thing, returned by `cancel`, and means the state machine forbids what you asked.
The codes are stable, so branch on `error.code` rather than on the status alone.

## Auth

Set `API_KEYS` on the server to a comma-separated list of keys, each at least 16 characters.
Generate them with `openssl rand -hex 32`.
Send one as `Authorization: Bearer <key>`; the scheme is case-insensitive.

Any configured key is accepted, so rotation is: add the new key, restart, move workers over, remove the old one.

If `API_KEYS` is unset the API is open and the server says so loudly at boot.
That is fine on a laptop and wrong everywhere else.

Two things to know before you point a worker at anything but localhost:

- A bearer token over plain HTTP is a token in cleartext, and Conduit does not speak TLS.
  Put a reverse proxy in front of it. See [DEPLOYMENT.md](DEPLOYMENT.md).
- The key is all-or-nothing. Holding it means claiming, cancelling, and reading every job in the instance.
  Per-key identity and tenant scoping are [phase 2](ROADMAP.md#phase-2---more-than-one-tenant).

## Wire format, for a client that is not Go

Two fields serialise the way Go serialises them rather than the way you would design them.
Both are fixed in the same breaking change as the rest of the wire format, so they are documented rather than papered over.

**`task.payload` is base64.**
It is a `[]byte` on the server, so `encoding/json` base64-encodes it.
Decode it before you look at it, and encode when you enqueue.
It is absent entirely when empty.

```
jq -r '.job.task.payload // "" | @base64d'    # shell
base64.b64decode(job["task"]["payload"])       # python
```

**`task.timeout` is an integer count of nanoseconds.**
`30000000000` is 30 seconds.
It is advisory for a remote worker: nothing enforces it on your side, so honour it if you want the timeout the enqueuer asked for.

Times are RFC 3339 in UTC.
`attempt` counts claims, not failures, and increments on every claim including one that follows a lease expiry.

## Status codes

| Code | Meaning | What a worker should do |
| --- | --- | --- |
| `200` | Done | Continue |
| `204` | Nothing due (claim only) | Sleep, poll again |
| `400 invalid_request` | Malformed body, missing `lease_token`, negative `lease_seconds` | Fix the client; retrying will not help |
| `401 unauthorized` | Missing or wrong API key | Fix the key |
| `404 not_found` | No such job | Stop; claim another |
| `409 lease_lost` | Someone else owns this job now | Abandon it; claim another |
| `500 internal_error` | Server or database problem | Back off and retry the call |

Every error body is `{"error": {"code", "message", "request_id"}}`.
Quote `request_id` when reporting a problem; it appears in the server logs for the same request.

## Running the in-process pool alongside remote workers

The server also runs its own worker pool over the handlers registered in `cmd/server/main.go`, and by default that pool claims from **every** queue.
It will therefore win jobs meant for your remote workers, find no registered handler, and dead-letter them.

So the moment you run a remote worker, set `WORKER_QUEUES` on the server to the queues it should handle itself, and give your remote workers their own queue names.
That filter applies to both paths the in-process pool receives work on, the reconciler and the Kafka consumer.

There is no switch that turns the pool off, so if the server should only ever hand work out, point `WORKER_QUEUES` at a queue name nothing is ever enqueued to.

## Not there yet

Named here so you can tell a missing feature from a bug:

- No long-poll on claim, so dispatch latency is your poll interval.
- No queue priority; jobs come out in `scheduled_at` order.
- No filter on task name, only on queue.
- No per-worker identity, so the API cannot tell you which worker holds a job.

All four are [phase 5](ROADMAP.md#phase-5---operability-under-real-load).
