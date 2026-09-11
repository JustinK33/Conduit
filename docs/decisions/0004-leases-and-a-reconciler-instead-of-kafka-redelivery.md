# 0004. Leases and a reconciler, not Kafka redelivery

Status: accepted.
Date: 2025-07 (`a8cb35f`), replacing the approach in `15ce3d3`.

## Context

Two things have to happen that a message broker does not do.

**Delayed retry.** A job that fails needs to run again later, with exponential backoff.
`15ce3d3` implemented that by computing `Delay(attempt)`, writing `scheduled_at` into the future, and re-publishing the job to Kafka.
The comment it shipped with says what it assumed: "another consumer will pick it up after Kafka redelivery and the math will be smaller."
Kafka has no delayed delivery.
The consumer got the message back immediately, saw `scheduled_at` was still in the future, and re-published it again.
Backoff became a spin, and the only thing throttling it was the 60-second in-worker sleep for jobs due soon.

**Crash recovery.** A worker that dies mid-job leaves the row `RUNNING` with nobody running it.
Kafka cannot help here at all: the message was consumed and, as measured, the offset moves on regardless.
Nothing outside Postgres knows the job is orphaned.

## Decision

Both problems are solved by the same mechanism: a lease with an expiry, and a loop that looks for expired ones.

`ClaimNextJob` sets `lease_expires_at = now + CONDUIT_RECONCILER_RUNNING_LEASE` and mints a `lease_token`.
While a job runs, `startLeaseRenewal` pushes the expiry out at half the lease interval, so a live worker keeps its claim indefinitely.
A dead worker stops renewing.

The reconciler (`internal/reconciler/reconciler.go`) ticks every `CONDUIT_RECONCILER_INTERVAL` and does two things per tick:

1. `RequeueExpiredRunning` moves rows whose lease has expired back to `PENDING`, or to `DEAD` if they are out of retries.
2. `ClaimNextJob` in a loop, up to `CONDUIT_RECONCILER_BATCH_SIZE`, submitting each claimed job to the pool with `SubmitBlocking`.

Step 2 is what makes delayed retry work: a retried job is written `PENDING` with a future `scheduled_at` and is not published to Kafka at all.
`ClaimNextJob` filters on `scheduled_at <= NOW()`, so the job is simply invisible until it is due.
No spin, no wasted broker traffic, no in-worker sleeping.

The tick interval backs off to `CONDUIT_RECONCILER_IDLE_INTERVAL` (default 15s) when a tick finds nothing, so an idle deployment is not polling once a second forever.

## Consequences

Good:

- Retry backoff is real. A job scheduled an hour out costs nothing until it is due.
- Crash recovery needs no coordination, no heartbeat topic, and no external supervisor. It is one query on an index (`jobs_running_lease_idx`).
- The reconciler is also the safety net for every other dispatch failure, and the measured results show it doing far more of that than expected. When Kafka delivers a job the worker pool cannot accept, the reconciler is the only thing that ever runs it.

Costs, stated plainly:

- **Recovery time is bounded below by the lease duration.** A worker killed one second into a job holds that job for the full remaining lease. With the default `CONDUIT_RECONCILER_RUNNING_LEASE` of 5 minutes, a `SIGKILL` means up to a five-minute delay before anything retries. The README measures this with a 15-second lease to keep the run short; scale the number by your lease setting. Shortening the lease shortens recovery and raises the risk of requeuing a job that is merely slow, which is why the fencing token from [0003](0003-redlock-over-postgres-advisory-locks.md) has to exist.
- **Dispatch throughput is capped at `CONDUIT_RECONCILER_BATCH_SIZE` per tick.** With the defaults that is 100 jobs per second per instance, and the claims are sequential round trips, so the real figure is lower. Whenever the Kafka path is not delivering, that cap is the system's throughput. The README's drain measurements show exactly this.
- A lease renewal goroutine per in-flight job, ticking at lease/2.
- Two ways a job can reach a worker, which is a branch in `jobWorker.Run` that has to stay correct in both directions.
