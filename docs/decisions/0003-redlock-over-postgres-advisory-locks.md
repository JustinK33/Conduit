# 0003. Redlock over Postgres advisory locks, and why it matters less than it looks

Status: accepted, with reservations recorded below.
Date: 2025-07 (`a8cb35f`).

## Context

Two service instances can be told about the same job at the same time.
Kafka delivery is at-least-once, and the reconciler can hand out a job that a Kafka consumer is already carrying.
Something has to decide which one runs it.

Postgres could decide. `pg_advisory_xact_lock(hashtext(job_id))` inside the claim transaction is one function call, no new dependency, and it releases automatically when the transaction ends.
The reason it was not used: an advisory lock held for the duration of a job holds a Postgres connection for the duration of a job.
A job whose `Task.Timeout` is two minutes would pin a pooled connection for two minutes.
`WORKER_CONCURRENCY` of 8 across four instances is 32 connections doing nothing but holding locks, and the pool is sized for query traffic, not for that.

Redis was already in the stack, and a lock in Redis costs no Postgres connection.

## Decision

`internal/lock/redlock.go` implements the Redlock quorum protocol over N independent Redis nodes: `SetNX` with a random token on each node, success when a majority responds within the drift-adjusted validity window, release by Lua compare-and-delete so a node never deletes someone else's lock.
The default deployment runs three nodes (`redis`, `redis-2`, `redis-3` in `docker-compose.yml`), so quorum is two.

`jobWorker.Run` acquires `job:exec:<id>` before executing and releases it in a `defer` (`cmd/server/main.go`).
Failing to acquire is not a failure of the job: `releaseClaim` puts the row back to `PENDING` so whoever holds the lock can finish, and this instance moves on.

## Consequences

The important consequence is what this does **not** buy.

A lock cannot prevent double execution, and Redlock specifically cannot.
The lock TTL is 30 seconds (hardcoded in `main.go`); a job with a longer timeout outlives its own lock and keeps running with no lock at all.
Nothing renews it, and `Lock.Expiry` is never checked after acquisition.
More generally, any lock can expire, or its holder can be stopped by a GC pause or a paused container, while the work is still in flight.
The lock holder cannot know that it lost the lock.

So the thing that actually prevents a stale worker from corrupting state is not the lock.
It is `lease_token`: `ClaimNextJob` mints a token, and every subsequent write carries it in the `UPDATE ... WHERE ... AND lease_token = $14` predicate (`internal/store/store.go`).
A worker whose lease was requeued by the reconciler finds its token no longer matches and its write affects zero rows, which surfaces as `ErrInvalidTransition`.
That is a fencing token in the Kleppmann sense, and it is the correctness mechanism.

Which makes Redlock defence in depth, and worth naming as such:

- It saves duplicated *effort*, not correctness. Two instances will not both fire the same webhook at the same moment under normal operation, which matters because the side effect is outside Postgres and no fencing token can undo it.
- It is a fixed per-job cost of N Redis round trips on acquire plus N on release, on the critical path of every single job. The README's measured results include a run with a single Redis node instead of three, to put a number on the quorum specifically.
- Under contention it costs up to 3 attempts at 100ms apart before giving up, so a contended job can sit for ~200ms and then be released back to `PENDING` rather than run.
- Three Redis containers exist for this and nothing else. Redis is not a cache here and not a store; if the lock went away, so would Redis.

Given all of that, `pg_advisory_xact_lock` would have been the simpler decision, at the cost of a held connection per in-flight job.
Redlock stands because the connection cost is real and because three Redis nodes are cheap to run, not because the system needs a quorum protocol to be correct.
That is a preference, not a proof, and it is written down here so nobody later mistakes it for one.

Also unfixed: the lock TTL is not derived from the job's timeout, and there is no lock renewal loop even though there **is** a lease renewal loop for Postgres.
Making the TTL configurable and renewing alongside the lease is the obvious next step.
