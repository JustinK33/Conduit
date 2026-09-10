# 0001. Postgres is the source of truth, not Redis and not Kafka

Status: accepted.
Date: 2025-07 (`a8cb35f`).

## Context

A job queue has to answer one question after a crash: what work was outstanding?
Whatever storage answers that question is the source of truth, and everything else is a cache or a courier.

Three candidates were on the table, and each was already in the stack for another reason.

Redis was already there for locking.
It is the fastest option and the obvious one for a queue, but a job that is scheduled a week out has to survive a week.
Redis persistence is configurable and real, yet the failure mode is a silently truncated AOF or a lost RDB window, and there is no way to ask Redis "give me one due job and mark it claimed, atomically, without anyone else getting it" without either Lua or a data model built out of several structures kept in sync by hand.

Kafka was already there for transport.
Using the log as the queue is a well-worn pattern, but topics have retention.
A job that sits `PENDING` past `retention.ms` falls off the log, and the only record that it ever existed is a consumer offset.
Kafka also cannot answer "show me every failed job from yesterday", which is the first thing anyone operating a queue asks.

Postgres was already there because jobs have a state machine, and state machines want transactions.

## Decision

Postgres holds the jobs table and is the only durable record of a job's state.
`internal/store/store.go` owns every transition, and `ClaimNextJob` does the claim with `SELECT ... FOR UPDATE SKIP LOCKED` inside a CTE that also performs the `UPDATE`, so claiming is one statement and one round trip.

`SKIP LOCKED` is the reason this works without a queue manager.
Any number of instances can run the same query concurrently; each one either gets a distinct row or immediately gets nothing and moves on.
There is no coordinator, no partition assignment, and no rebalancing.

Enqueue commits to Postgres before it does anything else (`internal/service/job_service.go`).
The Kafka publish happens after, in a goroutine, and its failure is a warning.

## Consequences

Good:

- Crash recovery is a query, not a protocol. Every outstanding job is one `WHERE state = 'PENDING'` away.
- Operators get `psql`. State, attempt count, last error, and lease holder are all inspectable with SQL, which is why `GET /api/jobs?state=FAILED` was a twenty-line handler and not a subsystem.
- Transitions are guarded in one place. `CanTransition` plus the `lease_token` predicate in `UpdateJob` means an illegal or stale write fails instead of corrupting the row.

Costs, stated plainly:

- Every state change is a round trip to a single writer. A successful job costs at least two writes (claim, then terminal state) and a retry costs four. That is the throughput ceiling for job execution, and it is much lower than the intake ceiling. See the README's measured results for both numbers.
- Postgres is now a hard dependency for liveness, not just durability. `/ready` fails without it and no work moves.
- The queue's scaling story is Postgres's scaling story. There is no sharding here. Above the point where one Postgres can absorb the write rate, this design needs partitioned tables or a different store, and neither is built.

Rejected alternative worth naming: Redis as the store with Postgres as an archive.
That is strictly more moving parts for a system whose write rate is already bounded by handler execution, not by the store.
