# Design

Why Conduit is shaped the way it is, what it deliberately is not, and what building it taught me.
[ARCHITECTURE.md](ARCHITECTURE.md) is the reference for the same system: full diagram, state machine, package-by-package.
[decisions/](decisions/) is the seven decision records, each with a column for the uncomfortable part.

## What it does

You POST a job, Conduit runs it, and it keeps running it across process crashes, broker restarts, and handler failures without anyone watching.

A job can be one-off or recurring.
`POST /api/schedules` stores a cron expression and a task template in Postgres, and every instance polls that table, so recurring work needs no second process and no leader election: see [SCHEDULES.md](SCHEDULES.md).

Your code runs it one of two ways.
Either a worker you write in any language claims jobs over HTTP and reports the outcome back, which is the pull API in [WORKERS.md](WORKERS.md), or you use one of the two handlers that ship in-process: `webhook` for outbound HTTP delivery and `sql.etl` for Postgres-to-Postgres pipelines defined in JSON.
A remote worker gets the same lease, the same fencing token, and the same retry policy as an in-process one, because both paths write through the same code.

## The transport holds no authority

The design decision everything else follows from is that the transport holds no authority.
A job is durable the moment the Postgres insert commits, and the wake-up to a worker happens afterwards, so the HTTP response doesn't wait on it.
That would normally be a data-loss bug, since a dropped wake-up means nothing ever consumes the job.
It isn't one here, because the reconciler polls Postgres directly with `FOR UPDATE SKIP LOCKED` and feeds the same worker pool.
The transport is the fast path and the database is the floor.

That is what lets the whole thing be two containers.
The default `CONDUIT_TRANSPORT=postgres` sends wake-ups over `LISTEN`/`NOTIFY` and the default `CONDUIT_LOCK=advisory` guards execution with `pg_try_advisory_lock`, so an install is Postgres and one process.
Kafka and a three-node Redis Redlock quorum are still there, one environment variable away, and [what Kafka buys](BENCHMARKS.md#what-kafka-buys) is what they measure to.

Crash recovery works the same way rather than being a separate mechanism.
A worker that claims a job takes a lease with an expiry and renews it while running, so a process that dies mid-job leaves a `RUNNING` row with a stale `lease_expires_at`.
`RequeueExpiredRunning` finds those and moves them back to `PENDING` with `last_error` set to say why.
Nothing has to notice the crash.
Every state change goes through `CanTransition` and carries the claim's `lease_token` in its `WHERE` clause, so a worker whose lease already expired writes zero rows instead of overwriting a job someone else now owns.

## The path a job takes

An enqueue writes the job to Postgres as `PENDING` and returns; the wake-up behind it is fire-and-forget.
On the default transport that wake-up is a `pg_notify` carrying the job id, which cuts the reconciler's sleep short so it claims immediately instead of at the end of its idle interval; on the Kafka transport a consumer group delivers the message directly.
Either way the job reaches the worker pool, which is bounded by a semaphore and a buffered channel, and `jobWorker` does the four things that have to happen in order: ask that task's circuit breaker, take the execution lock so two instances can't run one job twice, execute the registered handler under `Task.Timeout`, then report the outcome back through `JobService`.
A remote worker enters at the same place from the other side: it claims a job, which is one `FOR UPDATE SKIP LOCKED` statement that hands back the row and its `lease_token`, and reports the outcome with that token.
Both paths end in the same two compare-and-swap statements, so a job's fate is written the same way whoever ran it.

A retryable failure computes a backoff delay and moves the job `RUNNING -> PENDING` with a future `scheduled_at` in one statement; an unknown task name goes straight to `DEAD` rather than looping.
The reconciler runs on its own ticker doing the two jobs no transport can: pulling `PENDING` work that was never delivered, and rescuing `RUNNING` rows whose lease ran out.
It is also the reason a transport is optional at all - it is already the guaranteed delivery path, so the transport only ever makes dispatch faster.

## What this is not

Conduit is not Temporal.
There is no durable execution, no replay of a function from an event history, no signals or timers or child workflows, and no DAG.
A job is one call to one handler, and if you need step three to depend on step two you need something else.
It is not Sidekiq or Asynq either.
Your code can run a job now, but it runs in *your* process behind an HTTP claim, not inside a library you imported: there is no decorator, no autodiscovery, no serialised closure, and no shared type between your worker and the queue.
[ADR 0006](decisions/0006-a-pull-api-instead-of-a-worker-sdk.md) is why that trade was made, and [ADR 0005](decisions/0005-webhook-as-the-execution-model.md) is what it replaced.
There is no UI, and there are no dependencies between jobs.

Celery is the closest comparison, and the one worth drawing carefully, because the scope is nearly the same: one task, retries with backoff, delayed execution, a cron ticker, workers fed by a broker.
Three things differ.
The broker is not the queue here: in Celery the message *is* the job and the result backend is a side table, so losing RabbitMQ loses queued work, whereas killing Kafka in the measurements cost 255x on dispatch latency and zero jobs.
Delivery semantics are not a flag: Celery acks on receipt by default and silently drops a task whose worker dies, `acks_late=True` upgrades that to at-least-once with a visibility timeout, and neither mode has any equivalent of the fencing token, so two Celery workers can both finish the same task and both report success.
And defining a task costs more here.
`@app.task` on any function is the entire reason people reach for Celery, and the equivalent in Conduit is a process that polls `POST /api/jobs/claim` and reports back, which is more code than a decorator and buys you a worker that can be written in any language and does not have to trust the queue's runtime.
Celery is a library you import. Conduit is a service you claim from.

The stack is the honest problem.
Measured against its own numbers, Postgres alone with `FOR UPDATE SKIP LOCKED` would serve this entire design: it is the source of truth, the claim mechanism, the scheduler, and the recovery path already.
Kafka buys a 255x improvement in median dispatch latency and nothing else, which is a real win if you care about the difference between 15 ms and 3.8 s, and a broker to operate if you don't.
Redis buys cross-instance mutual exclusion that the fencing token already covers for correctness, and measurably nothing for throughput; it earns its place only because a duplicated *side effect* is not something a fencing token can undo.
Below the throughput where dispatch latency matters, the three-dependency stack is not worth it, and the version of Conduit that dropped Kafka and Redis would be smaller, cheaper, and almost as good.
I built all three because I wanted to know exactly what each one cost, and now [the numbers](BENCHMARKS.md) say so.

## What building this taught me

**`sarama.OffsetNewest` silently discards every job enqueued before the consumer group finishes joining.**
A 2,000-job drain with the reconciler disabled stranded 1,332 jobs `PENDING` at `attempt=0` without one log line, and this happens on every deploy and rebalance, not just at first boot.
The reason it was never a data-loss incident is the reconciler, so the architecture's central claim held up under a bug that would otherwise have been catastrophic.

**A retry engine nobody calls still passes its unit tests.**
`internal/retry` computed exponential backoff correctly and had tests to prove it, and the worker never applied the result, so jobs entered `FAILED` and stopped there forever.
The same pass found `Attempt` never incremented and `Task.Timeout` parsed but never applied: testing the retry engine was never the same thing as testing that jobs retry.

**A distributed lock is not a fencing token.**
Redlock stops two workers starting the same job at once and does nothing about a worker whose lease expired mid-execution writing `COMPLETED` over a job already requeued and handed to someone else.
That needed `lease_token` in the `UPDATE ... WHERE` clause, so everything Conduit claims about at-least-once rests on one predicate rather than on three Redis nodes.
