# Benchmarks

Every number here comes from `make measure` and `make bench`.

```bash
make bench      # microbenchmarks, no infra, about 55 seconds
make measure    # every number below, about 12 minutes
```

`make measure` starts a webhook sink behind the `loadtest` Compose profile and runs [`loadtest/measure.sh`](../loadtest/measure.sh), which recreates the `app` container between runs to change its configuration.
It needs `k6`, `jq`, and `python3`, and it will disrupt anything else you have running on that stack.
Individual sections run on their own: `loadtest/measure.sh drain`, `./loadtest/measure.sh crash`.

## The hardware caveat

All numbers below were taken on an Apple M4, 10 cores, 16 GB RAM, macOS 26.3.1, with every service in Docker Compose on the same host.
That means the load generator, the broker, three Redis nodes, Postgres, and the app all contend for the same 10 cores, so treat these as relative and reproducible rather than as a capacity number for real hardware.

## Intake

k6, 100 VUs, 10 s ramp / 30 s hold / 10 s ramp-down, `CONDUIT_WORKER_CONCURRENCY=8`.

| | |
| --- | --- |
| Accepted | 4,684 req/s, 234,279 jobs |
| API latency | p50 1.49 ms, p95 8.03 ms, p99 18.19 ms |
| Failed requests | 0 |
| Peak queue depth | 234,263 `PENDING` |

This is a re-measurement after the pull API landed, with `CONDUIT_API_KEYS` unset so the auth middleware is the pass-through.
The previous run was 4,864 req/s at p50 1.39 ms, so the four new endpoints and the queue filter cost nothing outside run-to-run variance.

This measures intake only, and the peak queue depth is the tell: the API accepts jobs about 90x faster than the system executes them.
The default k6 task name is `k6-load-test`, which has no registered handler, so every one of those jobs is designed to fail on execution.
When these ran, they failed through a single process-global circuit breaker, which then opened and starved the real work, so an intake run and an execution run could not be the same run.
Breakers are per registered task now, with one shared between all unregistered names, so `k6-load-test` can only open that shared one and `webhook` keeps running.
The two runs stay separate anyway: 234,000 jobs queued ahead of a drain is not a drain measurement.

## Execution

2,000 real `webhook` jobs against a local sink, timed from the first enqueue until all 2,000 reach `COMPLETED` or `DEAD`.
Latency is end-to-end wall clock out of Postgres (`updated_at - created_at`), not time spent inside the worker.

The default stack is Postgres `LISTEN`/`NOTIFY` plus advisory locks, which `loadtest/measure.sh drain-postgres` runs.
The only knob that moves this number is `CONDUIT_RECONCILER_BATCH_SIZE`, because on this transport the reconciler is the only claim path there is: `CONDUIT_WORKER_QUEUE_SIZE` and `CONDUIT_WORKER_CONCURRENCY` measured as noise, alone and together.
It is the reason the default is 500 rather than 100.

Run-to-run variance on this box is large enough that a single pair of runs proves nothing, so the A/B is six pairs, each pair back to back on the same machine state:

| Pair | batch 100 (old default) | batch 500 (current) |
| --- | --- | --- |
| 1 | 571.4 | 542.0 |
| 2 | 324.1 | 613.5 |
| 3 | 260.4 | 279.3 |
| 4 | 251.3 | 259.7 |
| 5 | 234.7 | 291.5 |
| 6 | 174.8 | 318.5 |

Throughput alone is not a clean win: 500 takes five of six pairs, but three of those margins are inside the noise band.
The tail is the unambiguous part.
Worst e2e p95 across the six runs was **4.45 s at batch 100 against 1.68 s at batch 500**, and batch 100's worst `max` was 5.40 s against 1.80 s.
A claim budget of 100 per one-second tick means a 2,000-job burst needs twenty ticks in the best case, and every tick it misses is a second of latency for everything behind it.

Tuning past the current defaults buys nothing measurable:

| Config | jobs/s (per run) | worst e2e p95 |
| --- | --- | --- |
| Defaults: concurrency 8, queue 256, batch 500 | 606.1, 298.1, 276.6 | 1.27 s |
| Concurrency 32, queue 4096, batch 2000, 50 Postgres conns | 419.3, 305.3, 249.1 | 1.15 s |

That is phase 4's goal met: the untuned number is no longer the bad one, so the first thing an evaluator measures is not a misconfiguration.

On the faster runs `drain_s` converges on `enqueue_s` (3.30 s against 3.18 s), which means the 20-way-parallel `curl` enqueuer is the ceiling, not Conduit.
Treat ~600 jobs/s as a floor for the defaults on this hardware, not a capacity figure.

The previous defaults were measured on the **Kafka plus Redlock** stack, which `loadtest/measure.sh` still pins in `base_app`, and the dynamics there are not the same: a burst past `CONDUIT_WORKER_QUEUE_SIZE` fills the pool's buffer, dispatch falls back to the reconciler, and the queue size is what dominates.
Those rows are kept because they are the A/B for the transport, not because they describe the defaults:

| Config (Kafka transport, Redlock) | jobs/s | e2e p50 | e2e p99 |
| --- | --- | --- | --- |
| Concurrency 8, queue 256, batch 100 | 39.5 - 78.7 (5 runs) | 18.5 s | 26.8 s |
| `CONDUIT_WORKER_QUEUE_SIZE=4096` | 195.3 | 1.69 s | - |
| `CONDUIT_RECONCILER_BATCH_SIZE=2000` | 133.4 | 0.22 s | 14.0 s |
| All of the above, concurrency 32 | 530.5 | 1.86 s | 3.10 s |

Mean time inside a worker is 5.76 ms (`conduit_server_job_duration_seconds_sum / _count` over the 2,000-job run), so eight workers should manage roughly 1,400 jobs/s.
On that stack they managed 55, which is what made "the configuration is the bottleneck, not the architecture" the phase 4 headline.
The number turned out to be a Kafka-transport artefact as much as a defaults one: the same untuned defaults on `LISTEN`/`NOTIFY` were already doing ~260-320 jobs/s before the batch size changed.

Run-to-run variance on the Kafka row is about +/- 40% (39.6, 56.3, 54.0, 78.7, 39.5 jobs/s), which is why it is a range.
Anything in either table under a 2x difference is noise.

## What Kafka buys

20 jobs enqueued one second apart onto an otherwise idle queue, so each one is dispatched with nothing ahead of it.

| Dispatch path | p50 | p95 | max |
| --- | --- | --- | --- |
| Postgres `LISTEN`/`NOTIFY` (default) | 6.3, 6.5 ms | 8.9, 9.7 ms | 11.8, 13.7 ms |
| Kafka consumer | 14.9 ms | 1,537.5 ms | 2,568.6 ms |
| No transport at all (reconciler only) | 3,796.3 ms | 13,159.8 ms | 14,201.4 ms |

The Postgres row is two runs, because beating Kafka on all three columns is a large enough claim to want it reproduced. It reproduced.

Having a transport is worth roughly 600x on median dispatch latency.
Which transport is Kafka's contribution, and the answer is that it costs.
The reconciler-only numbers are not a bug: an idle reconciler backs off to `CONDUIT_RECONCILER_IDLE_INTERVAL=15s`, and a 13.2 s p95 is exactly what a 15 s poll looks like.
Throughput is unaffected by all three, because at volume every path converges on the same worker pool.

The `NOTIFY` path wins because it does strictly less.
A notification carries a job id and no authority, so all it does is cut the reconciler's sleep short and let the same `FOR UPDATE SKIP LOCKED` claim run immediately; the whole round trip is one `pg_notify` on a connection the app already holds.
Kafka's p50 is a broker round trip on top of that, and its p95 is consumer-group rebalancing, which is the cost of a component that maintains its own partition assignment and offsets to deliver a payload that gets thrown away.

That is the case for the default, and it also closes the gap this section used to end with: Kafka was best-effort at runtime but mandatory at boot, because `queue.NewKafkaClient` returned an error if the broker was unreachable and `run()` exited.
It is now only constructed for `CONDUIT_TRANSPORT=kafka`, so the tolerance and the requirement finally agree.

## What Redlock buys

Same 2,000-job drain, three-node Redlock quorum versus a single Redis node.

| Config | jobs/s (per run) |
| --- | --- |
| `redis:6379,redis-2:6379,redis-3:6379` | 39.6, 56.3, 54.0, 78.7, 39.5 |
| `redis:6379` | 67.7, 54.2, 54.0, 65.5 |

No measurable difference, and the ranges overlap completely.
The quorum is not free (N acquire plus N release round trips per job, roughly 200 ms on the contended path), it is just far cheaper than the dispatch ceiling above it.
It also isn't what makes execution safe: [ADR 0003](decisions/0003-redlock-over-postgres-advisory-locks.md) explains why the `lease_token` fencing check in `UpdateJob` is the actual correctness mechanism and Redlock is defence against duplicated *effort*, like an outbound webhook no fencing token can undo.

Which is why it is no longer the default.
`CONDUIT_LOCK=advisory` gets the same defence out of `pg_try_advisory_lock` with no extra service, and one property Redlock cannot have: an advisory lock belongs to its database session, so a `SIGKILL`ed process releases every lock it held the moment the server notices the socket is gone.
The crash numbers below are what that fixes.

## Crash recovery

20 jobs held in `RUNNING` against a sink that sleeps 8 seconds, `CONDUIT_RECONCILER_RUNNING_LEASE=15s`, then `docker compose kill -s KILL app` and an immediate restart.

| | Run 1 | Run 2 | Run 3 |
| --- | --- | --- | --- |
| All 20 requeued to `PENDING` | 16.34 s | 23.65 s | 16.39 s |
| All 20 terminal | 936.98 s | 37.91 s | 31.37 s |

Requeue time is bounded below by the lease, which is the design working: nothing can requeue a job until its lease is provably dead, so a 15 s lease costs at least 15 s.
The default `CONDUIT_RECONCILER_RUNNING_LEASE` is 5 minutes, so a real crash recovers in minutes, not seconds.

That 936.98 s outlier is not explainable from the code and did not reproduce in two subsequent runs, so it is reported rather than averaged away.
What the logs *did* explain: after a `SIGKILL`, the dead process's `job:exec:<id>` Redlock keys survive with their full 30 s TTL, so the restarted process burns 300 lock-acquire failures and up to 16 attempts per job churning claim-and-release until they expire.
Redlock's TTL is hardcoded and never renewed, which means a crash costs the lock TTL on top of the lease.

Those runs were measured with Redlock. Re-run on the current default, `CONDUIT_LOCK=advisory`:

| | Run 1 | Run 2 | Run 3 |
| --- | --- | --- | --- |
| All 20 requeued to `PENDING` | 24.73 s | 25.06 s | not observed |
| All 20 terminal | 24.84 s | 25.17 s | 24.76 s |

The requeue floor is unchanged, because it is the lease and never the lock.
What changed is the gap between the two rows: 0.11 s here against 7 to 21 s under Redlock, and 920 s in that one outlier.
That gap *was* the churn. An advisory lock belongs to a database session, so a `SIGKILL`ed process leaves nothing behind to wait out, and the restarted process claims each job once instead of fighting keys the dead process still owns.

Run 3's blank is a sampling artifact, not a failure: the poller counts `RUNNING` rows every 250 ms, and with no lock churn to slow it down the requeue and the re-execution both landed inside one interval, so the count never read zero. All 20 still reached `COMPLETED`, in 24.76 s.

## Microbenchmarks

`make bench`, in-process, no infra. These bound the hot paths, they do not predict system throughput.

| | |
| --- | --- |
| `CanTransition` | 1.36 ns/op, 0 allocs |
| Circuit breaker, closed | 3.77 ns/op |
| Circuit breaker, 32 goroutines | 128.8 ns/op |
| Backoff with jitter | 6.87 ns/op |
| Pool dispatch | 733 ns/op, 1,363,948 jobs/s, 2 allocs |
| Pool submit, contended | 3,395 ns/op, 294,517 jobs/s |
| Cron parse | 195 - 294 ns/op, 12 - 14 allocs |

The pool can move 1.4 million jobs/s and the system drains 530.
Nothing in Go is the bottleneck here; the network round trips and the dispatch configuration are.
