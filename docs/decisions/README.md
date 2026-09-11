# Decision records

Short records of the architectural decisions that shaped Conduit, in the order they were made.
Each one states the context, the decision, and the consequences, including the ones that cost something.

| # | Decision | The uncomfortable part |
|---|---|---|
| [0001](0001-postgres-as-the-source-of-truth.md) | Postgres is the source of truth, not Redis and not Kafka | Every state change is a round trip to a single writer, and there is no sharding story |
| [0002](0002-kafka-as-transport-not-as-the-queue.md) | Kafka is transport, not the queue | A broker to operate, bought purely for dispatch latency, and it is mandatory at boot despite being best-effort at runtime |
| [0003](0003-redlock-over-postgres-advisory-locks.md) | Redlock over Postgres advisory locks | The fencing token is what makes this correct, so Redlock is defence in depth and `pg_advisory_xact_lock` would have been simpler |
| [0004](0004-leases-and-a-reconciler-instead-of-kafka-redelivery.md) | Leases and a reconciler, not Kafka redelivery | Crash recovery is bounded below by the lease duration, five minutes by default |
| [0005](0005-webhook-as-the-execution-model.md) | A webhook is the execution model | A job cannot run in your process, so Conduit is not a library |
| [0006](0006-a-pull-api-instead-of-a-worker-sdk.md) | Workers pull over HTTP instead of importing an SDK | One shared API key with no per-worker identity, and a claimed job that vanishes costs a full lease before anyone notices |

New records: copy the format, take the next number, and be specific about what the decision costs.
A record that only lists benefits is not finished.
