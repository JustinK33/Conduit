# SQL ELT Pipelines

Conduit can run SQL ELT pipelines as durable background jobs.
The built-in `sql.etl` task reads a JSON pipeline spec from `task.payload`, validates it, and executes an `INSERT INTO target SELECT ...` statement against the configured Postgres database.

## Use Case

Use this when an application needs lightweight operational analytics without adopting a full workflow platform.
For example, an ecommerce service can write raw orders into `raw.orders`, then create a schedule that aggregates paid orders into `analytics.daily_revenue` at 03:00 every night.
The queue handles retries, leases, cancellation, backoff, and metrics while Postgres performs the actual extract, transform, and load work.

## Why This Is Useful

Developers can add reliable data workflows to an existing Go service without running Airflow, Dagster, or a separate scheduler.
The cron expression lives in Conduit's own `schedules` table, so nothing outside the stack decides when a pipeline runs.
The system is small enough for product teams but still demonstrates production mechanics that matter in real deployments.
Those mechanics include idempotent enqueue, durable job state, `FOR UPDATE SKIP LOCKED` claiming, retry backoff, Redis-backed execution locks, and Prometheus metrics.

## Task Contract

Submit a job with `task.name` set to `sql.etl`.
Put the pipeline spec straight into `task.payload`, which carries JSON inline.
The pipeline spec must contain `extract_sql`, `target_table`, and `target_columns`.
The optional `write_mode` can be `append` or `upsert`.
When `write_mode` is `upsert`, `conflict_columns` must name the target key columns.

```json
{
  "extract_sql": "SELECT ordered_at::date AS revenue_day, COUNT(*) AS order_count, SUM(order_total) AS gross_revenue FROM raw.orders WHERE status = 'paid' GROUP BY ordered_at::date",
  "target_table": "analytics.daily_revenue",
  "target_columns": ["revenue_day", "order_count", "gross_revenue"],
  "write_mode": "upsert",
  "conflict_columns": ["revenue_day"]
}
```

The executor builds this SQL:

```sql
INSERT INTO "analytics"."daily_revenue" ("revenue_day", "order_count", "gross_revenue")
SELECT ordered_at::date AS revenue_day, COUNT(*) AS order_count, SUM(order_total) AS gross_revenue
FROM raw.orders
WHERE status = 'paid'
GROUP BY ordered_at::date
ON CONFLICT ("revenue_day") DO UPDATE SET "order_count" = EXCLUDED."order_count", "gross_revenue" = EXCLUDED."gross_revenue"
```

## Safety Rules

The `extract_sql` query must start with `SELECT` or `WITH`.
Write-oriented tokens such as `INSERT`, `UPDATE`, `DELETE`, `DROP`, `TRUNCATE`, `ALTER`, and `CREATE` are rejected before execution.
Target table and column names are validated as SQL identifiers and quoted through pgx.
Bad pipeline specs are marked as permanent errors so they do not burn retry capacity.

## Local Demo

Apply the optional demo migration after the base jobs migration.

```bash
docker exec -i conduit-postgres-1 psql -U conduit -d conduit < migrations/002_create_elt_demo.sql
```

Start the stack and enqueue the sample ELT job.

```bash
make up
make enqueue-elt
```

Check the job and query the target table.

```bash
make list state=COMPLETED
docker exec -it conduit-postgres-1 psql -U conduit -d conduit -c 'SELECT * FROM analytics.daily_revenue ORDER BY revenue_day;'
```

## Making It Nightly

`make enqueue-elt` runs the pipeline once.
A schedule is what makes it recurring, and it takes the same `task` object:

```bash
curl -X POST localhost:8080/api/schedules -H 'content-type: application/json' \
  -d "{\"name\":\"nightly-revenue\",\"cron\":\"0 3 * * *\",\"task\":{\"name\":\"sql.etl\",\"queue\":\"default\",\"timeout\":\"10m\",\"max_retries\":3,\"payload\":$(cat examples/daily_revenue_pipeline.json)}}"
```

```bash
curl localhost:8080/api/schedules   # next_run_at is the next 03:00 UTC
```

Every fired job carries `conduit.schedule` in its metadata, so the run history is one query:

```sql
SELECT id, state, scheduled_at, completed_at, last_error
FROM jobs
WHERE metadata->>'conduit.schedule' = 'nightly-revenue'
ORDER BY scheduled_at DESC;
```

The cron expression is UTC, a fire missed while the stack was down is skipped rather than replayed, and running several instances still produces one job per night.
[../SCHEDULES.md](../SCHEDULES.md) is the full contract.

An aggregate that recomputes the whole table each run is idempotent, so a retry is free.
One that appends is not, which is what `write_mode: upsert` and `conflict_columns` are for.

## Resume Story

This is no longer only a task queue.
It is a reliable data workflow runtime for operational analytics.
A strong resume bullet would be:

> Built Conduit, a Go-based data workflow runtime that executes SQL ELT pipelines through Kafka-backed durable jobs, Postgres state machines, Redis distributed locks, retry backoff, lease recovery, and Prometheus observability.

## Next Improvements

Store execution statistics such as rows loaded, runtime, and last successful watermark in job metadata.
Add a `sql.explain` preflight mode that captures `EXPLAIN (FORMAT JSON)` output for slow pipeline optimization.
Add connector tasks for S3 CSV ingestion, HTTP JSON extraction, and Postgres-to-Postgres replication.
Add replace-partition destination mode for partitioned analytics tables.
