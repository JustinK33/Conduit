# Schedules

A schedule is a cron expression plus a task template.
Conduit stores it in Postgres, and every instance polls the table and enqueues an ordinary job when it comes due.
There is no separate scheduler process, no leader election, and nothing to run alongside Conduit.

## Creating one

```bash
curl -X POST http://localhost:8080/api/schedules \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "nightly-revenue",
    "cron": "0 3 * * *",
    "task": {
      "name": "sql.etl",
      "queue": "default",
      "max_retries": 3,
      "timeout": "10m",
      "payload": {"source_table": "raw.orders", "target_table": "analytics.daily_revenue"}
    }
  }'
```

```json
{
  "id": "5f1c3a4e-8b2d-4c11-9f63-7ac0d1e42b98",
  "name": "nightly-revenue",
  "cron": "0 3 * * *",
  "task": {"name": "sql.etl", "queue": "default", "max_retries": 3, "timeout": "10m", "payload": {"...": "..."}},
  "enabled": true,
  "next_run_at": "2026-09-23T03:00:00Z",
  "created_at": "2026-09-22T11:04:17Z",
  "updated_at": "2026-09-22T11:04:17Z"
}
```

`task` is the same object `POST /api/jobs` takes, so anything you can enqueue once you can enqueue on a schedule.
The `name` is unique, which is what makes a retried create idempotent: two POSTs cannot leave you with two schedules firing the same task.

`GET /api/schedules` lists them with `next_run_at`, `last_run_at`, and `last_job_id`, which is how you check a schedule is alive without opening `psql`.
`DELETE /api/schedules/:id` removes one; jobs it already enqueued are untouched and finish or fail on their own terms.

All three routes sit behind the same `CONDUIT_API_KEYS` as `/api/jobs`.
Creating a schedule is creating unbounded future work, so an open schedules endpoint is worse than an open enqueue.

## Cron syntax

Five fields: minute, hour, day-of-month, month, day-of-week.

| Field | Range | Notes |
|---|---|---|
| minute | 0-59 | |
| hour | 0-23 | |
| day-of-month | 1-31 | |
| month | 1-12 | Numbers only, so `1`, not `JAN` |
| day-of-week | 0-6 | `0` is Sunday. Numbers only, so `1`, not `MON` |

Each field takes `*`, a number, a list (`1,15`), a range (`1-5`), or a step (`*/15`).

```
*/15 * * * *      every fifteen minutes
0 3 * * *         03:00 every day
0 9 * * 1-5       09:00 on weekdays
0 0 1 * *         midnight on the first of the month
30 2,14 * * *     02:30 and 14:30
```

Three things the parser does not accept, so that it does not silently mean something else:

- **A step on a range.** `1-5/2` is rejected rather than read as `1-5`.
- **A step that does not divide from the field's minimum.** `*/15` in the day-of-month field counts from 1, so it means days 1, 16, and 31, not days 15 and 30.
- **Names.** `MON` and `JAN` are errors, not zeroes.

One deviation from Vixie cron worth knowing: when both day-of-month and day-of-week are restricted, Conduit requires **both** to match.
Vixie cron would fire if **either** matched.
`0 0 13 * 5` means "midnight on Friday the 13th" here, and "midnight on the 13th, and midnight every Friday" in `crontab`.
Restricting only one of the two behaves identically in both, which covers nearly every real schedule.

An expression that cannot be parsed is rejected at create time with a `400`.
A schedule that could not be parsed would log an error every tick forever and never run, which is a worse way to find out.

## Everything is UTC

`cron` is evaluated in UTC, and `next_run_at` and `last_run_at` are UTC, like every other timestamp in Conduit.
There are no per-schedule timezones, so `0 3 * * *` is 03:00 UTC, not 03:00 wherever the deployment happens to be.

For a job that must run at a local wall-clock hour across a DST boundary, write the UTC hour you want and accept that it moves by one relative to local time, or run two schedules and let the task decide.

## What "due" means

The scheduler polls every `CONDUIT_SCHEDULER_TICK_INTERVAL` (default `30s`) and fires up to `CONDUIT_SCHEDULER_MAX_CONCURRENT_RUNS` schedules per tick (default `5`).

A schedule is therefore late by at most one tick: one due at 03:00:00 enqueues by 03:00:30.
Cron granularity is a minute, so a 30-second tick is half a cron unit, and lowering it buys precision cron cannot express.

The per-tick budget is a `LIMIT`, not a rate limit.
With more than five schedules due in the same instant, the oldest `next_run_at` go first and the rest fire on the following tick.

## Catch-up is skip, not replay

A schedule whose `next_run_at` is a day in the past fires **once** and then advances to the next instant strictly after now.
It does not replay the 24 missed hourly runs, and the log line records how many it skipped:

```json
{"level":"info","component":"scheduler","schedule":"hourly-rollup","job_id":"...","fired_for":"2026-09-21T11:00:00Z","next_run_at":"2026-09-22T12:00:00Z","skipped_occurrences":23,"message":"scheduler: fired schedule"}
```

Replaying a day of missed runs on boot is almost never what the author of a nightly aggregate wanted, and it is the shape of an outage that turns into a thundering herd the moment the outage ends.
If you need the missed windows processed, the task's payload should describe a range rather than "now", and one run can cover the gap.

## Many instances, one job

Every instance with `CONDUIT_SCHEDULER_ENABLED=true` polls the same table and they all see the same due schedule.
All of them try to fire it.
Exactly one job comes out, and this is how:

1. Each instance enqueues with `idempotency_key = "sched:<schedule id>:<fire unix>"`. The key is derived from the fire instant, not the wall clock, so all ten replicas compute the same string.
2. `jobs_idempotency_key_idx` makes the second insert impossible, and `Enqueue` answers a duplicate key by returning the existing job's id instead of an error. Nine replicas get the winner's job id back and do nothing else.
3. The advance is `UPDATE schedules SET next_run_at = $next WHERE id = $1 AND next_run_at = $observed`, so exactly one replica's `UPDATE` matches and the rest affect zero rows.

That is the whole coordination story: no leader election, no advisory lock, no new failure mode.
It holds at three replicas and at the ten `deploy/k8s/hpa.yaml` scales to.

In practice you will usually see one instance logging every fire of a given schedule, which is not a sign the others are asleep.
Whichever instance ticks first after the fire instant advances the row in a few milliseconds, so by the time the next instance ticks the schedule is no longer due and it has nothing to report.
The losers log at debug (`scheduler: another instance advanced this schedule`), and stopping the instance that appears to own a schedule makes another pick it up within one tick.

Enqueue happens **before** the advance, deliberately.
A crash in between re-fires the same instant on the next tick, where the idempotency key absorbs it.
Advancing first would lose that fire silently.

The `sched:` idempotency prefix is reserved.
Enqueueing your own job with a key starting `sched:` risks colliding with a schedule's fire and being answered with that job's id.

`CONDUIT_SCHEDULER_ENABLED=false` on every instance means no schedule ever fires; the rows sit there with a `next_run_at` in the past.
An instance that boots with it off logs a warning saying so.

## Finding a schedule's jobs

Every fired job carries two metadata keys:

```json
{"metadata": {"conduit.schedule_id": "5f1c3a4e-...", "conduit.schedule": "nightly-revenue"}}
```

`last_job_id` on the schedule points at the most recent one, and the idempotency key tells you which instant a job belongs to:

```sql
SELECT id, state, scheduled_at, completed_at, last_error
FROM jobs
WHERE metadata->>'conduit.schedule' = 'nightly-revenue'
ORDER BY scheduled_at DESC
LIMIT 20;
```

Note that `CONDUIT_RETENTION_COMPLETED` prunes that history after seven days by default.
See [DEPLOYMENT.md](DEPLOYMENT.md#retention).

## No PATCH

There is no update route.
Pausing a schedule is `DELETE` and re-create, and changing a cron expression is the same two calls.
That loses `last_run_at` and `last_job_id` and nothing else, and one column plus one route is a cheap thing to add when somebody actually needs the history preserved across an edit.
