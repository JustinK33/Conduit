-- Schedules: recurring job definitions, evaluated by internal/scheduler.
--
-- A row here is a template plus a cron expression, not a job. Firing a schedule
-- enqueues an ordinary row in `jobs`, so retries, leases, backoff, the pull API,
-- and the state machine all apply to a scheduled run exactly as they do to one
-- somebody POSTed by hand.
--
-- The task columns are flattened the same way jobs flattens models.Task, so the
-- two tables stay readable side by side in psql. task_id is absent on purpose:
-- it is minted per run at fire time, because two runs of one schedule are two
-- different jobs.
CREATE TABLE IF NOT EXISTS schedules (
    id                  TEXT        PRIMARY KEY,
    -- name is the operator's handle for the schedule and is unique, so a
    -- redeployed config cannot quietly create a second copy of the same job.
    name                TEXT        NOT NULL UNIQUE,
    cron_expr           TEXT        NOT NULL,

    -- Task template (flattened from models.Task, minus the per-run id)
    task_name           TEXT        NOT NULL,
    task_queue          TEXT        NOT NULL DEFAULT 'default',
    task_payload        BYTEA,
    task_max_retries    INT         NOT NULL DEFAULT 3,
    task_timeout_ns     BIGINT      NOT NULL DEFAULT 0,
    task_metadata       JSONB,

    enabled             BOOLEAN     NOT NULL DEFAULT TRUE,
    -- next_run_at is the fire instant, and also the optimistic-concurrency
    -- token: the advance is conditional on the value the firing instance read,
    -- so exactly one replica of many wins each fire. See AdvanceSchedule.
    next_run_at         TIMESTAMPTZ NOT NULL,
    last_run_at         TIMESTAMPTZ,
    last_job_id         TEXT,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Serves DueSchedules, which is the only read on the hot path: every instance
-- runs it every tick, and a disabled schedule should cost nothing to skip.
CREATE INDEX IF NOT EXISTS schedules_due_idx
    ON schedules (next_run_at ASC)
    WHERE enabled;
