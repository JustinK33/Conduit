-- task_queue routes claims as of the pull-based worker API, so it needs an
-- index and it needs exactly one spelling of "the default queue".

-- The column has defaulted to 'default' since 001, but CreateJob always writes
-- task_queue explicitly, so a job enqueued without one landed as ''. That was
-- invisible while nothing read the column and is a routing bug now: a worker
-- claiming from 'default' would not see those rows. Enqueue normalises going
-- forward; this fixes anything already written.
UPDATE jobs SET task_queue = 'default' WHERE task_queue = '';

-- Serves the queue filter in ClaimNextJob, which orders by scheduled_at over
-- PENDING rows only.
CREATE INDEX IF NOT EXISTS jobs_pending_queue_idx ON jobs (task_queue, scheduled_at ASC) WHERE state = 'PENDING';
