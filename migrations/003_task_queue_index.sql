-- Index for filtered claim on task_queue.
-- Serves the worker queue filter in ClaimNextJob.

CREATE INDEX IF NOT EXISTS jobs_pending_queue_idx ON jobs (task_queue, scheduled_at ASC) WHERE state = 'PENDING';
