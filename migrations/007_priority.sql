-- Higher priority is claimed first. 0 is the default, so existing jobs keep
-- their order relative to each other.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS task_priority INT NOT NULL DEFAULT 0;
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS task_priority INT NOT NULL DEFAULT 0;

-- The claim orders by priority before scheduled_at, so the pending indexes do too.
DROP INDEX IF EXISTS jobs_pending_idx;
DROP INDEX IF EXISTS jobs_pending_queue_idx;
DROP INDEX IF EXISTS jobs_pending_tenant_idx;
CREATE INDEX IF NOT EXISTS jobs_pending_priority_idx
    ON jobs (task_priority DESC, scheduled_at ASC)
    WHERE state = 'PENDING';
CREATE INDEX IF NOT EXISTS jobs_pending_queue_priority_idx
    ON jobs (task_queue, task_priority DESC, scheduled_at ASC)
    WHERE state = 'PENDING';
CREATE INDEX IF NOT EXISTS jobs_pending_tenant_priority_idx
    ON jobs (tenant_id, task_queue, task_priority DESC, scheduled_at ASC)
    WHERE state = 'PENDING';
