-- Retention and the backlog gauge both ask about finished jobs, and until now
-- nothing indexed them: migration 001 covers PENDING and RUNNING only, because
-- those are the states the claim and lease-recovery paths read.
--
-- A finished job is addressed by completed_at in both new callers - the
-- retention delete asks "older than this" and the gauge asks "how many" - so one
-- partial index per terminal state serves both. Partial rather than a plain
-- index on completed_at, so neither index carries the PENDING rows that make up
-- the hot path.
CREATE INDEX IF NOT EXISTS jobs_completed_retention_idx
    ON jobs (completed_at ASC)
    WHERE state = 'COMPLETED';

CREATE INDEX IF NOT EXISTS jobs_dead_retention_idx
    ON jobs (completed_at ASC)
    WHERE state = 'DEAD';
