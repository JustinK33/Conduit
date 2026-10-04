-- The lease reaper used to treat task_max_retries = 0 as "no budget", so a job
-- whose worker kept dying was requeued forever. Enqueue now stores the engine's
-- budget instead of 0, and these rows get the same one: 5, which is
-- retry.DefaultMaxAttempts.
UPDATE jobs SET task_max_retries = 5 WHERE task_max_retries <= 0;
UPDATE schedules SET task_max_retries = 5 WHERE task_max_retries <= 0;

-- Retention prunes DEAD jobs by completed_at, and cancel and the reaper never
-- set it, so those jobs were kept forever. updated_at is when they went DEAD.
UPDATE jobs SET completed_at = updated_at WHERE state = 'DEAD' AND completed_at IS NULL;
