-- Tenants. Every job and schedule belongs to one, and an API key acts for
-- exactly one. Existing rows land in 'default', which is also the tenant the
-- keys in CONDUIT_API_KEYS act for, so a deployment that never issues a key
-- sees no change.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'default';
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'default';

-- Idempotency keys and schedule names are the caller's vocabulary, so two
-- tenants picking the same one must not collide.
DROP INDEX IF EXISTS jobs_idempotency_key_idx;
CREATE UNIQUE INDEX IF NOT EXISTS jobs_tenant_idempotency_key_idx
    ON jobs (tenant_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

ALTER TABLE schedules DROP CONSTRAINT IF EXISTS schedules_name_key;
CREATE UNIQUE INDEX IF NOT EXISTS schedules_tenant_name_idx
    ON schedules (tenant_id, name);

-- The API always lists within one tenant now, so the list indexes lead with it.
DROP INDEX IF EXISTS jobs_created_idx;
DROP INDEX IF EXISTS jobs_state_created_idx;
CREATE INDEX IF NOT EXISTS jobs_tenant_created_idx
    ON jobs (tenant_id, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS jobs_tenant_state_created_idx
    ON jobs (tenant_id, state, created_at DESC, id DESC);

-- A remote worker claims within its tenant. The reconciler still claims across
-- all of them through jobs_pending_queue_idx.
CREATE INDEX IF NOT EXISTS jobs_pending_tenant_idx
    ON jobs (tenant_id, task_queue, scheduled_at ASC)
    WHERE state = 'PENDING';

-- Only the hash is stored. A key is 32 random bytes, so a plain SHA-256 is
-- enough; a slow hash buys nothing against a secret nobody can guess.
CREATE TABLE IF NOT EXISTS api_keys (
    id          TEXT        PRIMARY KEY,
    tenant_id   TEXT        NOT NULL,
    name        TEXT        NOT NULL DEFAULT '',
    key_hash    BYTEA       NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at  TIMESTAMPTZ
);
