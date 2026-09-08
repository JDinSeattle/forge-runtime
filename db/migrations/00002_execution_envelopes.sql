-- +goose Up
ALTER TABLE runs ADD COLUMN pending_commands jsonb NOT NULL DEFAULT '[]';
ALTER TABLE runs ADD COLUMN traceparent text NOT NULL DEFAULT '';
ALTER TABLE effects ADD COLUMN canonical_args bytea NOT NULL DEFAULT '';
ALTER TABLE model_attempts ADD COLUMN attempt_id text;
ALTER TABLE model_attempts ADD COLUMN started_at timestamptz NOT NULL DEFAULT clock_timestamp();
ALTER TABLE model_attempts ADD COLUMN deadline timestamptz;
ALTER TABLE model_attempts ADD COLUMN error_code text;
CREATE UNIQUE INDEX attempts_identity ON model_attempts(tenant_id,attempt_id) WHERE attempt_id IS NOT NULL;
ALTER TABLE runs ADD COLUMN retained_from_seq bigint NOT NULL DEFAULT 1;
CREATE INDEX runs_recoverable ON runs(tenant_id,lease_until,created_at) WHERE state IN ('running','cancel_requested','needs_reconciliation');
ALTER TABLE tenant_runtime ADD CONSTRAINT active_limit CHECK (active_count <= max_active);
ALTER TABLE runners ADD CONSTRAINT slot_limit CHECK (reserved_slots <= max_slots);

-- +goose Down
ALTER TABLE runners DROP CONSTRAINT slot_limit;
ALTER TABLE tenant_runtime DROP CONSTRAINT active_limit;
DROP INDEX runs_recoverable;
ALTER TABLE runs DROP COLUMN retained_from_seq;
DROP INDEX attempts_identity;
ALTER TABLE model_attempts DROP COLUMN error_code, DROP COLUMN deadline, DROP COLUMN started_at, DROP COLUMN attempt_id;
ALTER TABLE effects DROP COLUMN canonical_args;
ALTER TABLE runs DROP COLUMN traceparent, DROP COLUMN pending_commands;
