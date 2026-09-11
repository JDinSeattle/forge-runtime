-- +goose Up
-- Operational compatibility metadata is separate from the unknown state body.
-- Do not rewrite snapshots, commands, versions, leases or accounting here.
ALTER TABLE runs ADD COLUMN snapshot_hold_at timestamptz;
ALTER TABLE runs ADD COLUMN snapshot_hold_reason text;
ALTER TABLE runs ADD COLUMN snapshot_hold_schema text;
ALTER TABLE runs ADD COLUMN snapshot_hold_sha256 text;
ALTER TABLE runs ADD CONSTRAINT snapshot_hold_complete CHECK (
    (snapshot_hold_at IS NULL AND snapshot_hold_reason IS NULL AND snapshot_hold_schema IS NULL AND snapshot_hold_sha256 IS NULL)
    OR (snapshot_hold_at IS NOT NULL AND snapshot_hold_reason IS NOT NULL AND snapshot_hold_sha256 IS NOT NULL
        AND snapshot_hold_reason IN ('unsupported_schema','malformed_schema')
        AND snapshot_hold_schema IS NOT NULL AND length(snapshot_hold_schema)<=64
        AND snapshot_hold_sha256 ~ '^[0-9a-f]{64}$'));
CREATE INDEX runs_snapshot_holds ON runs(snapshot_hold_at,tenant_id,id) WHERE snapshot_hold_at IS NOT NULL;

-- +goose Down
DROP INDEX runs_snapshot_holds;
ALTER TABLE runs DROP CONSTRAINT snapshot_hold_complete;
ALTER TABLE runs DROP COLUMN snapshot_hold_at, DROP COLUMN snapshot_hold_reason,
    DROP COLUMN snapshot_hold_schema, DROP COLUMN snapshot_hold_sha256;
