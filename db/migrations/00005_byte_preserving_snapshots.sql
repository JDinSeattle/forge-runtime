-- +goose Up
-- JSONB rewrites nested RawMessage tool arguments, breaking their byte-exact
-- SHA256 identity after recovery. Snapshots/commands are opaque versioned JSON
-- envelopes; indexed relational columns remain the query surface.
-- This initial development migration cannot repair previously reformatted
-- snapshots. Such runs must remain paused and be reconciled from effect
-- canonical_args and authenticated receipts, never silently rehashed.
ALTER TABLE runs ALTER COLUMN snapshot TYPE json USING snapshot::json;
ALTER TABLE runs ALTER COLUMN pending_commands TYPE json USING pending_commands::json;
ALTER TABLE run_snapshots ALTER COLUMN body TYPE json USING body::json;
-- +goose Down
ALTER TABLE run_snapshots ALTER COLUMN body TYPE jsonb USING body::jsonb;
ALTER TABLE runs ALTER COLUMN pending_commands TYPE jsonb USING pending_commands::jsonb;
ALTER TABLE runs ALTER COLUMN snapshot TYPE jsonb USING snapshot::jsonb;
