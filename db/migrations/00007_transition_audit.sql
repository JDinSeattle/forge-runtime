-- +goose Up
-- SSE is compact and may expire. Keep the exact pure-reducer inputs with the
-- resulting snapshot so a heartbeat-updated lease can be replayed faithfully.
ALTER TABLE run_snapshots ADD COLUMN input_state json;
ALTER TABLE run_snapshots ADD COLUMN input_event json;
-- +goose Down
ALTER TABLE run_snapshots DROP COLUMN input_event;
ALTER TABLE run_snapshots DROP COLUMN input_state;
