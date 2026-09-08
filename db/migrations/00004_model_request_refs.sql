-- +goose Up
ALTER TABLE model_attempts ADD COLUMN request_ref text;
-- +goose Down
ALTER TABLE model_attempts DROP COLUMN request_ref;
