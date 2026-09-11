-- +goose Up
-- Reserved bounded metadata. Scheduling remains tenant rotation then FIFO.
ALTER TABLE runs ADD COLUMN priority smallint NOT NULL DEFAULT 0
    CHECK (priority BETWEEN -2 AND 2);

-- +goose Down
ALTER TABLE runs DROP COLUMN priority;
