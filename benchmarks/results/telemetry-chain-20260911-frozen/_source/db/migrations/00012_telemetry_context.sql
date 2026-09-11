-- +goose Up
-- Observability metadata is independent of reducer snapshots and request hashes.
-- Missing historical context/admission is deliberately not reconstructed.
ALTER TABLE runs ADD COLUMN runnable_at timestamptz;
ALTER TABLE runs ADD COLUMN lease_yielded boolean NOT NULL DEFAULT false;
ALTER TABLE runs ADD COLUMN last_claim_traceparent text;
ALTER TABLE model_attempts ADD COLUMN traceparent text;
ALTER TABLE runs ADD CONSTRAINT claim_traceparent_bounded CHECK(last_claim_traceparent IS NULL OR length(last_claim_traceparent)=55);
ALTER TABLE model_attempts ADD CONSTRAINT attempt_traceparent_bounded CHECK(traceparent IS NULL OR length(traceparent)=55);
-- +goose Down
ALTER TABLE model_attempts DROP COLUMN traceparent;
ALTER TABLE runs DROP COLUMN runnable_at, DROP COLUMN last_claim_traceparent, DROP COLUMN lease_yielded;
