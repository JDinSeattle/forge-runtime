-- +goose Up
ALTER TABLE provider_quotas ADD COLUMN committed_tokens bigint NOT NULL DEFAULT 0 CHECK (committed_tokens >= 0);
ALTER TABLE provider_quotas ADD COLUMN window_duration_ms bigint NOT NULL DEFAULT 60000 CHECK (window_duration_ms > 0);
ALTER TABLE provider_quotas ADD COLUMN window_generation bigint NOT NULL DEFAULT 1 CHECK (window_generation > 0);
ALTER TABLE provider_quotas ADD COLUMN consecutive_failures integer NOT NULL DEFAULT 0 CHECK (consecutive_failures >= 0);
ALTER TABLE provider_quotas ADD COLUMN failure_threshold integer NOT NULL DEFAULT 5 CHECK (failure_threshold > 0);
ALTER TABLE provider_quotas ADD COLUMN breaker_cooldown_ms bigint NOT NULL DEFAULT 30000 CHECK (breaker_cooldown_ms > 0);
ALTER TABLE provider_quotas ADD COLUMN probe_tenant_id text;
ALTER TABLE provider_quotas ADD COLUMN probe_reservation_id text;
ALTER TABLE provider_quotas ADD CONSTRAINT probe_identity CHECK ((probe_tenant_id IS NULL) = (probe_reservation_id IS NULL));

-- Reservation identity is the globally correlated model attempt identity within
-- a tenant. Retrying an HTTP request requires a new attempt, never a new ID for
-- an already dispatched reservation.
COMMENT ON COLUMN quota_reservations.id IS 'Model attempt ID; exactly one quota reservation per tenant and model attempt';
ALTER TABLE quota_reservations ADD COLUMN request_hash text NOT NULL DEFAULT '';
ALTER TABLE quota_reservations ADD COLUMN window_generation bigint NOT NULL DEFAULT 1 CHECK (window_generation > 0);
ALTER TABLE quota_reservations ADD COLUMN dispatched_at timestamptz;
ALTER TABLE quota_reservations ADD COLUMN actual_tokens bigint CHECK (actual_tokens >= 0);
ALTER TABLE quota_reservations ADD CONSTRAINT actual_cost_nonnegative CHECK (actual_microusd >= 0);
ALTER TABLE quota_reservations ADD COLUMN settlement_kind text CHECK (settlement_kind IN ('actual_usage','not_dispatched'));
ALTER TABLE quota_reservations ADD COLUMN outcome text CHECK (outcome IN ('success','failure','neutral'));
ALTER TABLE quota_reservations ADD COLUMN settled_at timestamptz;
CREATE INDEX reservations_expiring ON quota_reservations(credential_group,request_deadline) WHERE NOT request_slot_released;

-- +goose Down
DROP INDEX reservations_expiring;
ALTER TABLE quota_reservations DROP COLUMN settled_at, DROP COLUMN outcome,
    DROP COLUMN settlement_kind, DROP CONSTRAINT actual_cost_nonnegative,
    DROP COLUMN actual_tokens, DROP COLUMN dispatched_at,
    DROP COLUMN window_generation, DROP COLUMN request_hash;
ALTER TABLE provider_quotas DROP CONSTRAINT probe_identity,
    DROP COLUMN probe_reservation_id, DROP COLUMN probe_tenant_id,
    DROP COLUMN breaker_cooldown_ms, DROP COLUMN failure_threshold,
    DROP COLUMN consecutive_failures, DROP COLUMN window_generation,
    DROP COLUMN window_duration_ms, DROP COLUMN committed_tokens;
