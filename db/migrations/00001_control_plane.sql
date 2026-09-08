-- +goose Up
CREATE TABLE tenants (
    id text PRIMARY KEY, name text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE memberships (
    tenant_id text NOT NULL REFERENCES tenants(id), principal_id text NOT NULL,
    role text NOT NULL CHECK(role IN ('viewer','developer','admin')),
    PRIMARY KEY(tenant_id, principal_id)
);
CREATE TABLE api_tokens (
    token_hash text PRIMARY KEY, principal_id text NOT NULL,
    expires_at timestamptz NOT NULL, revoked_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE projects (
    tenant_id text NOT NULL REFERENCES tenants(id), id text NOT NULL,
    name text NOT NULL, repo_source text NOT NULL, repo_profile jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(tenant_id,id)
);
CREATE TABLE tenant_runtime (
    tenant_id text PRIMARY KEY REFERENCES tenants(id),
    max_active integer NOT NULL DEFAULT 2 CHECK(max_active > 0),
    active_count integer NOT NULL DEFAULT 0 CHECK(active_count >= 0),
    last_dispatched_at timestamptz NOT NULL DEFAULT '-infinity'
);
CREATE TABLE runners (
    id text PRIMARY KEY, endpoint text NOT NULL,
    max_slots integer NOT NULL CHECK(max_slots > 0),
    reserved_slots integer NOT NULL DEFAULT 0 CHECK(reserved_slots >= 0),
    enabled boolean NOT NULL DEFAULT true
);
CREATE TABLE runs (
    tenant_id text NOT NULL, id text NOT NULL, project_id text NOT NULL,
    principal_id text NOT NULL, parent_run_id text, task text NOT NULL,
    base_commit text NOT NULL, input_snapshot text NOT NULL DEFAULT '',
    state text NOT NULL CHECK(state IN ('queued','running','waiting_approval','cancel_requested','cancelled','completed','failed','budget_exhausted','needs_reconciliation')),
    version bigint NOT NULL CHECK(version > 0),
    lease_epoch bigint NOT NULL DEFAULT 0 CHECK(lease_epoch >= 0),
    lease_owner text NOT NULL DEFAULT '', lease_until timestamptz,
    runner_id text REFERENCES runners(id), workspace_id text NOT NULL DEFAULT '',
    snapshot jsonb NOT NULL, config_snapshot jsonb NOT NULL,
    next_event_seq bigint NOT NULL DEFAULT 1,
    not_before timestamptz NOT NULL DEFAULT '-infinity',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(tenant_id,id),
    FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id),
    FOREIGN KEY(tenant_id,parent_run_id) REFERENCES runs(tenant_id,id)
);
CREATE INDEX runs_dispatch ON runs(tenant_id,created_at,id) WHERE state='queued';
CREATE INDEX runs_expired ON runs(lease_until) WHERE state IN ('running','cancel_requested');
CREATE TABLE runner_allocations (
    tenant_id text NOT NULL, run_id text NOT NULL, runner_id text NOT NULL REFERENCES runners(id),
    lease_epoch bigint NOT NULL, state text NOT NULL CHECK(state IN ('reserved','active','released','uncertain')),
    slots integer NOT NULL CHECK(slots > 0), memory_bytes bigint NOT NULL DEFAULT 0,
    PRIMARY KEY(tenant_id,run_id), FOREIGN KEY(tenant_id,run_id) REFERENCES runs(tenant_id,id)
);
CREATE TABLE run_snapshots (
    tenant_id text NOT NULL, run_id text NOT NULL, version bigint NOT NULL,
    step_seq bigint NOT NULL, schema_version integer NOT NULL, body jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(tenant_id,run_id,version), FOREIGN KEY(tenant_id,run_id) REFERENCES runs(tenant_id,id)
);
CREATE TABLE run_messages (
    tenant_id text NOT NULL, run_id text NOT NULL, seq bigint NOT NULL,
    principal_id text NOT NULL, body text NOT NULL, applied_step bigint,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(tenant_id,run_id,seq), FOREIGN KEY(tenant_id,run_id) REFERENCES runs(tenant_id,id)
);
CREATE TABLE steps (
    tenant_id text NOT NULL, run_id text NOT NULL, seq bigint NOT NULL,
    kind text NOT NULL, status text NOT NULL, input_hash text NOT NULL,
    output_ref text, PRIMARY KEY(tenant_id,run_id,seq),
    FOREIGN KEY(tenant_id,run_id) REFERENCES runs(tenant_id,id)
);
CREATE TABLE model_attempts (
    tenant_id text NOT NULL, run_id text NOT NULL, step_seq bigint NOT NULL,
    attempt integer NOT NULL, provider text NOT NULL, model_id text NOT NULL,
    status text NOT NULL, raw_ref text, usage jsonb, request_id text,
    price_version text NOT NULL,
    PRIMARY KEY(tenant_id,run_id,step_seq,attempt),
    FOREIGN KEY(tenant_id,run_id,step_seq) REFERENCES steps(tenant_id,run_id,seq)
);
CREATE TABLE effects (
    tenant_id text NOT NULL, run_id text NOT NULL, step_seq bigint NOT NULL,
    ordinal integer NOT NULL, operation_id text NOT NULL,
    kind text NOT NULL, args jsonb NOT NULL, args_hash text NOT NULL,
    expected_revision bigint NOT NULL CHECK(expected_revision >= 0),
    epoch bigint NOT NULL CHECK(epoch > 0), policy_version text NOT NULL,
    status text NOT NULL, receipt_ref text,
    PRIMARY KEY(tenant_id,operation_id), UNIQUE(tenant_id,run_id,step_seq,ordinal),
    UNIQUE(tenant_id,run_id,operation_id),
    FOREIGN KEY(tenant_id,run_id,step_seq) REFERENCES steps(tenant_id,run_id,seq)
);
CREATE TABLE approvals (
    tenant_id text NOT NULL, id text NOT NULL, run_id text NOT NULL,
    effect_id text NOT NULL, args_hash text NOT NULL,
    workspace_revision bigint NOT NULL, policy_version text NOT NULL,
    decision text CHECK(decision IN ('allow','deny')), version bigint NOT NULL DEFAULT 1,
    principal_id text, decided_at timestamptz,
    PRIMARY KEY(tenant_id,id),
    FOREIGN KEY(tenant_id,run_id,effect_id) REFERENCES effects(tenant_id,run_id,operation_id)
);
CREATE TABLE run_events (
    tenant_id text NOT NULL, run_id text NOT NULL, seq bigint NOT NULL,
    type text NOT NULL, schema_version integer NOT NULL DEFAULT 1,
    step_seq bigint, attempt_id text, payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(tenant_id,run_id,seq), FOREIGN KEY(tenant_id,run_id) REFERENCES runs(tenant_id,id)
);
CREATE TABLE idempotency_keys (
    tenant_id text NOT NULL REFERENCES tenants(id), principal_id text NOT NULL,
    route text NOT NULL, key text NOT NULL, request_hash text NOT NULL,
    resource_id text NOT NULL, response jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz NOT NULL,
    PRIMARY KEY(tenant_id,principal_id,route,key)
);
CREATE TABLE provider_quotas (
    credential_group text PRIMARY KEY, max_concurrent integer NOT NULL CHECK(max_concurrent > 0),
    max_tokens bigint NOT NULL CHECK(max_tokens >= 0),
    max_microusd bigint NOT NULL CHECK(max_microusd >= 0),
    active_requests integer NOT NULL DEFAULT 0 CHECK(active_requests >= 0),
    reserved_tokens bigint NOT NULL DEFAULT 0 CHECK(reserved_tokens >= 0),
    committed_microusd bigint NOT NULL DEFAULT 0 CHECK(committed_microusd >= 0),
    reserved_microusd bigint NOT NULL DEFAULT 0 CHECK(reserved_microusd >= 0),
    window_ends_at timestamptz NOT NULL,
    breaker_until timestamptz
);
CREATE TABLE quota_reservations (
    tenant_id text NOT NULL, id text NOT NULL, run_id text NOT NULL,
    credential_group text NOT NULL REFERENCES provider_quotas(credential_group),
    tokens bigint NOT NULL CHECK(tokens >= 0), microusd bigint NOT NULL CHECK(microusd >= 0),
    actual_microusd bigint, status text NOT NULL CHECK(status IN ('reserved','settled','unknown')),
    request_slot_released boolean NOT NULL DEFAULT false,
    request_deadline timestamptz NOT NULL,
    PRIMARY KEY(tenant_id,id), FOREIGN KEY(tenant_id,run_id) REFERENCES runs(tenant_id,id)
);
CREATE TABLE artifacts (
    tenant_id text NOT NULL, id text NOT NULL, run_id text NOT NULL,
    kind text NOT NULL, object_key text NOT NULL, sha256 text NOT NULL,
    byte_size bigint NOT NULL CHECK(byte_size >= 0),
    state text NOT NULL CHECK(state IN ('staging','ready','deleted')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(tenant_id,id), FOREIGN KEY(tenant_id,run_id) REFERENCES runs(tenant_id,id)
);

-- API connections MUST use a non-owner, non-BYPASSRLS role. Service workers
-- have an explicit separate role. Missing transaction context denies access.
-- +goose StatementBegin
DO $$
DECLARE tab text;
BEGIN
  FOREACH tab IN ARRAY ARRAY['projects','runs','run_snapshots','run_messages','steps',
    'model_attempts','effects','approvals','run_events','idempotency_keys',
    'quota_reservations','artifacts','runner_allocations'] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', tab);
    EXECUTE format('CREATE POLICY tenant_boundary ON %I USING (tenant_id = current_setting(''forge.tenant_id'', true)) WITH CHECK (tenant_id = current_setting(''forge.tenant_id'', true))', tab);
  END LOOP;
END $$;
-- +goose StatementEnd

-- +goose Down
DROP TABLE artifacts, quota_reservations, provider_quotas, idempotency_keys,
    run_events, approvals, effects, model_attempts, steps, run_messages,
    run_snapshots, runner_allocations, runs, runners, tenant_runtime,
    projects, api_tokens, memberships, tenants;
