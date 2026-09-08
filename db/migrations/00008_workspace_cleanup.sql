-- +goose Up
CREATE TABLE workspace_cleanup (
    tenant_id text NOT NULL, run_id text NOT NULL, id text NOT NULL UNIQUE,
    runner_id text NOT NULL REFERENCES runners(id), terminal_version bigint NOT NULL,
    workspace_revision bigint NOT NULL, runner_epoch bigint NOT NULL CHECK(runner_epoch>0),
    phase text NOT NULL CHECK(phase IN ('requested','sealed','released')),
    snapshot_ref text, lease_owner text NOT NULL, lease_until timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(), released_at timestamptz,
    PRIMARY KEY(tenant_id,run_id), FOREIGN KEY(tenant_id,run_id) REFERENCES runs(tenant_id,id),
    FOREIGN KEY(tenant_id,snapshot_ref) REFERENCES artifacts(tenant_id,id),
    CHECK(phase='requested' OR snapshot_ref IS NOT NULL)
);
ALTER TABLE workspace_cleanup ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON workspace_cleanup USING (tenant_id=current_setting('forge.tenant_id',true)) WITH CHECK (tenant_id=current_setting('forge.tenant_id',true));
-- +goose Down
DROP TABLE workspace_cleanup;
