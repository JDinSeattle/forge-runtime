package persistence

import (
	"encoding/json"
	"testing"
)

func TestSubmitWithoutParentPreservesPreRetryIdempotencyHashInput(t *testing.T) {
	req := SubmitRequest{TenantID: "tenant", PrincipalID: "principal", ProjectID: "project", Task: "task", BaseCommit: "base", Config: Config{Provider: "fake", Model: "fixture", MaxModelRounds: 2, MaxToolCalls: 3, MaxCost: 1000, MaxRuntimeSeconds: 60}}
	// This byte order/omission contract predates parent_run_id. An additive
	// field must not invalidate already persisted idempotency request hashes.
	const legacy = `{"tenant_id":"tenant","principal_id":"principal","project_id":"project","task":"task","base_commit":"base","config":{"provider":"fake","model":"fixture","max_model_rounds":2,"max_tool_calls":3,"max_cost_microusd":1000,"max_runtime_seconds":60}}`
	raw, err := json.Marshal(req)
	if err != nil || string(raw) != legacy {
		t.Fatalf("legacy request changed: %s %v", raw, err)
	}
}
