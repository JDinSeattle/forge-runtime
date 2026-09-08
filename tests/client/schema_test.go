package client_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/httpapi"
	api "github.com/JDinSeattle/forge-runtime/internal/httpcontract"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	flow "github.com/JDinSeattle/forge-runtime/internal/runtime"
	"github.com/go-chi/chi/v5"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"go.yaml.in/yaml/v3"
)

func document(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile("../../api/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err = yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if doc["openapi"] != "3.1.0" {
		t.Fatal("contract is not OpenAPI 3.1")
	}
	return doc
}
func validate(t *testing.T, doc map[string]any, name string, value any) {
	t.Helper()
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource("https://forge.invalid/openapi.json", doc); err != nil {
		t.Fatal(err)
	}
	schema, err := compiler.Compile("https://forge.invalid/openapi.json#/components/schemas/" + name)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var decoded any
	if err = json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if err = schema.Validate(decoded); err != nil {
		t.Fatalf("%s does not match live Go wire model: %v", name, err)
	}
}

func TestSchemaMatchesActualGoWireModelsAndGeneratedTypes(t *testing.T) {
	doc := document(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	state := flow.NewState("tenant", "run", flow.Limits{MaxModelRounds: 4, MaxToolCalls: 20, MaxCost: 10000, Deadline: now.Add(time.Minute)})
	state.Lease = domain.Lease{Owner: "worker", Epoch: 1, Until: now.Add(time.Second)}
	state.PendingEffect = &flow.Effect{ID: "effect", Kind: "read_file", Args: json.RawMessage(`{"path":"x.go"}`), ArgsHash: strings.Repeat("a", 64), Status: flow.EffectPlanned}
	state.Approval = &flow.ApprovalBinding{EffectID: "effect", ArgsHash: strings.Repeat("a", 64), Version: 1}
	run := persistence.Run{TenantID: "tenant", ID: "run", ProjectID: "project", PrincipalID: "person", Task: "fix it", BaseCommit: "commit", State: state, Config: persistence.Config{Provider: "fake", Model: "fake", MaxModelRounds: 4, MaxToolCalls: 20, MaxCost: 10000, MaxRuntimeSeconds: 60}, CreatedAt: now, CoveredSeq: 1}
	for name, value := range map[string]any{
		"Run": run, "State": state, "Budget": httpapi.Budget{},
		"Submit":   httpapi.SubmitBody{Task: "fix it", BaseCommit: "commit", ConfigID: "demo"},
		"Project":  persistence.Project{TenantID: "tenant", ID: "project", Name: "name", SourceID: "source", ProfileID: "python", CreatedAt: now},
		"Approval": persistence.Approval{ID: "approval", RunID: "run", Binding: *state.Approval},
		"Artifact": persistence.Artifact{TenantID: "tenant", ID: "artifact", RunID: "run", Kind: "patch", ObjectKey: "tenant/run/patch", SHA256: strings.Repeat("a", 64), State: "ready", CreatedAt: now},
		"Event":    persistence.Event{RunID: "run", Seq: 1, Type: "run.created", SchemaVersion: 1, Payload: json.RawMessage(`{"status":"queued"}`), CreatedAt: now},
		"APIError": httpapi.APIError{Code: "forbidden", Message: "denied", RequestID: "request"},
	} {
		t.Run(name, func(t *testing.T) { validate(t, doc, name, value) })
	}
	// All runtime fields used by the HTTP projection survive generated-client
	// decoding. This catches spelling/type drift, not just valid generic JSON.
	raw, _ := json.Marshal(run)
	var generated api.Run
	if err := json.Unmarshal(raw, &generated); err != nil {
		t.Fatal(err)
	}
	roundTrip, _ := json.Marshal(generated)
	var actual, decoded any
	_ = json.Unmarshal(raw, &actual)
	_ = json.Unmarshal(roundTrip, &decoded)
	if !reflect.DeepEqual(actual, decoded) {
		t.Fatalf("generated Run loses or changes wire fields\nactual=%s\ngenerated=%s", raw, roundTrip)
	}
}

func TestSchemaRejectsMalformedBudgetsAndUnknownRequestProperties(t *testing.T) {
	doc := document(t)
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource("https://forge.invalid/openapi.json", doc); err != nil {
		t.Fatal(err)
	}
	schema, err := compiler.Compile("https://forge.invalid/openapi.json#/components/schemas/Submit")
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{
		`{"task":"x","base_commit":"x","config_id":"demo","budget":{"max_tool_calls":0}}`,
		`{"task":"x","base_commit":"x","config_id":"demo","budget":{"max_cost_microusd":-1}}`,
		`{"task":"x","base_commit":"x","config_id":"demo","budget":{},"provider":"untrusted"}`,
	} {
		var value any
		_ = json.Unmarshal([]byte(raw), &value)
		if err = schema.Validate(value); err == nil {
			t.Fatal("invalid request accepted", raw)
		}
	}
}

func TestEveryHTTPRouteHasMatchingContractOperation(t *testing.T) {
	doc := document(t)
	paths := doc["paths"].(map[string]any)
	declared := map[string]bool{}
	for path, raw := range paths {
		for method := range raw.(map[string]any) {
			if method == "get" || method == "post" {
				declared[strings.ToUpper(method)+" "+path] = true
			}
		}
	}
	server := (&httpapi.Server{}).Handler()
	seen := map[string]bool{}
	err := chi.Walk(server.(chi.Routes), func(method, path string, handler http.Handler, middleware ...func(http.Handler) http.Handler) error {
		// Prometheus is mounted with Handle; its documented GET representation is
		// the client contract rather than every method accepted by the middleware.
		if path == "/metrics" && method != "GET" {
			return nil
		}
		key := method + " " + path
		if !declared[key] {
			return fmt.Errorf("handler missing from OpenAPI: %s", key)
		}
		seen[key] = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for key := range declared {
		if !seen[key] {
			t.Error("OpenAPI operation has no handler:", key)
		}
	}
}

func TestActualUnauthenticatedFailureHasTypedErrorContract(t *testing.T) {
	server := (&httpapi.Server{}).Handler()
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/runs/run", nil).WithContext(context.Background())
	server.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatal(response.Code)
	}
	var body any
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	validate(t, document(t), "APIError", body)
	var generated api.APIError
	if err := json.Unmarshal(response.Body.Bytes(), &generated); err != nil {
		t.Fatal(err)
	}
	if generated.RequestId == "" || generated.Code != "forbidden" {
		t.Fatalf("error correlation=%+v", generated)
	}
}
