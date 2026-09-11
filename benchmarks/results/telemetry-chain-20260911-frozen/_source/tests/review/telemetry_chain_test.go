package review_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/application"
	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/httpapi"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/provider"
	"github.com/JDinSeattle/forge-runtime/internal/quota"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	"github.com/JDinSeattle/forge-runtime/internal/runnerclient"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
	"github.com/JDinSeattle/forge-runtime/internal/telemetry"
)

type chainModel struct {
	mu    sync.Mutex
	calls int
	patch string
}

func (p *chainModel) Capabilities(ctx context.Context, m string) (provider.Capabilities, error) {
	return provider.NewFake().Capabilities(ctx, m)
}
func (p *chainModel) Stream(ctx context.Context, r provider.ModelRequest, emit func(provider.ModelEvent) error) (provider.ModelTurn, error) {
	p.mu.Lock()
	p.calls++
	n := p.calls
	p.mu.Unlock()
	if n == 1 {
		return provider.ModelTurn{}, &provider.Error{Kind: provider.ErrRateLimited, RetryAfter: 10 * time.Millisecond, Detail: "deterministic local limit"}
	}
	name, args := "finish", "{}"
	switch r.StepID {
	case "1":
		name, args = "run_command", `{"command":["fixture"]}`
	case "2":
		name, args = "apply_patch", p.patch
	case "3":
	default:
		return provider.ModelTurn{}, fmt.Errorf("unexpected fixture step %s", r.StepID)
	}
	return provider.NewFake(provider.Script{Chunks: []provider.Chunk{{Kind: "tool_start", CallID: "call_" + r.StepID, Name: name}, {Kind: "tool_delta", CallID: "call_" + r.StepID, Delta: args}, {Kind: "tool_end", CallID: "call_" + r.StepID}}, FinishReason: "tool_calls", Usage: provider.Usage{Input: provider.TokenCount{Known: true, Value: 50}, Output: provider.TokenCount{Known: true, Value: 20}}}).Stream(ctx, r, emit)
}
func chainMetric(t *testing.T, m *telemetry.Telemetry, name string) float64 {
	t.Helper()
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var result float64
	for _, f := range families {
		if f.GetName() == name {
			for _, v := range f.Metric {
				switch {
				case v.Counter != nil:
					result += v.Counter.GetValue()
				case v.Histogram != nil:
					result += float64(v.Histogram.GetSampleCount())
				case v.Gauge != nil:
					result += v.Gauge.GetValue()
				}
			}
		}
	}
	return result
}
func chainSave(t *testing.T, path string, value any) {
	t.Helper()
	b, e := json.MarshalIndent(value, "", "  ")
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(path, append(b, '\n'), 0600); e != nil {
		t.Fatal(e)
	}
}

// This test invokes production API, Store, Driver, gRPC and runner/SQLite. Only
// the provider and sandbox execution are explicit local protocol fixtures.
// The independently launched Collector is the sole writer of its trace file.
func TestReviewTelemetryProductionChain(t *testing.T) {
	endpoint, traceFile, output := os.Getenv("FORGE_RECOVERY_OTLP_ENDPOINT"), os.Getenv("FORGE_RECOVERY_OTLP_OUTPUT"), os.Getenv("FORGE_TELEMETRY_OUTPUT")
	if endpoint == "" || traceFile == "" || output == "" {
		t.Skip("requires separate loopback Collector and private output directory")
	}
	if !strings.HasPrefix(endpoint, "http://127.0.0.1:") || !filepath.IsAbs(traceFile) || !filepath.IsAbs(output) {
		t.Fatal("explicit local evidence only")
	}
	if err := os.MkdirAll(output, 0700); err != nil {
		t.Fatal(err)
	}
	base, store, apiStore := reviewAPIStore(t)
	restrictApprovalAuthTables(t, base, store, apiStore)
	ctx, cancel := context.WithTimeout(base, 45*time.Second)
	defer cancel()
	observations := map[string]*telemetry.Telemetry{}
	for _, name := range []string{"forge-api", "forge-worker", "forge-runner"} {
		m, e := telemetry.Setup(ctx, name, endpoint)
		if e != nil {
			t.Fatal(e)
		}
		observations[name] = m
		t.Cleanup(func() {
			c, end := context.WithTimeout(context.Background(), 5*time.Second)
			defer end()
			_ = m.Shutdown(c)
		})
	}
	store.Telemetry = observations["forge-worker"]
	apiStore.Telemetry = observations["forge-api"]
	dir := t.TempDir()
	source := filepath.Join(dir, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	before, after := "def clamp(v, lo, hi):\n    return min(lo, max(v, hi))\n", "def clamp(v, lo, hi):\n    return max(lo, min(v, hi))\n"
	if err := os.WriteFile(filepath.Join(source, "clamp.py"), []byte(before), 0600); err != nil {
		t.Fatal(err)
	}
	sourceHash, err := runner.ComputeSourceHash(ctx, source, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	objects, err := artifact.NewLocalStore(filepath.Join(dir, "objects"), 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer objects.Close()
	signer, err := runner.NewSigner([]byte(strings.Repeat("e", 32)))
	if err != nil {
		t.Fatal(err)
	}
	backend := &sandbox.TestBackend{StartFunc: func(ctx context.Context, s sandbox.JobSpec) (sandbox.Job, error) {
		body, e := os.ReadFile(filepath.Join(s.Workspace, "clamp.py"))
		if e != nil {
			return sandbox.Job{}, e
		}
		exit := 0
		if s.Command[0] == "target" && string(body) != after {
			exit = 1
		}
		return sandbox.Job{ID: s.ID, Started: true, ExitCode: exit, Output: []byte("protocol-only fixture; no repository process executed")}, nil
	}}
	journal := filepath.Join(dir, "journal.sqlite")
	engine, err := runner.Open(runner.Config{Telemetry: observations["forge-runner"], RootDir: filepath.Join(dir, "runner"), JournalPath: journal, Artifacts: objects, Backend: backend, Signer: signer, Sources: map[string]string{"fixture": source}, Profiles: map[string]sandbox.Profile{"python": {ID: "python", TargetCommand: []string{"target"}, VerifyCommand: []string{"regression"}}}})
	if err != nil {
		t.Fatal(err)
	}
	var closeOnce sync.Once
	closeEngine := func() { closeOnce.Do(func() { _ = engine.Close() }) }
	defer closeEngine()
	socketDir, err := os.MkdirTemp("/tmp", "e43-rpc-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(socketDir)
	socket := filepath.Join(socketDir, "runner.sock")
	rpc, err := runnerclient.Serve(runnerclient.ServerConfig{UnixSocket: socket, Telemetry: observations["forge-runner"]}, engine)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		c, end := context.WithTimeout(context.Background(), 3*time.Second)
		defer end()
		_ = rpc.Close(c)
	}()
	client, err := runnerclient.Dial(ctx, runnerclient.ClientConfig{UnixSocket: socket, Telemetry: observations["forge-worker"]})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err = store.BootstrapTenant(ctx, "chain_tenant", "chain_principal", "developer"); err != nil {
		t.Fatal(err)
	}
	project, err := store.CreateProject(ctx, "chain_tenant", "chain fixture", "fixture", "python")
	if err != nil {
		t.Fatal(err)
	}
	if err = store.RegisterRunner(ctx, "chain_runner", "private-unix", 2); err != nil {
		t.Fatal(err)
	}
	q := quota.New(store.Pool)
	if err = q.Configure(ctx, quota.Config{CredentialGroup: "chain", MaxConcurrent: 2, MaxTokens: 1000000, MaxCost: 1000000, WindowDuration: time.Hour, FailureThreshold: 3, BreakerCooldown: time.Second}); err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256([]byte(before))
	patch, _ := json.Marshal(runner.PatchArgs{Files: []runner.FileEdit{{Path: "clamp.py", ExpectedSHA256: hex.EncodeToString(h[:]), Content: &after}}})
	model := &chainModel{patch: string(patch)}
	config := persistence.Config{Provider: "fake", Model: "fake", MaxModelRounds: 6, MaxToolCalls: 6, MaxCost: 1000000, MaxRuntimeSeconds: 60}
	api := httptest.NewServer((&httpapi.Server{Store: apiStore, Artifacts: objects, Telemetry: observations["forge-api"], Configs: map[string]persistence.Config{"fixture": config}, Sources: map[string]httpapi.Source{"fixture": {BaseCommit: sourceHash, ProfileID: "python"}}}).Handler())
	defer api.Close()
	token, err := store.IssueToken(ctx, "chain_principal", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	const canary = "E43_PRIVATE_PROMPT_CANARY_719"
	submit := func(key string, parent domain.ID) (domain.ID, bool) {
		body, _ := json.Marshal(map[string]any{"task": canary, "base_commit": sourceHash, "config_id": "fixture", "parent_run_id": parent})
		if parent == "" {
			body, _ = json.Marshal(map[string]any{"task": canary, "base_commit": sourceHash, "config_id": "fixture"})
		}
		request, e := http.NewRequestWithContext(ctx, "POST", api.URL+"/v1/projects/"+string(project.ID)+"/runs", bytes.NewReader(body))
		if e != nil {
			t.Fatal(e)
		}
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("X-Forge-Tenant", "chain_tenant")
		request.Header.Set("Idempotency-Key", key)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Baggage", "secret="+canary)
		res, e := api.Client().Do(request)
		if e != nil {
			t.Fatal(e)
		}
		defer res.Body.Close()
		raw, e := io.ReadAll(res.Body)
		if e != nil || res.StatusCode != 202 {
			t.Fatalf("submit=%d %s %v", res.StatusCode, raw, e)
		}
		var v struct {
			Run    domain.ID `json:"run_id"`
			Reused bool      `json:"reused"`
		}
		if e = json.Unmarshal(raw, &v); e != nil {
			t.Fatal(e)
		}
		return v.Run, v.Reused
	}
	runID, reused := submit("first", "")
	if reused {
		t.Fatal("new submission reused")
	}
	same, reused := submit("first", "")
	if same != runID || !reused {
		t.Fatal("idempotency drift")
	}
	d := application.Driver{Telemetry: observations["forge-worker"], Store: store, Quota: q, Runner: client, RunnerID: "chain_runner", Signer: signer, Artifacts: objects, Providers: map[string]provider.Provider{"fake": model}, Models: map[string]application.ModelSpec{"fake/fake": {CredentialGroup: "chain", PriceVersion: "fixture-zero", ContextTokens: 65536, MaxOutputTokens: 1024, RequestTimeout: 5 * time.Second}}, Sources: map[string]application.SourceSpec{"fixture": {Hash: sourceHash, HasTarget: true}}, TrustedVerification: true, LeaseDuration: 2 * time.Second}
	interrupted, approved := false, false
	claims := 0
	var final persistence.Run
	for ctx.Err() == nil {
		final, err = store.GetRun(ctx, "chain_tenant", runID)
		if err != nil {
			t.Fatal(err)
		}
		if final.State.Status.Terminal() {
			break
		}
		if final.State.Status == domain.StatusWaitingApproval {
			reply := approvalRequest(ctx, api, final, token, true)
			if reply.err != nil || reply.status != 200 {
				t.Fatalf("approval: %d %s %v", reply.status, reply.body, reply.err)
			}
			again := approvalRequest(ctx, api, final, token, true)
			if again.err != nil || again.status != 200 {
				t.Fatalf("duplicate approval %d", again.status)
			}
			approved = true
		}
		claimed, e := store.ClaimOnRunner(ctx, fmt.Sprintf("worker-%d", claims), 2*time.Second, "chain_runner")
		if errors.Is(e, domain.ErrNotFound) {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if e != nil {
			t.Fatal(e)
		}
		claims++
		driveCtx, stopDrive := context.WithCancel(ctx)
		d.Fault = func(point string) error {
			if point == "after_model_result_before_transition" && !interrupted {
				interrupted = true
				stopDrive()
				return context.Canceled
			}
			return nil
		}
		driveErr := d.Drive(driveCtx, claimed)
		stopDrive()
		if driveErr != nil {
			t.Logf("drive yielded at durable boundary: %v", driveErr)
		}
	}
	if ctx.Err() != nil || final.State.Status != domain.StatusCompleted || final.State.Verification != domain.VerificationVerified || !interrupted || !approved || model.calls != 4 {
		t.Fatalf("chain incomplete state=%s verification=%s interrupted=%t approved=%t calls=%d err=%v", final.State.Status, final.State.Verification, interrupted, approved, model.calls, ctx.Err())
	}
	// Re-observe completed run/READY metadata through production APIs: no request
	// or artifact-publication count may be charged for these replays.
	if err = d.Drive(ctx, final); err != nil {
		t.Fatal(err)
	}
	artifacts, err := store.ListArtifacts(ctx, final.TenantID, final.ID, "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range artifacts {
		if err = store.PublishArtifact(ctx, a); err != nil {
			t.Fatal(err)
		}
	}
	child, reused := submit("retry-child", runID)
	if reused || child == runID {
		t.Fatal("retry child identity")
	}
	closeEngine()
	if err = store.ObserveScheduler(ctx); err != nil {
		t.Fatal(err)
	}
	metrics := map[string]map[string]float64{}
	for name, m := range observations {
		values := map[string]float64{}
		for _, metric := range []string{"model_attempts_total", "provider_rate_limits_total", "lease_expirations_total", "reconciliation_total", "dispatch_latency_seconds", "artifact_bytes_total", "run_transitions_total", "runner_operation_duration_seconds", "run_drives_inflight", "effect_operations_inflight", "model_requests_inflight", "queue_depth", "active_runs", "reserved_budget_usd"} {
			values[metric] = chainMetric(t, m, "forge_runtime_"+metric)
		}
		metrics[name] = values
		// Scrape the actual HTTP handler, never synthesize Prometheus exposition.
		scrape := httptest.NewServer(m.Handler())
		res, e := scrape.Client().Get(scrape.URL)
		if e != nil {
			t.Fatal(e)
		}
		raw, e := io.ReadAll(res.Body)
		res.Body.Close()
		scrape.Close()
		if e != nil {
			t.Fatal(e)
		}
		if strings.Contains(string(raw), canary) || strings.Contains(string(raw), string(runID)) {
			t.Fatal("high-cardinality metric label")
		}
		if e = os.WriteFile(filepath.Join(output, name+".prom"), raw, 0600); e != nil {
			t.Fatal(e)
		}

	}
	worker := metrics["forge-worker"]
	if worker["model_attempts_total"] != 4 || worker["provider_rate_limits_total"] != 1 || worker["lease_expirations_total"] != 1 || worker["dispatch_latency_seconds"] != float64(claims) {
		t.Fatalf("wrong dispatch/replay metrics %+v claims=%d", worker, claims)
	}
	if worker["queue_depth"] != 1 || worker["active_runs"] != 0 {
		t.Fatalf("post-terminal scheduler gauges: %v", worker)
	}
	var reserved int64
	if err = store.Pool.QueryRow(ctx, `SELECT coalesce(sum(reserved_microusd),0)::bigint FROM provider_quotas`).Scan(&reserved); err != nil {
		t.Fatal(err)
	}
	if worker["reserved_budget_usd"] != float64(reserved)/1e6 {
		t.Fatalf("budget gauge disagrees with ledger: %v vs %d", worker, reserved)
	}
	for service, values := range metrics {
		for _, name := range []string{"run_drives_inflight", "effect_operations_inflight", "model_requests_inflight"} {
			if values[name] != 0 {
				t.Fatalf("%s leaked %s: %v", service, name, values[name])
			}
		}
	}
	var committed, bytesReady int64
	if err = store.Pool.QueryRow(ctx, `SELECT count(*)-1 FROM run_snapshots WHERE tenant_id=$1 AND run_id=$2`, final.TenantID, runID).Scan(&committed); err != nil {
		t.Fatal(err)
	}
	if err = store.Pool.QueryRow(ctx, `SELECT sum(byte_size) FROM artifacts WHERE tenant_id=$1 AND run_id=$2`, final.TenantID, runID).Scan(&bytesReady); err != nil {
		t.Fatal(err)
	}
	if worker["run_transitions_total"]+metrics["forge-api"]["run_transitions_total"] != float64(committed) || worker["artifact_bytes_total"] != float64(bytesReady) {
		t.Fatalf("committed counter mismatch snapshots=%d bytes=%d metrics=%v", committed, bytesReady, metrics)
	}
	journalDB, err := sql.Open("sqlite", journal)
	if err != nil {
		t.Fatal(err)
	}
	defer journalDB.Close()
	var operations int
	if err = journalDB.QueryRowContext(ctx, `SELECT count(*) FROM operations`).Scan(&operations); err != nil {
		t.Fatal(err)
	}
	if metrics["forge-runner"]["runner_operation_duration_seconds"] != float64(operations) {
		t.Fatalf("runner execution/replay mismatch metrics=%v operations=%d", metrics["forge-runner"], operations)
	}
	var journalIDs []string
	jr, err := journalDB.QueryContext(ctx, `SELECT id FROM operations WHERE tenant_id=? AND run_id=? ORDER BY id`, final.TenantID, final.ID)
	if err != nil {
		t.Fatal(err)
	}
	for jr.Next() {
		var id string
		if err = jr.Scan(&id); err != nil {
			t.Fatal(err)
		}
		journalIDs = append(journalIDs, id)
	}
	if err = jr.Err(); err != nil {
		t.Fatal(err)
	}
	jr.Close()
	var attempts []chainAttempt
	rows, err := store.Pool.Query(ctx, `SELECT attempt_id,step_seq,attempt,status,coalesce(traceparent,'') FROM model_attempts WHERE tenant_id=$1 AND run_id=$2 ORDER BY step_seq,attempt`, final.TenantID, final.ID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var a chainAttempt
		if err = rows.Scan(&a.ID, &a.Step, &a.Number, &a.Status, &a.Traceparent); err != nil {
			t.Fatal(err)
		}
		attempts = append(attempts, a)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	for _, m := range observations {
		if err = m.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
	}
	raw, graph := chainCollector(t, ctx, traceFile, canary, runID, final.Traceparent, claims, operations)
	if err = chainDurableGraph(graph, attempts, journalIDs, final.LastClaimTraceparent); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(output, "collector-captured.jsonl"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	var apiRole, sessionRole string
	if err = apiStore.Pool.QueryRow(ctx, `SELECT current_user,session_user`).Scan(&apiRole, &sessionRole); err != nil {
		t.Fatal(err)
	}
	chainSave(t, filepath.Join(output, "report.json"), map[string]any{"passed": true, "run": final, "pg_enqueue_traceparent": final.Traceparent, "pg_last_claim_traceparent": final.LastClaimTraceparent, "pg_attempts": attempts, "journal_operation_ids": journalIDs, "retry_child": child, "metrics": metrics, "claims": claims, "journal_operations": operations, "pg_committed_transitions": committed, "pg_ready_bytes": bytesReady, "collector_graph": graph, "api_current_user": apiRole, "api_session_user": sessionRole, "scope": "production TCP API/PG/Driver/gRPC/SQLite, deterministic local provider, TestBackend protocol verification only; drive context cancellation after persisted result, no process kill/no Docker/no paid model"})
}

type chainAttribute struct {
	Key   string `json:"key"`
	Value struct {
		String string `json:"stringValue"`
		Int    string `json:"intValue"`
	} `json:"value"`
}
type chainSpan struct {
	Trace      string           `json:"traceId"`
	ID         string           `json:"spanId"`
	Parent     string           `json:"parentSpanId"`
	Name       string           `json:"name"`
	Attributes []chainAttribute `json:"attributes"`
	Links      []struct {
		Trace      string           `json:"traceId"`
		ID         string           `json:"spanId"`
		Attributes []chainAttribute `json:"attributes"`
	} `json:"links"`
	Events []struct {
		Name string `json:"name"`
	} `json:"events"`
	Service, Schema string
}

func chainAttributeValue(attrs []chainAttribute, key string) string {
	for _, a := range attrs {
		if a.Key == key {
			return a.Value.String
		}
	}
	return ""
}
func chainGraph(raw []byte, runID domain.ID, parent string, claims, operations int) (map[string]chainSpan, error) {
	graph := map[string]chainSpan{}
	for _, line := range bytes.Split(raw, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var record struct {
			Resources []struct {
				Resource struct {
					Attributes []chainAttribute `json:"attributes"`
				} `json:"resource"`
				Scopes []struct {
					Schema string      `json:"schemaUrl"`
					Spans  []chainSpan `json:"spans"`
				} `json:"scopeSpans"`
			} `json:"resourceSpans"`
		}
		if err := json.Unmarshal(line, &record); err != nil {
			return nil, err
		}
		for _, r := range record.Resources {
			service := chainAttributeValue(r.Resource.Attributes, "service.name")
			for _, scope := range r.Scopes {
				for _, span := range scope.Spans {
					span.Service, span.Schema = service, scope.Schema
					if _, exists := graph[span.ID]; exists {
						return nil, errors.New("duplicate collector span ID")
					}
					graph[span.ID] = span
				}
			}
		}
	}
	parts := strings.Split(parent, "-")
	if len(parts) != 4 {
		return nil, errors.New("missing persisted context")
	}
	anchor, ok := graph[parts[2]]
	if !ok || anchor.Trace != parts[1] || anchor.Name != "forge.run.enqueue" {
		return nil, errors.New("PG enqueue context does not identify collector span")
	}
	counts := map[string]int{}
	links := map[string]int{}
	phases := map[string]bool{}
	events := map[string]bool{}
	for _, s := range graph {
		if s.Schema != "https://opentelemetry.io/schemas/1.38.0" {
			return nil, fmt.Errorf("unpinned schema for %s", s.Name)
		}
		for _, link := range s.Links {
			target, ok := graph[link.ID]
			if !ok || target.Trace != link.Trace {
				return nil, errors.New("dangling trace link")
			}
			links[chainAttributeValue(link.Attributes, "forge.relationship")]++
		}
		for _, e := range s.Events {
			if e.Name == "forge.deduplicated" && s.Name == "forge.run.enqueue" && chainAttributeValue(s.Attributes, "forge.run_id") != string(runID) {
				return nil, errors.New("idempotent enqueue has a fictitious run identity")
			}
			events[e.Name] = true
		}
		if s.Trace != anchor.Trace {
			continue
		}
		counts[s.Name]++
		if strings.HasPrefix(s.Name, "forge.") && s.Parent == "" {
			return nil, fmt.Errorf("missing parent for %s", s.Name)
		}
		if strings.HasPrefix(s.Name, "forge.run.") || s.Name == "forge.model.request" || s.Name == "forge.runner.operation" {
			if chainAttributeValue(s.Attributes, "forge.run_id") != string(runID) {
				return nil, fmt.Errorf("wrong run identity for %s", s.Name)
			}
		}
		if phase := chainAttributeValue(s.Attributes, "forge.verification.phase"); phase != "" {
			phases[phase] = true
		}
		if s.Parent != "" {
			p, ok := graph[s.Parent]
			if !ok || p.Trace != s.Trace {
				return nil, fmt.Errorf("broken parent of %s", s.Name)
			}
			if strings.HasPrefix(s.Name, "forge.runner.server/") && (p.Name != strings.Replace(s.Name, "server/", "client/", 1) || s.Service != "forge-runner" || p.Service != "forge-worker") {
				return nil, errors.New("RPC propagation/service mismatch")
			}
			if s.Name == "forge.runner.operation" && (p.Name != "forge.runner.server/StartOperation" || s.Service != "forge-runner") {
				return nil, errors.New("detached operation lost RPC context")
			}
			if s.Name == "forge.run.drive" && p.Name != "forge.run.claim" {
				return nil, errors.New("drive lost committed claim context")
			}
			if s.Name == "forge.model.request" && p.Name != "forge.run.model_attempt" {
				return nil, errors.New("request lost attempt context")
			}
		}
	}
	if counts["forge.model.request"] != 4 || counts["forge.run.claim"] != claims || counts["forge.runner.operation"] != operations {
		return nil, fmt.Errorf("unexpected real execution span counts: %v", counts)
	}
	for _, name := range []string{"forge.run.queue", "forge.run.step", "forge.run.verification", "forge.runner.server/PrepareWorkspace", "forge.runner.server/AdoptWorkspace"} {
		if counts[name] == 0 {
			return nil, fmt.Errorf("missing %s", name)
		}
	}
	for _, name := range []string{"run_retry", "provider_retry", "lease_takeover"} {
		if links[name] == 0 {
			return nil, fmt.Errorf("missing real %s link", name)
		}
	}
	for _, phase := range []string{"initial_target", "final_target", "regression"} {
		if !phases[phase] {
			return nil, fmt.Errorf("missing phase %s", phase)
		}
	}
	if !events["forge.receipt_replayed"] {
		return nil, errors.New("no durable receipt replay observed")
	}
	if !events["forge.result_replayed"] {
		return nil, errors.New("no saved-model replay observed")
	}
	return graph, nil
}
func chainCollector(t *testing.T, ctx context.Context, path, canary string, runID domain.ID, parent string, claims, operations int) ([]byte, map[string]chainSpan) {
	t.Helper()
	var last error
	for ctx.Err() == nil {
		raw, e := os.ReadFile(path)
		if e == nil {
			if bytes.Contains(raw, []byte(canary)) {
				t.Fatal("collector leaked private content")
			}
			graph, e := chainGraph(raw, runID, parent, claims, operations)
			if e == nil {
				return raw, graph
			}
			last = e
		} else {
			last = e
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("collector production graph incomplete: %v", last)
	return nil, nil
}

type chainAttempt struct {
	ID          string `json:"id"`
	Step        uint64 `json:"step"`
	Number      int    `json:"number"`
	Status      string `json:"status"`
	Traceparent string `json:"traceparent"`
}

func chainDurableGraph(graph map[string]chainSpan, attempts []chainAttempt, operations []string, lastClaim string) error {
	parts := strings.Split(lastClaim, "-")
	if len(parts) != 4 || graph[parts[2]].Trace != parts[1] || graph[parts[2]].Name != "forge.run.claim" {
		return errors.New("durable last claim lacks collector identity")
	}
	for _, a := range attempts {
		p := strings.Split(a.Traceparent, "-")
		if len(p) != 4 {
			return errors.New("durable attempt context absent")
		}
		span := graph[p[2]]
		if span.Trace != p[1] || span.Name != "forge.run.model_attempt" || chainAttributeValue(span.Attributes, "forge.attempt.id") != a.ID {
			return errors.New("attempt identity/context mismatch")
		}
		requests := 0
		for _, s := range graph {
			if s.Parent == span.ID && s.Name == "forge.model.request" && chainAttributeValue(s.Attributes, "forge.attempt.id") == a.ID {
				requests++
			}
		}
		if requests != 1 {
			return errors.New("durable attempt must have exactly one actual provider request in this fixture")
		}
	}
	for _, id := range operations {
		n := 0
		for _, s := range graph {
			if s.Name == "forge.runner.operation" && chainAttributeValue(s.Attributes, "forge.operation.id") == id {
				n++
			}
		}
		if n != 1 {
			return errors.New("SQLite operation must match exactly one new runner execution")
		}
	}
	return nil
}
