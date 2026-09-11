package benchmarks

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/application"
	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/provider"
	"github.com/JDinSeattle/forge-runtime/internal/quota"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	"github.com/JDinSeattle/forge-runtime/internal/testutil"
	anthropicoption "github.com/anthropics/anthropic-sdk-go/option"
	openaioption "github.com/openai/openai-go/v3/option"
)

const interruptedText = "F03 provisional text: these proposed tools must not execute."

// Both streams contain a fully ended tool item and another incomplete JSON
// argument. Neither has a final turn/usage marker. Bytes are synthetic and safe
// to retain; no remote provider or credential is involved.
func interruptedNativeEvents(native string) []map[string]any {
	if native == "anthropic" {
		return []map[string]any{
			{"type": "message_start", "message": map[string]any{"id": "msg_f03", "type": "message", "role": "assistant", "model": "f03-native", "content": []any{}, "usage": map[string]any{"input_tokens": 12, "output_tokens": 1}}},
			{"type": "content_block_start", "index": 0, "content_block": map[string]any{"type": "text", "text": ""}},
			{"type": "content_block_delta", "index": 0, "delta": map[string]any{"type": "text_delta", "text": interruptedText}},
			{"type": "content_block_stop", "index": 0},
			{"type": "content_block_start", "index": 1, "content_block": map[string]any{"type": "tool_use", "id": "closed_tool", "name": "read_file", "input": map[string]any{}}},
			{"type": "content_block_delta", "index": 1, "delta": map[string]any{"type": "input_json_delta", "partial_json": `{"path":"never-read.txt"}`}},
			{"type": "content_block_stop", "index": 1},
			{"type": "content_block_start", "index": 2, "content_block": map[string]any{"type": "tool_use", "id": "partial_tool", "name": "apply_patch", "input": map[string]any{}}},
			{"type": "content_block_delta", "index": 2, "delta": map[string]any{"type": "input_json_delta", "partial_json": `{"files":[{"path":"never-written.txt","content":"`}},
		}
	}
	item := map[string]any{"id": "item_closed", "type": "function_call", "call_id": "closed_tool", "name": "read_file", "arguments": `{"path":"never-read.txt"}`, "status": "completed"}
	return []map[string]any{
		{"type": "response.created", "response": map[string]any{"id": "resp_f03", "status": "in_progress"}},
		{"type": "response.output_text.delta", "delta": interruptedText},
		{"type": "response.output_item.added", "output_index": 0, "item": map[string]any{"id": "item_closed", "type": "function_call", "call_id": "closed_tool", "name": "read_file", "arguments": ""}},
		{"type": "response.function_call_arguments.delta", "item_id": "item_closed", "delta": `{"path":"never-read.txt"}`},
		{"type": "response.function_call_arguments.done", "item_id": "item_closed", "arguments": item["arguments"], "name": "read_file"},
		{"type": "response.output_item.done", "output_index": 0, "item": item},
		{"type": "response.output_item.added", "output_index": 1, "item": map[string]any{"id": "item_partial", "type": "function_call", "call_id": "partial_tool", "name": "apply_patch", "arguments": ""}},
		{"type": "response.function_call_arguments.delta", "item_id": "item_partial", "delta": `{"files":[{"path":"never-written.txt","content":"`},
	}
}

type nativeCutRecord struct {
	RequestAt     time.Time         `json:"request_at"`
	Path          string            `json:"path"`
	Request       json.RawMessage   `json:"request"`
	AttemptHeader string            `json:"attempt_header"`
	Frames        []json.RawMessage `json:"frames"`
	FlushedAt     time.Time         `json:"flushed_at"`
	ClosedAt      time.Time         `json:"closed_at"`
	Closed        bool              `json:"tcp_closed_before_final_chunk"`
	Error         string            `json:"error,omitempty"`
}
type nativeCutServer struct {
	mu      sync.Mutex
	native  string
	records []nativeCutRecord
}

func (f *nativeCutServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	record := nativeCutRecord{RequestAt: time.Now().UTC(), Path: req.URL.Path, AttemptHeader: req.Header.Get("X-Client-Request-Id")}
	defer func() { f.mu.Lock(); f.records = append(f.records, record); f.mu.Unlock() }()
	body, err := io.ReadAll(io.LimitReader(req.Body, 1<<20))
	if err != nil {
		record.Error = err.Error()
		http.Error(w, "fixture read failed", 500)
		return
	}
	record.Request = body
	expectPath := "/responses"
	if f.native == "anthropic" {
		expectPath = "/v1/messages"
	}
	if req.Method != http.MethodPost || req.URL.Path != expectPath {
		record.Error = "unexpected native route"
		http.Error(w, record.Error, 400)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("x-request-id", "f03-native-request")
	w.Header().Set("request-id", "f03-native-request")
	for _, event := range interruptedNativeEvents(f.native) {
		raw, _ := json.Marshal(event)
		if _, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event["type"], raw); err != nil {
			record.Error = err.Error()
			return
		}
		if err = http.NewResponseController(w).Flush(); err != nil {
			record.Error = err.Error()
			return
		}
		record.Frames = append(record.Frames, raw)
	}
	record.FlushedAt = time.Now().UTC()
	// Close a chunked HTTP response without its final chunk. This produces a real
	// transport truncation, rather than a complete HTTP body lacking a marker.
	conn, _, err := http.NewResponseController(w).Hijack()
	if err != nil {
		record.Error = err.Error()
		return
	}
	err = conn.Close()
	record.ClosedAt = time.Now().UTC()
	record.Closed = err == nil
	if err != nil {
		record.Error = err.Error()
	}
}
func (f *nativeCutServer) snapshot() []nativeCutRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]nativeCutRecord(nil), f.records...)
}

type observedNativeProvider struct {
	provider.Provider
	Events     []provider.ModelEvent `json:"events"`
	Turn       provider.ModelTurn    `json:"returned_turn"`
	Error      string                `json:"returned_error"`
	ErrorKind  provider.ErrorKind    `json:"error_kind"`
	FinishedAt time.Time             `json:"finished_at"`
	Calls      int                   `json:"calls"`
}

func (p *observedNativeProvider) Stream(ctx context.Context, req provider.ModelRequest, emit func(provider.ModelEvent) error) (provider.ModelTurn, error) {
	p.Calls++
	turn, err := p.Provider.Stream(ctx, req, func(event provider.ModelEvent) error { p.Events = append(p.Events, event); return emit(event) })
	p.Turn, p.FinishedAt = turn, time.Now().UTC()
	if err != nil {
		p.Error = err.Error()
		var e *provider.Error
		if errors.As(err, &e) {
			p.ErrorKind = e.Kind
		}
	}
	return turn, err
}

// No container or filesystem execution is simulated as successful. Prepare is
// the sole allowed runner interaction; every other method records and rejects.
type interruptedRecordingRunner struct {
	hash       string
	prepares   int
	operations []string
}

func (r *interruptedRecordingRunner) reject(name string) error {
	r.operations = append(r.operations, name)
	return fmt.Errorf("unexpected runner operation %s", name)
}
func (r *interruptedRecordingRunner) PrepareWorkspace(_ context.Context, p runner.PrepareRequest) (runner.Workspace, error) {
	r.prepares++
	return runner.Workspace{TenantID: p.TenantID, RunID: p.RunID, ID: p.WorkspaceID, SourceID: p.SourceID, ProfileID: p.ProfileID, Epoch: p.Epoch, Revision: 1, BaselineHash: r.hash}, nil
}
func (r *interruptedRecordingRunner) AdoptWorkspace(context.Context, runner.WorkspaceRequest) (runner.StopReceipt, error) {
	return runner.StopReceipt{}, r.reject("adopt")
}
func (r *interruptedRecordingRunner) StartOperation(_ context.Context, p runner.OperationRequest) (runner.Operation, error) {
	return runner.Operation{}, r.reject("start:" + p.Kind)
}
func (r *interruptedRecordingRunner) InspectOperation(context.Context, runner.InspectRequest) (runner.Operation, error) {
	return runner.Operation{}, r.reject("inspect")
}
func (r *interruptedRecordingRunner) CancelOperation(context.Context, runner.InspectRequest) (runner.Operation, error) {
	return runner.Operation{}, r.reject("cancel")
}
func (r *interruptedRecordingRunner) StopWorkspace(context.Context, runner.WorkspaceRequest) (runner.StopReceipt, error) {
	return runner.StopReceipt{}, r.reject("stop")
}
func (r *interruptedRecordingRunner) SealSnapshot(context.Context, runner.WorkspaceRequest) (runner.Snapshot, error) {
	return runner.Snapshot{}, r.reject("seal")
}
func (r *interruptedRecordingRunner) ReleaseWorkspace(context.Context, runner.WorkspaceRequest) (runner.ReleaseResult, error) {
	return runner.ReleaseResult{}, r.reject("release")
}

type interruptedLedger struct {
	Phase        string            `json:"phase"`
	CapturedAt   time.Time         `json:"captured_at"`
	Run          json.RawMessage   `json:"run"`
	Attempts     []json.RawMessage `json:"attempts"`
	Reservations []json.RawMessage `json:"reservations"`
	Quotas       []json.RawMessage `json:"quotas"`
	Events       []json.RawMessage `json:"events"`
	Effects      []json.RawMessage `json:"effects"`
	Artifacts    []json.RawMessage `json:"artifacts"`
}

func interruptedLedgerSnapshot(ctx context.Context, s *persistence.Store, phase string) (interruptedLedger, error) {
	var raw []byte
	err := s.Pool.QueryRow(ctx, `SELECT jsonb_build_object('captured_at',clock_timestamp(),'run',(SELECT to_jsonb(r) FROM runs r),'attempts',coalesce((SELECT jsonb_agg(a ORDER BY attempt) FROM model_attempts a),'[]'),'reservations',coalesce((SELECT jsonb_agg(q) FROM quota_reservations q),'[]'),'quotas',coalesce((SELECT jsonb_agg(q) FROM provider_quotas q),'[]'),'events',coalesce((SELECT jsonb_agg(e ORDER BY seq) FROM run_events e),'[]'),'effects',coalesce((SELECT jsonb_agg(e) FROM effects e),'[]'),'artifacts',coalesce((SELECT jsonb_agg(a ORDER BY kind) FROM artifacts a),'[]'))`).Scan(&raw)
	var snapshot interruptedLedger
	if err == nil {
		err = json.Unmarshal(raw, &snapshot)
	}
	snapshot.Phase = phase
	return snapshot, err
}

func validateInterruptedLedger(snapshot interruptedLedger, slotReleased bool) error {
	if len(snapshot.Attempts) != 1 || len(snapshot.Reservations) != 1 || len(snapshot.Quotas) != 1 || len(snapshot.Effects) != 0 {
		return fmt.Errorf("wrong row counts: attempts=%d reservations=%d quotas=%d effects=%d", len(snapshot.Attempts), len(snapshot.Reservations), len(snapshot.Quotas), len(snapshot.Effects))
	}
	var attempt, reservation, q map[string]any
	for _, part := range []struct {
		raw json.RawMessage
		dst *map[string]any
	}{{snapshot.Attempts[0], &attempt}, {snapshot.Reservations[0], &reservation}, {snapshot.Quotas[0], &q}} {
		if err := json.Unmarshal(part.raw, part.dst); err != nil {
			return err
		}
	}
	if attempt["status"] != "failed" || attempt["raw_ref"] != nil || attempt["usage"] != nil || attempt["error_code"] != "stream_interrupted" {
		return fmt.Errorf("attempt was completed or failed for wrong reason: %s", snapshot.Attempts[0])
	}
	if reservation["id"] != attempt["attempt_id"] || reservation["status"] != "unknown" || reservation["microusd"] != float64(10240) || reservation["tokens"] != float64(9216) || reservation["actual_microusd"] != nil || reservation["actual_tokens"] != nil || reservation["settled_at"] != nil || reservation["dispatched_at"] == nil || reservation["request_slot_released"] != slotReleased {
		return fmt.Errorf("unknown reservation lost or settled: %s", snapshot.Reservations[0])
	}
	expectedActive := float64(1)
	if slotReleased {
		expectedActive = 0
	}
	if q["reserved_microusd"] != float64(10240) || q["reserved_tokens"] != float64(9216) || q["committed_microusd"] != float64(0) || q["committed_tokens"] != float64(0) || q["active_requests"] != expectedActive {
		return fmt.Errorf("quota obligation changed: %s", snapshot.Quotas[0])
	}
	for i, raw := range snapshot.Events {
		var e map[string]any
		if err := json.Unmarshal(raw, &e); err != nil {
			return err
		}
		if e["seq"] != float64(i+1) {
			return fmt.Errorf("event gap")
		}
		if e["type"] == "model.completed" {
			return fmt.Errorf("incomplete model was completed")
		}
	}
	for _, raw := range snapshot.Artifacts {
		var a map[string]any
		if err := json.Unmarshal(raw, &a); err != nil {
			return err
		}
		if a["kind"] == "model_response" {
			return fmt.Errorf("incomplete response published")
		}
	}
	return nil
}

func waitDatabaseTime(ctx context.Context, s *persistence.Store, target time.Time) error {
	for {
		var now time.Time
		if err := s.Pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			return err
		}
		if !now.Before(target) {
			return nil
		}
		timer := time.NewTimer(min(target.Sub(now)+time.Millisecond, 100*time.Millisecond))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func TestNativeStreamInterruptedLedgerEvidence(t *testing.T) {
	if os.Getenv("FORGE_RUN_MODEL_STREAM_FAULT") != "1" {
		t.Skip("set FORGE_RUN_MODEL_STREAM_FAULT=1 and FORGE_TEST_DATABASE_URL for local native HTTP/private PG acceptance")
	}
	for _, native := range []string{"openai", "anthropic"} {
		t.Run(native, func(t *testing.T) { runNativeInterruption(t, native) })
	}
}
func runNativeInterruption(t *testing.T, native string) {
	started := time.Now().UTC()
	out := filepath.Join("results", "local", "model-stream-interrupted-"+native+"-"+started.Format("20060102T150405.000000000Z"))
	if err := os.MkdirAll(out, 0755); err != nil {
		t.Fatal(err)
	}
	report := map[string]any{"schema_version": 1, "started_at": started, "native": native, "passed": false, "scope": "Native SDK HTTP stream truncation through production application.Driver and real private PostgreSQL; recording runner, no model service or container"}
	defer func() {
		report["test_failed"] = t.Failed()
		report["finished_at"] = time.Now().UTC()
		sustainedWriteReport(t, out, report)
	}()
	// Snapshot before execution; subsequent source edits cannot relabel this run.
	sources := map[string]string{}
	for _, source := range []string{"benchmarks/model_stream_fault_test.go", "go.mod", "go.sum", "internal/application/driver.go", "internal/application/model.go", "internal/application/context.go", "internal/application/pricing.go", "internal/provider/openai.go", "internal/provider/anthropic.go", "internal/provider/assembly.go", "internal/provider/types.go", "internal/provider/errors.go", "internal/provider/transport.go", "internal/quota/store.go", "internal/quota/types.go", "internal/persistence/model_pricing.go", "internal/persistence/execution.go", "internal/persistence/store.go", "internal/persistence/scheduler.go", "internal/testutil/database.go"} {
		raw, err := os.ReadFile(filepath.Join("..", source))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		sources[source] = hex.EncodeToString(sum[:])
		dest := filepath.Join(out, "source-snapshot", source+".txt")
		if err = os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(dest, raw, 0644); err != nil {
			t.Fatal(err)
		}
	}
	commit, _ := exec.Command("git", "rev-parse", "HEAD").Output()
	status, _ := exec.Command("git", "status", "--porcelain").Output()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.Open(executable)
	if err != nil {
		t.Fatal(err)
	}
	binaryHash := sha256.New()
	_, hashErr := io.Copy(binaryHash, binary)
	closeErr := binary.Close()
	if hashErr != nil || closeErr != nil {
		t.Fatalf("hash executing binary: %v / %v", hashErr, closeErr)
	}
	buildID, err := exec.Command("go", "tool", "buildid", executable).Output()
	if err != nil {
		t.Fatal(err)
	}
	report["manifest"] = map[string]any{"git_base_commit": strings.TrimSpace(string(commit)), "working_tree_dirty": len(status) > 0, "source_sha256": sources, "machine": machine(), "executing_binary_sha256": hex.EncodeToString(binaryHash.Sum(nil)), "executing_binary_build_id": strings.TrimSpace(string(buildID))}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := testutil.Database(t)
	var version, schema, fsync, syncCommit string
	if err := s.Pool.QueryRow(ctx, `SELECT version(),current_schema(),current_setting('fsync'),current_setting('synchronous_commit')`).Scan(&version, &schema, &fsync, &syncCommit); err != nil {
		t.Fatal(err)
	}
	report["database"] = map[string]any{"version": version, "private_schema": schema, "fsync": fsync, "synchronous_commit": syncCommit, "pool_max": s.Pool.Config().MaxConns, "role": "test owner worker role; no HTTP API/RLS assertion"}
	if err := s.BootstrapTenant(ctx, "f03_tenant", "f03_developer", "developer"); err != nil {
		t.Fatal(err)
	}
	project, err := s.CreateProject(ctx, "f03_tenant", "interrupted native turn", "f03_source", "f03_profile")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.RegisterRunner(ctx, "f03_runner", "fixture://record-only", 1); err != nil {
		t.Fatal(err)
	}
	q := quota.New(s.Pool)
	if err = q.Configure(ctx, quota.Config{CredentialGroup: "f03-local", MaxConcurrent: 4, MaxTokens: 1_000_000, MaxCost: 100_000_000, WindowDuration: time.Hour, FailureThreshold: 3, BreakerCooldown: time.Second}); err != nil {
		t.Fatal(err)
	}
	objects, err := artifact.NewLocalStore(filepath.Join(t.TempDir(), "objects"), 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = objects.Close() })
	signer, err := runner.NewSigner([]byte(strings.Repeat("f", 32)))
	if err != nil {
		t.Fatal(err)
	}
	recordingRunner := &interruptedRecordingRunner{hash: strings.Repeat("a", 64)}
	fixture := &nativeCutServer{native: native}
	server := httptest.NewServer(fixture)
	defer server.Close()
	registry := provider.Registry{"f03-native": {ToolCalling: true, ContextWindow: 32768, MaxOutputTokens: 1024}}
	var adapter provider.Provider
	if native == "openai" {
		adapter = provider.NewOpenAI(registry, provider.Limits{}, openaioption.WithBaseURL(server.URL), openaioption.WithAPIKey("f03-local-synthetic"))
	} else {
		adapter = provider.NewAnthropic(registry, provider.Limits{}, anthropicoption.WithBaseURL(server.URL), anthropicoption.WithAPIKey("f03-local-synthetic"))
	}
	observer := &observedNativeProvider{Provider: adapter}
	defer func() {
		report["wire_requests"] = fixture.snapshot()
		report["adapter"] = map[string]any{"events": observer.Events, "returned_turn": observer.Turn, "returned_error": observer.Error, "error_kind": observer.ErrorKind, "finished_at": observer.FinishedAt, "calls": observer.Calls}
		report["runner"] = map[string]any{"prepare_calls": recordingRunner.prepares, "other_operations": recordingRunner.operations}
	}()
	d := &application.Driver{Store: s, Quota: q, Runner: recordingRunner, Signer: signer, Artifacts: objects, Providers: map[string]provider.Provider{native: observer}, Models: map[string]application.ModelSpec{native + "/f03-native": {CredentialGroup: "f03-local", PriceVersion: "synthetic-f03-v1", InputPrice: 1_000_000, OutputPrice: 2_000_000, ContextTokens: 8192, MaxOutputTokens: 1024, RequestTimeout: 8 * time.Second}}, Sources: map[string]application.SourceSpec{"f03_source": {Hash: recordingRunner.hash}}, LeaseDuration: 3 * time.Second}
	r, _, err := s.Submit(ctx, persistence.SubmitRequest{TenantID: "f03_tenant", PrincipalID: "f03_developer", ProjectID: project.ID, Task: "F03 synthetic stream fault; do not execute partial tools", BaseCommit: recordingRunner.hash, Config: persistence.Config{Provider: native, Model: "f03-native", MaxModelRounds: 3, MaxToolCalls: 4, MaxCost: 1_000_000, MaxRuntimeSeconds: 60}}, "f03-key")
	if err != nil {
		t.Fatal(err)
	}
	report["run_id"] = r.ID
	claimed, err := s.Claim(ctx, "f03-first-worker", 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	driveErr := d.Drive(ctx, claimed)
	if driveErr != nil {
		report["driver_return"] = driveErr.Error()
	}
	snapshots := []interruptedLedger{}
	defer func() { report["snapshots"] = snapshots }()
	capture := func(phase string, released bool) {
		snapshot, err := interruptedLedgerSnapshot(ctx, s, phase)
		if err != nil {
			t.Fatal(err)
		}
		snapshots = append(snapshots, snapshot)
		if err = validateInterruptedLedger(snapshot, released); err != nil {
			t.Fatal(err)
		}
	}
	capture("after_interrupted_driver_return", false)
	if driveErr == nil || observer.Calls != 1 || observer.ErrorKind != provider.ErrInterrupted || len(observer.Turn.ToolCalls) != 0 || observer.Turn.NativeState != nil || observer.Turn.Usage.Final || recordingRunner.prepares != 1 || len(recordingRunner.operations) != 0 {
		t.Fatalf("stream escaped incomplete boundary: drive=%v error=%s calls=%d runner=%+v", driveErr, observer.ErrorKind, observer.Calls, recordingRunner)
	}
	var deltas, toolDeltas, failed, completed int
	for _, e := range observer.Events {
		switch e.Type {
		case provider.EventTextDelta:
			if !e.Provisional {
				t.Fatal("text was final")
			}
			deltas++
		case provider.EventToolDelta:
			if !e.Provisional {
				t.Fatal("tool fragment was final")
			}
			toolDeltas++
		case provider.EventAttemptFailed:
			failed++
		case provider.EventAttemptCompleted:
			completed++
		}
	}
	if deltas == 0 || toolDeltas < 2 || failed != 1 || completed != 0 {
		t.Fatalf("adapter events missing required fragments/failure: %d/%d/%d/%d", deltas, toolDeltas, failed, completed)
	}
	var policyDue, deadline time.Time
	if err = s.Pool.QueryRow(ctx, `SELECT r.not_before,a.deadline FROM runs r JOIN model_attempts a ON a.tenant_id=r.tenant_id AND a.run_id=r.id WHERE r.id=$1`, r.ID).Scan(&policyDue, &deadline); err != nil {
		t.Fatal(err)
	}
	if err = waitDatabaseTime(ctx, s, policyDue); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := s.Claim(ctx, "f03-no-heartbeat-worker", 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	report["natural_lease"] = map[string]any{"epoch": reclaimed.State.Lease.Epoch, "until": reclaimed.State.Lease.Until, "request_deadline": deadline, "no_driver_or_heartbeat_started": true}
	if reclaimed.State.Lease.Epoch != 2 || !reclaimed.State.Lease.Until.Before(deadline) {
		t.Fatal("cannot observe lease expiry separately before request deadline")
	}
	capture("new_lease_before_expiry", false)
	if err = waitDatabaseTime(ctx, s, reclaimed.State.Lease.Until.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.LeaseProof(ctx, r.TenantID, r.ID, reclaimed.State.Lease.Owner, reclaimed.State.Lease.Epoch); !errors.Is(err, domain.ErrFenced) {
		t.Fatalf("naturally expired lease still valid: %v", err)
	}
	early, err := q.ExpireRequestSlots(ctx, "f03-local")
	if err != nil {
		t.Fatal(err)
	}
	report["early_request_slot_expirations"] = early
	if early != 0 {
		t.Fatal("request slot released merely by worker lease expiry")
	}
	capture("natural_worker_lease_expired_request_still_live", false)
	if err = waitDatabaseTime(ctx, s, deadline.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	expired, err := q.ExpireRequestSlots(ctx, "f03-local")
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := q.ExpireRequestSlots(ctx, "f03-local")
	if err != nil {
		t.Fatal(err)
	}
	report["request_slot_expirations"] = []int{expired, replayed}
	if expired != 1 || replayed != 0 {
		t.Fatalf("request slot expiry not once: %d/%d", expired, replayed)
	}
	capture("natural_request_deadline_expired_slot_only_released", true)
	if err = q.AbandonBeforeDispatch(ctx, string(r.TenantID), string(mustAttemptID(t, snapshots[0]))); !errors.Is(err, quota.ErrAlreadyDispatched) {
		t.Fatalf("dispatched request incorrectly refundable: %v", err)
	}
	capture("zero_cost_abandon_rejected", true)
	for _, snapshot := range snapshots {
		if len(snapshot.Effects) != 0 {
			t.Fatal("effect appeared")
		}
	}
	requests := fixture.snapshot()
	if len(requests) != 1 || !requests[0].Closed || requests[0].Error != "" || observer.Calls != 1 || len(recordingRunner.operations) != 0 {
		t.Fatalf("wire or dispatch count mismatch: %+v", requests)
	}
	report["limitations"] = []string{"Native provider APIs are served by a local HTTP fault fixture, never paid endpoints; rates are synthetic microUSD, not invoices.", "Production Driver, provider adapters/SDKs, persistence, artifacts and quota paths run unchanged. Runner Prepare is a recording fixture; no Docker/process/file effects execute.", "Driver's persisted failure status is failed with error_code stream_interrupted; incomplete means no complete turn/raw_ref/final usage, not a separate incomplete status enum.", "After failure the test claims a new lease and deliberately runs no Driver/heartbeat. Database-clock waits observe natural lease then request deadline expiry; no timestamps are changed by SQL.", "This single interrupted attempt is not redriven into its permitted retry; unknown obligation remains while request concurrency expires once. Cleanup drops only its own private schema and temporary objects."}
	report["passed"] = true
	t.Logf("passed=true native=%s attempt_incomplete=true effects=0 retained_microusd=10240 retained_tokens=9216 lease_expiry_no_refund=true request_slot_expirations=%d/%d", native, expired, replayed)
}
func mustAttemptID(t *testing.T, s interruptedLedger) domain.ID {
	t.Helper()
	var a struct {
		ID domain.ID `json:"attempt_id"`
	}
	if err := json.Unmarshal(s.Attempts[0], &a); err != nil {
		t.Fatal(err)
	}
	return a.ID
}

func TestInterruptedLedgerOracleRejectsReleasedExpense(t *testing.T) {
	s := interruptedLedger{Attempts: []json.RawMessage{json.RawMessage(`{"attempt_id":"attempt_fixture","status":"failed","error_code":"stream_interrupted","raw_ref":null,"usage":null}`)}, Reservations: []json.RawMessage{json.RawMessage(`{"id":"attempt_fixture","status":"unknown","microusd":10240,"tokens":9216,"dispatched_at":"2026-09-11T00:00:00Z","request_slot_released":true}`)}, Quotas: []json.RawMessage{json.RawMessage(`{"reserved_microusd":10240,"reserved_tokens":9216,"committed_microusd":0,"committed_tokens":0,"active_requests":0}`)}}
	if err := validateInterruptedLedger(s, true); err != nil {
		t.Fatal(err)
	}
	s.Quotas[0] = json.RawMessage(`{"reserved_microusd":0,"reserved_tokens":0,"committed_microusd":0,"committed_tokens":0,"active_requests":0}`)
	if err := validateInterruptedLedger(s, true); err == nil {
		t.Fatal("zeroed unknown obligation passed")
	}
}
