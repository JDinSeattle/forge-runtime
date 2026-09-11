package review_test

import (
	"bytes"
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
	osexec "os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/application"
	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/eventstream"
	"github.com/JDinSeattle/forge-runtime/internal/httpapi"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/quota"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	flow "github.com/JDinSeattle/forge-runtime/internal/runtime"
)

func compatibilityFuture(t *testing.T, ctx context.Context, s *persistence.Store, r persistence.Run, version string) {
	t.Helper()
	_, err := s.Pool.Exec(ctx, `UPDATE runs SET snapshot=jsonb_set(snapshot::jsonb,'{schema_version}',$3::jsonb)||'{"future_only":{"exact":9007199254740993,"html":"<>&"}}'::jsonb WHERE tenant_id=$1 AND id=$2`, r.TenantID, r.ID, version)
	if err != nil {
		t.Fatal(err)
	}
}

func compatibilityHTTP(t *testing.T, ctx context.Context, server *httptest.Server, tenant domain.ID, token, method, path string, payload ...any) (int, []byte) {
	t.Helper()
	var requestBody []byte
	if len(payload) > 0 {
		var err error
		requestBody, err = json.Marshal(payload[0])
		if err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, server.URL+path, bytes.NewReader(requestBody))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Forge-Tenant", string(tenant))
	req.Header.Set("Content-Type", "application/json")
	res, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return res.StatusCode, body
}

// Real PostgreSQL and production HTTP/claim paths. No model or runner is
// attached: future state must be rejected before any execution authority.
func TestReviewUnsupportedSnapshotDoesNotStarveHealthyRuns(t *testing.T) {
	ctx, owner, api := reviewAPIStore(t)
	restrictApprovalAuthTables(t, ctx, owner, api)
	bad := reviewRun(t, ctx, owner)
	healthy, _, err := owner.Submit(ctx, persistence.SubmitRequest{TenantID: bad.TenantID, PrincipalID: bad.PrincipalID, ProjectID: bad.ProjectID, Task: "healthy same tenant", BaseCommit: bad.BaseCommit, Config: bad.Config}, "healthy-same")
	if err != nil {
		t.Fatal(err)
	}
	if err = owner.BootstrapTenant(ctx, "healthy_other", "other_principal", "developer"); err != nil {
		t.Fatal(err)
	}
	project, err := owner.CreateProject(ctx, "healthy_other", "healthy", "local-fixture", "python")
	if err != nil {
		t.Fatal(err)
	}
	other, _, err := owner.Submit(ctx, persistence.SubmitRequest{TenantID: "healthy_other", PrincipalID: "other_principal", ProjectID: project.ID, Task: "healthy other tenant", BaseCommit: bad.BaseCommit, Config: bad.Config}, "healthy-other")
	if err != nil {
		t.Fatal(err)
	}
	compatibilityFuture(t, ctx, owner, bad, "3")
	if got, err := owner.GetRun(ctx, bad.TenantID, bad.ID); err == nil {
		t.Errorf("unsupported snapshot exposed as current projection: schema=%d version=%d", got.State.SchemaVersion, got.State.Version)
	}
	token, err := owner.IssueToken(ctx, bad.PrincipalID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer((&httpapi.Server{Store: api}).Handler())
	defer server.Close()
	status, raw := compatibilityHTTP(t, ctx, server, bad.TenantID, token, "GET", "/v1/runs/"+string(bad.ID))
	var response struct {
		Code      string `json:"code"`
		Retryable bool   `json:"retryable"`
	}
	_ = json.Unmarshal(raw, &response)
	if status != 409 || response.Code != "snapshot_migration_required" || response.Retryable {
		t.Errorf("future snapshot HTTP=%d code=%q retryable=%t", status, response.Code, response.Retryable)
	}
	seen := map[domain.ID]bool{}
	for n := range 5 {
		r, err := owner.ClaimOnRunner(ctx, fmt.Sprintf("compatibility-worker-%d", n), time.Minute, "review_runner")
		if err == nil {
			seen[r.ID] = true
		}
		t.Logf("claim %d run=%s err=%v", n, r.ID, err)
	}
	if seen[bad.ID] || !seen[healthy.ID] || !seen[other.ID] {
		t.Errorf("future snapshot monopolized claims: bad=%t same_tenant=%t other_tenant=%t", seen[bad.ID], seen[healthy.ID], seen[other.ID])
	}
	inspection, err := owner.InspectSnapshot(ctx, bad.TenantID, bad.ID)
	if err != nil || inspection.Hold == nil || inspection.Hold.ObservedSchema != "3" {
		t.Fatalf("hold not persisted: %+v %v", inspection, err)
	}
	reopened, err := persistence.Open(ctx, owner.Pool.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	var wg sync.WaitGroup
	for n := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := reopened.ClaimOnRunner(ctx, fmt.Sprintf("reopen-%d", n), time.Minute, "review_runner"); !errors.Is(err, domain.ErrNotFound) {
				t.Errorf("held run selected after reopen: %v", err)
			}
		}()
	}
	wg.Wait()
	after, err := owner.InspectSnapshot(ctx, bad.TenantID, bad.ID)
	if err != nil || !reflect.DeepEqual(inspection, after) {
		t.Fatalf("repeated polling modified hold: %v", err)
	}
	compatibilitySave(t, "fairness", map[string]any{"bad": bad.ID, "healthy_same_tenant": healthy.ID, "healthy_other_tenant": other.ID, "claimed": seen, "http_status": status, "http_body": string(raw), "inspection": inspection, "concurrent_reopened_claims": 8, "passed": true})
}

func compatibilitySave(t *testing.T, name string, value any) {
	t.Helper()
	output := os.Getenv("FORGE_SNAPSHOT_REVIEW_OUTPUT")
	if output == "" {
		return
	}
	if !filepath.IsAbs(output) {
		t.Fatal("snapshot review output must be absolute")
	}
	if record, ok := value.(map[string]any); ok {
		record["passed"] = !t.Failed()
	}
	if err := os.MkdirAll(output, 0700); err != nil {
		t.Fatal(err)
	}
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(output, name+".json"), append(raw, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestReviewUnsupportedSnapshotAmbiguousMembersFenceAndHold(t *testing.T) {
	for _, fixture := range []struct{ name, members string }{
		{"lower-then-upper", `"schema_version":1,"SCHEMA_VERSION":2`},
		{"upper-then-lower", `"SCHEMA_VERSION":2,"schema_version":1`},
		{"duplicate-two-one", `"schema_version":2,"schema_version":1`},
		{"duplicate-one-one", `"schema_version":1,"schema_version":1`},
		{"unicode-long-s", `"schema_version":1,"ſchema_version":2`},
		{"escaped-long-s", `"schema_version":1,"\u017fchema_version":2`},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			ctx, owner, api := reviewAPIStore(t)
			restrictApprovalAuthTables(t, ctx, owner, api)
			bad := reviewRun(t, ctx, owner)
			bad, err := owner.ClaimOnRunner(ctx, "original", time.Second, "review_runner")
			if err != nil {
				t.Fatal(err)
			}
			healthy, _, err := owner.Submit(ctx, persistence.SubmitRequest{TenantID: bad.TenantID, PrincipalID: bad.PrincipalID, ProjectID: bad.ProjectID, Task: "healthy after ambiguous header", BaseCommit: bad.BaseCommit, Config: bad.Config}, "healthy-ambiguous")
			if err != nil {
				t.Fatal(err)
			}
			var original string
			if err = owner.Pool.QueryRow(ctx, `SELECT snapshot::text FROM runs WHERE tenant_id=$1 AND id=$2`, bad.TenantID, bad.ID).Scan(&original); err != nil {
				t.Fatal(err)
			}
			if strings.Count(original, `"schema_version":2`) != 1 {
				t.Fatal("fixture requires one original schema member")
			}
			// Keep PostgreSQL json text, order, duplicates and escapes intact.
			ambiguous := strings.Replace(original, `"schema_version":2`, fixture.members, 1)
			if _, err = owner.Pool.Exec(ctx, `UPDATE runs SET snapshot=$3::json WHERE tenant_id=$1 AND id=$2`, bad.TenantID, bad.ID, ambiguous); err != nil {
				t.Fatal(err)
			}
			before := compatibilityRecords(t, ctx, owner, bad)
			token, err := owner.IssueToken(ctx, bad.PrincipalID, time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer((&httpapi.Server{Store: api}).Handler())
			defer server.Close()
			status, body := compatibilityHTTP(t, ctx, server, bad.TenantID, token, "GET", "/v1/runs/"+string(bad.ID))
			if status != 409 || !bytes.Contains(body, []byte(`"code":"snapshot_migration_required"`)) {
				t.Errorf("ambiguous state exposed: HTTP=%d body=%s", status, body)
			}
			_, _, proofErr := owner.LeaseProof(ctx, bad.TenantID, bad.ID, bad.State.Lease.Owner, bad.State.Lease.Epoch)
			_, heartbeatErr := owner.Heartbeat(ctx, bad.TenantID, bad.ID, bad.State.Lease.Owner, bad.State.Lease.Epoch, time.Second)
			eventErr := owner.AppendWorkerEvent(ctx, bad, "must_not_publish", json.RawMessage(`{}`))
			for action, got := range map[string]error{"proof": proofErr, "heartbeat": heartbeatErr, "worker_event": eventErr} {
				if !errors.Is(got, domain.ErrFenced) {
					t.Errorf("live lease authorized %s: %v", action, got)
				}
			}
			afterAuthority := compatibilityRecords(t, ctx, owner, bad)
			if !reflect.DeepEqual(before, afterAuthority) {
				t.Error("ambiguous header authority checks changed persistent records")
			}
			var until time.Time
			if err = owner.Pool.QueryRow(ctx, `SELECT lease_until FROM runs WHERE tenant_id=$1 AND id=$2`, bad.TenantID, bad.ID).Scan(&until); err != nil {
				t.Fatal(err)
			}
			if wait := time.Until(until) + 20*time.Millisecond; wait > 0 {
				time.Sleep(wait)
			}
			_, claimErr := owner.ClaimOnRunner(ctx, "replacement", time.Minute, "review_runner")
			if !errors.Is(claimErr, domain.ErrSnapshotMigration) {
				t.Errorf("ambiguous candidate not durably held: %v", claimErr)
			}
			inspection, err := owner.InspectSnapshot(ctx, bad.TenantID, bad.ID)
			if err != nil {
				t.Fatal(err)
			}
			if inspection.Hold == nil || inspection.SnapshotText != ambiguous {
				t.Error("ambiguous original bytes/hold missing")
			}
			afterHold := compatibilityRecords(t, ctx, owner, bad)
			if !reflect.DeepEqual(before, afterHold) {
				t.Error("hold changed original run evidence or execution authority")
			}
			next, nextErr := owner.ClaimOnRunner(ctx, "healthy", time.Minute, "review_runner")
			if nextErr != nil || next.ID != healthy.ID {
				t.Errorf("ambiguous candidate blocks same-tenant healthy run: id=%s err=%v", next.ID, nextErr)
			}
			if _, err = owner.ClaimOnRunner(ctx, "repeat", time.Minute, "review_runner"); !errors.Is(err, domain.ErrNotFound) {
				t.Errorf("held candidate selected repeatedly: %v", err)
			}
			compatibilitySave(t, "ambiguous-"+fixture.name, map[string]any{"before": before, "after_authority": afterAuthority, "after_hold": afterHold, "inspection": inspection, "http_status": status, "http_body": string(body), "healthy": healthy.ID, "claimed": next.ID})
		})
	}
}

func TestReviewUnsupportedSnapshotOperatorCLI(t *testing.T) {
	binary := os.Getenv("FORGE_REVIEW_ADMIN_BINARY")
	if binary == "" {
		t.Skip("requires explicitly built forge-admin binary")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("admin binary must be absolute")
	}
	ctx, s := isolatedStore(t)
	r := reviewRun(t, ctx, s)
	var original string
	if err := s.Pool.QueryRow(ctx, `SELECT snapshot::text FROM runs WHERE tenant_id=$1 AND id=$2`, r.TenantID, r.ID).Scan(&original); err != nil {
		t.Fatal(err)
	}
	compatibilityFuture(t, ctx, s, r, "3")
	if _, err := s.ClaimOnRunner(ctx, "cli-fixture", time.Minute, "review_runner"); !errors.Is(err, domain.ErrSnapshotMigration) {
		t.Fatal(err)
	}
	call := func(extra ...string) ([]byte, error) {
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		args := append([]string{"-tenant", string(r.TenantID), "-run", string(r.ID)}, extra...)
		cmd := osexec.CommandContext(callCtx, binary, args...)
		cmd.Env = []string{"PATH=/usr/bin:/bin", "FORGE_DATABASE_URL=" + s.Pool.Config().ConnString()}
		return cmd.CombinedOutput()
	}
	raw, err := call("snapshot-inspect")
	if err != nil {
		t.Fatalf("operator inspect failed: %s %v", raw, err)
	}
	var inspection persistence.SnapshotInspection
	if err = json.Unmarshal(raw, &inspection); err != nil || inspection.Hold == nil || !strings.Contains(inspection.SnapshotText, "9007199254740993") {
		t.Fatalf("CLI lost exact original data: %v", err)
	}
	denied, err := call("-expected-version", fmt.Sprint(inspection.Version), "-snapshot-sha256", inspection.SHA256, "snapshot-unhold")
	if err == nil || !strings.Contains(string(denied), "snapshot_migration_required") {
		t.Fatalf("CLI force-cleared future schema: %s %v", denied, err)
	}
	if _, err = s.Pool.Exec(ctx, `UPDATE runs SET snapshot=$3::json WHERE tenant_id=$1 AND id=$2`, r.TenantID, r.ID, original); err != nil {
		t.Fatal(err)
	}
	current, err := s.InspectSnapshot(ctx, r.TenantID, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	cleared, err := call("-expected-version", fmt.Sprint(current.Version), "-snapshot-sha256", current.SHA256, "snapshot-unhold")
	if err != nil {
		t.Fatalf("CLI safe unhold failed: %s %v", cleared, err)
	}
	var result persistence.SnapshotInspection
	if err = json.Unmarshal(cleared, &result); err != nil || result.Hold != nil || result.SnapshotText != original {
		t.Fatal("CLI unhold changed snapshot")
	}
	compatibilitySave(t, "operator-cli", map[string]any{"inspect_stdout": string(raw), "future_unhold_stderr": string(denied), "known_fixture_unhold_stdout": string(cleared), "scope": "real forge-admin process with private-schema administrator credentials; no snapshot conversion implemented", "passed": true})
}

// Include raw json column text separately: to_jsonb(row) alone would hide
// textual rewrites of snapshots, commands or append-only historical inputs.
func compatibilityRecords(t *testing.T, ctx context.Context, s *persistence.Store, r persistence.Run) map[string]string {
	t.Helper()
	result := map[string]string{}
	queries := map[string]string{
		"run":           `SELECT (to_jsonb(x)-ARRAY['snapshot_hold_at','snapshot_hold_reason','snapshot_hold_schema','snapshot_hold_sha256'])::text FROM runs x WHERE tenant_id=$1 AND id=$2`,
		"snapshot_text": `SELECT snapshot::text FROM runs WHERE tenant_id=$1 AND id=$2`,
		"commands_text": `SELECT pending_commands::text FROM runs WHERE tenant_id=$1 AND id=$2`,
		"snapshots":     `SELECT coalesce(json_agg(json_build_array(version,body::text,input_state::text,input_event::text) ORDER BY version)::text,'[]') FROM run_snapshots WHERE tenant_id=$1 AND run_id=$2`,
		"effects":       `SELECT coalesce(json_agg(row_to_json(x) ORDER BY operation_id)::text,'[]') FROM effects x WHERE tenant_id=$1 AND run_id=$2`,
		"attempts":      `SELECT coalesce(json_agg(row_to_json(x) ORDER BY step_seq,attempt)::text,'[]') FROM model_attempts x WHERE tenant_id=$1 AND run_id=$2`,
		"reservations":  `SELECT coalesce(json_agg(row_to_json(x) ORDER BY id)::text,'[]') FROM quota_reservations x WHERE tenant_id=$1 AND run_id=$2`,
		"events":        `SELECT coalesce(json_agg(row_to_json(x) ORDER BY seq)::text,'[]') FROM run_events x WHERE tenant_id=$1 AND run_id=$2`,
		"allocation":    `SELECT coalesce(json_agg(row_to_json(x))::text,'[]') FROM runner_allocations x WHERE tenant_id=$1 AND run_id=$2`,
		"quotas":        `SELECT coalesce(json_agg(row_to_json(x) ORDER BY credential_group)::text,'[]') FROM provider_quotas x WHERE $1::text<>'' AND $2::text<>''`,
		"runner_slots":  `SELECT reserved_slots::text FROM runners WHERE id='review_runner' AND $1::text<>'' AND $2::text<>''`,
		"tenant_active": `SELECT active_count::text FROM tenant_runtime WHERE tenant_id=$1 AND $2::text<>''`,
	}
	for name, query := range queries {
		var value string
		if err := s.Pool.QueryRow(ctx, query, r.TenantID, r.ID).Scan(&value); err != nil {
			t.Fatalf("capture %s: %v", name, err)
		}
		result[name] = value
	}
	return result
}

type compatibilityUncalledRunner struct{ runner.Service }

func (*compatibilityUncalledRunner) PrepareWorkspace(context.Context, runner.PrepareRequest) (runner.Workspace, error) {
	panic("unsupported snapshot reached runner PrepareWorkspace")
}
func (*compatibilityUncalledRunner) AdoptWorkspace(context.Context, runner.WorkspaceRequest) (runner.StopReceipt, error) {
	panic("unsupported snapshot reached runner AdoptWorkspace")
}
func (*compatibilityUncalledRunner) StartOperation(context.Context, runner.OperationRequest) (runner.Operation, error) {
	panic("unsupported snapshot reached runner StartOperation")
}

func TestReviewUnsupportedSnapshotPreservesUnknownLedgerAndAuthority(t *testing.T) {
	ctx, owner := isolatedStore(t)
	reviewRun(t, ctx, owner)
	r, err := owner.ClaimOnRunner(ctx, "known-worker", 2*time.Second, "review_runner")
	if err != nil {
		t.Fatal(err)
	}
	ref := domain.ID("compatibility_control_fixture")
	if err = owner.PublishArtifact(ctx, persistence.Artifact{TenantID: r.TenantID, RunID: r.ID, ID: ref, Kind: "control_fixture", ObjectKey: string(r.TenantID) + "/" + string(r.ID) + "/fixture", SHA256: strings.Repeat("0", 64)}); err != nil {
		t.Fatal(err)
	}
	advance := func(event flow.Event) {
		event.Owner, event.Epoch, event.ExpectedVersion = r.State.Lease.Owner, r.State.Lease.Epoch, r.State.Version
		r, err = owner.Advance(ctx, r.TenantID, r.ID, event)
		if err != nil {
			t.Fatal(err)
		}
	}
	advance(flow.Event{Kind: flow.EventWorkspaceReady, WorkspaceRevision: 1, OutputRef: string(ref)})
	advance(flow.Event{Kind: flow.EventContextBuilt, OutputRef: string(ref)})
	a, err := owner.BeginAttempt(ctx, r, string(ref), "fixture-price", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	q := quota.New(owner.Pool)
	if err = q.Configure(ctx, quota.Config{CredentialGroup: "compatibility", MaxConcurrent: 2, MaxTokens: 1000, MaxCost: 10000, WindowDuration: time.Hour, FailureThreshold: 3, BreakerCooldown: time.Second}); err != nil {
		t.Fatal(err)
	}
	_, err = q.Reserve(ctx, quota.Request{TenantID: string(r.TenantID), RunID: string(r.ID), AttemptID: string(a.ID), CredentialGroup: "compatibility", InputTokens: 10, MaxOutputTokens: 20, MaxCost: 1234, PriceVersion: "fixture-price", RequestDeadline: a.Deadline})
	if err != nil {
		t.Fatal(err)
	}
	if err = q.MarkDispatched(ctx, string(r.TenantID), string(a.ID)); err != nil {
		t.Fatal(err)
	}
	if err = q.MarkUnknown(ctx, string(r.TenantID), string(a.ID)); err != nil {
		t.Fatal(err)
	}
	advance(flow.Event{Kind: flow.EventModelCompleted, Complete: true, OutputRef: string(ref)})
	args := json.RawMessage(`{"command":["never-executed"]}`)
	h := sha256.Sum256(args)
	advance(flow.Event{Kind: flow.EventToolsValidated, Complete: true, OutputRef: string(ref), Effects: []flow.Effect{{ID: "compatibility_unknown", Kind: "run_command", Args: args, ArgsHash: hex.EncodeToString(h[:]), PolicyVersion: "fixture-v1"}}})
	if _, err = owner.Pool.Exec(ctx, `UPDATE effects SET status='unknown' WHERE tenant_id=$1 AND run_id=$2`, r.TenantID, r.ID); err != nil {
		t.Fatal(err)
	}
	compatibilityFuture(t, ctx, owner, r, "4294967296")
	before := compatibilityRecords(t, ctx, owner, r)
	d := application.Driver{Store: owner, Runner: &compatibilityUncalledRunner{}, RunnerID: "review_runner"}
	if err = d.Drive(ctx, r); !errors.Is(err, domain.ErrSnapshotMigration) {
		t.Fatalf("Drive interpreted future state: %v", err)
	}
	if _, _, err = owner.LeaseProof(ctx, r.TenantID, r.ID, r.State.Lease.Owner, r.State.Lease.Epoch); !errors.Is(err, domain.ErrFenced) {
		t.Fatalf("future state issued authority: %v", err)
	}
	if _, err = owner.Heartbeat(ctx, r.TenantID, r.ID, r.State.Lease.Owner, r.State.Lease.Epoch, time.Minute); !errors.Is(err, domain.ErrFenced) {
		t.Fatalf("future state renewed authority: %v", err)
	}
	if err = owner.Defer(ctx, r, time.Now().Add(time.Minute), "fixture.defer"); !errors.Is(err, domain.ErrFenced) {
		t.Fatalf("future state changed lease via Defer: %v", err)
	}
	if err = owner.AppendWorkerEvent(ctx, r, "fixture", json.RawMessage(`{}`)); !errors.Is(err, domain.ErrFenced) {
		t.Fatalf("future state accepted worker event: %v", err)
	}
	if _, err = owner.AcquireWorkspaceCleanup(ctx, r.TenantID, r.ID, "operator", 0); !errors.Is(err, domain.ErrSnapshotMigration) {
		t.Fatalf("future state entered cleanup: %v", err)
	}
	if delay := time.Until(r.State.Lease.Until) + 20*time.Millisecond; delay > 0 {
		time.Sleep(delay)
	}
	if _, err = owner.ClaimOnRunner(ctx, "replacement", time.Minute, "review_runner"); !errors.Is(err, domain.ErrSnapshotMigration) {
		t.Fatalf("expired future state not held: %v", err)
	}
	after := compatibilityRecords(t, ctx, owner, r)
	if !reflect.DeepEqual(before, after) {
		for k := range before {
			if before[k] != after[k] {
				t.Errorf("hold changed %s", k)
			}
		}
	}
	if after["runner_slots"] != "1" || after["tenant_active"] != "1" || !strings.Contains(after["reservations"], `"status":"unknown"`) {
		t.Fatal("unknown obligation lost")
	}
	inspection, err := owner.InspectSnapshot(ctx, r.TenantID, r.ID)
	if err != nil || inspection.Hold == nil {
		t.Fatalf("hold evidence absent: %v", err)
	}
	compatibilitySave(t, "unknown-ledger", map[string]any{"before": before, "after": after, "inspection": inspection, "runner_model_calls": 0, "scope": "synthetic control evidence and dispatched/unknown quota marker; no actual model or runner effect", "passed": true})
}

func TestReviewUnsupportedSnapshotHTTPAndOperatorRecovery(t *testing.T) {
	ctx, owner, api := reviewAPIStore(t)
	restrictApprovalAuthTables(t, ctx, owner, api)
	r := reviewRun(t, ctx, owner)
	var original string
	if err := owner.Pool.QueryRow(ctx, `SELECT snapshot::text FROM runs WHERE tenant_id=$1 AND id=$2`, r.TenantID, r.ID).Scan(&original); err != nil {
		t.Fatal(err)
	}
	objects, err := artifact.NewLocalStore(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer objects.Close()
	const content = "immutable evidence survives incompatible state\n"
	ref, err := objects.Put(ctx, r.TenantID, r.ID, "tool_output", strings.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	if err = owner.PublishArtifact(ctx, persistence.Artifact{TenantID: r.TenantID, RunID: r.ID, ID: "compatibility_file", Kind: ref.Kind, ObjectKey: ref.ObjectKey, SHA256: ref.SHA256, ByteSize: ref.Size}); err != nil {
		t.Fatal(err)
	}
	compatibilityFuture(t, ctx, owner, r, "3")
	if _, err = owner.ClaimOnRunner(ctx, "compatibility", time.Minute, "review_runner"); !errors.Is(err, domain.ErrSnapshotMigration) {
		t.Fatal(err)
	}
	inspection, err := owner.InspectSnapshot(ctx, r.TenantID, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = api.InspectSnapshot(ctx, r.TenantID, r.ID); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("runtime inspected raw state: %v", err)
	}
	if _, err = api.ClearSnapshotHold(ctx, r.TenantID, r.ID, inspection.Version, inspection.SHA256); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("runtime cleared hold: %v", err)
	}
	if _, err = owner.ClearSnapshotHold(ctx, r.TenantID, r.ID, inspection.Version, inspection.SHA256); !errors.Is(err, domain.ErrSnapshotMigration) {
		t.Fatalf("operator force-cleared future schema: %v", err)
	}
	token, err := owner.IssueToken(ctx, r.PrincipalID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	streams := eventstream.New(ctx, api, eventstream.Config{})
	defer streams.Close()
	server := httptest.NewServer((&httpapi.Server{Store: api, Artifacts: objects, Streams: streams}).Handler())
	defer server.Close()
	observations := map[string]any{}
	base := "/v1/runs/" + string(r.ID)
	for _, c := range []struct {
		method, path string
		body         any
	}{{"GET", base, nil}, {"GET", base + "/snapshot", nil}, {"GET", base + "/events", nil}, {"POST", base + "/cancel", nil}, {"POST", base + "/resume", map[string]any{"expected_version": r.State.Version}}} {
		status, raw := compatibilityHTTP(t, ctx, server, r.TenantID, token, c.method, c.path, c.body)
		var e httpapi.APIError
		_ = json.Unmarshal(raw, &e)
		if status != 409 || e.Code != "snapshot_migration_required" || e.Retryable || strings.Contains(string(raw), "future_only") {
			t.Errorf("%s %s returned %d %s", c.method, c.path, status, raw)
		}
		observations[c.method+c.path] = map[string]any{"status": status, "body": string(raw)}
	}
	if status, raw := compatibilityHTTP(t, ctx, server, r.TenantID, token, "GET", base+"/artifacts"); status != 200 || !strings.Contains(string(raw), "compatibility_file") {
		t.Fatalf("artifact listing unavailable: %d %s", status, raw)
	}
	if status, raw := compatibilityHTTP(t, ctx, server, r.TenantID, token, "GET", "/v1/artifacts/compatibility_file"); status != 200 || string(raw) != content {
		t.Fatalf("artifact bytes unavailable: %d %s", status, raw)
	}
	if err = owner.BootstrapTenant(ctx, "compatibility_foreign", r.PrincipalID, "viewer"); err != nil {
		t.Fatal(err)
	}
	if status, _ := compatibilityHTTP(t, ctx, server, "compatibility_foreign", token, "GET", base); status != 404 {
		t.Fatalf("foreign run exposed: %d", status)
	}
	if status, _ := compatibilityHTTP(t, ctx, server, "compatibility_foreign", token, "GET", "/v1/artifacts/compatibility_file"); status != 404 {
		t.Fatalf("foreign artifact exposed: %d", status)
	}
	// This test restores a previously saved complete known fixture. Production
	// unhold never edits a schema number or pretends to convert future fields.
	if _, err = owner.Pool.Exec(ctx, `UPDATE runs SET snapshot=$3::json WHERE tenant_id=$1 AND id=$2`, r.TenantID, r.ID, original); err != nil {
		t.Fatal(err)
	}
	if _, err = owner.GetRun(ctx, r.TenantID, r.ID); !errors.Is(err, domain.ErrSnapshotMigration) {
		t.Fatal("supported header automatically bypassed operator hold")
	}
	if _, err = owner.ClearSnapshotHold(ctx, r.TenantID, r.ID, inspection.Version, inspection.SHA256); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("stale snapshot hash accepted: %v", err)
	}
	current, err := owner.InspectSnapshot(ctx, r.TenantID, r.ID)
	if err != nil || !current.HeaderSupported {
		t.Fatalf("known fixture not recognized: %v", err)
	}
	if _, err = owner.ClearSnapshotHold(ctx, r.TenantID, r.ID, current.Version+1, current.SHA256); !errors.Is(err, domain.ErrConflict) {
		t.Fatal("stale version accepted")
	}
	if _, err = owner.Pool.Exec(ctx, `UPDATE runs SET lease_until=clock_timestamp()+interval '100 milliseconds' WHERE tenant_id=$1 AND id=$2`, r.TenantID, r.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = owner.ClearSnapshotHold(ctx, r.TenantID, r.ID, current.Version, current.SHA256); !errors.Is(err, domain.ErrFenced) {
		t.Fatalf("cleared during live lease: %v", err)
	}
	time.Sleep(120 * time.Millisecond)
	beforeClear := compatibilityRecords(t, ctx, owner, r)
	cleared, err := owner.ClearSnapshotHold(ctx, r.TenantID, r.ID, current.Version, current.SHA256)
	if err != nil || cleared.Hold != nil {
		t.Fatalf("safe clear failed: %v", err)
	}
	if afterClear := compatibilityRecords(t, ctx, owner, r); !reflect.DeepEqual(beforeClear, afterClear) {
		t.Fatal("unhold changed business state")
	}
	if claimed, err := owner.ClaimOnRunner(ctx, "compatible-worker", time.Minute, "review_runner"); err != nil || claimed.ID != r.ID {
		t.Fatalf("unheld run not claimable: %v", err)
	}
	var role, session string
	var super, bypass bool
	if err = api.Pool.QueryRow(ctx, `SELECT current_user,session_user,rolsuper,rolbypassrls FROM pg_roles WHERE rolname=current_user`).Scan(&role, &session, &super, &bypass); err != nil {
		t.Fatal(err)
	}
	compatibilitySave(t, "http-operator", map[string]any{"http": observations, "inspection_before": inspection, "known_fixture_before_unhold": current, "cleared": cleared, "role": map[string]any{"current_user": role, "session_user": session, "superuser": super, "bypass_rls": bypass, "scope": "administrator connection SET ROLE to restricted nonowner; not separate LOGIN"}, "artifact_content": content, "passed": true})
}

func TestReviewUnsupportedSnapshotApprovalRefusesUnknownState(t *testing.T) {
	ctx, owner, api := reviewAPIStore(t)
	restrictApprovalAuthTables(t, ctx, owner, api)
	r := reviewApprovalWaiting(t, ctx, owner)
	compatibilityFuture(t, ctx, owner, r, "3")
	before := compatibilityRecords(t, ctx, owner, r)
	token, err := owner.IssueToken(ctx, r.PrincipalID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer((&httpapi.Server{Store: api}).Handler())
	defer server.Close()
	reply := approvalRequest(ctx, server, r, token, true)
	var e httpapi.APIError
	_ = json.Unmarshal(reply.body, &e)
	if reply.err != nil || reply.status != 409 || e.Code != "snapshot_migration_required" {
		t.Fatalf("approval interpreted future state: %+v", reply)
	}
	after := compatibilityRecords(t, ctx, owner, r)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("approval modified future state")
	}
	compatibilitySave(t, "approval", map[string]any{"before": before, "after": after, "status": reply.status, "body": string(reply.body), "passed": true})
}

func TestReviewUnsupportedSnapshotMalformedHeadersRemainHeld(t *testing.T) {
	for _, version := range []string{"null", "1.5", `"1"`, "-1", "999999999999999999999999999999999999999999999999999999999999999999999999"} {
		t.Run(version, func(t *testing.T) {
			ctx, s := isolatedStore(t)
			r := reviewRun(t, ctx, s)
			compatibilityFuture(t, ctx, s, r, version)
			before := compatibilityRecords(t, ctx, s, r)
			if _, err := s.ClaimOnRunner(ctx, "worker", time.Minute, "review_runner"); !errors.Is(err, domain.ErrSnapshotMigration) {
				t.Fatalf("malformed header not quarantined: %v", err)
			}
			if _, err := s.ClaimOnRunner(ctx, "again", time.Minute, "review_runner"); !errors.Is(err, domain.ErrNotFound) {
				t.Fatalf("malformed header selected again: %v", err)
			}
			if after := compatibilityRecords(t, ctx, s, r); !reflect.DeepEqual(before, after) {
				t.Fatal("malformed header rewritten")
			}
		})
	}
}

func TestReviewUnsupportedSnapshotStopsCleanupAndRetentionOfTerminalEvidence(t *testing.T) {
	ctx, s := isolatedStore(t)
	r := reviewRun(t, ctx, s)
	identity := persistence.Identity{TenantID: r.TenantID, PrincipalID: r.PrincipalID, Role: "developer"}
	_, err := s.Cancel(ctx, identity, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	r, err = s.ClaimOnRunner(ctx, "stop-fixture", time.Minute, "review_runner")
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		id   domain.ID
		kind string
	}{{"compatibility_stop", "stop_receipt"}, {"compatibility_snapshot", "workspace_snapshot"}} {
		if err = s.PublishArtifact(ctx, persistence.Artifact{TenantID: r.TenantID, RunID: r.ID, ID: item.id, Kind: item.kind, ObjectKey: string(r.TenantID) + "/" + string(r.ID) + "/" + string(item.id), SHA256: strings.Repeat("0", 64)}); err != nil {
			t.Fatal(err)
		}
	}
	r, err = s.Advance(ctx, r.TenantID, r.ID, flow.Event{Kind: flow.EventCancellationConfirmed, ExpectedVersion: r.State.Version, Owner: r.State.Lease.Owner, Epoch: r.State.Lease.Epoch, Stop: &flow.StopReceipt{Ref: "compatibility_stop", NoActiveOperations: true}})
	if err != nil {
		t.Fatal(err)
	}
	cleanup, err := s.AcquireWorkspaceCleanup(ctx, r.TenantID, r.ID, "cleanup-fixture", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.CleanupProof(ctx, cleanup, false); err != nil {
		t.Fatal(err)
	}
	compatibilityFuture(t, ctx, s, r, "3")
	before := compatibilityRecords(t, ctx, s, r)
	if _, _, err = s.CleanupProof(ctx, cleanup, false); !errors.Is(err, domain.ErrFenced) {
		t.Fatalf("old cleanup issued new authority: %v", err)
	}
	if err = s.SealWorkspaceCleanup(ctx, cleanup, "compatibility_snapshot"); !errors.Is(err, domain.ErrFenced) {
		t.Fatalf("future state advanced cleanup: %v", err)
	}
	if err = s.CompleteWorkspaceCleanup(ctx, cleanup); !errors.Is(err, domain.ErrSnapshotMigration) {
		t.Fatalf("future state released cleanup: %v", err)
	}
	if n, err := s.TrimEvents(ctx, time.Now().Add(time.Hour), 1, 20); err != nil || n != 0 {
		t.Fatalf("future terminal audit trimmed: %d %v", n, err)
	}
	if after := compatibilityRecords(t, ctx, s, r); !reflect.DeepEqual(before, after) {
		t.Fatal("future terminal evidence changed")
	}
	if _, _, err = s.Submit(ctx, persistence.SubmitRequest{TenantID: r.TenantID, PrincipalID: r.PrincipalID, ProjectID: r.ProjectID, ParentRunID: r.ID, Task: r.Task, BaseCommit: r.BaseCommit, Config: r.Config}, "future-retry"); !errors.Is(err, domain.ErrSnapshotMigration) {
		t.Fatalf("retried future terminal input: %v", err)
	}
	var keys int
	if err = s.Pool.QueryRow(ctx, `SELECT count(*) FROM idempotency_keys WHERE key='future-retry'`).Scan(&keys); err != nil || keys != 0 {
		t.Fatalf("future retry created key: %d %v", keys, err)
	}
	compatibilitySave(t, "terminal-evidence", map[string]any{"before": before, "after": compatibilityRecords(t, ctx, s, r), "cleanup": cleanup, "retained_all_events": true, "new_retry_keys": keys, "passed": true, "scope": "synthetic no-execution stop/snapshot evidence, no Docker cleanup"})
}

func TestReviewUnsupportedSnapshotEndsExistingSSEGeneration(t *testing.T) {
	ctx, owner, api := reviewAPIStore(t)
	r := reviewRun(t, ctx, owner)
	token, err := owner.IssueToken(ctx, r.PrincipalID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	streams := eventstream.New(ctx, api, eventstream.Config{PollInterval: 5 * time.Millisecond})
	defer streams.Close()
	sub, err := streams.Subscribe(ctx, r.TenantID, r.ID, r.CoveredSeq)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	server := httptest.NewServer((&httpapi.Server{Store: api, Streams: streams}).Handler())
	defer server.Close()
	requestCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, "GET", server.URL+"/v1/runs/"+string(r.ID)+"/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Forge-Tenant", string(r.TenantID))
	response, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatalf("initial supported stream not admitted: %d", response.StatusCode)
	}
	type ending struct {
		body []byte
		err  error
	}
	ended := make(chan ending, 1)
	go func() { raw, err := io.ReadAll(response.Body); ended <- ending{raw, err} }()
	started := time.Now()
	compatibilityFuture(t, ctx, owner, r, "3")
	select {
	case err := <-sub.Errors:
		if !errors.Is(err, domain.ErrSnapshotMigration) {
			t.Fatalf("wrong terminal hub error: %v", err)
		}
	case <-requestCtx.Done():
		t.Fatal("incompatible hub kept polling forever")
	}
	var received ending
	select {
	case received = <-ended:
		if received.err != nil {
			t.Fatalf("old stream only closed by request cancellation: %v", received.err)
		}
	case <-requestCtx.Done():
		t.Fatal("old HTTP stream stayed open")
	}
	if strings.Contains(string(received.body), "future_only") {
		t.Fatal("future state sent to existing stream")
	}
	status, raw := compatibilityHTTP(t, ctx, server, r.TenantID, token, "GET", "/v1/runs/"+string(r.ID)+"/events")
	if status != 409 || !strings.Contains(string(raw), "snapshot_migration_required") {
		t.Fatalf("new generation admitted future snapshot: %d %s", status, raw)
	}
	compatibilitySave(t, "existing-sse", map[string]any{"initial_http_status": response.StatusCode, "old_body": string(received.body), "source_error": "snapshot_migration_required", "closed_without_client_cancel": true, "observed_close_ms": float64(time.Since(started).Microseconds()) / 1000, "new_http_status": status, "new_body": string(raw), "passed": true})
}
