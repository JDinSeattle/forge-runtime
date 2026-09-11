package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/provider"
	"github.com/JDinSeattle/forge-runtime/internal/quota"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
	"github.com/JDinSeattle/forge-runtime/internal/testutil"
)

type scriptedRepair struct {
	scripts []provider.Script
	calls   atomic.Int64
}

func (p *scriptedRepair) Capabilities(ctx context.Context, model string) (provider.Capabilities, error) {
	return provider.NewFake().Capabilities(ctx, model)
}
func (p *scriptedRepair) Stream(ctx context.Context, r provider.ModelRequest, emit func(provider.ModelEvent) error) (provider.ModelTurn, error) {
	p.calls.Add(1)
	step, err := strconv.Atoi(r.StepID)
	if err != nil || step < 1 || step > len(p.scripts) {
		return provider.ModelTurn{}, fmt.Errorf("bad scripted step %s", r.StepID)
	}
	return provider.NewFake(p.scripts[step-1]).Stream(ctx, r, emit)
}
func toolScript(id, name, args string) provider.Script {
	return provider.Script{Chunks: []provider.Chunk{{Kind: "tool_start", CallID: id, Name: name}, {Kind: "tool_delta", CallID: id, Delta: args}, {Kind: "tool_end", CallID: id}}, FinishReason: "tool_calls", Usage: provider.Usage{Input: provider.TokenCount{Value: 100, Known: true}, Output: provider.TokenCount{Value: 50, Known: true}}}
}
func repairSetup(t *testing.T) (*Driver, persistence.Run, *scriptedRepair, string) {
	return repairSetupConfigured(t, nil)
}
func repairSetupConfigured(t *testing.T, configure func(*persistence.Config)) (*Driver, persistence.Run, *scriptedRepair, string) {
	t.Helper()
	ctx := context.Background()
	s := testutil.Database(t)
	dir := t.TempDir()
	source := filepath.Join(dir, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	before := "def clamp(value, low, high):\n    return min(low, max(value, high))\n"
	after := "def clamp(value, low, high):\n    return max(low, min(value, high))\n"
	if err := os.WriteFile(filepath.Join(source, "clamp.py"), []byte(before), 0600); err != nil {
		t.Fatal(err)
	}
	sourceHash, err := runner.ComputeSourceHash(ctx, source, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := artifact.NewLocalStore(filepath.Join(dir, "artifacts"), 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = artifacts.Close() })
	signer, err := runner.NewSigner([]byte(strings.Repeat("s", 32)))
	if err != nil {
		t.Fatal(err)
	}
	backend := &sandbox.TestBackend{StartFunc: func(ctx context.Context, spec sandbox.JobSpec) (sandbox.Job, error) {
		data, err := os.ReadFile(filepath.Join(spec.Workspace, "clamp.py"))
		if err != nil {
			return sandbox.Job{}, err
		}
		exit := 0
		if spec.Command[0] == "target" && string(data) != after {
			exit = 1
		}
		return sandbox.Job{ID: spec.ID, Started: true, ExitCode: exit, Output: []byte("protocol fixture only; no Python executed")}, nil
	}}
	engine, err := runner.Open(runner.Config{RootDir: filepath.Join(dir, "runner"), JournalPath: filepath.Join(dir, "journal.db"), Artifacts: artifacts, Backend: backend, Signer: signer, Sources: map[string]string{"fixture": source}, Profiles: map[string]sandbox.Profile{"python": {ID: "python", TargetCommand: []string{"target"}, VerifyCommand: []string{"regression"}}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	if err = s.BootstrapTenant(ctx, "tenant", "developer", "developer"); err != nil {
		t.Fatal(err)
	}
	project, err := s.CreateProject(ctx, "tenant", "clamp fixture", "fixture", "python")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.RegisterRunner(ctx, "runner", "in-process-test", 4); err != nil {
		t.Fatal(err)
	}
	q := quota.New(s.Pool)
	if err = q.Configure(ctx, quota.Config{CredentialGroup: "fake", MaxConcurrent: 4, MaxTokens: 1_000_000, MaxCost: 100_000_000, WindowDuration: time.Hour, FailureThreshold: 3, BreakerCooldown: time.Second}); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256([]byte(before))
	patch, _ := json.Marshal(runner.PatchArgs{Files: []runner.FileEdit{{Path: "clamp.py", ExpectedSHA256: hex.EncodeToString(hash[:]), Content: &after}}})
	p := &scriptedRepair{scripts: []provider.Script{toolScript("read", "read_file", `{"path":"clamp.py"}`), toolScript("patch", "apply_patch", string(patch)), {Chunks: []provider.Chunk{{Kind: "text", Delta: "The repair is ready for independent verification."}}, FinishReason: "stop", Usage: provider.Usage{Input: provider.TokenCount{Known: true, Value: 100}, Output: provider.TokenCount{Known: true, Value: 20}}}}}
	d := &Driver{Store: s, Quota: q, Runner: engine, Signer: signer, Artifacts: artifacts, Providers: map[string]provider.Provider{"fake": p}, Models: map[string]ModelSpec{"fake/fake": {CredentialGroup: "fake", PriceVersion: "fixture-zero-v1", ContextTokens: 8192, MaxOutputTokens: 1024, RequestTimeout: 5 * time.Second}}, Sources: map[string]SourceSpec{"fixture": {Hash: sourceHash, HasTarget: true}}, TrustedVerification: true, LeaseDuration: 3 * time.Second}
	d.Logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	cfg := persistence.Config{Provider: "fake", Model: "fake", MaxModelRounds: 8, MaxToolCalls: 20, MaxCost: 1_000_000, MaxRuntimeSeconds: 60}
	if configure != nil {
		configure(&cfg)
	}
	r, _, err := s.Submit(ctx, persistence.SubmitRequest{TenantID: "tenant", PrincipalID: "developer", ProjectID: project.ID, Task: "Fix clamp boundary behavior.", BaseCommit: sourceHash, Config: cfg}, "repair")
	if err != nil {
		t.Fatal(err)
	}
	return d, r, p, filepath.Join(dir, "runner", "workspaces", string(r.ID), "clamp.py")
}
func TestDurableRepairLoop(t *testing.T) {
	d, r, p, path := repairSetup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	claimed, err := d.Store.Claim(ctx, "worker-1", d.leaseDuration())
	if err != nil {
		t.Fatal(err)
	}
	if err = d.Drive(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	got, err := d.Store.GetRun(ctx, r.TenantID, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State.Status != domain.StatusCompleted || got.State.Verification != domain.VerificationVerified {
		t.Fatalf("unexpected final state %+v", got.State)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "max(low, min(value, high))") {
		t.Fatal("real workspace patch absent")
	}
	if p.calls.Load() != 3 {
		t.Fatalf("model calls=%d", p.calls.Load())
	}
	var slots, active int
	if err = d.Store.Pool.QueryRow(ctx, `SELECT reserved_slots FROM runners WHERE id='runner'`).Scan(&slots); err != nil {
		t.Fatal(err)
	}
	if err = d.Store.Pool.QueryRow(ctx, `SELECT active_count FROM tenant_runtime WHERE tenant_id='tenant'`).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if slots != 0 || active != 0 {
		t.Fatalf("leaked capacity %d/%d", slots, active)
	}
	events, err := d.Store.Events(ctx, r.TenantID, r.ID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for n, e := range events {
		if e.Seq != uint64(n+1) {
			t.Fatal("event sequence gap")
		}
	}
}
func TestCompletedModelReusedAfterWorkerCrash(t *testing.T) {
	d, r, p, _ := repairSetup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var injected atomic.Bool
	d.Fault = func(point string) error {
		if point == "after_model_result_before_transition" && injected.CompareAndSwap(false, true) {
			return context.Canceled
		}
		return nil
	}
	claimed, err := d.Store.Claim(ctx, "old-worker", d.leaseDuration())
	if err != nil {
		t.Fatal(err)
	}
	if err = d.Drive(ctx, claimed); err != context.Canceled {
		t.Fatalf("expected injected stop, got %v", err)
	}
	if _, err = d.Store.Pool.Exec(ctx, `UPDATE runs SET lease_until=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND id=$2`, r.TenantID, r.ID); err != nil {
		t.Fatal(err)
	}
	recovered, err := d.Store.Claim(ctx, "new-worker", d.leaseDuration())
	if err != nil {
		t.Fatal(err)
	}
	if err = d.Drive(ctx, recovered); err != nil {
		t.Fatal(err)
	}
	got, err := d.Store.GetRun(ctx, r.TenantID, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State.Status != domain.StatusCompleted || p.calls.Load() != 3 {
		t.Fatalf("completed=%s calls=%d; persisted model result was not reused", got.State.Status, p.calls.Load())
	}
}
