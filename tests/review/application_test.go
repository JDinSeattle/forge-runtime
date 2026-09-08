package review_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/application"
	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/provider"
	"github.com/JDinSeattle/forge-runtime/internal/quota"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	flow "github.com/JDinSeattle/forge-runtime/internal/runtime"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
)

type reviewDispatchProbe struct{ calls atomic.Int64 }

type reviewUnavailableRunner struct{ runner.Service }

func (reviewUnavailableRunner) StopWorkspace(context.Context, runner.WorkspaceRequest) (runner.StopReceipt, error) {
	return runner.StopReceipt{}, sandbox.ErrUnavailable
}

func TestReviewFixedRunnerClaimAndTakeoverRemainOnAssignedTransport(t *testing.T) {
	ctx, s := isolatedStore(t)
	runA := reviewRun(t, ctx, s)
	if err := s.RegisterRunner(ctx, "review_runner_b", "review-in-process-b", 2); err != nil {
		t.Fatal(err)
	}
	runB, _, err := s.Submit(ctx, persistence.SubmitRequest{TenantID: runA.TenantID, PrincipalID: runA.PrincipalID, ProjectID: runA.ProjectID, Task: "runner B task", BaseCommit: runA.BaseCommit, Config: runA.Config}, "review-runner-b")
	if err != nil {
		t.Fatal(err)
	}
	a, err := s.ClaimOnRunner(ctx, "worker_a", time.Minute, "review_runner")
	if err != nil || a.ID != runA.ID || a.RunnerID != "review_runner" {
		t.Fatalf("runner A placement: %+v, %v", a, err)
	}
	b, err := s.ClaimOnRunner(ctx, "worker_b", time.Minute, "review_runner_b")
	if err != nil || b.ID != runB.ID || b.RunnerID != "review_runner_b" {
		t.Fatalf("runner B placement: %+v, %v", b, err)
	}
	// The guard must execute before any heartbeat, storage or RPC call: this
	// deliberately incomplete driver would panic if it began ordinary execution.
	d := application.Driver{RunnerID: "review_runner_b"}
	if err := d.Drive(ctx, a); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("wrong fixed transport accepted claim: %v", err)
	}
	const ref domain.ID = "review_affinity_receipt"
	if err := s.PublishArtifact(ctx, persistence.Artifact{TenantID: a.TenantID, RunID: a.ID, ID: ref, Kind: "review_evidence", ObjectKey: string(a.TenantID) + "/" + string(a.ID) + "/receipt", SHA256: strings.Repeat("0", 64), ByteSize: 0}); err != nil {
		t.Fatal(err)
	}
	a, err = s.Advance(ctx, a.TenantID, a.ID, flow.Event{Kind: flow.EventWorkspaceReady, ExpectedVersion: a.State.Version, Owner: a.State.Lease.Owner, Epoch: a.State.Lease.Epoch, WorkspaceRevision: 1, OutputRef: string(ref)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool.Exec(ctx, `UPDATE runs SET lease_until=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND id=$2`, a.TenantID, a.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimOnRunner(ctx, "wrong_takeover", time.Minute, "review_runner_b"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("runner B adopted runner A's existing allocation: %v", err)
	}
	recovered, err := s.ClaimOnRunner(ctx, "right_takeover", time.Minute, "review_runner")
	if err != nil || recovered.ID != a.ID || recovered.State.Lease.Epoch != a.State.Lease.Epoch+1 || recovered.State.Stage != domain.StageAdoptWorkspace {
		t.Fatalf("same-runner takeover: %+v, %v", recovered, err)
	}
	recovered, err = s.Advance(ctx, recovered.TenantID, recovered.ID, flow.Event{Kind: flow.EventWorkspaceAdopted, ExpectedVersion: recovered.State.Version, Owner: recovered.State.Lease.Owner, Epoch: recovered.State.Lease.Epoch, Stop: &flow.StopReceipt{Ref: string(ref), NoActiveOperations: true, WorkspaceRevision: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Defer(ctx, recovered, time.Now().Add(-time.Second), "review.deferred"); err != nil {
		t.Fatal(err)
	}
	// Process-free deferral releases execution slots but retains workspace
	// affinity; it must not make the run eligible through a different transport.
	if _, err := s.ClaimOnRunner(ctx, "wrong_released_takeover", time.Minute, "review_runner_b"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("released allocation lost workspace affinity: %v", err)
	}
	final, err := s.ClaimOnRunner(ctx, "right_released_takeover", time.Minute, "review_runner")
	if err != nil || final.ID != a.ID || final.RunnerID != "review_runner" || final.State.Lease.Epoch != recovered.State.Lease.Epoch+1 {
		t.Fatalf("same-runner released takeover: %+v, %v", final, err)
	}
	for _, id := range []string{"review_runner", "review_runner_b"} {
		var slots int
		if err := s.Pool.QueryRow(ctx, `SELECT reserved_slots FROM runners WHERE id=$1`, id).Scan(&slots); err != nil || slots != 1 {
			t.Fatalf("runner %s slots=%d err=%v", id, slots, err)
		}
	}
}

const reviewVerifierDiagnostic = "expected clamp(-3, 0, 4) == 0; observed 4"

type reviewFeedbackModel struct {
	native   bool
	patch    string
	calls    atomic.Int64
	observed atomic.Bool
}

func (p *reviewFeedbackModel) Capabilities(ctx context.Context, model string) (provider.Capabilities, error) {
	return provider.NewFake().Capabilities(ctx, model)
}
func (p *reviewFeedbackModel) Stream(ctx context.Context, req provider.ModelRequest, emit func(provider.ModelEvent) error) (provider.ModelTurn, error) {
	p.calls.Add(1)
	name, args := "finish", "{}"
	if req.StepID == "2" {
		seen, paired := false, false
		for _, m := range req.Messages {
			if strings.Contains(m.Text, reviewVerifierDiagnostic) && strings.Contains(m.Text, "target_passed=false") {
				seen = true
				if m.Role == "tool" && m.ToolCallID == "review_finish_1" && m.IsError {
					paired = true
				}
			}
		}
		if !seen || !paired || (p.native && req.NativeState == nil) {
			return provider.ModelTurn{}, fmt.Errorf("durable verifier feedback missing: seen=%t paired=%t native=%t", seen, paired, req.NativeState != nil)
		}
		p.observed.Store(true)
		name, args = "apply_patch", p.patch
	} else if req.StepID != "1" && req.StepID != "3" {
		return provider.ModelTurn{}, fmt.Errorf("unexpected review step %s", req.StepID)
	}
	id := "review_" + name + "_" + req.StepID
	script := provider.Script{Chunks: []provider.Chunk{{Kind: "tool_start", CallID: id, Name: name}, {Kind: "tool_delta", CallID: id, Delta: args}, {Kind: "tool_end", CallID: id}}, FinishReason: "tool_calls", Usage: provider.Usage{Input: provider.TokenCount{Known: true, Value: 50}, Output: provider.TokenCount{Known: true, Value: 20}}}
	turn, err := provider.NewFake(script).Stream(ctx, req, emit)
	if err == nil && p.native {
		turn.NativeState = &provider.NativeState{Provider: "fake", Raw: json.RawMessage(`{"review_native_history":"opaque-to-driver"}`)}
	}
	return turn, err
}

func TestReviewFailedVerifierDiagnosticsEnableNextRepair(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(fmt.Sprintf("native_%t", native), func(t *testing.T) {
			ctx, s := isolatedStore(t)
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
			objects, err := artifact.NewLocalStore(filepath.Join(dir, "objects"), 8<<20)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = objects.Close() })
			signer, err := runner.NewSigner([]byte(strings.Repeat("f", 32)))
			if err != nil {
				t.Fatal(err)
			}
			var failedChecks atomic.Int64
			backend := &sandbox.TestBackend{StartFunc: func(ctx context.Context, spec sandbox.JobSpec) (sandbox.Job, error) {
				body, err := os.ReadFile(filepath.Join(spec.Workspace, "clamp.py"))
				if err != nil {
					return sandbox.Job{}, err
				}
				exit, output := 0, "protocol fixture: expected contents observed; no Python process executed"
				if spec.Command[0] == "target" && string(body) != after {
					failedChecks.Add(1)
					exit, output = 1, reviewVerifierDiagnostic
				}
				return sandbox.Job{ID: spec.ID, Started: true, ExitCode: exit, Output: []byte(output)}, nil
			}}
			engineConfig := runner.Config{RootDir: filepath.Join(dir, "runner"), JournalPath: filepath.Join(dir, "journal.db"), Artifacts: objects, Backend: backend, Signer: signer, Sources: map[string]string{"fixture": source}, Profiles: map[string]sandbox.Profile{"python": {ID: "python", TargetCommand: []string{"target"}, VerifyCommand: []string{"regression"}}}}
			engine, err := runner.Open(engineConfig)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = engine.Close() })
			if err := s.BootstrapTenant(ctx, "review_tenant", "review_principal", "developer"); err != nil {
				t.Fatal(err)
			}
			project, err := s.CreateProject(ctx, "review_tenant", "verifier feedback fixture", "fixture", "python")
			if err != nil {
				t.Fatal(err)
			}
			if err := s.RegisterRunner(ctx, "review_runner", "review-in-process", 1); err != nil {
				t.Fatal(err)
			}
			q := quota.New(s.Pool)
			if err := q.Configure(ctx, quota.Config{CredentialGroup: "review_group", MaxConcurrent: 1, MaxTokens: 1_000_000, MaxCost: 1_000_000, WindowDuration: time.Hour, FailureThreshold: 3, BreakerCooldown: time.Second}); err != nil {
				t.Fatal(err)
			}
			hash := sha256.Sum256([]byte(before))
			patch, err := json.Marshal(runner.PatchArgs{Files: []runner.FileEdit{{Path: "clamp.py", ExpectedSHA256: hex.EncodeToString(hash[:]), Content: &after}}})
			if err != nil {
				t.Fatal(err)
			}
			p := &reviewFeedbackModel{native: native, patch: string(patch)}
			d := application.Driver{Store: s, Quota: q, Artifacts: objects, Signer: signer, Runner: engine, LeaseDuration: 10 * time.Second, TrustedVerification: true,
				Sources: map[string]application.SourceSpec{"fixture": {Hash: sourceHash, HasTarget: true}}, Providers: map[string]provider.Provider{"fake": p}, Models: map[string]application.ModelSpec{"fake/fake": {CredentialGroup: "review_group", PriceVersion: "review-v1", ContextTokens: 65536, MaxOutputTokens: 1024, RequestTimeout: 5 * time.Second}}}
			r, _, err := s.Submit(ctx, persistence.SubmitRequest{TenantID: "review_tenant", PrincipalID: "review_principal", ProjectID: project.ID, Task: "Repair the target failure only after observing its trusted diagnostics.", BaseCommit: sourceHash, Config: persistence.Config{Provider: "fake", Model: "fake", MaxModelRounds: 5, MaxToolCalls: 5, MaxRuntimeSeconds: 60}}, "feedback")
			if err != nil {
				t.Fatal(err)
			}
			claimed, err := s.Claim(ctx, "review_worker", 10*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if err := d.Drive(ctx, claimed); err != nil {
				t.Fatal(err)
			}
			got, err := s.GetRun(ctx, r.TenantID, r.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.State.Status != domain.StatusCompleted || got.State.Verification != domain.VerificationVerified || !p.observed.Load() || p.calls.Load() != 3 || failedChecks.Load() != 2 {
				t.Fatalf("feedback loop did not complete: status=%s verification=%s observed=%t model_calls=%d failed_checks=%d", got.State.Status, got.State.Verification, p.observed.Load(), p.calls.Load(), failedChecks.Load())
			}
			d.RunnerID = "review_runner"
			fault := "cleanup_after_snapshot_publication"
			if native {
				fault = "cleanup_after_release_before_commit"
			}
			t.Run(fault, func(t *testing.T) { reviewCleanupRecovery(t, ctx, &d, engine, engineConfig, got, after, fault) })
		})
	}
}

func (p *reviewDispatchProbe) Capabilities(context.Context, string) (provider.Capabilities, error) {
	return provider.Capabilities{ToolCalling: true, ContextWindow: 2, MaxOutputTokens: 1}, nil
}
func (p *reviewDispatchProbe) Stream(context.Context, provider.ModelRequest, func(provider.ModelEvent) error) (provider.ModelTurn, error) {
	p.calls.Add(1)
	return provider.ModelTurn{}, context.Canceled
}

func TestReviewModelInputMustFitFrozenReservationBeforeDispatch(t *testing.T) {
	ctx, s := isolatedStore(t)
	reviewRun(t, ctx, s)
	r, err := s.Claim(ctx, "review_worker", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	objects, err := artifact.NewLocalStore(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = objects.Close() })
	// This is a valid persisted portable context, clearly larger than the one
	// input token permitted by the frozen quote. No paid provider is contacted.
	body, err := json.Marshal(map[string]any{"messages": []provider.Message{{Role: "user", Text: strings.Repeat("distinct input words ", 100)}}})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := objects.Put(ctx, r.TenantID, r.ID, "context", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	const artifactID domain.ID = "review_context"
	if err := s.PublishArtifact(ctx, persistence.Artifact{TenantID: r.TenantID, RunID: r.ID, ID: artifactID, Kind: ref.Kind, ObjectKey: ref.ObjectKey, SHA256: ref.SHA256, ByteSize: ref.Size}); err != nil {
		t.Fatal(err)
	}
	for _, event := range []flow.Event{{Kind: flow.EventWorkspaceReady, WorkspaceRevision: 1, OutputRef: string(artifactID)}, {Kind: flow.EventContextBuilt, OutputRef: string(artifactID)}} {
		event.ExpectedVersion, event.Owner, event.Epoch = r.State.Version, r.State.Lease.Owner, r.State.Lease.Epoch
		r, err = s.Advance(ctx, r.TenantID, r.ID, event)
		if err != nil {
			t.Fatal(err)
		}
	}
	q := quota.New(s.Pool)
	if err := q.Configure(ctx, quota.Config{CredentialGroup: "review_group", MaxConcurrent: 1, MaxTokens: 2, MaxCost: 2, WindowDuration: time.Hour, FailureThreshold: 3, BreakerCooldown: time.Second}); err != nil {
		t.Fatal(err)
	}
	p := &reviewDispatchProbe{}
	signer, err := runner.NewSigner([]byte(strings.Repeat("r", 32)))
	if err != nil {
		t.Fatal(err)
	}
	d := application.Driver{Store: s, Quota: q, Artifacts: objects, LeaseDuration: 10 * time.Second,
		Signer: signer, Runner: reviewUnavailableRunner{},
		Providers: map[string]provider.Provider{"fake": p}, Models: map[string]application.ModelSpec{"fake/fixture": {
			CredentialGroup: "review_group", PriceVersion: "review-v1", InputPrice: 1_000_000, OutputPrice: 1_000_000,
			ContextTokens: 1, MaxOutputTokens: 1, RequestTimeout: 5 * time.Second,
		}}}
	_ = d.Drive(ctx, r)
	if n := p.calls.Load(); n != 0 {
		t.Fatalf("sent a 2,100-byte input with a frozen one-token reservation: provider dispatches=%d", n)
	}
}
