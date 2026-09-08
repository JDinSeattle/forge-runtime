package review_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
)

func runnerFixture(t *testing.T, backend sandbox.Backend, now func() time.Time) (runner.Config, runner.WorkspaceRequest) {
	t.Helper()
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "fixture.txt"), []byte("review\n"), 0600); err != nil {
		t.Fatal(err)
	}
	objects, err := artifact.NewLocalStore(t.TempDir(), 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = objects.Close() })
	signer, err := runner.NewSigner([]byte("independent-review-only-key-32bytes"))
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	cfg := runner.Config{
		RootDir: root, JournalPath: filepath.Join(root, "journal.db"), Artifacts: objects,
		Backend: backend, Signer: signer, Now: now,
		Sources:  map[string]string{"fixture": source},
		Profiles: map[string]sandbox.Profile{"fixture": {ID: "fixture", VerifyCommand: []string{"test-fixture"}}},
	}
	issued := now()
	claims := runner.Claims{TenantID: "review_tenant", RunID: "review_run", WorkspaceID: "review_workspace", Epoch: 1,
		IssuedAt: issued, ExpiresAt: issued.Add(30 * time.Second), Permissions: []string{"prepare", "execute", "inspect", "cancel", "adopt", "release"}}
	grant, err := signer.Sign(claims, issued.Add(40*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	return cfg, runner.WorkspaceRequest{TenantID: claims.TenantID, RunID: claims.RunID, WorkspaceID: claims.WorkspaceID, Epoch: 1, Grant: grant}
}

func operation(w runner.WorkspaceRequest) runner.OperationRequest {
	args := json.RawMessage(`{"command":["test-fixture"]}`)
	digest := sha256.Sum256(args)
	return runner.OperationRequest{WorkspaceRequest: w, OperationID: "review_operation", ExpectedRevision: 1,
		Kind: "run_command", Args: args, ArgsHash: hex.EncodeToString(digest[:]), PolicyVersion: "review_policy", Deadline: time.Now().Add(time.Minute)}
}

func TestReviewStoppedWorkspaceRejectsDelayedSameEpochAdoption(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cfg, w := runnerFixture(t, &sandbox.TestBackend{}, time.Now)
	e, err := runner.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if _, err := e.PrepareWorkspace(ctx, runner.PrepareRequest{WorkspaceRequest: w, SourceID: "fixture", ProfileID: "fixture"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.StopWorkspace(ctx, w); err != nil {
		t.Fatal(err)
	}
	if _, err := e.StartOperation(ctx, operation(w)); !errors.Is(err, domain.ErrFenced) {
		t.Fatalf("stopped workspace admitted an old execution grant: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = runner.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	// The adoption request was authorized under this same lease before stop and
	// arrives late after journal reopen. It must not undo the durable stop fence.
	if _, err := e.AdoptWorkspace(ctx, w); !errors.Is(err, domain.ErrFenced) {
		t.Fatalf("delayed same-epoch adoption reopened cancelled workspace: %v", err)
	}
	if _, err := e.StartOperation(ctx, operation(w)); !errors.Is(err, domain.ErrFenced) {
		t.Fatalf("journal reopen forgot execution stop fence: %v", err)
	}
}

func TestReviewReleaseRechecksGrantAfterWaitingForWriter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	initial := time.Now()
	var timestamp atomic.Int64
	timestamp.Store(initial.UnixNano())
	var observe atomic.Bool
	authorized := make(chan struct{})
	var once sync.Once
	now := func() time.Time {
		at := time.Unix(0, timestamp.Load())
		if observe.Load() {
			once.Do(func() { close(authorized) })
		}
		return at
	}
	finishJob := make(chan struct{})
	backend := &sandbox.TestBackend{
		StartFunc: func(_ context.Context, s sandbox.JobSpec) (sandbox.Job, error) {
			return sandbox.Job{ID: s.ID, Started: true, Running: true}, nil
		},
		InspectFunc: func(ctx context.Context, id string) (sandbox.Job, error) {
			select {
			case <-finishJob:
				return sandbox.Job{ID: id, Started: true, ExitCode: 0}, nil
			case <-ctx.Done():
				return sandbox.Job{}, ctx.Err()
			}
		},
	}
	cfg, w := runnerFixture(t, backend, now)
	e, err := runner.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if _, err = e.PrepareWorkspace(ctx, runner.PrepareRequest{WorkspaceRequest: w, SourceID: "fixture", ProfileID: "fixture"}); err != nil {
		t.Fatal(err)
	}
	if _, err = e.StartOperation(ctx, operation(w)); err != nil {
		t.Fatal(err)
	}
	observe.Store(true)
	done := make(chan error, 1)
	go func() { _, err := e.ReleaseWorkspace(ctx, w); done <- err }()
	select {
	case <-authorized:
	case <-ctx.Done():
		t.Fatal("release did not attempt authorization")
	}
	timestamp.Store(initial.Add(time.Minute).UnixNano())
	close(finishJob)
	select {
	case err := <-done:
		if !errors.Is(err, domain.ErrFenced) {
			t.Fatalf("release accepted an expired grant after waiting for the writer: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("release did not finish")
	}
	if _, err := os.Stat(filepath.Join(cfg.RootDir, "workspaces", string(w.WorkspaceID), "fixture.txt")); err != nil {
		t.Fatalf("expired release removed the workspace: %v", err)
	}
}

func TestReviewCancelPreservesAlreadyCompletedJobOutcome(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	completed := func(_ context.Context, id string) (sandbox.Job, error) {
		return sandbox.Job{ID: id, Started: true, ExitCode: 0}, nil
	}
	backend := &sandbox.TestBackend{
		StartFunc:   func(ctx context.Context, s sandbox.JobSpec) (sandbox.Job, error) { return completed(ctx, s.ID) },
		InspectFunc: completed, CancelFunc: completed,
	}
	cfg, w := runnerFixture(t, backend, time.Now)
	reached := make(chan struct{})
	cfg.Fault = func(point string) error {
		if point == "before_receipt" {
			close(reached)
			return errors.New("independent review: receipt publication interrupted")
		}
		return nil
	}
	e, err := runner.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.PrepareWorkspace(ctx, runner.PrepareRequest{WorkspaceRequest: w, SourceID: "fixture", ProfileID: "fixture"}); err != nil {
		_ = e.Close()
		t.Fatal(err)
	}
	o := operation(w)
	if _, err := e.StartOperation(ctx, o); err != nil {
		_ = e.Close()
		t.Fatal(err)
	}
	select {
	case <-reached:
	case <-ctx.Done():
		_ = e.Close()
		t.Fatal("operation did not reach interrupted receipt boundary")
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Fault = nil
	e, err = runner.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	result, err := e.CancelOperation(ctx, runner.InspectRequest{WorkspaceRequest: w, OperationID: o.OperationID})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != runner.Succeeded {
		t.Fatalf("already completed successful job was relabeled %s by a later cancel request", result.Status)
	}
}

type rejectingQuota struct{}

func (rejectingQuota) VerifyWorkspaceQuota(context.Context, string, int64) error {
	return errors.New("independent review: no enforced quota")
}

func TestReviewDockerRejectsMissingOrUnverifiedWorkspaceQuota(t *testing.T) {
	// A local protocol stub makes no real Docker calls. It advertises otherwise
	// sufficient daemon features so the test specifically reaches the disk gate.
	dir := t.TempDir()
	binary := filepath.Join(dir, "docker-stub")
	marker := filepath.Join(dir, "unexpected-command")
	t.Setenv("FORGE_REVIEW_DOCKER_MARKER", marker)
	script := `#!/bin/sh
if [ "$1" = "info" ]; then
  printf '%s\n' '{"SecurityOptions":["name=rootless"],"CgroupVersion":"2","MemoryLimit":true,"PidsLimit":true,"CpuCfsQuota":true,"CpuCfsPeriod":true}'
  exit 0
fi
printf '%s\n' "$1" > "$FORGE_REVIEW_DOCKER_MARKER"
exit 42
`
	if err := os.WriteFile(binary, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	for name, verifier := range map[string]sandbox.WorkspaceQuotaVerifier{"missing": nil, "rejected": rejectingQuota{}} {
		t.Run(name, func(t *testing.T) {
			d := sandbox.Docker{Binary: binary, WorkspaceQuota: verifier}
			_, err := d.Start(context.Background(), sandbox.JobSpec{ID: "review_job", Workspace: dir,
				Profile: sandbox.Profile{WorkspaceQuotaBytes: 1 << 20}})
			if !errors.Is(err, sandbox.ErrUnavailable) {
				t.Fatalf("missing disk enforcement did not fail closed: %v", err)
			}
			if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("Docker command admission preceded quota verification: %v", err)
			}
		})
	}
}
