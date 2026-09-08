package review_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/application"
	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
)

func reviewCleanupRecovery(t *testing.T, ctx context.Context, d *application.Driver, engine *runner.Engine, cfg runner.Config, before persistence.Run, expected, fault string) {
	t.Helper()
	// Real files/SQLite + real published artifacts; no process is started by the
	// explicit verifier backend. The saved artifact is a filtered code snapshot.
	stateBefore, err := json.Marshal(before.State)
	if err != nil {
		t.Fatal(err)
	}
	var capacityBefore int
	if err := d.Store.Pool.QueryRow(ctx, `SELECT active_count FROM tenant_runtime WHERE tenant_id=$1`, before.TenantID).Scan(&capacityBefore); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("independent cleanup crash window")
	d.Fault = func(point string) error {
		if point == fault {
			return injected
		}
		return nil
	}
	c, err := d.CleanupWorkspace(ctx, before.TenantID, before.ID, "cleanup_owner_1", 0)
	if !errors.Is(err, injected) {
		t.Fatalf("cleanup fault was not reached: cleanup=%+v err=%v", c, err)
	}
	path := filepath.Join(cfg.RootDir, "workspaces", string(before.ID), "clamp.py")
	_, statErr := os.Stat(path)
	if fault == "cleanup_after_snapshot_publication" && statErr != nil {
		t.Fatalf("snapshot publication deleted working files: %v", statErr)
	}
	if fault == "cleanup_after_release_before_commit" && !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("release did not remove files: %v", statErr)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := runner.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	d.Runner, d.Fault = reopened, nil
	// Recovery uses a different controller identity and a new cleanup lease,
	// while the original execution lease and cleanup fencing epoch stay fixed.
	if _, err := d.Store.Pool.Exec(ctx, `UPDATE workspace_cleanup SET lease_until=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND run_id=$2`, before.TenantID, before.ID); err != nil {
		t.Fatal(err)
	}
	done, err := d.CleanupWorkspace(ctx, before.TenantID, before.ID, "cleanup_owner_2", 0)
	if err != nil || done.Phase != "released" || done.ID != c.ID || done.RunnerEpoch != c.RunnerEpoch || done.SnapshotRef == "" {
		t.Fatalf("cleanup recovery: %+v err=%v", done, err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("released workspace remains: %v", err)
	}
	meta, err := d.Store.GetArtifact(ctx, before.TenantID, domain.ID(done.SnapshotRef))
	if err != nil || meta.Kind != "workspace_snapshot" || meta.RunID != before.ID {
		t.Fatalf("saved code snapshot metadata: %+v %v", meta, err)
	}
	reader, err := d.Artifacts.Open(ctx, before.TenantID, before.ID, artifact.Ref{TenantID: meta.TenantID, RunID: meta.RunID, Kind: meta.Kind, ObjectKey: meta.ObjectKey, SHA256: meta.SHA256, Size: meta.ByteSize})
	if err != nil {
		t.Fatal(err)
	}
	var snapshot struct {
		Workspace runner.Workspace `json:"workspace"`
		Files     map[string]struct {
			Content []byte `json:"content"`
		} `json:"files"`
	}
	err = json.NewDecoder(reader).Decode(&snapshot)
	reader.Close()
	if err != nil || string(snapshot.Files["clamp.py"].Content) != expected || snapshot.Workspace.Revision != before.State.WorkspaceRevision {
		t.Fatalf("saved code snapshot lost the actual repair: %+v err=%v", snapshot.Workspace, err)
	}
	repeated, err := d.CleanupWorkspace(ctx, before.TenantID, before.ID, "cleanup_owner_3", 0)
	if err != nil || repeated.ID != done.ID || repeated.SnapshotRef != done.SnapshotRef || repeated.Phase != "released" {
		t.Fatalf("completed cleanup replay: %+v %v", repeated, err)
	}
	after, err := d.Store.GetRun(ctx, before.TenantID, before.ID)
	if err != nil {
		t.Fatal(err)
	}
	stateAfter, err := json.Marshal(after.State)
	if err != nil {
		t.Fatal(err)
	}
	if string(stateBefore) != string(stateAfter) {
		t.Fatalf("cleanup changed terminal execution state\nbefore=%s\nafter=%s", stateBefore, stateAfter)
	}
	var releasedEvents, capacityAfter int
	if err := d.Store.Pool.QueryRow(ctx, `SELECT count(*) FROM run_events WHERE tenant_id=$1 AND run_id=$2 AND type='workspace.released'`, before.TenantID, before.ID).Scan(&releasedEvents); err != nil {
		t.Fatal(err)
	}
	if err := d.Store.Pool.QueryRow(ctx, `SELECT active_count FROM tenant_runtime WHERE tenant_id=$1`, before.TenantID).Scan(&capacityAfter); err != nil {
		t.Fatal(err)
	}
	if releasedEvents != 1 || capacityAfter != capacityBefore {
		t.Fatalf("cleanup duplicated event/capacity release: events=%d capacity=%d->%d", releasedEvents, capacityBefore, capacityAfter)
	}
}

func TestReviewCleanupRequiresTerminalSettledAgedStateAndIndependentLease(t *testing.T) {
	ctx, s := isolatedStore(t)
	if err := s.BootstrapTenant(ctx, "review_tenant", "review_principal", "developer"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool.Exec(ctx, `UPDATE tenant_runtime SET max_active=32`); err != nil {
		t.Fatal(err)
	}
	if err := s.RegisterRunner(ctx, "review_runner", "review-in-process", 32); err != nil {
		t.Fatal(err)
	}
	p, err := s.CreateProject(ctx, "review_tenant", "cleanup review", "fixture", "python")
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range []domain.RunStatus{domain.StatusRunning, domain.StatusWaitingApproval, domain.StatusNeedsReconciliation, domain.StatusCancelRequested} {
		r := retentionRun(t, ctx, s, p.ID, string(status), status)
		if _, err := s.AcquireWorkspaceCleanup(ctx, r.TenantID, r.ID, "owner", 0); !errors.Is(err, domain.ErrTransition) {
			t.Fatalf("cleanup admitted %s: %v", status, err)
		}
	}
	r := retentionRun(t, ctx, s, p.ID, "cleanup_terminal", domain.StatusCompleted)
	if _, err := s.AcquireWorkspaceCleanup(ctx, r.TenantID, r.ID, "owner", time.Hour); !errors.Is(err, domain.ErrReconciliation) {
		t.Fatalf("fresh terminal cleanup admitted: %v", err)
	}
	if _, err := s.Pool.Exec(ctx, `UPDATE runner_allocations SET state='uncertain' WHERE tenant_id=$1 AND run_id=$2`, r.TenantID, r.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AcquireWorkspaceCleanup(ctx, r.TenantID, r.ID, "owner", 0); !errors.Is(err, domain.ErrReconciliation) {
		t.Fatalf("unreleased allocation cleanup admitted: %v", err)
	}
	if _, err := s.Pool.Exec(ctx, `UPDATE runner_allocations SET state='released' WHERE tenant_id=$1 AND run_id=$2`, r.TenantID, r.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool.Exec(ctx, `INSERT INTO effects(tenant_id,run_id,step_seq,ordinal,operation_id,kind,args,canonical_args,args_hash,expected_revision,epoch,policy_version,status) VALUES($1,$2,0,-1,'review_cleanup_effect','verify','{}',$3,$4,1,1,'review','unknown')`, r.TenantID, r.ID, []byte("{}"), strings.Repeat("0", 64)); err != nil {
		t.Fatal(err)
	}
	for _, status := range []string{"unknown", "in_flight", "failed"} {
		if _, err := s.Pool.Exec(ctx, `UPDATE effects SET status=$3 WHERE tenant_id=$1 AND run_id=$2`, r.TenantID, r.ID, status); err != nil {
			t.Fatal(err)
		}
		if _, err := s.AcquireWorkspaceCleanup(ctx, r.TenantID, r.ID, "owner", 0); !errors.Is(err, domain.ErrReconciliation) {
			t.Fatalf("effect=%s without final receipt cleanup admitted: %v", status, err)
		}
	}
	if _, err := s.Pool.Exec(ctx, `DELETE FROM effects WHERE tenant_id=$1 AND run_id=$2`, r.TenantID, r.ID); err != nil {
		t.Fatal(err)
	}
	first, err := s.AcquireWorkspaceCleanup(ctx, r.TenantID, r.ID, "owner_1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AcquireWorkspaceCleanup(ctx, r.TenantID, r.ID, "owner_2", 0); !errors.Is(err, domain.ErrCapacity) {
		t.Fatalf("concurrent cleanup owner admitted: %v", err)
	}
	if _, _, err := s.CleanupProof(ctx, first, true); !errors.Is(err, domain.ErrFenced) {
		t.Fatalf("release permitted before a READY snapshot: %v", err)
	}
	if err := s.SealWorkspaceCleanup(ctx, first, "absent_snapshot"); err == nil {
		t.Fatal("cleanup sealed absent artifact")
	}
	if _, err := s.Pool.Exec(ctx, `UPDATE workspace_cleanup SET lease_until=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND run_id=$2`, r.TenantID, r.ID); err != nil {
		t.Fatal(err)
	}
	second, err := s.AcquireWorkspaceCleanup(ctx, r.TenantID, r.ID, "owner_2", 0)
	if err != nil || second.ID != first.ID || second.RunnerEpoch != first.RunnerEpoch || second.RunnerEpoch != r.State.Lease.Epoch+1 {
		t.Fatalf("cleanup lease takeover changed immutable fencing identity: %+v %v", second, err)
	}
	if _, _, err := s.CleanupProof(ctx, first, false); !errors.Is(err, domain.ErrFenced) {
		t.Fatalf("old cleanup owner still authorized: %v", err)
	}
	after, err := s.GetRun(ctx, r.TenantID, r.ID)
	if err != nil || after.State.Version != r.State.Version || after.State.Lease != r.State.Lease || after.State.Status != r.State.Status {
		t.Fatalf("cleanup lease changed execution state: %+v %v", after.State, err)
	}
}
