package runner

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
)

func interruptedImport(t *testing.T, point string) (Config, PrepareRequest, InitializationState) {
	t.Helper()
	old, c, r := fixture(t, nil, nil)
	if _, err := old.ReleaseWorkspace(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	old.Close()
	c.VolumeSlots = []sandbox.VolumeSpec{{ID: "slot", MountPath: t.TempDir()}}
	c.TestVolumeVerifier = func(context.Context, sandbox.VolumeSpec) error { return nil }
	c.Fault = func(p string) error {
		if p == point {
			return errors.New("simulated interruption")
		}
		return nil
	}
	e, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	r.WorkspaceID = "partial"
	r.Epoch = 2
	r = authorized(t, c.Signer, r)
	prep := PrepareRequest{WorkspaceRequest: r, SourceID: "fixture", ProfileID: "python"}
	if _, err = e.PrepareWorkspace(context.Background(), prep); err == nil {
		t.Fatal("fault not reached")
	}
	state, err := e.InspectInitialization(context.Background(), r.WorkspaceID)
	if err != nil {
		t.Fatal(err)
	}
	if state.WorkspaceExists || state.Operations != 0 || state.SourceHash == "" || state.Epoch != 2 || state.Released {
		t.Fatalf("unbound initialization: %+v", state)
	}
	e.Close()
	c.Fault = nil
	return c, prep, state
}
func TestInterruptedInitializationResumesOnlyPinnedSource(t *testing.T) {
	for _, point := range []string{"after_volume_lease", "after_import_file", "after_import_before_workspace"} {
		t.Run(point, func(t *testing.T) {
			c, r, state := interruptedImport(t, point)
			e, err := Open(c)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			old := r
			old.Epoch = 1
			old.WorkspaceRequest = authorized(t, c.Signer, old.WorkspaceRequest)
			if _, err = e.PrepareWorkspace(context.Background(), old); !errors.Is(err, domain.ErrFenced) {
				t.Fatalf("old initialization owner: %v", err)
			}
			r.Epoch = 3
			r.WorkspaceRequest = authorized(t, c.Signer, r.WorkspaceRequest)
			w, err := e.PrepareWorkspace(context.Background(), r)
			if err != nil {
				t.Fatal(err)
			}
			if w.BaselineHash != state.SourceHash || w.Epoch != 3 || w.Revision != 1 {
				t.Fatalf("resumed wrong bytes: %+v", w)
			}
			observed, err := e.readTree(context.Background(), e.workspacePath(r.WorkspaceID))
			if err != nil || treeHash(observed) != state.SourceHash {
				t.Fatalf("checkout mismatch %v", err)
			}
		})
	}
}
func TestInterruptedInitializationNeverOverwritesOrRebinds(t *testing.T) {
	for _, damage := range []string{"source_changed", "partial_changed", "unexpected_cache", "legacy_unbound"} {
		t.Run(damage, func(t *testing.T) {
			c, r, _ := interruptedImport(t, "after_import_file")
			e, err := Open(c)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			base := filepath.Join(c.VolumeSlots[0].MountPath, "workspace-"+string(r.WorkspaceID), "baseline")
			var changed string
			switch damage {
			case "source_changed":
				changed = filepath.Join(c.Sources["fixture"], "main.py")
			case "partial_changed":
				changed = filepath.Join(base, "main.py")
			case "unexpected_cache":
				changed = filepath.Join(base, ".cache")
			case "legacy_unbound":
				_, err = e.journal.db.Exec(`UPDATE volume_leases SET source_hash='' WHERE workspace_id=?`, r.WorkspaceID)
				if err != nil {
					t.Fatal(err)
				}
			}
			if changed != "" {
				if err = os.WriteFile(changed, []byte("do not overwrite"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = e.PrepareWorkspace(context.Background(), r); !errors.Is(err, domain.ErrConflict) && !errors.Is(err, domain.ErrReconciliation) {
				t.Fatalf("accepted damaged import: %v", err)
			}
			if changed != "" {
				raw, err := os.ReadFile(changed)
				if err != nil || string(raw) != "do not overwrite" {
					t.Fatal("recovery changed preexisting bytes")
				}
			}
			state, err := e.InspectInitialization(context.Background(), r.WorkspaceID)
			if err != nil || state.Released || state.WorkspaceExists || state.Operations != 0 {
				t.Fatalf("damaged lease lost: %+v %v", state, err)
			}
		})
	}
}
func TestUndispatchedOperationCancellationHasDurableProof(t *testing.T) {
	e, c, r := fixture(t, nil, nil)
	e.config.Fault = func(p string) error {
		if p == "after_prepared" {
			return errors.New("stop before goroutine")
		}
		return nil
	}
	if _, err := e.StartOperation(context.Background(), request(r, "undispatched", "run_command", CommandArgs{Command: []string{"unused"}}, 1)); err == nil {
		t.Fatal("missing fault")
	}
	e.Close()
	c.Fault = nil
	var err error
	e, err = Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	op, err := e.CancelOperation(context.Background(), InspectRequest{WorkspaceRequest: r, OperationID: "undispatched"})
	if err != nil || op.Status != Cancelled || op.AfterRevision != 1 || op.Receipt.ObjectKey == "" {
		t.Fatalf("never started recovery: %+v %v", op, err)
	}
}
func TestAllReturnedArtifactKindsAreDurablyPinned(t *testing.T) {
	e, c, r := fixture(t, nil, nil)
	if _, err := e.StartOperation(context.Background(), request(r, "pinned", "list_files", struct{}{}, 1)); err != nil {
		t.Fatal(err)
	}
	op := waitOperation(t, e, r, "pinned", true)
	stop, err := e.StopWorkspace(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := e.SealSnapshot(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	e.Close()
	e, err = Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for _, key := range []string{op.Receipt.ObjectKey, stop.Ref.ObjectKey, snap.Artifact.ObjectKey} {
		var count int
		if err = e.journal.db.QueryRow(`SELECT count(*) FROM runner_artifacts WHERE object_key=?`, key).Scan(&count); err != nil || count != 1 {
			t.Fatalf("lost pin %s: %d %v", key, count, err)
		}
	}
}

func TestArtifactPinIdentityAllowsKindButRejectsCorruption(t *testing.T) {
	e, _, r := fixture(t, nil, nil)
	if _, err := e.StartOperation(context.Background(), request(r, "pin-identity", "list_files", struct{}{}, 1)); err != nil {
		t.Fatal(err)
	}
	op := waitOperation(t, e, r, "pin-identity", true)
	ref := op.Receipt
	ref.Kind = "same-content-other-use"
	if err := e.journal.pinArtifact(context.Background(), ref); err != nil {
		t.Fatalf("same content kind variation rejected: %v", err)
	}
	ref.Size++
	if err := e.journal.pinArtifact(context.Background(), ref); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("corrupt size accepted: %v", err)
	}
	ref = op.Receipt
	ref.SHA256 = "corrupt"
	if err := e.journal.pinArtifact(context.Background(), ref); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("corrupt hash accepted: %v", err)
	}
}

type intentProbe struct {
	*sandbox.TestBackend
	removed bool
}

func (p *intentProbe) CancelNeverDispatched(context.Context, string) (sandbox.Job, error) {
	p.removed = true
	return sandbox.Job{NeverStarted: true}, nil
}
func TestStartIntentPreventsNeverStartedAssumption(t *testing.T) {
	e, _, r := fixture(t, nil, nil)
	e.config.Fault = func(p string) error {
		if p == "after_prepared" {
			return errors.New("pause")
		}
		return nil
	}
	req := request(r, "intent-gap", "run_command", CommandArgs{Command: []string{"unused"}}, 1)
	if _, err := e.StartOperation(context.Background(), req); err == nil {
		t.Fatal("missing prepared fault")
	}
	if _, err := e.journal.db.Exec(`UPDATE operations SET dispatch_started=1,docker_start_intent=1 WHERE id=?`, req.OperationID); err != nil {
		t.Fatal(err)
	}
	probe := &intentProbe{TestBackend: &sandbox.TestBackend{InspectFunc: func(context.Context, string) (sandbox.Job, error) { return sandbox.Job{ID: "fixture"}, nil }, CancelFunc: func(context.Context, string) (sandbox.Job, error) { return sandbox.Job{}, domain.ErrReconciliation }}}
	// Keep the existing test volume policy; only replace adapter on the open test
	// engine to avoid claiming this protocol stub is an execution backend.
	e.config.Backend = probe
	if _, err := e.CancelOperation(context.Background(), InspectRequest{WorkspaceRequest: r, OperationID: req.OperationID}); !errors.Is(err, domain.ErrReconciliation) {
		t.Fatalf("ambiguous intent accepted: %v", err)
	}
	if probe.removed {
		t.Fatal("intent=1 used never-dispatched removal")
	}
	op, err := e.journal.operation(context.Background(), req.OperationID)
	if err != nil || op.Status.Terminal() {
		t.Fatalf("ambiguous operation settled: %+v %v", op, err)
	}
}

func TestCreatedUndispatchedCancellationReachesLockedProof(t *testing.T) {
	for _, action := range []string{"cancel", "stop", "adopt"} {
		t.Run(action, func(t *testing.T) {
			e, _, r := fixture(t, nil, nil)
			e.config.Fault = func(p string) error {
				if p == "after_prepared" {
					return errors.New("pause")
				}
				return nil
			}
			req := request(r, "created-gap", "run_command", CommandArgs{Command: []string{"unused"}}, 1)
			if _, err := e.StartOperation(context.Background(), req); err == nil {
				t.Fatal("missing fault")
			}
			if _, err := e.journal.db.Exec(`UPDATE operations SET dispatch_started=1,docker_start_intent=0 WHERE id=?`, req.OperationID); err != nil {
				t.Fatal(err)
			}
			calls := 0
			probe := &intentProbe{TestBackend: &sandbox.TestBackend{InspectFunc: func(context.Context, string) (sandbox.Job, error) { return sandbox.Job{ID: "fixture"}, nil }, CancelFunc: func(context.Context, string) (sandbox.Job, error) {
				calls++
				return sandbox.Job{}, domain.ErrReconciliation
			}}}
			e.config.Backend = probe
			switch action {
			case "cancel":
				if _, err := e.CancelOperation(context.Background(), InspectRequest{WorkspaceRequest: r, OperationID: req.OperationID}); err != nil {
					t.Fatal(err)
				}
			case "stop":
				receipt, err := e.StopWorkspace(context.Background(), r)
				if err != nil || !receipt.NoActiveOperations {
					t.Fatalf("stop failed %+v %v", receipt, err)
				}
			case "adopt":
				receipt, err := e.AdoptWorkspace(context.Background(), r)
				if err != nil || !receipt.NoActiveOperations {
					t.Fatalf("adopt failed %+v %v", receipt, err)
				}
			}
			if calls != 0 || !probe.removed {
				t.Fatalf("bypassed locked no-dispatch proof: ordinary cancels=%d removed=%v", calls, probe.removed)
			}
			op, err := e.journal.operation(context.Background(), req.OperationID)
			if err != nil || op.Status != Cancelled || op.Receipt.ObjectKey == "" {
				t.Fatalf("cancellation lacks receipt %+v %v", op, err)
			}
		})
	}
}
