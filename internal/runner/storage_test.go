package runner

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
)

func TestStoppedWorkspaceCannotReopenAfterRestart(t *testing.T) {
	e, c, r := fixture(t, nil, nil)
	receipt, err := e.StopWorkspace(context.Background(), r)
	if err != nil || !receipt.NoActiveOperations || !receipt.Workspace.Stopped {
		t.Fatalf("stop %+v %v", receipt, err)
	}
	e.Close()
	e, err = Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for _, epoch := range []uint64{1, 2} {
		r.Epoch = epoch
		r = authorized(t, c.Signer, r)
		if _, err = e.AdoptWorkspace(context.Background(), r); !errors.Is(err, domain.ErrFenced) {
			t.Fatalf("reopened at epoch %d: %v", epoch, err)
		}
		if _, err = e.StartOperation(context.Background(), request(r, "late", "list_files", struct{}{}, 1)); !errors.Is(err, domain.ErrFenced) {
			t.Fatalf("start after stop: %v", err)
		}
	}
}
func TestNoncanonicalArgsNeverReserveOperation(t *testing.T) {
	e, _, r := fixture(t, nil, nil)
	for _, raw := range []string{`{ "a":1}`, `{"z":1,"a":2}`, `{"a":1,"a":2}`, `{"html":"<script>"}`} {
		req := request(r, "invalid", "list_files", struct{}{}, 1)
		req.Args = json.RawMessage(raw)
		req.ArgsHash = hashBytes(req.Args)
		if _, err := e.StartOperation(context.Background(), req); !errors.Is(err, domain.ErrInvalid) {
			t.Fatalf("accepted %s: %v", raw, err)
		}
	}
	if _, err := e.journal.operation(context.Background(), "invalid"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal("invalid input persisted")
	}
}
func TestPublicReceiptContainsNoGrant(t *testing.T) {
	e, _, r := fixture(t, nil, nil)
	if _, err := e.StartOperation(context.Background(), request(r, "receipt", "list_files", struct{}{}, 1)); err != nil {
		t.Fatal(err)
	}
	o := waitOperation(t, e, r, "receipt", true)
	f, err := e.config.Artifacts.Open(context.Background(), r.TenantID, r.RunID, o.Receipt)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	raw, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), r.Grant) || o.Request.Grant != "" {
		t.Fatal("bearer capability leaked into public receipt")
	}
}
func TestCleanVerificationExcludesGeneratedFiles(t *testing.T) {
	var observed atomic.Bool
	backend := &sandbox.TestBackend{StartFunc: func(_ context.Context, s sandbox.JobSpec) (sandbox.Job, error) {
		if !s.TrustedVerification {
			t.Error("verification flag missing")
		}
		for _, name := range []string{".venv/poison.py", "__pycache__/main.pyc", ".git/config"} {
			if _, err := os.Stat(filepath.Join(s.Workspace, name)); !os.IsNotExist(err) {
				t.Errorf("generated file entered candidate: %s", name)
			}
		}
		content, err := os.ReadFile(filepath.Join(s.Workspace, "main.py"))
		if err != nil || !strings.Contains(string(content), "a + b") {
			t.Error("candidate omitted delivered patch")
		}
		observed.Store(true)
		return sandbox.Job{ID: s.ID, Started: true}, nil
	}}
	e, _, r := fixture(t, nil, backend)
	for _, name := range []string{".venv/poison.py", "__pycache__/main.pyc", ".git/config"} {
		p := filepath.Join(e.workspacePath(r.WorkspaceID), name)
		os.MkdirAll(filepath.Dir(p), 0700)
		os.WriteFile(p, []byte("poison"), 0600)
	}
	if _, err := e.StartOperation(context.Background(), patch(r, "patch", 1)); err != nil {
		t.Fatal(err)
	}
	p := waitOperation(t, e, r, "patch", true)
	if _, err := e.StartOperation(context.Background(), request(r, "verify", "verify", VerifyArgs{}, 2)); err != nil {
		t.Fatal(err)
	}
	v := waitOperation(t, e, r, "verify", true)
	if !observed.Load() || v.Status != Succeeded || v.AfterRevision != 2 || v.AfterHash != p.AfterHash {
		t.Fatalf("verification lost candidate binding: %+v", v)
	}
}
func TestPoolLeaseSurvivesCrashAndReusesOnlyAfterRelease(t *testing.T) {
	old, c, r := fixture(t, nil, nil)
	if _, err := old.ReleaseWorkspace(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	old.Close()
	mount := t.TempDir()
	c.VolumeSlots = []sandbox.VolumeSpec{{ID: "slot", MountPath: mount}}
	c.TestVolumeVerifier = func(context.Context, sandbox.VolumeSpec) error { return nil }
	var fault atomic.Bool
	c.Fault = func(point string) error {
		if point == "after_volume_lease" && !fault.Swap(true) {
			return errors.New("crash after reservation")
		}
		return nil
	}
	e, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	r.WorkspaceID = "pooled"
	r.RunID = "pooled-run"
	r = authorized(t, c.Signer, r)
	prep := PrepareRequest{WorkspaceRequest: r, SourceID: "fixture", ProfileID: "python"}
	if _, err = e.PrepareWorkspace(context.Background(), prep); err == nil {
		t.Fatal("fault not reached")
	}
	e.Close()
	c.Fault = nil
	e, err = Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	next := r
	next.WorkspaceID = "next"
	next = authorized(t, c.Signer, next)
	if _, err = e.PrepareWorkspace(context.Background(), PrepareRequest{WorkspaceRequest: next, SourceID: "fixture", ProfileID: "python"}); !errors.Is(err, domain.ErrCapacity) {
		t.Fatalf("unconfirmed import lost capacity: %v", err)
	}
	if _, err = e.PrepareWorkspace(context.Background(), prep); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(e.workspacePath(r.WorkspaceID), mount+string(os.PathSeparator)) {
		t.Fatal("workspace escaped fixed slot")
	}
	if _, err = e.ReleaseWorkspace(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if _, err = e.PrepareWorkspace(context.Background(), PrepareRequest{WorkspaceRequest: next, SourceID: "fixture", ProfileID: "python"}); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(mount); err != nil {
		t.Fatal("release removed pool mount")
	}
}
func TestPoolUnknownOperationRetainsCapacity(t *testing.T) {
	old, c, r := fixture(t, nil, nil)
	old.ReleaseWorkspace(context.Background(), r)
	old.Close()
	c.VolumeSlots = []sandbox.VolumeSpec{{ID: "slot", MountPath: t.TempDir()}}
	c.TestVolumeVerifier = func(context.Context, sandbox.VolumeSpec) error { return nil }
	c.Backend = &sandbox.TestBackend{StartFunc: func(context.Context, sandbox.JobSpec) (sandbox.Job, error) {
		return sandbox.Job{}, errors.New("lost start response")
	}}
	e, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	r.WorkspaceID = "pooled"
	r = authorized(t, c.Signer, r)
	if _, err = e.PrepareWorkspace(context.Background(), PrepareRequest{WorkspaceRequest: r, SourceID: "fixture", ProfileID: "python"}); err != nil {
		t.Fatal(err)
	}
	if _, err = e.StartOperation(context.Background(), request(r, "unknown", "run_command", CommandArgs{Command: []string{"ignored"}}, 1)); err != nil {
		t.Fatal(err)
	}
	waitOperation(t, e, r, "unknown", false)
	if _, err = e.ReleaseWorkspace(context.Background(), r); !errors.Is(err, domain.ErrReconciliation) {
		t.Fatalf("unknown release: %v", err)
	}
	next := r
	next.WorkspaceID = "another"
	next = authorized(t, c.Signer, next)
	if _, err = e.PrepareWorkspace(context.Background(), PrepareRequest{WorkspaceRequest: next, SourceID: "fixture", ProfileID: "python"}); !errors.Is(err, domain.ErrCapacity) {
		t.Fatalf("unknown released slot: %v", err)
	}
}

func TestOldReleaseAfterSlotReusePreservesNewWorkspace(t *testing.T) {
	old, c, r := fixture(t, nil, nil)
	old.ReleaseWorkspace(context.Background(), r)
	old.Close()
	c.VolumeSlots = []sandbox.VolumeSpec{{ID: "slot", MountPath: t.TempDir()}}
	c.TestVolumeVerifier = func(context.Context, sandbox.VolumeSpec) error { return nil }
	e, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	r.WorkspaceID = "old-pooled"
	r = authorized(t, c.Signer, r)
	if _, err = e.PrepareWorkspace(context.Background(), PrepareRequest{WorkspaceRequest: r, SourceID: "fixture", ProfileID: "python"}); err != nil {
		t.Fatal(err)
	}
	if _, err = e.ReleaseWorkspace(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	next := r
	next.WorkspaceID = "new-pooled"
	next.TenantID = "other-tenant"
	next.RunID = "other-run"
	next = authorized(t, c.Signer, next)
	if _, err = e.PrepareWorkspace(context.Background(), PrepareRequest{WorkspaceRequest: next, SourceID: "fixture", ProfileID: "python"}); err != nil {
		t.Fatal(err)
	}
	e.Close()
	e, err = Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if _, err = e.ReleaseWorkspace(context.Background(), r); err != nil {
		t.Fatalf("repeated settled release: %v", err)
	}
	if _, err = e.StartOperation(context.Background(), request(next, "list", "list_files", struct{}{}, 1)); err != nil {
		t.Fatalf("old release affected new tenant: %v", err)
	}
	op := waitOperation(t, e, next, "list", true)
	if op.Status != Succeeded {
		t.Fatalf("new tenant files removed: %+v", op)
	}
}
