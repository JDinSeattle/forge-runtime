package runner

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
)

func fixture(t *testing.T, fault func(string) error, backend sandbox.Backend) (*Engine, Config, WorkspaceRequest) {
	t.Helper()
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "main.py"), []byte("def add(a, b):\n    return a - b\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "test_main.py"), []byte("assert add(1, 2) == 3\n"), 0600); err != nil {
		t.Fatal(err)
	}
	store, err := artifact.NewLocalStore(filepath.Join(root, "artifacts"), 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	signer, err := NewSigner([]byte(strings.Repeat("k", 32)))
	if err != nil {
		t.Fatal(err)
	}
	if backend == nil {
		backend = &sandbox.TestBackend{}
	}
	config := Config{RootDir: filepath.Join(root, "runner"), JournalPath: filepath.Join(root, "journal.sqlite"), Artifacts: store, Backend: backend, Signer: signer, Profiles: map[string]sandbox.Profile{"python": {ID: "python", VerifyCommand: []string{"python", "-m", "pytest"}}}, Sources: map[string]string{"fixture": source}, Fault: fault}
	e, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	r := authorized(t, signer, WorkspaceRequest{TenantID: "tenant", RunID: "run", WorkspaceID: "workspace", Epoch: 1})
	w, err := e.PrepareWorkspace(context.Background(), PrepareRequest{WorkspaceRequest: r, SourceID: "fixture", ProfileID: "python"})
	if err != nil {
		t.Fatal(err)
	}
	if w.Revision != 1 {
		t.Fatal("initial revision")
	}
	return e, config, r
}

func authorized(t *testing.T, s *Signer, r WorkspaceRequest) WorkspaceRequest {
	t.Helper()
	now := time.Now()
	token, err := s.Sign(Claims{TenantID: r.TenantID, RunID: r.RunID, WorkspaceID: r.WorkspaceID, Epoch: r.Epoch, IssuedAt: now, ExpiresAt: now.Add(time.Minute), Permissions: []string{"prepare", "execute", "inspect", "cancel", "adopt", "verify", "snapshot", "release"}}, now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	r.Grant = token
	return r
}
func request(r WorkspaceRequest, id domain.ID, kind string, args any, revision uint64) OperationRequest {
	raw, _ := json.Marshal(args)
	raw, _ = domain.CanonicalJSON(raw)
	return OperationRequest{WorkspaceRequest: r, OperationID: id, ExpectedRevision: revision, Kind: kind, Args: raw, ArgsHash: hashBytes(raw), PolicyVersion: "policy-v1", Deadline: time.Now().Add(30 * time.Second).UTC()}
}
func patch(r WorkspaceRequest, id domain.ID, revision uint64) OperationRequest {
	content := "def add(a, b):\n    return a + b\n"
	return request(r, id, "apply_patch", PatchArgs{Files: []FileEdit{{Path: "main.py", ExpectedSHA256: hashBytes([]byte("def add(a, b):\n    return a - b\n")), Content: &content}}}, revision)
}
func waitOperation(t *testing.T, e *Engine, r WorkspaceRequest, id domain.ID, terminal bool) Operation {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		op, err := e.InspectOperation(context.Background(), InspectRequest{WorkspaceRequest: r, OperationID: id})
		if err != nil {
			t.Fatal(err)
		}
		if op.Status.Terminal() || !terminal && op.Status == Unknown {
			return op
		}
		if time.Now().After(deadline) {
			t.Fatalf("operation stayed %s: %s", op.Status, op.Error)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestConcurrentIdempotentPatchWritesExactlyOnce(t *testing.T) {
	var writes atomic.Int64
	e, _, r := fixture(t, func(point string) error {
		if point == "after_patch_file" {
			writes.Add(1)
		}
		return nil
	}, nil)
	req := patch(r, "operation", 1)
	var wg sync.WaitGroup
	for range 24 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := e.StartOperation(context.Background(), req); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	o := waitOperation(t, e, r, "operation", true)
	if o.Status != Succeeded || o.AfterRevision != 2 || o.Receipt.SHA256 == "" || writes.Load() != 1 {
		t.Fatalf("incorrect effect result: %+v writes=%d", o, writes.Load())
	}
	if _, err := e.config.Artifacts.Stat(context.Background(), r.TenantID, r.RunID, o.Receipt); err != nil {
		t.Fatal(err)
	}
	changed := req
	changed.PolicyVersion = "policy-v2"
	if _, err := e.StartOperation(context.Background(), changed); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("mutable operation binding: %v", err)
	}
	var count int
	if err := e.journal.db.QueryRow("SELECT count(*) FROM operations").Scan(&count); err != nil || count != 1 {
		t.Fatalf("ledger count %d: %v", count, err)
	}
}

func TestAppliedPatchReceiptWindowRecoversWithoutReplay(t *testing.T) {
	var faulted atomic.Bool
	var writes atomic.Int64
	e, c, r := fixture(t, func(point string) error {
		if point == "after_patch_file" {
			writes.Add(1)
		}
		if point == "before_receipt" && !faulted.Swap(true) {
			return errors.New("injected crash window")
		}
		return nil
	}, nil)
	if _, err := e.StartOperation(context.Background(), patch(r, "operation", 1)); err != nil {
		t.Fatal(err)
	}
	var o Operation
	deadline := time.Now().Add(5 * time.Second)
	for {
		o, _ = e.journal.operation(context.Background(), "operation")
		if o.Status == Unknown {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fault did not reach journal: %s", o.Status)
		}
		time.Sleep(time.Millisecond)
	}
	if o.Status != Unknown {
		t.Fatalf("fault not injected: %s", o.Status)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	c.Fault = nil
	reopened, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	o = waitOperation(t, reopened, r, "operation", true)
	if o.Status != Succeeded || o.AfterRevision != 2 || writes.Load() != 1 {
		t.Fatalf("receipt recovery replayed patch: %+v", o)
	}
}

func TestPartialMultiFilePatchRemainsUnknownAndBlocksAdoption(t *testing.T) {
	var once atomic.Bool
	e, c, r := fixture(t, func(point string) error {
		if point == "after_patch_file" && !once.Swap(true) {
			return errors.New("power loss between files")
		}
		return nil
	}, nil)
	first, second := "fixed main\n", "fixed tests\n"
	req := request(r, "operation", "apply_patch", PatchArgs{Files: []FileEdit{{Path: "main.py", ExpectedSHA256: hashBytes([]byte("def add(a, b):\n    return a - b\n")), Content: &first}, {Path: "test_main.py", ExpectedSHA256: hashBytes([]byte("assert add(1, 2) == 3\n")), Content: &second}}}, 1)
	if _, err := e.StartOperation(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	o := waitOperation(t, e, r, "operation", false)
	if o.Status != Unknown {
		t.Fatal("partial patch falsely settled")
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	c.Fault = nil
	reopened, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	r.Epoch = 2
	r = authorized(t, c.Signer, r)
	if _, err = reopened.AdoptWorkspace(context.Background(), r); !errors.Is(err, domain.ErrReconciliation) {
		t.Fatalf("partial patch adoption: %v", err)
	}
	if _, err = reopened.StartOperation(context.Background(), request(r, "next", "list_files", struct{}{}, 1)); !errors.Is(err, domain.ErrFenced) {
		t.Fatalf("unknown writer reservation released: %v", err)
	}
	main, _ := os.ReadFile(filepath.Join(reopened.workspacePath(r.WorkspaceID), "main.py"))
	test, _ := os.ReadFile(filepath.Join(reopened.workspacePath(r.WorkspaceID), "test_main.py"))
	if string(main) != first || string(test) == second {
		t.Fatal("partial effect was replayed during adoption")
	}
}

func TestUnknownStartDoesNotCreateAnotherJob(t *testing.T) {
	var starts atomic.Int64
	backend := &sandbox.TestBackend{StartFunc: func(context.Context, sandbox.JobSpec) (sandbox.Job, error) {
		starts.Add(1)
		return sandbox.Job{}, errors.New("RPC lost after dispatch")
	}}
	e, c, r := fixture(t, nil, backend)
	req := request(r, "operation", "run_command", CommandArgs{Command: []string{"python", "-m", "pytest"}}, 1)
	if _, err := e.StartOperation(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	o := waitOperation(t, e, r, "operation", false)
	if o.Status != Unknown {
		t.Fatal("uncertain start treated as failed")
	}
	for range 5 {
		if _, err := e.StartOperation(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	r.Epoch = 2
	r = authorized(t, c.Signer, r)
	if _, err := e.AdoptWorkspace(context.Background(), r); !errors.Is(err, domain.ErrReconciliation) {
		t.Fatalf("missing daemon evidence allowed adoption: %v", err)
	}
	if starts.Load() != 1 {
		t.Fatal("unknown command executed more than once")
	}
}

func TestScopeAndPathBoundaries(t *testing.T) {
	e, c, r := fixture(t, nil, nil)
	other := r
	other.TenantID = "other"
	other = authorized(t, c.Signer, other)
	if _, err := e.StartOperation(context.Background(), request(other, "other-op", "list_files", struct{}{}, 1)); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("cross-tenant operation: %v", err)
	}
	for i, name := range []string{"../outside", "/etc/passwd", ".git/config", "a/../../outside"} {
		content := "oops"
		req := request(r, domain.ID("bad-"+string(rune('a'+i))), "apply_patch", PatchArgs{Files: []FileEdit{{Path: name, ExpectedSHA256: "absent", Content: &content}}}, 1)
		if _, err := e.StartOperation(context.Background(), req); !errors.Is(err, domain.ErrInvalid) {
			t.Fatalf("path %q: %v", name, err)
		}
	}
	out := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(out, []byte("private"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(out, filepath.Join(e.workspacePath(r.WorkspaceID), "escape")); err != nil {
		t.Fatal(err)
	}
	if _, err := e.StartOperation(context.Background(), request(r, "symlink", "list_files", struct{}{}, 1)); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("workspace symlink followed: %v", err)
	}
}

func TestGrantExpiryPermissionAndLeaseMargin(t *testing.T) {
	s, _ := NewSigner([]byte(strings.Repeat("k", 32)))
	now := time.Now()
	claims := Claims{TenantID: "tenant", RunID: "run", WorkspaceID: "workspace", Epoch: 1, Permissions: []string{"inspect"}, IssuedAt: now, ExpiresAt: now.Add(10 * time.Second)}
	if _, err := s.Sign(claims, now.Add(10*time.Second)); !errors.Is(err, domain.ErrFenced) {
		t.Fatal("grant exceeded lease margin")
	}
	token, err := s.Sign(claims, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	r := WorkspaceRequest{TenantID: "tenant", RunID: "run", WorkspaceID: "workspace", Epoch: 1, Grant: token}
	if err = s.Verify(token, r, "execute", now); !errors.Is(err, domain.ErrForbidden) {
		t.Fatal("read grant executes tools")
	}
	if err = s.Verify(token, r, "inspect", claims.ExpiresAt); !errors.Is(err, domain.ErrFenced) {
		t.Fatal("expired grant accepted")
	}
	if err = s.Verify(token+"x", r, "inspect", now); !errors.Is(err, domain.ErrForbidden) {
		t.Fatal("tampered signature accepted")
	}
}

func TestFencedAdoptionCancelsOldJobBeforeNewWrites(t *testing.T) {
	var mu sync.Mutex
	job := sandbox.Job{}
	var starts int
	backend := &sandbox.TestBackend{StartFunc: func(_ context.Context, s sandbox.JobSpec) (sandbox.Job, error) {
		mu.Lock()
		defer mu.Unlock()
		starts++
		job = sandbox.Job{ID: s.ID, Running: true, Started: true}
		return job, nil
	}, InspectFunc: func(context.Context, string) (sandbox.Job, error) { mu.Lock(); defer mu.Unlock(); return job, nil }, CancelFunc: func(context.Context, string) (sandbox.Job, error) {
		mu.Lock()
		defer mu.Unlock()
		job.Running = false
		job.ExitCode = 137
		return job, nil
	}}
	e, c, r := fixture(t, nil, backend)
	req := request(r, "operation", "run_command", CommandArgs{Command: []string{"sleep", "5"}}, 1)
	if _, err := e.StartOperation(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		mu.Lock()
		running := job.Running
		mu.Unlock()
		if running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fake job did not start")
		}
		time.Sleep(time.Millisecond)
	}
	old := r
	r.Epoch = 2
	r = authorized(t, c.Signer, r)
	receipt, err := e.AdoptWorkspace(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if !receipt.NoActiveOperations || receipt.Workspace.ActiveOperation != "" || receipt.Workspace.Epoch != 2 {
		t.Fatalf("adoption receipt %+v", receipt)
	}
	mu.Lock()
	if job.Running || starts != 1 {
		t.Error("adoption overlapped or duplicated old process")
	}
	mu.Unlock()
	if _, err = e.StartOperation(context.Background(), request(old, "stale", "list_files", struct{}{}, 1)); !errors.Is(err, domain.ErrFenced) {
		t.Fatalf("old epoch admission: %v", err)
	}
	if _, err = e.StartOperation(context.Background(), request(r, "new", "list_files", struct{}{}, 1)); err != nil {
		t.Fatal(err)
	}
	o := waitOperation(t, e, r, "new", true)
	if o.Status != Succeeded {
		t.Fatalf("new owner cannot run: %s", o.Error)
	}
}

func TestStableDaemonJobRecoveredAcrossJournalReopen(t *testing.T) {
	var starts atomic.Int64
	var done atomic.Bool
	backend := &sandbox.TestBackend{StartFunc: func(_ context.Context, s sandbox.JobSpec) (sandbox.Job, error) {
		starts.Add(1)
		return sandbox.Job{ID: s.ID, Started: true, Running: true}, nil
	}, InspectFunc: func(_ context.Context, id string) (sandbox.Job, error) {
		return sandbox.Job{ID: id, Started: true, Running: !done.Load(), ExitCode: 0}, nil
	}}
	e, c, r := fixture(t, func(point string) error {
		if point == "after_job_start" {
			return errors.New("runner receipt write crashed")
		}
		return nil
	}, backend)
	if _, err := e.StartOperation(context.Background(), request(r, "job", "run_command", CommandArgs{Command: []string{"python", "-m", "pytest"}}, 1)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		o, _ := e.journal.operation(context.Background(), "job")
		if o.Status == Unknown {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fault boundary not reached")
		}
		time.Sleep(time.Millisecond)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	done.Store(true)
	c.Fault = nil
	reopened, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	o := waitOperation(t, reopened, r, "job", true)
	if o.Status != Succeeded || starts.Load() != 1 || o.Receipt.ObjectKey == "" {
		t.Fatalf("stable job was replayed or result lost: %+v", o)
	}
}

func TestSnapshotReleasePreserveReceiptAndCloseAdmission(t *testing.T) {
	e, _, r := fixture(t, nil, nil)
	if _, err := e.StartOperation(context.Background(), patch(r, "patch", 1)); err != nil {
		t.Fatal(err)
	}
	op := waitOperation(t, e, r, "patch", true)
	snapshot, err := e.SealSnapshot(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Workspace.Revision != 2 || snapshot.Hash != op.AfterHash {
		t.Fatal("snapshot revision/content mismatch")
	}
	result, err := e.ReleaseWorkspace(context.Background(), r)
	if err != nil || !result.Released {
		t.Fatalf("release: %+v %v", result, err)
	}
	if _, err = os.Stat(e.workspacePath(r.WorkspaceID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("released workspace still exists")
	}
	if _, err = e.config.Artifacts.Stat(context.Background(), r.TenantID, r.RunID, op.Receipt); err != nil {
		t.Fatal("release deleted operation evidence")
	}
	if _, err = e.StartOperation(context.Background(), request(r, "late", "list_files", struct{}{}, 2)); !errors.Is(err, domain.ErrFenced) {
		t.Fatalf("released workspace admitted a write: %v", err)
	}
}

func TestWorkspaceLockExcludesOtherProcesses(t *testing.T) {
	if os.Getenv("FORGE_LOCK_HELPER") == "1" {
		_, _ = os.Stdout.WriteString("ready\n")
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		unlock, err := lockWorkspace(ctx, os.Getenv("FORGE_LOCK_PATH"))
		if err != nil {
			t.Fatal(err)
		}
		defer unlock()
		_, _ = os.Stdout.WriteString("acquired\n")
		return
	}
	name := filepath.Join(t.TempDir(), "workspace.lock")
	unlock, err := lockWorkspace(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "-test.run=^TestWorkspaceLockExcludesOtherProcesses$")
	cmd.Env = append(os.Environ(), "FORGE_LOCK_HELPER=1", "FORGE_LOCK_PATH="+name)
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill() }()
	scanner := bufio.NewScanner(pipe)
	if !scanner.Scan() || scanner.Text() != "ready" {
		t.Fatal("helper did not initialize")
	}
	acquired := make(chan bool, 1)
	go func() { acquired <- scanner.Scan() && scanner.Text() == "acquired" }()
	select {
	case <-acquired:
		t.Fatal("another runner process entered a locked workspace")
	case <-time.After(50 * time.Millisecond):
	}
	unlock()
	select {
	case ok := <-acquired:
		if !ok {
			t.Fatal("helper failed to acquire released workspace")
		}
	case <-time.After(time.Second):
		t.Fatal("workspace lock leaked")
	}
	if err = cmd.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestGrantRecheckedAfterWaitingForWorkspaceLock(t *testing.T) {
	e, _, r := fixture(t, nil, nil)
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	var checks atomic.Int64
	e.config.Now = func() time.Time { checks.Add(1); return time.Unix(0, now.Load()) }
	unlock, err := e.lock(context.Background(), r.WorkspaceID)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	result := make(chan error, 1)
	go func() {
		_, err := e.StartOperation(context.Background(), request(r, "blocked", "list_files", struct{}{}, 1))
		result <- err
	}()
	deadline := time.Now().Add(time.Second)
	for checks.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("request did not reach admission")
		}
		time.Sleep(time.Millisecond)
	}
	now.Add(int64(2 * time.Minute))
	unlock()
	if err := <-result; !errors.Is(err, domain.ErrFenced) {
		t.Fatalf("expired capability entered after lock wait: %v", err)
	}
	if _, err := e.journal.operation(context.Background(), "blocked"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expired capability created an effect: %v", err)
	}
}

func TestExecutableModeSurvivesImportAndAppearsInDiff(t *testing.T) {
	e, c, r := fixture(t, nil, nil)
	source := c.Sources["fixture"]
	script := []byte("#!/bin/sh\nexit 0\n")
	if err := os.WriteFile(filepath.Join(source, "verify.sh"), script, 0755); err != nil {
		t.Fatal(err)
	}
	r.WorkspaceID = "executable-workspace"
	r.RunID = "executable-run"
	r = authorized(t, c.Signer, r)
	w, err := e.PrepareWorkspace(context.Background(), PrepareRequest{WorkspaceRequest: r, SourceID: "fixture", ProfileID: "python"})
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(e.workspacePath(r.WorkspaceID), "verify.sh"))
	if err != nil || info.Mode().Perm()&0111 == 0 {
		t.Fatal("executable source lost mode")
	}
	expected, err := ComputeSourceHash(context.Background(), source, 0, 0)
	if err != nil || expected != w.BaselineHash {
		t.Fatal("source hash differs from imported executable manifest")
	}
	if err = os.Chmod(filepath.Join(e.workspacePath(r.WorkspaceID), "verify.sh"), 0666); err != nil {
		t.Fatal(err)
	}
	if _, err = e.StartOperation(context.Background(), request(r, "diff", "get_diff", struct{}{}, 1)); err != nil {
		t.Fatal(err)
	}
	o := waitOperation(t, e, r, "diff", true)
	var diff struct{ Files []DiffEntry }
	if err = json.Unmarshal(o.Result, &diff); err != nil {
		t.Fatal(err)
	}
	if len(diff.Files) != 1 || !diff.Files[0].BeforeExecutable || diff.Files[0].AfterExecutable {
		t.Fatalf("mode-only diff lost: %s", o.Result)
	}
}

func FuzzToolPathsStayRelative(f *testing.F) {
	for _, path := range []string{"main.py", "../outside", "a/b.py", "a/../../etc/passwd", "/etc/passwd", ".git/config", "a\\..\\b"} {
		f.Add(path)
	}
	f.Fuzz(func(t *testing.T, name string) {
		if safePath(name) != nil {
			return
		}
		joined := filepath.Join("/task/workspace", name)
		relative, err := filepath.Rel("/task/workspace", joined)
		if err != nil || relative == ".." || strings.HasPrefix(relative, "../") || filepath.IsAbs(relative) {
			t.Fatalf("accepted escaping path %q", name)
		}
	})
}
