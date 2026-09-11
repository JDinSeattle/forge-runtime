package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
)

type Engine struct {
	slots          map[string]sandbox.VolumeSpec
	allocations    map[domain.ID]string
	storageUnlocks []func()
	config         Config
	journal        *journal
	mu             sync.Mutex
	closed         bool
	cancels        map[domain.ID]context.CancelFunc
	wg             sync.WaitGroup
}

func Open(config Config) (*Engine, error) {
	if config.RootDir == "" || config.JournalPath == "" || config.Artifacts == nil || config.Backend == nil || config.Signer == nil {
		return nil, domain.ErrInvalid
	}
	if !filepath.IsAbs(config.RootDir) || !filepath.IsAbs(config.JournalPath) {
		return nil, domain.ErrInvalid
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.MaxFileBytes <= 0 {
		config.MaxFileBytes = 1 << 20
	}
	if config.MaxWorkspaceBytes <= 0 {
		config.MaxWorkspaceBytes = 32 << 20
	}
	for _, name := range []string{"workspaces", "baselines", "locks"} {
		if err := os.MkdirAll(filepath.Join(config.RootDir, name), 0700); err != nil {
			return nil, err
		}
	}
	j, err := openJournal(config.JournalPath)
	if err != nil {
		return nil, err
	}
	identity, err := j.identity(context.Background())
	if err != nil {
		j.db.Close()
		return nil, err
	}
	e := &Engine{config: config, journal: j, cancels: map[domain.ID]context.CancelFunc{}}
	if err = e.openStorage(); err != nil {
		for _, unlock := range e.storageUnlocks {
			unlock()
		}
		j.db.Close()
		return nil, err
	}
	if err = artifact.RegisterJournal(context.Background(), config.Artifacts, config.JournalPath, identity); err != nil {
		_ = e.Close()
		return nil, err
	}
	return e, nil
}

// Close stops runner-side waiting and new admission, preserving durable jobs for
// inspection. It does not assert business cancellation or remove containers.
func (e *Engine) Close() error {
	e.mu.Lock()
	e.closed = true
	for _, cancel := range e.cancels {
		cancel()
	}
	e.mu.Unlock()
	e.wg.Wait()
	err := e.journal.db.Close()
	for _, unlock := range e.storageUnlocks {
		unlock()
	}
	return err
}
func (e *Engine) workspacePath(id domain.ID) string {
	if base := e.storageBase(id); base != "" {
		return filepath.Join(base, "checkout")
	}
	return filepath.Join(e.config.RootDir, "workspaces", string(id))
}
func (e *Engine) baselinePath(id domain.ID) string {
	if base := e.storageBase(id); base != "" {
		return filepath.Join(base, "baseline")
	}
	return filepath.Join(e.config.RootDir, "baselines", string(id))
}
func (e *Engine) lock(ctx context.Context, id domain.ID) (func(), error) {
	if id.Validate() != nil {
		return nil, domain.ErrInvalid
	}
	unlock, err := lockWorkspace(ctx, filepath.Join(e.config.RootDir, "locks", string(id)+".lock"))
	if err != nil {
		return nil, err
	}
	if err = e.verifyStorage(ctx, id); err != nil {
		unlock()
		return nil, err
	}
	return unlock, nil
}
func (e *Engine) fault(point string) error {
	if e.config.Fault != nil {
		return e.config.Fault(point)
	}
	return nil
}
func (e *Engine) faultAt(point string, r OperationRequest) error {
	if err := e.fault(point); err != nil {
		return err
	}
	if e.config.OperatorFault != nil {
		return e.config.OperatorFault(point, r)
	}
	return nil
}
func (e *Engine) authorize(r WorkspaceRequest, permission string) error {
	e.mu.Lock()
	closed := e.closed
	e.mu.Unlock()
	if closed {
		return errors.New("runner is shutting down")
	}
	return e.config.Signer.Verify(r.Grant, r, permission, e.config.Now())
}

func (e *Engine) PrepareWorkspace(ctx context.Context, r PrepareRequest) (Workspace, error) {
	if err := e.authorize(r.WorkspaceRequest, "prepare"); err != nil {
		return Workspace{}, err
	}
	source, ok := e.config.Sources[r.SourceID]
	if !ok {
		return Workspace{}, domain.ErrForbidden
	}
	if _, ok = e.config.Profiles[r.ProfileID]; !ok {
		return Workspace{}, domain.ErrForbidden
	}
	unlock, err := e.lock(ctx, r.WorkspaceID)
	if err != nil {
		return Workspace{}, err
	}
	defer unlock()
	if err = e.authorize(r.WorkspaceRequest, "prepare"); err != nil {
		return Workspace{}, err
	}
	if existing, err := e.journal.workspace(ctx, r.WorkspaceID); err == nil {
		if err = workspaceBinding(existing, r.WorkspaceRequest); err != nil {
			return Workspace{}, err
		}
		if existing.SourceID != r.SourceID || existing.ProfileID != r.ProfileID {
			return Workspace{}, domain.ErrConflict
		}
		if r.Epoch != existing.Epoch || existing.Released || existing.Stopped {
			return Workspace{}, domain.ErrFenced
		}
		return existing, nil
	} else if !errors.Is(err, domain.ErrNotFound) {
		return Workspace{}, err
	}
	files, err := e.readTree(ctx, source)
	if err != nil {
		return Workspace{}, err
	}
	digest := treeHash(files)
	if err = e.allocateStorage(ctx, r, digest); err != nil {
		return Workspace{}, err
	}
	for _, dir := range []string{e.baselinePath(r.WorkspaceID), e.workspacePath(r.WorkspaceID)} {
		if err = e.resumeImport(ctx, files, dir, r); err != nil {
			return Workspace{}, err
		}
	}
	if err = e.makeWorkspaceWritable(r.WorkspaceID); err != nil {
		return Workspace{}, err
	}
	if err = e.syncWorkspace(ctx, r.WorkspaceID); err != nil {
		return Workspace{}, err
	}
	if err = e.faultAt("after_import_before_workspace", OperationRequest{WorkspaceRequest: r.WorkspaceRequest}); err != nil {
		return Workspace{}, err
	}
	w := Workspace{TenantID: r.TenantID, RunID: r.RunID, ID: r.WorkspaceID, SourceID: r.SourceID, ProfileID: r.ProfileID, Epoch: r.Epoch, Revision: 1, BaselineHash: digest}
	return e.journal.createWorkspace(ctx, w)
}

func (e *Engine) StartOperation(ctx context.Context, r OperationRequest) (Operation, error) {
	permission := "execute"
	if r.Kind == "verify" {
		permission = "verify"
	}
	if err := e.authorize(r.WorkspaceRequest, permission); err != nil {
		return Operation{}, err
	}
	if r.OperationID.Validate() != nil || r.ExpectedRevision == 0 || r.PolicyVersion == "" || r.ArgsHash != hashBytes(r.Args) || len(r.Args) > 256<<10 || !json.Valid(r.Args) || !strings.HasPrefix(strings.TrimSpace(string(r.Args)), "{") || !r.Deadline.After(e.config.Now()) {
		return Operation{}, domain.ErrInvalid
	}
	if err := domain.ValidateCanonicalJSON(r.Args); err != nil {
		return Operation{}, err
	}
	switch r.Kind {
	case "list_files", "read_file", "search_code", "apply_patch", "run_command", "get_diff", "verify":
	default:
		return Operation{}, domain.ErrInvalid
	}
	w, err := e.journal.workspace(ctx, r.WorkspaceID)
	if err != nil {
		return Operation{}, err
	}
	if err = workspaceBinding(w, r.WorkspaceRequest); err != nil {
		return Operation{}, err
	}
	if w.Epoch != r.Epoch || w.Adopting || w.Released || w.Stopped {
		return Operation{}, domain.ErrFenced
	}
	// Fast idempotent read avoids blocking a retry behind its running operation.
	if old, err := e.journal.operation(ctx, r.OperationID); err == nil {
		if !sameBinding(old.Request, r) {
			return Operation{}, domain.ErrConflict
		}
		w, err := e.journal.workspace(ctx, r.WorkspaceID)
		if err != nil {
			return Operation{}, err
		}
		if workspaceBinding(w, r.WorkspaceRequest) != nil {
			return Operation{}, domain.ErrForbidden
		}
		if w.Epoch != r.Epoch || w.Adopting || w.Stopped {
			return Operation{}, domain.ErrFenced
		}
		return old, nil
	} else if !errors.Is(err, domain.ErrNotFound) {
		return Operation{}, err
	}
	if isProcess(r.Kind) {
		if err = e.config.Backend.Check(ctx); err != nil {
			return Operation{}, err
		}
	}
	unlock, err := e.lock(ctx, r.WorkspaceID)
	if err != nil {
		return Operation{}, err
	}
	if err = e.authorize(r.WorkspaceRequest, permission); err != nil {
		unlock()
		return Operation{}, err
	}
	// A concurrent request may have completed while this one waited for the
	// workspace lock. Resolve its immutable ID before rechecking file hashes.
	if existing, lookupErr := e.journal.operation(ctx, r.OperationID); lookupErr == nil {
		defer unlock()
		return e.existingOperation(ctx, r, existing)
	} else if !errors.Is(lookupErr, domain.ErrNotFound) {
		unlock()
		return Operation{}, lookupErr
	}
	before, err := e.readTree(ctx, e.workspacePath(r.WorkspaceID))
	if err != nil {
		unlock()
		return Operation{}, err
	}
	jobBinding := strings.Join([]string{e.config.RootDir, string(r.TenantID), string(r.RunID), string(r.WorkspaceID), string(r.OperationID)}, "\n")
	o := Operation{Request: r, JobID: "forge-" + hashBytes([]byte(jobBinding))[:40], BeforeHash: treeHash(before)}
	if r.Kind == "apply_patch" {
		_, after, err := e.patchPlan(r.Args, before)
		if err != nil {
			unlock()
			return Operation{}, err
		}
		o.ExpectedAfterHash = treeHash(after)
	}
	o, created, err := e.journal.reserve(ctx, o)
	if err != nil || !created {
		unlock()
		return o, err
	}
	if err = e.faultAt("after_prepared", o.Request); err != nil {
		unlock()
		return o, err
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		unlock()
		return o, errors.New("runner shutting down after prepare")
	}
	opCtx, cancel := context.WithDeadline(context.Background(), r.Deadline)
	e.cancels[r.OperationID] = cancel
	e.wg.Add(1)
	e.mu.Unlock()
	go func() {
		defer e.wg.Done()
		defer unlock()
		defer cancel()
		defer func() { e.mu.Lock(); delete(e.cancels, r.OperationID); e.mu.Unlock() }()
		e.execute(opCtx, o, before)
	}()
	return o, nil
}

func (e *Engine) existingOperation(ctx context.Context, r OperationRequest, o Operation) (Operation, error) {
	if !sameBinding(o.Request, r) {
		return Operation{}, domain.ErrConflict
	}
	w, err := e.journal.workspace(ctx, r.WorkspaceID)
	if err != nil {
		return Operation{}, err
	}
	if err = workspaceBinding(w, r.WorkspaceRequest); err != nil {
		return Operation{}, err
	}
	if w.Epoch != r.Epoch || w.Adopting || w.Released || w.Stopped {
		return Operation{}, domain.ErrFenced
	}
	return o, nil
}

func (e *Engine) execute(ctx context.Context, o Operation, before tree) {
	if err := e.journal.markRunning(context.Background(), o.Request.OperationID); err != nil {
		return
	}
	current, err := e.journal.operation(context.Background(), o.Request.OperationID)
	if err != nil {
		e.unknown(o, err)
		return
	}
	if current.CancelRequested {
		e.complete(o, Cancelled, json.RawMessage(`{"never_started":true}`), context.Canceled)
		return
	}
	var result json.RawMessage
	var execErr error
	status := Succeeded
	if isProcess(o.Request.Kind) {
		w, err := e.journal.workspace(ctx, o.Request.WorkspaceID)
		if err != nil {
			e.unknown(o, err)
			return
		}
		profile := e.config.Profiles[w.ProfileID]
		command := profile.VerifyCommand
		if o.Request.Kind == "run_command" {
			var args CommandArgs
			execErr = decodeArgs(o.Request.Args, &args)
			command = args.Command
		} else {
			var args VerifyArgs
			execErr = decodeArgs(o.Request.Args, &args)
			if args.Target {
				command = profile.TargetCommand
			}
		}
		if execErr != nil || len(command) == 0 {
			if execErr == nil {
				execErr = domain.ErrInvalid
			}
			e.complete(o, Failed, nil, execErr)
			return
		}
		jobWorkspace := e.workspacePath(o.Request.WorkspaceID)
		if o.Request.Kind == "verify" {
			jobWorkspace, err = e.cleanCandidate(ctx, o, before)
			if err != nil {
				e.unknown(o, err)
				return
			}
		}
		job, err := e.config.Backend.Start(ctx, sandbox.JobSpec{ID: o.JobID, Workspace: jobWorkspace, OperationID: o.Request.OperationID, Epoch: o.Request.Epoch, WorkspaceID: o.Request.WorkspaceID, TenantID: o.Request.TenantID, RunID: o.Request.RunID, Profile: profile, Command: command, Deadline: o.Request.Deadline, TrustedVerification: o.Request.Kind == "verify", BeforeStart: func(ctx context.Context) error {
			result, err := e.journal.db.ExecContext(ctx, `UPDATE operations SET docker_start_intent=1 WHERE id=? AND status='running' AND cancel_requested=0`, o.Request.OperationID)
			if err != nil {
				return err
			}
			if n, _ := result.RowsAffected(); n != 1 {
				return domain.ErrReconciliation
			}
			return nil
		}})
		if err != nil {
			e.unknown(o, err)
			return
		}
		if err = e.faultAt("after_job_start", o.Request); err != nil {
			e.unknown(o, err)
			return
		}
		for job.Running {
			select {
			case <-ctx.Done():
				e.mu.Lock()
				closing := e.closed
				e.mu.Unlock()
				if closing {
					e.unknown(o, ctx.Err())
					return
				}
				cancelCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				job, err = e.config.Backend.Cancel(cancelCtx, o.JobID)
				cancel()
				if err != nil || job.Running {
					if err == nil {
						err = domain.ErrReconciliation
					}
					e.unknown(o, err)
					return
				}
				if job.Interrupted {
					status = Cancelled
				}
			case <-time.After(50 * time.Millisecond):
				job, err = e.config.Backend.Inspect(ctx, o.JobID)
				if err != nil {
					e.unknown(o, err)
					return
				}
			}
		}
		if !job.Started {
			e.unknown(o, fmt.Errorf("job exists but start was not confirmed"))
			return
		}
		if job.ExitCode != 0 && status != Cancelled {
			status = Failed
		}
		if err = e.faultAt("after_job_exit", o.Request); err != nil {
			e.unknown(o, err)
			return
		}
		result, _ = json.Marshal(job)
	} else {
		result, execErr = e.fileTool(ctx, o, before)
		if execErr != nil {
			status = Failed
		}
	}
	if err := e.faultAt("before_receipt", o.Request); err != nil {
		e.unknown(o, err)
		return
	}
	e.complete(o, status, result, execErr)
}

func (e *Engine) unknown(o Operation, err error) {
	if err == nil {
		err = domain.ErrReconciliation
	}
	_ = e.journal.markUnknown(context.Background(), o.Request.OperationID, err.Error())
}

func (e *Engine) complete(o Operation, status Status, result json.RawMessage, executionErr error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	after, err := e.readTree(ctx, e.workspacePath(o.Request.WorkspaceID))
	if err != nil {
		e.unknown(o, err)
		return
	}
	if err = e.syncWorkspace(ctx, o.Request.WorkspaceID); err != nil {
		e.unknown(o, err)
		return
	}
	afterHash := treeHash(after)
	if o.Request.Kind == "verify" && status != Cancelled {
		if err = e.validateVerification(ctx, o, afterHash); err != nil {
			e.unknown(o, err)
			return
		}
	}
	if o.Request.Kind == "apply_patch" && status != Succeeded && afterHash != o.BeforeHash && afterHash != o.ExpectedAfterHash {
		e.unknown(o, fmt.Errorf("partially applied patch; original operation must be reconciled"))
		return
	}
	o.Status, o.Result, o.AfterHash = status, result, afterHash
	o.AfterRevision = o.Request.ExpectedRevision
	if afterHash != o.BeforeHash {
		o.AfterRevision++
	}
	if executionErr != nil {
		o.Error = executionErr.Error()
	}
	if current, readErr := e.journal.operation(ctx, o.Request.OperationID); readErr == nil && current.CancelRequested {
		o.CancelRequested = true
	}
	// Execution grants are bearer capabilities, not part of a downloadable
	// receipt. Keep only the immutable operation binding and observed facts.
	o.Request.Grant = ""
	data, err := json.Marshal(o)
	if err != nil {
		e.unknown(o, err)
		return
	}
	err = artifact.WithPublication(ctx, e.config.Artifacts, func(locked context.Context) error {
		ref, err := e.config.Artifacts.Put(locked, o.Request.TenantID, o.Request.RunID, "operation_receipt", bytes.NewReader(data))
		if err != nil {
			return err
		}
		o.Receipt = ref
		if err = e.journal.pinArtifact(locked, ref); err != nil {
			return err
		}
		if err = e.faultAt("after_receipt_before_commit", o.Request); err != nil {
			return err
		}
		_, err = e.journal.finish(locked, o)
		return err
	})
	if err != nil {
		e.unknown(o, err)
	}
}

func (e *Engine) InspectOperation(ctx context.Context, r InspectRequest) (Operation, error) {
	if err := e.authorize(r.WorkspaceRequest, "inspect"); err != nil {
		return Operation{}, err
	}
	o, err := e.boundOperation(ctx, r)
	if err != nil {
		return Operation{}, err
	}
	if o.Status.Terminal() {
		return o, nil
	}
	// A live local execution may finish without acquiring a second flock. Its
	// current durable status remains the authoritative response in the meantime.
	e.mu.Lock()
	_, live := e.cancels[r.OperationID]
	e.mu.Unlock()
	if live {
		return o, nil
	}
	unlock, err := e.lock(ctx, r.WorkspaceID)
	if err != nil {
		return o, err
	}
	defer unlock()
	return e.reconcileLocked(ctx, o, false)
}

func (e *Engine) boundOperation(ctx context.Context, r InspectRequest) (Operation, error) {
	w, err := e.journal.workspace(ctx, r.WorkspaceID)
	if err != nil {
		return Operation{}, err
	}
	if err = workspaceBinding(w, r.WorkspaceRequest); err != nil {
		return Operation{}, err
	}
	if r.Epoch < w.Epoch {
		return Operation{}, domain.ErrFenced
	}
	o, err := e.journal.operation(ctx, r.OperationID)
	if err != nil {
		return Operation{}, err
	}
	if o.Request.WorkspaceID != r.WorkspaceID || o.Request.TenantID != r.TenantID || o.Request.RunID != r.RunID {
		return Operation{}, domain.ErrForbidden
	}
	return o, nil
}

func (e *Engine) reconcileLocked(ctx context.Context, o Operation, cancelJob bool) (Operation, error) {
	current, err := e.journal.operation(ctx, o.Request.OperationID)
	if err != nil {
		return o, err
	}
	if current.Status.Terminal() {
		return current, nil
	}
	o = current
	var dispatched, startIntent bool
	if err = e.journal.db.QueryRowContext(ctx, `SELECT dispatch_started,docker_start_intent FROM operations WHERE id=?`, o.Request.OperationID).Scan(&dispatched, &startIntent); err != nil {
		return o, err
	}
	if !dispatched && (cancelJob || o.CancelRequested) {
		e.complete(o, Cancelled, json.RawMessage(`{"never_started":true,"dispatch_not_started":true}`), nil)
		return e.journal.operation(ctx, o.Request.OperationID)
	}
	if isProcess(o.Request.Kind) {
		job, err := e.config.Backend.Inspect(ctx, o.JobID)
		if err != nil {
			e.unknown(o, err)
			return e.journal.operation(ctx, o.Request.OperationID)
		}
		interrupted := false
		if (job.Running || !job.Started) && (cancelJob || o.CancelRequested || !o.Request.Deadline.After(e.config.Now())) {
			if !job.Started && !startIntent {
				if safe, ok := e.config.Backend.(interface {
					CancelNeverDispatched(context.Context, string) (sandbox.Job, error)
				}); ok {
					job, err = safe.CancelNeverDispatched(ctx, o.JobID)
				} else {
					err = domain.ErrReconciliation
				}
			} else {
				job, err = e.config.Backend.Cancel(ctx, o.JobID)
			}
			if err != nil {
				e.unknown(o, err)
				return o, err
			}
			interrupted = job.Interrupted
		}
		if job.Running {
			return o, nil
		}
		if !job.Started && job.NeverStarted {
			result, _ := json.Marshal(job)
			e.complete(o, Cancelled, result, nil)
			return e.journal.operation(ctx, o.Request.OperationID)
		}
		if !job.Started {
			e.unknown(o, domain.ErrReconciliation)
			return e.journal.operation(ctx, o.Request.OperationID)
		}
		status := Succeeded
		if job.ExitCode != 0 {
			status = Failed
		}
		if interrupted {
			status = Cancelled
		}
		result, _ := json.Marshal(job)
		e.complete(o, status, result, nil)
	} else {
		after, err := e.readTree(ctx, e.workspacePath(o.Request.WorkspaceID))
		if err != nil {
			return o, err
		}
		hash := treeHash(after)
		switch {
		case o.Request.Kind == "apply_patch" && hash == o.ExpectedAfterHash:
			e.complete(o, Succeeded, json.RawMessage(`{"reconciled":true}`), nil)
		case hash == o.BeforeHash:
			e.complete(o, Failed, json.RawMessage(`{"reconciled":true,"unchanged":true}`), errors.New("unconfirmed filesystem operation left workspace unchanged"))
		default:
			e.unknown(o, domain.ErrReconciliation)
		}
	}
	return e.journal.operation(ctx, o.Request.OperationID)
}

// SealSnapshot excludes active writers and publishes immutable bytes under the
// control-plane artifact store. The workspace itself remains runner-local.
func (e *Engine) SealSnapshot(ctx context.Context, r WorkspaceRequest) (Snapshot, error) {
	if err := e.authorize(r, "snapshot"); err != nil {
		return Snapshot{}, err
	}
	unlock, err := e.lock(ctx, r.WorkspaceID)
	if err != nil {
		return Snapshot{}, err
	}
	defer unlock()
	if err = e.authorize(r, "snapshot"); err != nil {
		return Snapshot{}, err
	}
	w, err := e.journal.workspace(ctx, r.WorkspaceID)
	if err != nil {
		return Snapshot{}, err
	}
	if err = workspaceBinding(w, r); err != nil {
		return Snapshot{}, err
	}
	if w.Epoch != r.Epoch || w.Released {
		return Snapshot{}, domain.ErrFenced
	}
	if w.ActiveOperation != "" {
		return Snapshot{}, domain.ErrReconciliation
	}
	files, err := e.readTree(ctx, e.workspacePath(r.WorkspaceID))
	if err != nil {
		return Snapshot{}, err
	}
	snapshot := Snapshot{Workspace: w, Hash: treeHash(files)}
	data, err := json.Marshal(struct {
		Workspace Workspace `json:"workspace"`
		Files     tree      `json:"files"`
	}{w, files})
	if err != nil {
		return Snapshot{}, err
	}
	err = artifact.WithPublication(ctx, e.config.Artifacts, func(locked context.Context) error {
		var err error
		snapshot.Artifact, err = e.config.Artifacts.Put(locked, r.TenantID, r.RunID, "workspace_snapshot", bytes.NewReader(data))
		if err != nil {
			return err
		}
		return e.journal.pinArtifact(locked, snapshot.Artifact)
	})
	return snapshot, err
}

// Release marks ownership closed before removing task-owned files. It refuses
// active/unknown operations; their journal and receipts are never garbage-collected here.
func (e *Engine) ReleaseWorkspace(ctx context.Context, r WorkspaceRequest) (ReleaseResult, error) {
	if err := e.authorize(r, "release"); err != nil {
		return ReleaseResult{}, err
	}
	unlock, err := e.lock(ctx, r.WorkspaceID)
	if err != nil {
		return ReleaseResult{}, err
	}
	defer unlock()
	if err = e.authorize(r, "release"); err != nil {
		return ReleaseResult{}, err
	}
	w, err := e.journal.workspace(ctx, r.WorkspaceID)
	if err != nil {
		return ReleaseResult{}, err
	}
	if err = workspaceBinding(w, r); err != nil {
		return ReleaseResult{}, err
	}
	if w.Epoch != r.Epoch {
		return ReleaseResult{}, domain.ErrFenced
	}
	if w.ActiveOperation != "" {
		return ReleaseResult{}, domain.ErrReconciliation
	}
	result, err := e.journal.db.ExecContext(ctx, "UPDATE workspaces SET released=1,adopting=1 WHERE id=? AND epoch=? AND active_operation=''", r.WorkspaceID, r.Epoch)
	if err != nil {
		return ReleaseResult{}, err
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return ReleaseResult{}, domain.ErrFenced
	}
	if err = e.releaseStorage(ctx, r.WorkspaceID); err != nil {
		return ReleaseResult{}, err
	}
	return ReleaseResult{WorkspaceID: r.WorkspaceID, Released: true}, nil
}

func (e *Engine) CancelOperation(ctx context.Context, r InspectRequest) (Operation, error) {
	if err := e.authorize(r.WorkspaceRequest, "cancel"); err != nil {
		return Operation{}, err
	}
	o, err := e.boundOperation(ctx, r)
	if err != nil {
		return o, err
	}
	if o.Status.Terminal() {
		return o, nil
	}
	// A replacement worker must fence the old workspace owner before cancelling
	// its operation. The workspace remains closed until AdoptWorkspace proves
	// settlement; cancelling a job alone is not an ownership-transfer receipt.
	w, err := e.journal.workspace(ctx, r.WorkspaceID)
	if err != nil {
		return o, err
	}
	if r.Epoch > w.Epoch {
		if _, err = e.journal.fence(ctx, r.WorkspaceRequest); err != nil {
			return o, err
		}
	}
	if err = e.journal.requestCancel(ctx, r.OperationID); err != nil {
		return o, err
	}
	e.mu.Lock()
	cancel, live := e.cancels[r.OperationID]
	if live {
		cancel()
	}
	e.mu.Unlock()
	// The local executor reacts to its cancelled context and releases the writer
	// lock. Orphan jobs are inspected/cancelled only after acquiring that lock,
	// so durable start intent and the observed interruption stay in one decision.
	unlock, err := e.lock(ctx, r.WorkspaceID)
	if err != nil {
		return o, err
	}
	defer unlock()
	return e.reconcileLocked(ctx, o, true)
}

func (e *Engine) AdoptWorkspace(ctx context.Context, r WorkspaceRequest) (StopReceipt, error) {
	if err := e.authorize(r, "adopt"); err != nil {
		return StopReceipt{}, err
	}
	if _, err := e.journal.fence(ctx, r); err != nil {
		return StopReceipt{}, err
	}
	return e.stopAndAdopt(ctx, r, true)
}
func (e *Engine) StopWorkspace(ctx context.Context, r WorkspaceRequest) (StopReceipt, error) {
	if err := e.authorize(r, "cancel"); err != nil {
		return StopReceipt{}, err
	}
	if _, err := e.journal.stopFence(ctx, r); err != nil {
		return StopReceipt{}, err
	}
	return e.stopAndAdopt(ctx, r, false)
}
func (e *Engine) stopAndAdopt(ctx context.Context, r WorkspaceRequest, reopen bool) (StopReceipt, error) {
	w, err := e.journal.workspace(ctx, r.WorkspaceID)
	if err != nil {
		return StopReceipt{}, err
	}
	if w.Epoch != r.Epoch {
		return StopReceipt{}, domain.ErrFenced
	}
	if w.ActiveOperation != "" {
		if err = e.journal.requestCancel(ctx, w.ActiveOperation); err != nil {
			return StopReceipt{}, err
		}
		e.mu.Lock()
		if cancel := e.cancels[w.ActiveOperation]; cancel != nil {
			cancel()
		}
		e.mu.Unlock()
	}
	unlock, err := e.lock(ctx, r.WorkspaceID)
	if err != nil {
		return StopReceipt{}, err
	}
	defer unlock()
	permission := "cancel"
	if reopen {
		permission = "adopt"
	}
	if err = e.authorize(r, permission); err != nil {
		return StopReceipt{}, err
	}
	w, err = e.journal.workspace(ctx, r.WorkspaceID)
	if err != nil {
		return StopReceipt{}, err
	}
	if w.Epoch != r.Epoch {
		return StopReceipt{}, domain.ErrFenced
	}
	if w.ActiveOperation != "" {
		o, err := e.journal.operation(ctx, w.ActiveOperation)
		if err != nil {
			return StopReceipt{}, err
		}
		o, err = e.reconcileLocked(ctx, o, true)
		if err != nil {
			return StopReceipt{}, err
		}
		if !o.Status.Terminal() {
			return StopReceipt{}, domain.ErrReconciliation
		}
	}
	if reopen {
		w, err = e.journal.completeAdoption(ctx, r)
	} else {
		w, err = e.journal.workspace(ctx, r.WorkspaceID)
	}
	if err != nil {
		return StopReceipt{}, err
	}
	receipt := StopReceipt{Workspace: w, NoActiveOperations: true}
	raw, _ := json.Marshal(receipt)
	err = artifact.WithPublication(ctx, e.config.Artifacts, func(locked context.Context) error {
		var err error
		receipt.Ref, err = e.config.Artifacts.Put(locked, r.TenantID, r.RunID, "workspace_stop", bytes.NewReader(raw))
		if err != nil {
			return err
		}
		return e.journal.pinArtifact(locked, receipt.Ref)
	})
	return receipt, err
}

func isProcess(kind string) bool { return kind == "run_command" || kind == "verify" }

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}
func readBounded(ctx context.Context, r io.Reader, max int64) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(contextReader{ctx, r}, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > max {
		return nil, domain.ErrCapacity
	}
	return b, nil
}

var _ Service = (*Engine)(nil)
