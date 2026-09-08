// Package application coordinates durable intentions and authenticated receipts.
// The reducer decides control flow; this driver owns I/O and crash windows.
package application

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/provider"
	"github.com/JDinSeattle/forge-runtime/internal/quota"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	flow "github.com/JDinSeattle/forge-runtime/internal/runtime"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
	"github.com/JDinSeattle/forge-runtime/internal/telemetry"
)

var errReload = errors.New("control state changed")
var errDeferred = errors.New("run durably deferred")

type ModelSpec struct {
	CredentialGroup string `json:"credential_group"`
	PriceVersion    string `json:"price_version"`
	// Rates are micro-US dollars per million tokens. When ExactPricing is false,
	// InputPrice must bound every input/cache class whose exact price is unknown;
	// OutputPrice is the output ceiling. They authorize reservation, not invoices.
	InputPrice        domain.Money  `json:"input_price"`
	OutputPrice       domain.Money  `json:"output_price"`
	CacheReadPrice    *domain.Money `json:"cache_read_price,omitempty"`
	CacheWrite5mPrice *domain.Money `json:"cache_write_5m_price,omitempty"`
	CacheWrite1hPrice *domain.Money `json:"cache_write_1h_price,omitempty"`
	ExactPricing      bool          `json:"exact_pricing"`
	MaxOutputTokens   int64         `json:"max_output_tokens"`
	ContextTokens     int64         `json:"context_tokens"`
	RequestTimeout    time.Duration `json:"request_timeout_ns"`
}
type SourceSpec struct {
	Hash      string
	HasTarget bool
}
type Driver struct {
	Store               *persistence.Store
	Quota               *quota.Store
	Runner              runner.Service
	RunnerID            string
	Signer              *runner.Signer
	Artifacts           artifact.Store
	Providers           map[string]provider.Provider
	Models              map[string]ModelSpec
	Sources             map[string]SourceSpec
	TrustedVerification bool
	Logger              *slog.Logger
	LeaseDuration       time.Duration
	ProviderFactory     func(persistence.Run) provider.Provider
	Fault               func(string) error
	Telemetry           *telemetry.Telemetry
}

func (d *Driver) fault(point string) error {
	if d.Fault != nil {
		return d.Fault(point)
	}
	return nil
}
func (d *Driver) leaseDuration() time.Duration {
	if d.LeaseDuration > 0 {
		return d.LeaseDuration
	}
	return 30 * time.Second
}
func (d *Driver) RunWorker(ctx context.Context, owner string, slots int) error {
	if slots < 1 || slots > 64 {
		return domain.ErrInvalid
	}
	var wg sync.WaitGroup
	for n := range slots {
		wg.Go(func() {
			worker := fmt.Sprintf("%s-%d", owner, n)
			ticker := time.NewTicker(150 * time.Millisecond)
			defer ticker.Stop()
			for {
				if ctx.Err() != nil {
					return
				}
				var r persistence.Run
				var err error
				if d.RunnerID != "" {
					r, err = d.Store.ClaimOnRunner(ctx, worker, d.leaseDuration(), d.RunnerID)
				} else {
					r, err = d.Store.Claim(ctx, worker, d.leaseDuration())
				}
				if err == nil {
					err = d.Drive(ctx, r)
					if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, domain.ErrFenced) && !errors.Is(err, errDeferred) && d.Logger != nil {
						d.Logger.Warn("run yielded", "run_id", r.ID, "error", err)
					}
				} else if !errors.Is(err, domain.ErrNotFound) && !errors.Is(err, domain.ErrCapacity) && d.Logger != nil {
					d.Logger.Warn("claim failed", "error", err)
				}
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
		})
	}
	wg.Wait()
	return ctx.Err()
}
func (d *Driver) Drive(parent context.Context, claimed persistence.Run) error {
	if d.RunnerID != "" && claimed.RunnerID != d.RunnerID {
		return domain.ErrForbidden
	}
	status := claimed.State.Status
	if d.Telemetry != nil {
		var end func(domain.RunStatus)
		parent, end = d.Telemetry.StartRun(telemetry.ContextFromTraceparent(parent, claimed.Traceparent))
		defer func() { end(status) }()
		if claimed.State.Lease.Epoch == 1 {
			d.Telemetry.ObserveDispatchLatency(time.Since(claimed.CreatedAt))
		}
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(d.leaseDuration() / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				hctx, end := context.WithTimeout(ctx, 3*time.Second)
				_, err := d.Store.Heartbeat(hctx, claimed.TenantID, claimed.ID, claimed.State.Lease.Owner, claimed.State.Lease.Epoch, d.leaseDuration())
				end()
				if err != nil {
					cancel()
					return
				}
			}
		}
	}()
	defer func() { cancel(); <-heartbeatDone }()
	for ctx.Err() == nil {
		r, err := d.Store.GetRun(ctx, claimed.TenantID, claimed.ID)
		if err != nil {
			return err
		}
		status = r.State.Status
		if r.State.Status.Terminal() || r.State.Status == domain.StatusWaitingApproval || r.State.Status == domain.StatusQueued {
			return nil
		}
		if r.State.Lease.Owner != claimed.State.Lease.Owner || r.State.Lease.Epoch != claimed.State.Lease.Epoch {
			return domain.ErrFenced
		}
		if len(r.Commands) == 0 {
			return d.deferRun(ctx, r, time.Now().Add(5*time.Second), "run.reconciliation_wait")
		}
		event, err := d.execute(ctx, r, r.Commands[0])
		if errors.Is(err, errReload) {
			continue
		}
		if errors.Is(err, errDeferred) || errors.Is(err, domain.ErrFenced) || errors.Is(err, context.Canceled) {
			return err
		}
		if err != nil {
			if r.State.Status == domain.StatusCancelRequested || r.State.Status == domain.StatusNeedsReconciliation || errors.Is(err, domain.ErrReconciliation) {
				return d.deferRun(ctx, r, time.Now().Add(5*time.Second), "run.reconciliation_wait")
			}
			if d.Logger != nil {
				d.Logger.Warn("step failed", "run_id", r.ID, "stage", r.State.Stage, "error", err)
			}
			event = flow.Event{Kind: flow.EventFailed, Reason: err.Error()}
		}
		event.ExpectedVersion = r.State.Version
		event.Owner = r.State.Lease.Owner
		event.Epoch = r.State.Lease.Epoch
		if _, err = d.Store.Advance(ctx, r.TenantID, r.ID, event); errors.Is(err, domain.ErrConflict) || errors.Is(err, domain.ErrTransition) {
			fresh, readErr := d.Store.GetRun(ctx, r.TenantID, r.ID)
			if readErr != nil {
				return readErr
			}
			if fresh.State.Version == r.State.Version {
				return err
			}
			continue
		} else if err != nil {
			return err
		}
	}
	return ctx.Err()
}
func (d *Driver) deferRun(ctx context.Context, r persistence.Run, until time.Time, kind string) error {
	if err := d.Store.Defer(ctx, r, until, kind); err != nil {
		return err
	}
	return errDeferred
}
func (d *Driver) grant(ctx context.Context, r persistence.Run, permissions ...string) (runner.WorkspaceRequest, error) {
	lease, now, err := d.Store.LeaseProof(ctx, r.TenantID, r.ID, r.State.Lease.Owner, r.State.Lease.Epoch)
	if err != nil {
		return runner.WorkspaceRequest{}, err
	}
	expires := lease.Until.Add(-d.Signer.Skew)
	if max := now.Add(20 * time.Second); expires.After(max) {
		expires = max
	}
	claims := runner.Claims{TenantID: r.TenantID, RunID: r.ID, WorkspaceID: r.ID, Epoch: lease.Epoch, Permissions: permissions, IssuedAt: now, ExpiresAt: expires}
	token, err := d.Signer.Sign(claims, lease.Until)
	return runner.WorkspaceRequest{TenantID: r.TenantID, RunID: r.ID, WorkspaceID: r.ID, Epoch: lease.Epoch, Grant: token}, err
}
func (d *Driver) publish(ctx context.Context, ref artifact.Ref) (string, error) {
	if _, err := d.Artifacts.Stat(ctx, ref.TenantID, ref.RunID, ref); err != nil {
		return "", err
	}
	digest := sha256.Sum256([]byte(string(ref.TenantID) + ":" + string(ref.RunID) + ":" + ref.Kind + ":" + ref.SHA256))
	id := domain.ID("artifact_" + hex.EncodeToString(digest[:]))
	err := d.Store.PublishArtifact(ctx, persistence.Artifact{TenantID: ref.TenantID, ID: id, RunID: ref.RunID, Kind: ref.Kind, ObjectKey: ref.ObjectKey, SHA256: ref.SHA256, ByteSize: ref.Size})
	if err == nil && d.Telemetry != nil {
		d.Telemetry.ObserveArtifact(ref.Kind, ref.Size)
	}
	return string(id), err
}
func (d *Driver) put(ctx context.Context, r persistence.Run, kind string, v any) (string, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	ref, err := d.Artifacts.Put(ctx, r.TenantID, r.ID, kind, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	return d.publish(ctx, ref)
}
func (d *Driver) load(ctx context.Context, r persistence.Run, id string, target any) error {
	meta, err := d.Store.GetArtifact(ctx, r.TenantID, domain.ID(id))
	if err != nil {
		return err
	}
	if meta.RunID != r.ID {
		return domain.ErrForbidden
	}
	reader, err := d.Artifacts.Open(ctx, r.TenantID, r.ID, artifact.Ref{TenantID: r.TenantID, RunID: r.ID, Kind: meta.Kind, ObjectKey: meta.ObjectKey, SHA256: meta.SHA256, Size: meta.ByteSize})
	if err != nil {
		return err
	}
	defer reader.Close()
	return json.NewDecoder(io.LimitReader(reader, 8<<20)).Decode(target)
}

type baseline struct {
	SourceHash   string `json:"source_hash"`
	TargetFailed bool   `json:"target_failed"`
	ReceiptRef   string `json:"receipt_ref"`
	Revision     uint64 `json:"revision"`
}

func (d *Driver) execute(ctx context.Context, r persistence.Run, command flow.Command) (flow.Event, error) {
	switch command.Kind {
	case flow.CommandInitializeWorkspace:
		project, err := d.Store.GetProject(ctx, r.TenantID, r.ProjectID)
		if err != nil {
			return flow.Event{}, err
		}
		source, ok := d.Sources[project.SourceID]
		if !ok || source.Hash != r.BaseCommit {
			return flow.Event{}, domain.ErrConflict
		}
		request, err := d.grant(ctx, r, "prepare", "adopt")
		if err != nil {
			return flow.Event{}, err
		}
		workspace, err := d.Runner.PrepareWorkspace(ctx, runner.PrepareRequest{WorkspaceRequest: request, SourceID: project.SourceID, ProfileID: project.ProfileID})
		if errors.Is(err, domain.ErrFenced) {
			if _, err = d.Runner.AdoptWorkspace(ctx, request); err == nil {
				workspace, err = d.Runner.PrepareWorkspace(ctx, runner.PrepareRequest{WorkspaceRequest: request, SourceID: project.SourceID, ProfileID: project.ProfileID})
			}
		}
		if err != nil {
			return flow.Event{}, err
		}
		if workspace.BaselineHash != source.Hash {
			return flow.Event{}, fmt.Errorf("%w: registered source mutated before import", domain.ErrUntrusted)
		}
		evidence := baseline{SourceHash: source.Hash, Revision: workspace.Revision}
		if source.HasTarget {
			op, receipt, err := d.systemOperation(ctx, r, "initial_target", -1, "verify", json.RawMessage(`{"target":true}`), workspace.Revision)
			if err != nil {
				return flow.Event{}, err
			}
			var job sandbox.Job
			if err = json.Unmarshal(op.Result, &job); err != nil {
				return flow.Event{}, err
			}
			evidence.TargetFailed = job.Started && !job.Running && job.ExitCode != 0
			evidence.ReceiptRef = receipt.Ref
			evidence.Revision = op.AfterRevision
		}
		ref, err := d.put(ctx, r, "baseline", evidence)
		return flow.Event{Kind: flow.EventWorkspaceReady, OutputRef: ref, WorkspaceRevision: evidence.Revision}, err
	case flow.CommandAdoptWorkspace:
		request, err := d.grant(ctx, r, "adopt")
		if err != nil {
			return flow.Event{}, err
		}
		stopped, err := d.Runner.AdoptWorkspace(ctx, request)
		if err != nil {
			return flow.Event{}, err
		}
		ref, err := d.publish(ctx, stopped.Ref)
		return flow.Event{Kind: flow.EventWorkspaceAdopted, Stop: &flow.StopReceipt{Ref: ref, NoActiveOperations: stopped.NoActiveOperations, WorkspaceRevision: stopped.Workspace.Revision}}, err
	case flow.CommandBuildContext:
		return d.buildContext(ctx, r)
	case flow.CommandCallModel:
		return d.callModel(ctx, r)
	case flow.CommandValidateTools:
		return d.validateTools(ctx, r)
	case flow.CommandExecuteEffect, flow.CommandInspectEffect:
		if command.Effect == nil {
			return flow.Event{}, domain.ErrInvalid
		}
		op, err := d.operation(ctx, r, *command.Effect, command.Kind == flow.CommandInspectEffect)
		if err != nil {
			if command.Kind == flow.CommandExecuteEffect && !errors.Is(err, errReload) && !errors.Is(err, domain.ErrFenced) && !errors.Is(err, context.Canceled) {
				return flow.Event{Kind: flow.EventEffectUncertain, Reason: err.Error()}, nil
			}
			return flow.Event{}, err
		}
		receipt, err := d.receipt(ctx, op)
		kind := flow.EventEffectCompleted
		if command.Kind == flow.CommandInspectEffect {
			kind = flow.EventReconciled
		}
		return flow.Event{Kind: kind, Receipt: &receipt}, err
	case flow.CommandIngestResults:
		ref, err := d.put(ctx, r, "tool_batch", map[string]any{"step": r.State.StepSeq, "revision": r.State.WorkspaceRevision})
		return flow.Event{Kind: flow.EventResultsIngested, OutputRef: ref}, err
	case flow.CommandVerify:
		return d.verify(ctx, r)
	case flow.CommandFinalize:
		op, _, err := d.systemOperation(ctx, r, "final_diff", -4, "get_diff", json.RawMessage(`{}`), r.State.WorkspaceRevision)
		if err != nil {
			return flow.Event{}, err
		}
		if _, err = d.put(ctx, r, "patch_manifest", json.RawMessage(op.Result)); err != nil {
			return flow.Event{}, err
		}
		patch, err := unifiedPatch(op.Result)
		if err != nil {
			return flow.Event{}, err
		}
		object, err := d.Artifacts.Put(ctx, r.TenantID, r.ID, "patch", bytes.NewReader(patch))
		if err != nil {
			return flow.Event{}, err
		}
		ref, err := d.publish(ctx, object)
		return flow.Event{Kind: flow.EventFinalized, OutputRef: ref}, err
	case flow.CommandStopExecution:
		return d.stop(ctx, r)
	default:
		return flow.Event{}, fmt.Errorf("%w: unsupported persisted command %s", domain.ErrInvalid, command.Kind)
	}
}
func operationID(r persistence.Run, ordinal int) domain.ID {
	return domain.ID(fmt.Sprintf("%s_step_%d_op_%d", r.ID, r.State.StepSeq, ordinal))
}
func (d *Driver) systemOperation(ctx context.Context, r persistence.Run, name string, ordinal int, kind string, args json.RawMessage, revision uint64) (runner.Operation, flow.EffectReceipt, error) {
	digest := sha256.Sum256(args)
	e := flow.Effect{ID: domain.ID(fmt.Sprintf("%s_%d_%s", r.ID, r.State.StepSeq, name)), Kind: kind, Args: args, ArgsHash: hex.EncodeToString(digest[:]), ExpectedRevision: revision, DispatchEpoch: r.State.Lease.Epoch, PolicyVersion: "trusted-profile-v1"}
	e, err := d.Store.PlanSystemEffect(ctx, r, e, ordinal)
	if err != nil {
		return runner.Operation{}, flow.EffectReceipt{}, err
	}
	op, err := d.operation(ctx, r, e, false)
	if err != nil {
		return op, flow.EffectReceipt{}, err
	}
	receipt, err := d.receipt(ctx, op)
	if err != nil {
		return op, receipt, err
	}
	return op, receipt, d.Store.SettleSystemEffect(ctx, r, receipt)
}
func (d *Driver) operation(ctx context.Context, r persistence.Run, e flow.Effect, inspectOnly bool) (result runner.Operation, resultErr error) {
	if d.Telemetry != nil {
		var end func(telemetry.Outcome)
		ctx, end = d.Telemetry.StartEffect(ctx, e.Kind)
		defer func() {
			outcome := telemetry.Success
			if resultErr != nil || result.Status == runner.Unknown {
				outcome = telemetry.Unknown
			} else if result.Status == runner.Failed {
				outcome = telemetry.Failed
			} else if result.Status == runner.Cancelled {
				outcome = telemetry.Cancelled
			}
			end(outcome)
		}()
	}
	request, err := d.grant(ctx, r, "inspect", "execute", "verify")
	if err != nil {
		return runner.Operation{}, err
	}
	op, err := d.Runner.InspectOperation(ctx, runner.InspectRequest{WorkspaceRequest: request, OperationID: e.ID})
	if errors.Is(err, domain.ErrNotFound) && !inspectOnly && e.DispatchEpoch == r.State.Lease.Epoch {
		if err = d.fault("before_start_operation"); err != nil {
			return op, err
		}
		op, err = d.Runner.StartOperation(ctx, runner.OperationRequest{WorkspaceRequest: request, OperationID: e.ID, ExpectedRevision: e.ExpectedRevision, Kind: e.Kind, Args: e.Args, ArgsHash: e.ArgsHash, PolicyVersion: e.PolicyVersion, Deadline: r.State.Limits.Deadline})
	}
	if err != nil {
		return op, err
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for !op.Status.Terminal() {
		if op.Status == runner.Unknown {
			return op, domain.ErrReconciliation
		}
		select {
		case <-ctx.Done():
			return op, ctx.Err()
		case <-ticker.C:
		}
		current, err := d.Store.GetRun(ctx, r.TenantID, r.ID)
		if err != nil {
			return op, err
		}
		if current.State.Status == domain.StatusCancelRequested {
			return op, errReload
		}
		request, err = d.grant(ctx, r, "inspect")
		if err != nil {
			return op, err
		}
		op, err = d.Runner.InspectOperation(ctx, runner.InspectRequest{WorkspaceRequest: request, OperationID: e.ID})
		if err != nil {
			return op, err
		}
	}
	if op.Request.TenantID != r.TenantID || op.Request.RunID != r.ID || op.Request.WorkspaceID != r.ID || op.Request.OperationID != e.ID || op.Request.ArgsHash != e.ArgsHash || op.Request.Kind != e.Kind || op.Request.PolicyVersion != e.PolicyVersion || op.Request.ExpectedRevision != e.ExpectedRevision || op.Request.Epoch != e.DispatchEpoch {
		return op, domain.ErrUntrusted
	}
	return op, nil
}
func (d *Driver) receipt(ctx context.Context, op runner.Operation) (flow.EffectReceipt, error) {
	if !op.Status.Terminal() {
		return flow.EffectReceipt{}, domain.ErrReconciliation
	}
	ref, err := d.publish(ctx, op.Receipt)
	return flow.EffectReceipt{EffectID: op.Request.OperationID, ArgsHash: op.Request.ArgsHash, Epoch: op.Request.Epoch, Status: flow.EffectStatus(op.Status), BeforeRevision: op.Request.ExpectedRevision, AfterRevision: op.AfterRevision, Ref: ref, Settled: true}, err
}
func (d *Driver) stop(ctx context.Context, r persistence.Run) (flow.Event, error) {
	request, err := d.grant(ctx, r, "cancel", "inspect")
	if err != nil {
		return flow.Event{}, err
	}
	stopped, err := d.Runner.StopWorkspace(ctx, request)
	if errors.Is(err, domain.ErrNotFound) && r.State.WorkspaceRevision == 0 {
		ref, err := d.put(ctx, r, "stop_receipt", map[string]string{"proof": "authenticated runner reports no prepared workspace"})
		return flow.Event{Kind: flow.EventCancellationConfirmed, Stop: &flow.StopReceipt{Ref: ref, NoActiveOperations: true}}, err
	}
	if err != nil {
		return flow.Event{}, err
	}
	if e := r.State.PendingEffect; e != nil && (e.Status == flow.EffectInFlight || e.Status == flow.EffectUnknown) {
		op, err := d.Runner.InspectOperation(ctx, runner.InspectRequest{WorkspaceRequest: request, OperationID: e.ID})
		if err != nil {
			return flow.Event{}, err
		}
		receipt, err := d.receipt(ctx, op)
		return flow.Event{Kind: flow.EventReconciled, Receipt: &receipt}, err
	}
	ref, err := d.publish(ctx, stopped.Ref)
	return flow.Event{Kind: flow.EventCancellationConfirmed, Stop: &flow.StopReceipt{Ref: ref, NoActiveOperations: stopped.NoActiveOperations, WorkspaceRevision: stopped.Workspace.Revision}}, err
}
func (d *Driver) verify(ctx context.Context, r persistence.Run) (flow.Event, error) {
	items, err := d.Store.ListArtifacts(ctx, r.TenantID, r.ID, "", 1000)
	if err != nil {
		return flow.Event{}, err
	}
	var b baseline
	found := false
	for _, item := range items {
		if item.Kind == "baseline" {
			if err = d.load(ctx, r, string(item.ID), &b); err != nil {
				return flow.Event{}, err
			}
			found = true
			break
		}
	}
	if !found {
		return flow.Event{}, domain.ErrUntrusted
	}
	project, err := d.Store.GetProject(ctx, r.TenantID, r.ProjectID)
	if err != nil {
		return flow.Event{}, err
	}
	source := d.Sources[project.SourceID]
	targetPassed := false
	refs := []string{b.ReceiptRef}
	if source.HasTarget {
		op, receipt, err := d.systemOperation(ctx, r, "final_target", -2, "verify", json.RawMessage(`{"target":true}`), r.State.WorkspaceRevision)
		if err != nil {
			return flow.Event{}, err
		}
		var job sandbox.Job
		if err = json.Unmarshal(op.Result, &job); err != nil {
			return flow.Event{}, err
		}
		targetPassed = op.Status == runner.Succeeded && job.Started && !job.Running && job.ExitCode == 0
		refs = append(refs, receipt.Ref)
		if op.AfterRevision != r.State.WorkspaceRevision {
			return flow.Event{}, domain.ErrReconciliation
		}
	}
	op, receipt, err := d.systemOperation(ctx, r, "regression", -3, "verify", json.RawMessage(`{}`), r.State.WorkspaceRevision)
	if err != nil {
		return flow.Event{}, err
	}
	var job sandbox.Job
	if err = json.Unmarshal(op.Result, &job); err != nil {
		return flow.Event{}, err
	}
	if op.AfterRevision != r.State.WorkspaceRevision {
		return flow.Event{}, domain.ErrReconciliation
	}
	refs = append(refs, receipt.Ref)
	evidence := flow.VerificationEvidence{Trusted: d.TrustedVerification, WorkspaceRevision: r.State.WorkspaceRevision, BaselineTargetFailed: b.TargetFailed, TargetPassed: targetPassed, RegressionPassed: op.Status == runner.Succeeded && job.Started && !job.Running && job.ExitCode == 0}
	ref, err := d.put(ctx, r, "verification_report", struct {
		Evidence   flow.VerificationEvidence `json:"evidence"`
		Receipts   []string                  `json:"receipts"`
		SourceHash string                    `json:"source_hash"`
	}{evidence, refs, b.SourceHash})
	evidence.ReportRef = ref
	return flow.Event{Kind: flow.EventVerificationCompleted, Verification: &evidence}, err
}
