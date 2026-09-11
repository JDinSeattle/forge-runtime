package runtime

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
)

// Transition is deterministic and never performs I/O. On error it returns the
// original state and no commands. A caller must commit the returned state,
// snapshot, effect intentions and business events atomically using a database
// CAS on ExpectedVersion and the lease. Only then may it dispatch commands.
func Transition(state State, event Event) (State, []Command, error) {
	fail := func(err error) (State, []Command, error) { return state, nil, err }
	if err := validateState(state); err != nil {
		return fail(err)
	}
	if event.ExpectedVersion != state.Version {
		return fail(fmt.Errorf("%w: expected run version %d, have %d", domain.ErrConflict, event.ExpectedVersion, state.Version))
	}
	if state.Status.Terminal() {
		return fail(domain.ErrTerminal)
	}
	if state.Version == math.MaxUint64 || event.At.IsZero() || event.Cost < 0 {
		return fail(fmt.Errorf("%w: invalid event clock, cost, or exhausted version", domain.ErrInvalid))
	}
	if !controlEvent(event.Kind) && event.Kind != EventClaimed {
		if event.Owner != state.Lease.Owner || event.Epoch != state.Lease.Epoch || !state.Lease.ValidAt(event.At) {
			return fail(domain.ErrFenced)
		}
	}
	// Cancellation dominates late model/tool/verification responses. Actual
	// receipts can still be reconciled, but can never reopen the normal loop.
	if state.Status == domain.StatusCancelRequested && !allowedDuringStop(event.Kind) {
		return fail(fmt.Errorf("%w: cancellation is awaiting executor acknowledgment", domain.ErrTransition))
	}
	if state.Status == domain.StatusNeedsReconciliation && !allowedDuringReconciliation(event.Kind) {
		return fail(domain.ErrReconciliation)
	}
	if state.Status == domain.StatusWaitingApproval && event.Kind != EventApprovalDecided && event.Kind != EventCancelRequested {
		return fail(fmt.Errorf("%w: approval is pending", domain.ErrTransition))
	}
	next := cloneState(state)
	var commands []Command
	var err error
	switch event.Kind {
	case EventClaimed:
		commands, err = claim(&next, event)
	case EventCapacityRejected:
		// This event means PrepareWorkspace definitively refused admission. It
		// cannot discard existing work or an uncertain operation. Persistence
		// additionally checks the durable ledgers and releases the allocation in
		// the same transaction that records this transition and its retry time.
		if next.Status != domain.StatusRunning || next.Stage != domain.StageInitialize || next.WorkspaceRevision != 0 || next.StepSeq != 0 || next.ModelRounds != 0 || next.ToolCalls != 0 || next.Cost != 0 || next.OutputRef != "" || next.PendingEffect != nil || len(next.RemainingEffects) != 0 || next.Approval != nil || next.ResumeStage != "" {
			err = fmt.Errorf("%w: capacity rejection requires an uninitialized run", domain.ErrTransition)
			break
		}
		if event.NotBefore == nil || !event.NotBefore.After(event.At) {
			err = fmt.Errorf("%w: capacity rejection requires a future retry time", domain.ErrInvalid)
			break
		}
		next.Status = domain.StatusQueued
		next.Lease = domain.Lease{Epoch: next.Lease.Epoch}
	case EventWorkspaceReady:
		if err = requireStage(next, domain.StageInitialize); err == nil {
			if event.OutputRef == "" || event.WorkspaceRevision == 0 {
				err = fmt.Errorf("%w: missing durable workspace receipt or revision", domain.ErrInvalid)
				break
			}
			next.WorkspaceRevision = event.WorkspaceRevision
			next.OutputRef = event.OutputRef
			next.Stage = domain.StageBuildContext
			commands = command(CommandBuildContext)
		}
	case EventWorkspaceAdopted:
		commands, err = adopt(&next, event)
	case EventContextBuilt:
		if err = requireStage(next, domain.StageBuildContext); err != nil {
			break
		}
		if event.OutputRef == "" {
			err = fmt.Errorf("%w: context is not durably referenced", domain.ErrInvalid)
			break
		}
		if err = observeProgress(&next, event); err != nil {
			break
		}
		if budgetReached(next, event, true, false) {
			commands = stop(&next, domain.StatusBudgetExhausted, "model budget reached")
			break
		}
		if next.Progress != nil && next.Limits.MaxNoProgressBatches > 0 && next.Progress.RepeatedBatches >= next.Limits.MaxNoProgressBatches {
			commands = stop(&next, domain.StatusBudgetExhausted, "repeated_no_progress")
			break
		}
		next.OutputRef = event.OutputRef
		next.Stage = domain.StageModel
		next.ModelRounds++
		next.StepSeq++
		commands = command(CommandCallModel)
	case EventModelCompleted:
		if err = requireStage(next, domain.StageModel); err != nil {
			break
		}
		if !event.Complete || event.OutputRef == "" {
			err = fmt.Errorf("%w: partial or unpersisted model response", domain.ErrInvalid)
			break
		}
		next.Cost, err = next.Cost.Add(event.Cost)
		if err != nil {
			break
		}
		next.OutputRef = event.OutputRef
		if budgetReached(next, event, false, false) {
			commands = stop(&next, domain.StatusBudgetExhausted, "cost or runtime budget reached")
		} else if event.Finish {
			next.Stage = domain.StageVerify
			commands = command(CommandVerify)
		} else {
			next.Stage = domain.StageValidateTools
			commands = command(CommandValidateTools)
		}
	case EventToolsValidated:
		if err = requireStage(next, domain.StageValidateTools); err != nil {
			break
		}
		if event.OutputRef == "" || !event.Complete || len(event.Effects) == 0 {
			err = fmt.Errorf("%w: validated tool batch requires a durable result and complete calls", domain.ErrInvalid)
			break
		}
		if len(event.Effects) > 128 {
			err = fmt.Errorf("%w: tool batch exceeds 128 calls", domain.ErrInvalid)
			break
		}
		seen := make(map[domain.ID]struct{}, len(event.Effects))
		for _, effect := range event.Effects {
			if err = validateEffect(effect); err != nil {
				break
			}
			if _, exists := seen[effect.ID]; exists {
				err = fmt.Errorf("%w: duplicate operation ID in tool batch", domain.ErrConflict)
				break
			}
			seen[effect.ID] = struct{}{}
		}
		if err != nil {
			break
		}
		next.OutputRef = event.OutputRef
		next.RemainingEffects = cloneEffects(event.Effects)
		commands = scheduleEffect(&next, event)
	case EventEffectCompleted:
		if err = requireStage(next, domain.StageExecuteEffects); err != nil {
			break
		}
		commands, err = settleEffect(&next, event, false)
	case EventEffectUncertain:
		if err = requireStage(next, domain.StageExecuteEffects); err != nil {
			break
		}
		if !unsettled(next.PendingEffect) {
			err = fmt.Errorf("%w: no pending dispatched effect", domain.ErrTransition)
			break
		}
		next.PendingEffect.Status = EffectUnknown
		next.Status = domain.StatusNeedsReconciliation
		next.ResumeStage = domain.StageExecuteEffects
		next.Stage = domain.StageReconcile
		next.FailureReason = event.Reason
		commands = []Command{{Kind: CommandInspectEffect, Effect: cloneEffect(next.PendingEffect)}}
	case EventResultsIngested:
		if err = requireStage(next, domain.StageIngestResults); err != nil {
			break
		}
		if event.OutputRef == "" {
			err = fmt.Errorf("%w: missing persisted tool results", domain.ErrInvalid)
			break
		}
		next.OutputRef = event.OutputRef
		next.Stage = domain.StageBuildContext
		commands = command(CommandBuildContext)
	case EventApprovalDecided:
		commands, err = decideApproval(&next, event)
	case EventVerificationCompleted:
		commands, err = verify(&next, event)
	case EventFinalized:
		if err = requireStage(next, domain.StageFinalize); err != nil {
			break
		}
		if event.OutputRef == "" || next.VerificationReportRef == "" || next.VerificationRevision != next.WorkspaceRevision || next.Verification == domain.VerificationUnverified || unsettled(next.PendingEffect) || len(next.RemainingEffects) != 0 {
			err = fmt.Errorf("%w: finalization lacks settled effects or current verification", domain.ErrInvalid)
			break
		}
		next.OutputRef = event.OutputRef
		next.Status, next.Stage = domain.StatusCompleted, domain.StageStopped
		commands = command(CommandPublishTerminal)
	case EventCancelRequested:
		if next.Status == domain.StatusCancelRequested {
			// The application may return the current state for repeated cancellation;
			// the reducer does not allocate another event/version for a no-op.
			return state, nil, nil
		}
		commands = stop(&next, domain.StatusCancelled, "cancellation requested")
	case EventCancellationConfirmed:
		commands, err = confirmStop(&next, event)
	case EventResumeRequested:
		if next.Status != domain.StatusNeedsReconciliation {
			err = fmt.Errorf("%w: only reconciliation state is resumable", domain.ErrTransition)
			break
		}
		// Resume requests are control-plane intent. They neither issue a new
		// tool call nor bypass the existing effect's receipt. A worker must claim
		// a new epoch and adopt the original workspace before inspecting it.
		if next.Lease.ValidAt(event.At) {
			err = fmt.Errorf("%w: a worker still owns reconciliation", domain.ErrConflict)
			break
		}
		next.Lease.Owner, next.Lease.Until = "", event.At
	case EventReconciled:
		if next.Status != domain.StatusNeedsReconciliation && next.Status != domain.StatusCancelRequested {
			err = fmt.Errorf("%w: no uncertain effect to reconcile", domain.ErrTransition)
			break
		}
		if next.Status == domain.StatusNeedsReconciliation && next.Stage != domain.StageReconcile {
			err = fmt.Errorf("%w: workspace adoption barrier has not completed", domain.ErrTransition)
			break
		}
		commands, err = settleEffect(&next, event, true)
	case EventFailed:
		if strings.TrimSpace(event.Reason) == "" {
			err = fmt.Errorf("%w: failure requires a reason", domain.ErrInvalid)
			break
		}
		commands = stop(&next, domain.StatusFailed, event.Reason)
	case EventBudgetReached:
		commands = stop(&next, domain.StatusBudgetExhausted, event.Reason)
	default:
		err = fmt.Errorf("%w: unknown event kind %q", domain.ErrInvalid, event.Kind)
	}
	if err != nil {
		return fail(err)
	}
	next.Version++
	if err = validateState(next); err != nil {
		return fail(err)
	}
	return next, commands, nil
}

func claim(next *State, event Event) ([]Command, error) {
	if event.Lease == nil || !event.Lease.ValidAt(event.At) || event.Lease.Epoch <= next.Lease.Epoch || next.Lease.ValidAt(event.At) || event.Owner != event.Lease.Owner || event.Epoch != event.Lease.Epoch {
		return nil, domain.ErrFenced
	}
	if next.Status != domain.StatusQueued && next.Status != domain.StatusRunning && next.Status != domain.StatusNeedsReconciliation && next.Status != domain.StatusCancelRequested {
		return nil, domain.ErrTransition
	}
	next.Lease = *event.Lease
	if next.Status == domain.StatusCancelRequested {
		return command(CommandStopExecution), nil
	}
	if next.Status != domain.StatusNeedsReconciliation {
		next.Status = domain.StatusRunning
	}
	if next.WorkspaceRevision != 0 {
		if next.Stage != domain.StageAdoptWorkspace {
			next.ResumeStage = next.Stage
		}
		next.Stage = domain.StageAdoptWorkspace
		return command(CommandAdoptWorkspace), nil
	}
	if next.Stage != domain.StageInitialize || next.PendingEffect != nil {
		return nil, fmt.Errorf("%w: workspace absent for existing progress", domain.ErrInvalid)
	}
	return command(CommandInitializeWorkspace), nil
}

func adopt(next *State, event Event) ([]Command, error) {
	if next.Stage != domain.StageAdoptWorkspace || (next.Status != domain.StatusRunning && next.Status != domain.StatusNeedsReconciliation) {
		return nil, domain.ErrTransition
	}
	if event.Stop == nil || !event.Stop.NoActiveOperations || event.Stop.Ref == "" || event.Stop.WorkspaceRevision < next.WorkspaceRevision {
		return nil, fmt.Errorf("%w: adoption requires durable old-writer stop evidence", domain.ErrUntrusted)
	}
	if unsettled(next.PendingEffect) {
		next.ReconciliationRevision = event.Stop.WorkspaceRevision
		next.Status, next.Stage = domain.StatusNeedsReconciliation, domain.StageReconcile
		return []Command{{Kind: CommandInspectEffect, Effect: cloneEffect(next.PendingEffect)}}, nil
	}
	// A changed revision with no recorded effect cannot be attributed safely.
	if event.Stop.WorkspaceRevision != next.WorkspaceRevision {
		next.Status, next.Stage = domain.StatusNeedsReconciliation, domain.StageReconcile
		next.FailureReason = "workspace changed without a pending ledger effect"
		return nil, nil
	}
	next.Stage = next.ResumeStage
	next.ResumeStage = ""
	if next.Stage == domain.StageReconcile {
		return nil, domain.ErrReconciliation
	}
	if budgetReached(*next, event, false, next.Stage == domain.StageExecuteEffects) {
		return stop(next, domain.StatusBudgetExhausted, "budget reached while workspace was suspended"), nil
	}
	return commandForStage(next)
}

func decideApproval(next *State, event Event) ([]Command, error) {
	if next.Status != domain.StatusWaitingApproval || next.Stage != domain.StageApprovalGate || next.Approval == nil || next.PendingEffect == nil || event.Approval == nil {
		return nil, domain.ErrTransition
	}
	binding := event.Approval.Binding
	if binding != *next.Approval || binding.Granted || !approvalMatches(*next.Approval, *next.PendingEffect, next.WorkspaceRevision) {
		return nil, fmt.Errorf("%w: approval arguments, revision, policy or version changed", domain.ErrConflict)
	}
	if !event.Approval.Approve {
		return stop(next, domain.StatusFailed, "operation approval denied"), nil
	}
	if next.Approval.Version == math.MaxUint64 {
		return nil, domain.ErrOverflow
	}
	next.Approval.Granted = true
	next.Approval.Version++
	next.Status, next.Stage = domain.StatusQueued, domain.StageExecuteEffects
	return nil, nil // The scheduler reacquires a lease and runner adoption barrier.
}

func settleEffect(next *State, event Event, reconciliation bool) ([]Command, error) {
	if !unsettled(next.PendingEffect) || event.Receipt == nil {
		return nil, fmt.Errorf("%w: effect receipt without dispatched operation", domain.ErrTransition)
	}
	r, effect := event.Receipt, next.PendingEffect
	if r.EffectID != effect.ID || r.ArgsHash != effect.ArgsHash || r.Epoch != effect.DispatchEpoch || r.BeforeRevision != effect.ExpectedRevision || r.AfterRevision < r.BeforeRevision || r.Ref == "" || !r.Settled || (next.ReconciliationRevision != 0 && r.AfterRevision != next.ReconciliationRevision) {
		return nil, fmt.Errorf("%w: effect receipt binding or settlement is invalid", domain.ErrConflict)
	}
	if r.Status != EffectSucceeded && r.Status != EffectFailed && r.Status != EffectCancelled {
		return nil, fmt.Errorf("%w: effect outcome remains uncertain", domain.ErrReconciliation)
	}
	if !reconciliation && r.Epoch != next.Lease.Epoch {
		return nil, domain.ErrFenced
	}
	effect.Status, effect.ReceiptRef = r.Status, r.Ref
	next.WorkspaceRevision = r.AfterRevision
	next.ReconciliationRevision = 0
	next.OutputRef = r.Ref
	next.Verification = domain.VerificationUnverified
	next.VerificationReportRef = ""
	next.VerificationRevision = 0
	next.Approval = nil
	if next.Status == domain.StatusCancelRequested {
		// Keep the settled receipt for the final cancellation event and never
		// schedule another tool from the interrupted batch.
		return command(CommandStopExecution), nil
	}
	next.Status = domain.StatusRunning
	next.PendingEffect = nil
	next.FailureReason = ""
	if r.Status != EffectSucceeded {
		// A failed tool result is given back to the model. Later planned tools
		// may depend on it; discard those unstarted intentions instead.
		next.RemainingEffects = nil
	}
	return scheduleEffect(next, event), nil
}

func verify(next *State, event Event) ([]Command, error) {
	if err := requireStage(*next, domain.StageVerify); err != nil {
		return nil, err
	}
	v := event.Verification
	if v == nil || !v.Trusted || v.ReportRef == "" {
		return nil, domain.ErrUntrusted
	}
	if v.WorkspaceRevision != next.WorkspaceRevision {
		return nil, domain.ErrConflict
	}
	next.VerificationReportRef, next.VerificationRevision = v.ReportRef, v.WorkspaceRevision
	if !v.RegressionPassed || (v.BaselineTargetFailed && !v.TargetPassed) {
		next.Verification = domain.VerificationUnverified
		next.Stage = domain.StageBuildContext
		next.OutputRef = v.ReportRef
		return command(CommandBuildContext), nil
	}
	next.Verification = domain.VerificationRegressionOnly
	if v.BaselineTargetFailed && v.TargetPassed {
		next.Verification = domain.VerificationVerified
	}
	next.Stage = domain.StageFinalize
	return command(CommandFinalize), nil
}

func confirmStop(next *State, event Event) ([]Command, error) {
	if next.Status != domain.StatusCancelRequested || next.StopTarget == "" || event.Stop == nil || !event.Stop.NoActiveOperations || event.Stop.Ref == "" || event.Stop.WorkspaceRevision < next.WorkspaceRevision {
		return nil, fmt.Errorf("%w: stop has not been durably confirmed", domain.ErrUntrusted)
	}
	if unsettled(next.PendingEffect) {
		return nil, fmt.Errorf("%w: cancellation must settle the dispatched effect first", domain.ErrReconciliation)
	}
	if next.WorkspaceRevision != event.Stop.WorkspaceRevision {
		next.Verification = domain.VerificationUnverified
		next.VerificationReportRef = ""
		next.VerificationRevision = 0
	}
	next.WorkspaceRevision = event.Stop.WorkspaceRevision
	next.OutputRef = event.Stop.Ref
	next.Status, next.Stage = next.StopTarget, domain.StageStopped
	next.RemainingEffects = nil
	next.Approval = nil
	return command(CommandPublishTerminal), nil
}

func scheduleEffect(next *State, event Event) []Command {
	if len(next.RemainingEffects) == 0 {
		next.Stage = domain.StageIngestResults
		return command(CommandIngestResults)
	}
	if budgetReached(*next, event, false, true) {
		return stop(next, domain.StatusBudgetExhausted, "tool budget reached")
	}
	effect := next.RemainingEffects[0]
	next.RemainingEffects = next.RemainingEffects[1:]
	effect.ExpectedRevision = next.WorkspaceRevision
	effect.Status = EffectPlanned
	next.PendingEffect = &effect
	if effect.RequiresApproval {
		next.Approval = &ApprovalBinding{EffectID: effect.ID, ArgsHash: effect.ArgsHash, WorkspaceRevision: next.WorkspaceRevision, PolicyVersion: effect.PolicyVersion, Version: 1}
		next.Status, next.Stage = domain.StatusWaitingApproval, domain.StageApprovalGate
		next.Lease.Owner, next.Lease.Until = "", event.At
		return []Command{{Kind: CommandRequestApproval, Effect: cloneEffect(&effect), Binding: cloneApproval(next.Approval)}}
	}
	return dispatchEffect(next)
}

func dispatchEffect(next *State) []Command {
	next.Stage = domain.StageExecuteEffects
	next.PendingEffect.Status = EffectInFlight
	next.PendingEffect.DispatchEpoch = next.Lease.Epoch
	next.ToolCalls++
	return []Command{{Kind: CommandExecuteEffect, Effect: cloneEffect(next.PendingEffect)}}
}

func commandForStage(next *State) ([]Command, error) {
	switch next.Stage {
	case domain.StageBuildContext:
		return command(CommandBuildContext), nil
	case domain.StageModel:
		// This intention means inspect/reuse the persisted attempt first. The
		// driver owns the model-attempt ledger and may issue a bounded retry only
		// if it proves no completed result exists.
		return command(CommandCallModel), nil
	case domain.StageValidateTools:
		return command(CommandValidateTools), nil
	case domain.StageIngestResults:
		return command(CommandIngestResults), nil
	case domain.StageVerify:
		return command(CommandVerify), nil
	case domain.StageFinalize:
		return command(CommandFinalize), nil
	case domain.StageExecuteEffects:
		if next.PendingEffect == nil || next.PendingEffect.Status != EffectPlanned {
			return nil, domain.ErrReconciliation
		}
		if next.PendingEffect.RequiresApproval && (next.Approval == nil || !next.Approval.Granted || !approvalMatches(*next.Approval, *next.PendingEffect, next.WorkspaceRevision)) {
			return nil, fmt.Errorf("%w: approval no longer binds current operation", domain.ErrConflict)
		}
		return dispatchEffect(next), nil
	default:
		return nil, domain.ErrTransition
	}
}

func stop(next *State, target domain.RunStatus, reason string) []Command {
	next.StopTarget, next.Status, next.FailureReason = target, domain.StatusCancelRequested, reason
	return command(CommandStopExecution)
}

func budgetReached(state State, event Event, model, tool bool) bool {
	return (!state.Limits.Deadline.IsZero() && !event.At.Before(state.Limits.Deadline)) ||
		(state.Limits.MaxCost > 0 && state.Cost >= state.Limits.MaxCost) ||
		(model && state.Limits.MaxModelRounds > 0 && state.ModelRounds >= state.Limits.MaxModelRounds) ||
		(tool && state.Limits.MaxToolCalls > 0 && state.ToolCalls >= state.Limits.MaxToolCalls) ||
		(model && (state.ModelRounds == math.MaxUint64 || state.StepSeq == math.MaxUint64)) ||
		(tool && state.ToolCalls == math.MaxUint64)
}

func validateState(state State) error {
	if (state.SchemaVersion != 1 && state.SchemaVersion != SnapshotSchemaVersion) || state.Version == 0 || !state.Status.Valid() || !state.Stage.Valid() || state.Cost < 0 || state.Limits.MaxCost < 0 {
		return fmt.Errorf("%w: unsupported or malformed snapshot", domain.ErrInvalid)
	}
	if err := validateProgressState(state); err != nil {
		return err
	}
	if err := state.RunID.Validate(); err != nil {
		return err
	}
	if err := state.TenantID.Validate(); err != nil {
		return err
	}
	if state.Status.Terminal() && (state.Stage != domain.StageStopped || unsettled(state.PendingEffect)) {
		return fmt.Errorf("%w: terminal snapshot contains active work", domain.ErrInvalid)
	}
	if !state.Status.Terminal() && state.Stage == domain.StageStopped {
		return fmt.Errorf("%w: nonterminal snapshot has stopped stage", domain.ErrInvalid)
	}
	if state.Status == domain.StatusCancelRequested {
		if state.StopTarget != domain.StatusCancelled && state.StopTarget != domain.StatusFailed && state.StopTarget != domain.StatusBudgetExhausted {
			return fmt.Errorf("%w: invalid stop target", domain.ErrInvalid)
		}
	} else if state.StopTarget != "" && (!state.Status.Terminal() || state.StopTarget != state.Status) {
		return fmt.Errorf("%w: stop target contradicts run state", domain.ErrInvalid)
	}
	if state.Status == domain.StatusRunning && (state.Lease.Owner == "" || state.Lease.Epoch == 0) {
		return fmt.Errorf("%w: running snapshot has no lease owner", domain.ErrInvalid)
	}
	if state.Status == domain.StatusQueued && (state.Lease.Owner != "" || unsettled(state.PendingEffect)) {
		return fmt.Errorf("%w: queued snapshot retains active work", domain.ErrInvalid)
	}
	if state.Status == domain.StatusNeedsReconciliation && state.Stage != domain.StageReconcile && state.Stage != domain.StageAdoptWorkspace {
		return fmt.Errorf("%w: reconciliation snapshot has executable stage", domain.ErrInvalid)
	}
	if state.Status == domain.StatusWaitingApproval && (state.Stage != domain.StageApprovalGate || state.Lease.Owner != "" || state.PendingEffect == nil || state.PendingEffect.Status != EffectPlanned || !state.PendingEffect.RequiresApproval || state.Approval == nil || state.Approval.Granted) {
		return fmt.Errorf("%w: pending approval snapshot is inconsistent", domain.ErrInvalid)
	}
	if len(state.RemainingEffects) > 128 {
		return fmt.Errorf("%w: snapshot tool batch exceeds limit", domain.ErrInvalid)
	}
	seen := make(map[domain.ID]bool, len(state.RemainingEffects)+1)
	for _, effect := range state.RemainingEffects {
		if err := validateEffect(effect); err != nil {
			return err
		}
		if seen[effect.ID] {
			return fmt.Errorf("%w: duplicate operation in snapshot", domain.ErrInvalid)
		}
		seen[effect.ID] = true
	}
	if effect := state.PendingEffect; effect != nil {
		if seen[effect.ID] {
			return fmt.Errorf("%w: pending operation duplicated in remaining batch", domain.ErrInvalid)
		}
		identity := *effect
		identity.Status, identity.DispatchEpoch, identity.ReceiptRef = EffectPlanned, 0, ""
		if err := validateEffect(identity); err != nil {
			return err
		}
		if effect.ExpectedRevision == 0 {
			return fmt.Errorf("%w: effect has no workspace revision", domain.ErrInvalid)
		}
		switch effect.Status {
		case EffectPlanned:
			if effect.DispatchEpoch != 0 || effect.ReceiptRef != "" {
				return fmt.Errorf("%w: planned effect has execution receipt", domain.ErrInvalid)
			}
		case EffectInFlight, EffectUnknown:
			if effect.DispatchEpoch == 0 || effect.ReceiptRef != "" {
				return fmt.Errorf("%w: unresolved effect ledger is malformed", domain.ErrInvalid)
			}
			if state.Status != domain.StatusCancelRequested && state.Stage != domain.StageExecuteEffects && state.Stage != domain.StageAdoptWorkspace && state.Stage != domain.StageReconcile {
				return fmt.Errorf("%w: dispatched effect at unrelated stage", domain.ErrInvalid)
			}
		case EffectSucceeded, EffectFailed, EffectCancelled:
			if effect.DispatchEpoch == 0 || effect.ReceiptRef == "" {
				return fmt.Errorf("%w: settled effect lacks receipt", domain.ErrInvalid)
			}
		default:
			return fmt.Errorf("%w: unknown effect status", domain.ErrInvalid)
		}
	}
	if state.Approval != nil && (state.PendingEffect == nil || !state.PendingEffect.RequiresApproval || state.Approval.Version == 0 || !approvalMatches(*state.Approval, *state.PendingEffect, state.WorkspaceRevision)) {
		return fmt.Errorf("%w: approval does not bind pending operation", domain.ErrInvalid)
	}
	if state.Status == domain.StatusRunning && state.Stage == domain.StageExecuteEffects && !unsettled(state.PendingEffect) {
		return fmt.Errorf("%w: execution stage lacks dispatched effect", domain.ErrInvalid)
	}
	if state.Verification != domain.VerificationUnverified && state.Verification != domain.VerificationRegressionOnly && state.Verification != domain.VerificationVerified {
		return fmt.Errorf("%w: unknown verification status", domain.ErrInvalid)
	}
	if state.Verification != domain.VerificationUnverified && (state.VerificationReportRef == "" || state.VerificationRevision != state.WorkspaceRevision) {
		return fmt.Errorf("%w: verification does not bind workspace", domain.ErrInvalid)
	}
	return nil
}

func validateEffect(effect Effect) error {
	if err := domain.ValidateCanonicalJSON(effect.Args); err != nil {
		return err
	}
	if err := effect.ID.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(effect.Kind) == "" || effect.PolicyVersion == "" || len(effect.Args) > 256*1024 || !json.Valid(effect.Args) || !strings.HasPrefix(strings.TrimSpace(string(effect.Args)), "{") || effect.Status != "" && effect.Status != EffectPlanned || effect.DispatchEpoch != 0 || effect.ReceiptRef != "" {
		return fmt.Errorf("%w: malformed tool effect", domain.ErrInvalid)
	}
	digest := sha256.Sum256(effect.Args)
	if effect.ArgsHash != hex.EncodeToString(digest[:]) {
		return fmt.Errorf("%w: tool argument hash mismatch", domain.ErrConflict)
	}
	return nil
}

func approvalMatches(a ApprovalBinding, e Effect, revision uint64) bool {
	return a.EffectID == e.ID && a.ArgsHash == e.ArgsHash && a.WorkspaceRevision == revision && e.ExpectedRevision == revision && a.PolicyVersion == e.PolicyVersion
}

func requireStage(state State, stage domain.Stage) error {
	if state.Status != domain.StatusRunning || state.Stage != stage {
		return fmt.Errorf("%w: %s/%s cannot accept %s result", domain.ErrTransition, state.Status, state.Stage, stage)
	}
	return nil
}

func controlEvent(kind EventKind) bool {
	return kind == EventCancelRequested || kind == EventApprovalDecided || kind == EventResumeRequested
}

func allowedDuringStop(kind EventKind) bool {
	return kind == EventClaimed || kind == EventCancelRequested || kind == EventCancellationConfirmed || kind == EventReconciled
}

func allowedDuringReconciliation(kind EventKind) bool {
	return kind == EventClaimed || kind == EventCancelRequested || kind == EventResumeRequested || kind == EventReconciled || kind == EventWorkspaceAdopted
}

func unsettled(effect *Effect) bool {
	return effect != nil && (effect.Status == EffectInFlight || effect.Status == EffectUnknown)
}
func command(kind CommandKind) []Command { return []Command{{Kind: kind}} }
func cloneApproval(a *ApprovalBinding) *ApprovalBinding {
	if a == nil {
		return nil
	}
	result := *a
	return &result
}
func cloneEffect(e *Effect) *Effect {
	if e == nil {
		return nil
	}
	result := *e
	result.Args = append(json.RawMessage(nil), e.Args...)
	return &result
}
func cloneEffects(effects []Effect) []Effect {
	if effects == nil {
		return nil
	}
	result := make([]Effect, len(effects))
	for i := range effects {
		result[i] = *cloneEffect(&effects[i])
	}
	return result
}
func cloneState(state State) State {
	if state.Progress != nil {
		p := *state.Progress
		p.RecentEvidence = append([]string{}, p.RecentEvidence...)
		p.RecentTrees = append([]string{}, p.RecentTrees...)
		state.Progress = &p
	}
	state.PendingEffect = cloneEffect(state.PendingEffect)
	state.RemainingEffects = cloneEffects(state.RemainingEffects)
	state.Approval = cloneApproval(state.Approval)
	return state
}
