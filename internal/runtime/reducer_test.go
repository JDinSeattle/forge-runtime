package runtime

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
)

var clock = time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)

func apply(t *testing.T, state State, event Event) (State, []Command) {
	t.Helper()
	event = envelope(state, event)
	next, commands, err := Transition(state, event)
	if err != nil {
		t.Fatalf("%s in %s/%s: %v", event.Kind, state.Status, state.Stage, err)
	}
	if next.Version != state.Version+1 {
		t.Fatalf("non-atomic progress version: %d -> %d", state.Version, next.Version)
	}
	return next, commands
}

func envelope(state State, event Event) Event {
	event.ExpectedVersion = state.Version
	if event.At.IsZero() {
		event.At = clock
	}
	if event.Owner == "" {
		event.Owner = state.Lease.Owner
	}
	if event.Epoch == 0 {
		event.Epoch = state.Lease.Epoch
	}
	return event
}

func started(t *testing.T) State {
	t.Helper()
	s := NewState("tenant", "run", Limits{})
	lease := domain.Lease{Owner: "worker-a", Epoch: 1, Until: clock.Add(time.Minute)}
	s, _ = apply(t, s, Event{Kind: EventClaimed, Owner: lease.Owner, Epoch: lease.Epoch, Lease: &lease})
	s, _ = apply(t, s, Event{Kind: EventWorkspaceReady, WorkspaceRevision: 1, OutputRef: "artifact/workspace"})
	return s
}

func modelStage(t *testing.T) State {
	t.Helper()
	s, _ := apply(t, started(t), Event{Kind: EventContextBuilt, OutputRef: "artifact/context"})
	return s
}

func toolStage(t *testing.T) State {
	t.Helper()
	s, _ := apply(t, modelStage(t), Event{Kind: EventModelCompleted, Complete: true, OutputRef: "artifact/model"})
	return s
}

func effect(id domain.ID, approval bool) Effect {
	args := json.RawMessage(`{"patch":"fix","path":"src/main.py"}`)
	digest := sha256.Sum256(args)
	return Effect{ID: id, Kind: "apply_patch", Args: args, ArgsHash: hex.EncodeToString(digest[:]), PolicyVersion: "policy-v1", RequiresApproval: approval}
}

func dispatched(t *testing.T) State {
	t.Helper()
	s, c := apply(t, toolStage(t), Event{Kind: EventToolsValidated, Complete: true, OutputRef: "artifact/validated", Effects: []Effect{effect("operation-1", false)}})
	wantCommand(t, c, CommandExecuteEffect)
	return s
}

func receipt(s State) *EffectReceipt {
	e := s.PendingEffect
	return &EffectReceipt{EffectID: e.ID, ArgsHash: e.ArgsHash, Epoch: e.DispatchEpoch, Status: EffectSucceeded, BeforeRevision: e.ExpectedRevision, AfterRevision: e.ExpectedRevision + 1, Ref: "artifact/receipt", Settled: true}
}

func wantCommand(t *testing.T, commands []Command, want CommandKind) {
	t.Helper()
	if len(commands) != 1 || commands[0].Kind != want {
		t.Fatalf("commands = %#v; want only %s", commands, want)
	}
}

func reject(t *testing.T, state State, event Event, want error) {
	t.Helper()
	before, _ := json.Marshal(state)
	next, commands, err := Transition(state, event)
	if !errors.Is(err, want) {
		t.Fatalf("%s error = %v; want %v", event.Kind, err, want)
	}
	after, _ := json.Marshal(state)
	if string(before) != string(after) || !reflect.DeepEqual(next, state) || len(commands) != 0 {
		t.Fatal("rejected event mutated state or emitted I/O")
	}
}

func TestPartialModelNeverDispatchesTools(t *testing.T) {
	s := modelStage(t)
	for _, event := range []Event{
		{Kind: EventModelCompleted, Complete: false, OutputRef: "artifact/partial", Effects: []Effect{effect("operation-1", false)}},
		{Kind: EventModelCompleted, Complete: true},
	} {
		reject(t, s, envelope(s, event), domain.ErrInvalid)
	}
	next, commands := apply(t, s, Event{Kind: EventModelCompleted, Complete: true, OutputRef: "artifact/final"})
	wantCommand(t, commands, CommandValidateTools)
	if next.PendingEffect != nil {
		t.Fatal("model completion alone planned an unchecked effect")
	}
}

func TestOnlyCurrentUnexpiredLeaseMayAdvance(t *testing.T) {
	s := started(t)
	base := envelope(s, Event{Kind: EventContextBuilt, OutputRef: "artifact/context"})
	for _, event := range []Event{
		func() Event { e := base; e.Owner = "worker-b"; return e }(),
		func() Event { e := base; e.Epoch++; return e }(),
		func() Event { e := base; e.At = s.Lease.Until; return e }(),
	} {
		reject(t, s, event, domain.ErrFenced)
	}
	base.ExpectedVersion--
	reject(t, s, base, domain.ErrConflict)
	lease := domain.Lease{Owner: "worker-b", Epoch: 2, Until: clock.Add(2 * time.Minute)}
	reject(t, s, envelope(s, Event{Kind: EventClaimed, Owner: lease.Owner, Epoch: lease.Epoch, Lease: &lease}), domain.ErrFenced)
}

func TestToolArgumentIntegrityAndBatchUniqueness(t *testing.T) {
	s := toolStage(t)
	good := effect("operation-1", false)
	changed := good
	changed.Args = json.RawMessage(`{"path":"/etc/passwd"}`)
	for name, effects := range map[string][]Effect{
		"hash tampering":      {changed},
		"duplicate operation": {good, good},
	} {
		t.Run(name, func(t *testing.T) {
			reject(t, s, envelope(s, Event{Kind: EventToolsValidated, Complete: true, OutputRef: "artifact/validated", Effects: effects}), domain.ErrConflict)
		})
	}
	good.DispatchEpoch = 10
	reject(t, s, envelope(s, Event{Kind: EventToolsValidated, Complete: true, OutputRef: "artifact/validated", Effects: []Effect{good}}), domain.ErrInvalid)
}

func TestSequentialBatchUsesReceiptRevisionAndStopsOnFailure(t *testing.T) {
	s, commands := apply(t, toolStage(t), Event{Kind: EventToolsValidated, Complete: true, OutputRef: "artifact/validated", Effects: []Effect{effect("operation-1", false), effect("operation-2", false)}})
	wantCommand(t, commands, CommandExecuteEffect)
	if len(s.RemainingEffects) != 1 || s.ToolCalls != 1 {
		t.Fatal("more than one tool admitted")
	}
	s, commands = apply(t, s, Event{Kind: EventEffectCompleted, Receipt: receipt(s)})
	wantCommand(t, commands, CommandExecuteEffect)
	if s.PendingEffect.ID != "operation-2" || s.PendingEffect.ExpectedRevision != 2 || s.ToolCalls != 2 {
		t.Fatal("next write used stale revision")
	}
	r := receipt(s)
	r.Status = EffectFailed
	s, commands = apply(t, s, Event{Kind: EventEffectCompleted, Receipt: r})
	wantCommand(t, commands, CommandIngestResults)
	if s.PendingEffect != nil || s.Stage != domain.StageIngestResults {
		t.Fatal("failed effect replayed")
	}
}

func TestUnknownEffectRecoveryNeverReplaysOperation(t *testing.T) {
	s := dispatched(t)
	before := *s.PendingEffect
	s, commands := apply(t, s, Event{Kind: EventEffectUncertain, Reason: "StartOperation deadline exceeded"})
	wantCommand(t, commands, CommandInspectEffect)
	if s.Status != domain.StatusNeedsReconciliation {
		t.Fatal("unknown effect did not halt")
	}
	reject(t, s, envelope(s, Event{Kind: EventModelCompleted, Complete: true, OutputRef: "new-sample"}), domain.ErrReconciliation)
	lease := domain.Lease{Owner: "worker-b", Epoch: 2, Until: clock.Add(3 * time.Minute)}
	at := clock.Add(2 * time.Minute)
	s, commands = apply(t, s, Event{Kind: EventClaimed, At: at, Lease: &lease, Owner: lease.Owner, Epoch: lease.Epoch})
	wantCommand(t, commands, CommandAdoptWorkspace)
	reject(t, s, envelope(s, Event{Kind: EventReconciled, At: at, Receipt: receipt(s)}), domain.ErrTransition)
	s, commands = apply(t, s, Event{Kind: EventWorkspaceAdopted, At: at, Stop: &StopReceipt{Ref: "artifact/adopt", NoActiveOperations: true, WorkspaceRevision: 2}})
	wantCommand(t, commands, CommandInspectEffect)
	if !reflect.DeepEqual(s.PendingEffect.Args, before.Args) || s.PendingEffect.ID != before.ID || s.PendingEffect.DispatchEpoch != 1 {
		t.Fatal("recovery replaced the original operation identity")
	}
	stale := receipt(s)
	stale.AfterRevision = 1
	reject(t, s, envelope(s, Event{Kind: EventReconciled, At: at, Receipt: stale}), domain.ErrConflict)
	s, commands = apply(t, s, Event{Kind: EventReconciled, At: at, Receipt: receipt(s)})
	wantCommand(t, commands, CommandIngestResults)
	if s.WorkspaceRevision != 2 || s.Status != domain.StatusRunning || s.ToolCalls != 1 {
		t.Fatal("reconciliation duplicated operation or lost result")
	}
}

func TestAdoptionRequiresOldWriterStopEvidence(t *testing.T) {
	s := dispatched(t)
	at := clock.Add(2 * time.Minute)
	lease := domain.Lease{Owner: "worker-b", Epoch: 2, Until: at.Add(time.Minute)}
	s, _ = apply(t, s, Event{Kind: EventClaimed, At: at, Owner: lease.Owner, Epoch: lease.Epoch, Lease: &lease})
	reject(t, s, envelope(s, Event{Kind: EventWorkspaceAdopted, At: at, Stop: &StopReceipt{Ref: "artifact/receipt", NoActiveOperations: false, WorkspaceRevision: 1}}), domain.ErrUntrusted)
	reject(t, s, envelope(s, Event{Kind: EventEffectCompleted, At: at, Receipt: receipt(s)}), domain.ErrTransition)
}

func TestCancellationRequiresSettledReceiptAndNeverLosesToCompletion(t *testing.T) {
	s := dispatched(t)
	s, commands := apply(t, s, Event{Kind: EventCancelRequested})
	wantCommand(t, commands, CommandStopExecution)
	if s.Status != domain.StatusCancelRequested {
		t.Fatal("cancel request incorrectly claimed execution stopped")
	}
	reject(t, s, envelope(s, Event{Kind: EventEffectCompleted, Receipt: receipt(s)}), domain.ErrTransition)
	reject(t, s, envelope(s, Event{Kind: EventCancellationConfirmed, Stop: &StopReceipt{NoActiveOperations: true, Ref: "artifact/stopped", WorkspaceRevision: 2}}), domain.ErrReconciliation)
	s, commands = apply(t, s, Event{Kind: EventReconciled, Receipt: receipt(s)})
	wantCommand(t, commands, CommandStopExecution)
	if s.Status != domain.StatusCancelRequested {
		t.Fatal("late successful operation reopened a cancelled run")
	}
	s, commands = apply(t, s, Event{Kind: EventCancellationConfirmed, Stop: &StopReceipt{NoActiveOperations: true, Ref: "artifact/stopped", WorkspaceRevision: 2}})
	wantCommand(t, commands, CommandPublishTerminal)
	if s.Status != domain.StatusCancelled {
		t.Fatal("confirmed stop not terminal")
	}
	for _, kind := range []EventKind{EventClaimed, EventModelCompleted, EventResumeRequested, EventApprovalDecided, EventCancelRequested} {
		reject(t, s, envelope(s, Event{Kind: kind}), domain.ErrTerminal)
	}
}

func TestCancelBeforeClaimStillRequiresExplicitAcknowledgment(t *testing.T) {
	s := NewState("tenant", "run", Limits{})
	s, commands := apply(t, s, Event{Kind: EventCancelRequested})
	wantCommand(t, commands, CommandStopExecution)
	lease := domain.Lease{Owner: "worker", Epoch: 1, Until: clock.Add(time.Minute)}
	s, commands = apply(t, s, Event{Kind: EventClaimed, Lease: &lease, Owner: lease.Owner, Epoch: lease.Epoch})
	wantCommand(t, commands, CommandStopExecution)
	s, _ = apply(t, s, Event{Kind: EventCancellationConfirmed, Stop: &StopReceipt{Ref: "ledger/no-work-started", NoActiveOperations: true}})
	if s.Status != domain.StatusCancelled {
		t.Fatal("never-started run could not cancel")
	}
}

func TestApprovalBoundToAllOperationInputs(t *testing.T) {
	s, commands := apply(t, toolStage(t), Event{Kind: EventToolsValidated, Complete: true, OutputRef: "artifact/validated", Effects: []Effect{effect("operation-1", true)}})
	wantCommand(t, commands, CommandRequestApproval)
	if s.Lease.Owner != "" || s.Status != domain.StatusWaitingApproval {
		t.Fatal("waiting approval retains worker execution slot")
	}
	for name, mutate := range map[string]func(*ApprovalBinding){
		"operation":       func(a *ApprovalBinding) { a.EffectID = "other" },
		"arguments":       func(a *ApprovalBinding) { a.ArgsHash = "other" },
		"revision":        func(a *ApprovalBinding) { a.WorkspaceRevision++ },
		"policy":          func(a *ApprovalBinding) { a.PolicyVersion = "other" },
		"version":         func(a *ApprovalBinding) { a.Version++ },
		"already granted": func(a *ApprovalBinding) { a.Granted = true },
	} {
		t.Run(name, func(t *testing.T) {
			a := *s.Approval
			mutate(&a)
			reject(t, s, envelope(s, Event{Kind: EventApprovalDecided, Approval: &ApprovalDecision{Binding: a, Approve: true}}), domain.ErrConflict)
		})
	}
	s, commands = apply(t, s, Event{Kind: EventApprovalDecided, Approval: &ApprovalDecision{Binding: *s.Approval, Approve: true}})
	if len(commands) != 0 || s.Status != domain.StatusQueued || s.PendingEffect.Status != EffectPlanned {
		t.Fatal("approval executed tool without reacquiring authority")
	}
	lease := domain.Lease{Owner: "worker-b", Epoch: 2, Until: clock.Add(time.Minute)}
	s, commands = apply(t, s, Event{Kind: EventClaimed, Lease: &lease, Owner: lease.Owner, Epoch: lease.Epoch})
	wantCommand(t, commands, CommandAdoptWorkspace)
	s, commands = apply(t, s, Event{Kind: EventWorkspaceAdopted, Stop: &StopReceipt{Ref: "artifact/adopt", NoActiveOperations: true, WorkspaceRevision: 1}})
	wantCommand(t, commands, CommandExecuteEffect)
	if s.ToolCalls != 1 || s.PendingEffect.DispatchEpoch != 2 {
		t.Fatal("approved operation has wrong accounting or epoch")
	}
}

func TestVerificationDistinguishesTargetProofFromRegressionOnly(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		baseline, target, regressions bool
		status                        domain.VerificationStatus
		stage                         domain.Stage
	}{
		{"target and regression", true, true, true, domain.VerificationVerified, domain.StageFinalize},
		{"regression only", false, false, true, domain.VerificationRegressionOnly, domain.StageFinalize},
		{"target without baseline", false, true, true, domain.VerificationRegressionOnly, domain.StageFinalize},
		{"target still broken", true, false, true, domain.VerificationUnverified, domain.StageBuildContext},
		{"regression broken", true, true, false, domain.VerificationUnverified, domain.StageBuildContext},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, commands := apply(t, modelStage(t), Event{Kind: EventModelCompleted, Complete: true, Finish: true, OutputRef: "artifact/model"})
			wantCommand(t, commands, CommandVerify)
			v := &VerificationEvidence{Trusted: true, ReportRef: "artifact/verification", WorkspaceRevision: 1, BaselineTargetFailed: tc.baseline, TargetPassed: tc.target, RegressionPassed: tc.regressions}
			s, _ = apply(t, s, Event{Kind: EventVerificationCompleted, Verification: v})
			if s.Verification != tc.status || s.Stage != tc.stage {
				t.Fatalf("verification = %s/%s", s.Verification, s.Stage)
			}
			if s.Stage == domain.StageFinalize {
				s, _ = apply(t, s, Event{Kind: EventFinalized, OutputRef: "artifact/delivery"})
				if s.Status != domain.StatusCompleted {
					t.Fatal("final verified output not completed")
				}
				reject(t, s, envelope(s, Event{Kind: EventCancelRequested}), domain.ErrTerminal)
			}
		})
	}
}

func TestModelCannotForgeVerificationOrUseOldRevision(t *testing.T) {
	s, _ := apply(t, modelStage(t), Event{Kind: EventModelCompleted, Complete: true, Finish: true, OutputRef: "artifact/model"})
	v := &VerificationEvidence{ReportRef: "model/says-tested", WorkspaceRevision: 1, TargetPassed: true, RegressionPassed: true, BaselineTargetFailed: true}
	reject(t, s, envelope(s, Event{Kind: EventVerificationCompleted, Verification: v}), domain.ErrUntrusted)
	v.Trusted, v.WorkspaceRevision = true, 2
	reject(t, s, envelope(s, Event{Kind: EventVerificationCompleted, Verification: v}), domain.ErrConflict)
}

func TestBudgetStopsBeforeNewWorkAndWaitsForAcknowledgement(t *testing.T) {
	s := started(t)
	s.Limits.MaxModelRounds, s.ModelRounds = 2, 2
	s, commands := apply(t, s, Event{Kind: EventContextBuilt, OutputRef: "artifact/context"})
	wantCommand(t, commands, CommandStopExecution)
	if s.Status != domain.StatusCancelRequested || s.StopTarget != domain.StatusBudgetExhausted {
		t.Fatal("exhausted budget did not stop admission")
	}
	s, _ = apply(t, s, Event{Kind: EventCancellationConfirmed, Stop: &StopReceipt{Ref: "artifact/stopped", NoActiveOperations: true, WorkspaceRevision: 1}})
	if s.Status != domain.StatusBudgetExhausted {
		t.Fatal("wrong terminal outcome for exhausted budget")
	}
	s = modelStage(t)
	s.Cost, s.Limits.MaxCost = 9, 10
	s, commands = apply(t, s, Event{Kind: EventModelCompleted, Complete: true, OutputRef: "artifact/model", Cost: 1})
	wantCommand(t, commands, CommandStopExecution)
	if s.Cost != 10 {
		t.Fatal("paid request disappeared at budget boundary")
	}
}

func TestReducerOutputsDoNotAliasPersistedInputOrEvent(t *testing.T) {
	s := toolStage(t)
	e := envelope(s, Event{Kind: EventToolsValidated, Complete: true, OutputRef: "artifact/validated", Effects: []Effect{effect("operation-1", false), effect("operation-2", false)}})
	before, _ := json.Marshal(e)
	next, commands, err := Transition(s, e)
	if err != nil {
		t.Fatal(err)
	}
	commands[0].Effect.Args[1] = 'X'
	if next.PendingEffect.Args[1] == 'X' {
		t.Fatal("command aliases persisted next state")
	}
	next.RemainingEffects[0].Args[1] = 'Z'
	after, _ := json.Marshal(e)
	if string(before) != string(after) {
		t.Fatal("output aliases event payload")
	}
}

func TestMalformedSnapshotsFailClosedBeforeCommands(t *testing.T) {
	for name, mutate := range map[string]func(*State){
		"nonterminal stop target":            func(s *State) { s.Status = domain.StatusCancelRequested; s.StopTarget = domain.StatusQueued },
		"nonterminal stopped stage":          func(s *State) { s.Stage = domain.StageStopped },
		"running without authority":          func(s *State) { s.Lease.Owner = "" },
		"reconciliation at executable stage": func(s *State) { s.Status = domain.StatusNeedsReconciliation },
		"approval without effect":            func(s *State) { s.Approval = &ApprovalBinding{Version: 1} },
		"unknown snapshot schema":            func(s *State) { s.SchemaVersion++ },
	} {
		t.Run(name, func(t *testing.T) {
			s := started(t)
			mutate(&s)
			reject(t, s, envelope(s, Event{Kind: EventCancellationConfirmed, Stop: &StopReceipt{NoActiveOperations: true, Ref: "receipt", WorkspaceRevision: 1}}), domain.ErrInvalid)
		})
	}
	s := dispatched(t)
	s.PendingEffect.Status = "imaginary"
	reject(t, s, envelope(s, Event{Kind: EventEffectCompleted, Receipt: receipt(s)}), domain.ErrInvalid)
}

// These fuzz tests exercise invariants over arbitrarily interleaved late events,
// lease fencing and malformed JSON snapshots, rather than assert branch names.
func FuzzTerminalIsMonotonic(f *testing.F) {
	f.Add("model_completed", uint64(1))
	f.Add("claimed", uint64(math.MaxUint64))
	f.Fuzz(func(t *testing.T, kind string, epoch uint64) {
		s := NewState("tenant", "run", Limits{})
		s.Status, s.Stage = domain.StatusCancelled, domain.StageStopped
		e := Event{Kind: EventKind(kind), ExpectedVersion: s.Version, At: clock, Owner: "late-worker", Epoch: epoch, Complete: true, OutputRef: "artifact/late"}
		reject(t, s, e, domain.ErrTerminal)
	})
}

func FuzzMalformedSnapshotCannotPanic(f *testing.F) {
	seed, _ := json.Marshal(NewState("tenant", "run", Limits{}))
	f.Add(seed, "context_built")
	f.Add([]byte(`{"schema_version":4294967295}`), "claimed")
	malformed := NewState("tenant", "run", Limits{})
	malformed.Status, malformed.StopTarget = domain.StatusCancelRequested, domain.StatusQueued
	malformed.Lease = domain.Lease{Owner: "worker", Epoch: 1, Until: clock.Add(time.Minute)}
	badStop, _ := json.Marshal(malformed)
	f.Add(badStop, "cancellation_confirmed")
	f.Fuzz(func(t *testing.T, raw []byte, kind string) {
		if len(raw) > 256*1024 {
			t.Skip()
		}
		var s State
		if json.Unmarshal(raw, &s) != nil {
			return
		}
		e := envelope(s, Event{Kind: EventKind(kind), OutputRef: "artifact/result", Complete: true})
		one, c1, err1 := Transition(s, e)
		two, c2, err2 := Transition(s, e)
		if !reflect.DeepEqual(one, two) || !reflect.DeepEqual(c1, c2) || (err1 == nil) != (err2 == nil) {
			t.Fatal("transition is nondeterministic")
		}
		if err1 != nil && (!reflect.DeepEqual(one, s) || len(c1) != 0) {
			t.Fatal("invalid snapshot/event caused side effects")
		}
	})
}
