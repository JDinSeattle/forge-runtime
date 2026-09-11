package runtime

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
)

func progressBoundary(t *testing.T) State {
	s := started(t)
	s.Limits.MaxNoProgressBatches = 3
	s, _ = apply(t, s, Event{Kind: EventContextBuilt, OutputRef: "context_0"})
	s.Stage = domain.StageBuildContext // explicit fixture: one closed model batch
	return s
}
func progressInput(s State, fingerprint, tree string) Event {
	f := &ProgressFrame{ContextRef: "context_next", PolicyVersion: 1, StepSeq: s.StepSeq, AttemptID: domain.ID(fmt.Sprintf("attempt_%d", s.StepSeq)), Observations: []ProgressObservation{{EffectID: domain.ID(fmt.Sprintf("effect_%d", s.StepSeq)), ReceiptRef: "receipt", Fingerprint: fingerprint, BeforeHash: strings.Repeat("a", 64), AfterHash: tree}}}
	return envelope(s, Event{Kind: EventContextBuilt, OutputRef: f.ContextRef, Progress: f, ProgressRef: "report"})
}
func TestProgressRepeatedEvidenceStopsOnlyAfterReceipt(t *testing.T) {
	s := progressBoundary(t)
	for i := 0; i < 4; i++ {
		before, _ := json.Marshal(s)
		e := progressInput(s, strings.Repeat("b", 64), strings.Repeat("a", 64))
		next, commands, err := Transition(s, e)
		if err != nil {
			t.Fatal(err)
		}
		after, _ := json.Marshal(s)
		if string(before) != string(after) {
			t.Fatal("transition mutated input progress slices")
		}
		if next.Progress.RepeatedBatches != uint64(i) {
			t.Fatal(next.Progress)
		}
		reject(t, next, e, domain.ErrConflict) // durable CAS/replayed event cannot count twice
		if i < 3 {
			wantCommand(t, commands, CommandCallModel)
			next.Stage = domain.StageBuildContext
		} else {
			wantCommand(t, commands, CommandStopExecution)
			if next.Status != domain.StatusCancelRequested || next.StopTarget != domain.StatusBudgetExhausted || next.FailureReason != "repeated_no_progress" || next.StepSeq != s.StepSeq || next.ModelRounds != s.ModelRounds {
				t.Fatal(next)
			}
			reject(t, next, envelope(next, Event{Kind: EventCancellationConfirmed, Stop: &StopReceipt{Ref: "fake", NoActiveOperations: false, WorkspaceRevision: 1}}), domain.ErrUntrusted)
			next, commands = apply(t, next, Event{Kind: EventCancellationConfirmed, Stop: &StopReceipt{Ref: "authenticated", NoActiveOperations: true, WorkspaceRevision: 1}})
			wantCommand(t, commands, CommandPublishTerminal)
			if next.Status != domain.StatusBudgetExhausted {
				t.Fatal(next.Status)
			}
		}
		s = next
	}
}
func TestProgressNovelMessagesUnknownAndOscillation(t *testing.T) {
	s := progressBoundary(t)
	for i := 0; i < 6; i++ {
		e := progressInput(s, fmt.Sprintf("%064x", i), strings.Repeat("a", 64))
		s, _ = apply(t, s, e)
		if s.Progress.RepeatedBatches != 0 {
			t.Fatal("new evidence counted as repeated")
		}
		s.Stage = domain.StageBuildContext
	}
	// All following steps revisit existing evidence/content identities.
	for i := 0; i < 2; i++ {
		s, _ = apply(t, s, progressInput(s, fmt.Sprintf("%064x", i), strings.Repeat("a", 64)))
		s.Stage = domain.StageBuildContext
	}
	e := progressInput(s, fmt.Sprintf("%064x", 0), strings.Repeat("a", 64))
	e.Progress.MessageSeq = 1
	s, _ = apply(t, s, e)
	if s.Progress.RepeatedBatches != 0 || s.Progress.Classification != "message" {
		t.Fatal(s.Progress)
	}
	s.Stage = domain.StageBuildContext
	e = progressInput(s, fmt.Sprintf("%064x", 0), strings.Repeat("a", 64))
	e.Progress.MessageSeq = 1
	s, _ = apply(t, s, e)
	if s.Progress.RepeatedBatches != 1 {
		t.Fatal("same message reset twice")
	}
	s.Stage = domain.StageBuildContext
	e = progressInput(s, "", strings.Repeat("a", 64))
	e.Progress.MessageSeq = 1
	e.Progress.Observations[0].Indeterminate = true
	s, _ = apply(t, s, e)
	if s.Progress.Classification != "indeterminate" || s.Progress.RepeatedBatches != 0 {
		t.Fatal(s.Progress)
	}
	// Tree identity, not an ever-increasing revision, drives novelty.
	s = progressBoundary(t)
	for i, tree := range []string{"b", "a", "b", "a"} {
		e = progressInput(s, strings.Repeat("c", 64), strings.Repeat(tree, 64))
		s.WorkspaceRevision++
		s, _ = apply(t, s, e)
		if s.Progress.RepeatedBatches != uint64(i) {
			t.Fatal("tree oscillation reset progress", s.Progress)
		}
		if i < 3 {
			s.Stage = domain.StageBuildContext
		}
	}
}
func TestProgressCompatibilityAndMalformedBoundary(t *testing.T) {
	s := progressBoundary(t)
	for _, change := range []func(*Event){func(e *Event) { e.Progress = nil }, func(e *Event) { e.Progress.StepSeq++ }, func(e *Event) { e.Progress.ContextRef = "wrong" }, func(e *Event) { e.Progress.Observations[0].BeforeHash = "bad" }, func(e *Event) { e.Progress.Observations = append(e.Progress.Observations, e.Progress.Observations[0]) }} {
		e := progressInput(s, strings.Repeat("a", 64), strings.Repeat("a", 64))
		change(&e)
		got, commands, err := Transition(s, e)
		if err == nil || !reflect.DeepEqual(got, s) || len(commands) != 0 {
			t.Fatal("invalid frame accepted")
		}
	}
	legacy := started(t)
	legacy.SchemaVersion = 1
	if _, _, err := Transition(legacy, envelope(legacy, Event{Kind: EventContextBuilt, OutputRef: "legacy"})); err != nil {
		t.Fatal(err)
	}
	legacy.Limits.MaxNoProgressBatches = 3
	if !errors.Is(ValidateSnapshot(legacy), domain.ErrInvalid) {
		t.Fatal("v1 enabled new policy")
	}
	s.Limits.MaxModelRounds = s.ModelRounds
	next, commands := apply(t, s, progressInput(s, strings.Repeat("a", 64), strings.Repeat("a", 64)))
	if next.FailureReason != "model budget reached" {
		t.Fatal("other budget precedence changed")
	}
	wantCommand(t, commands, CommandStopExecution)
}

func TestProgressHistoryIsBoundedAndEvictionIsConservative(t *testing.T) {
	s := progressBoundary(t)
	for i := 0; i < 600; i++ {
		e := progressInput(s, fmt.Sprintf("%064x", i), fmt.Sprintf("%064x", i+1000))
		s, _ = apply(t, s, e)
		s.Stage = domain.StageBuildContext
		if s.Progress.RepeatedBatches != 0 {
			t.Fatal("novel observation counted as repeated")
		}
	}
	if len(s.Progress.RecentEvidence) != ProgressEvidenceWindow || len(s.Progress.RecentTrees) != ProgressTreeWindow {
		t.Fatal("history is not bounded")
	}
	s, _ = apply(t, s, progressInput(s, fmt.Sprintf("%064x", 0), fmt.Sprintf("%064x", 1000)))
	if s.Progress.Classification != "new_evidence" {
		t.Fatal("evicted evidence must conservatively look new")
	}
}
