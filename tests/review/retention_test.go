package review_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	flow "github.com/JDinSeattle/forge-runtime/internal/runtime"
)

func retentionRun(t *testing.T, ctx context.Context, s *persistence.Store, project domain.ID, label string, status domain.RunStatus) persistence.Run {
	t.Helper()
	r, _, err := s.Submit(ctx, persistence.SubmitRequest{TenantID: "review_tenant", PrincipalID: "review_principal", ProjectID: project, Task: "review " + label, BaseCommit: "review-base", Config: persistence.Config{Provider: "fake", Model: "fake", MaxModelRounds: 4, MaxToolCalls: 4, MaxCost: 1_000_000, MaxRuntimeSeconds: 300}}, label)
	if err != nil {
		t.Fatal(err)
	}
	identity := persistence.Identity{TenantID: r.TenantID, PrincipalID: r.PrincipalID, Role: "developer"}
	for i := range 4 {
		if _, _, err := s.AddMessage(ctx, identity, r.ID, fmt.Sprintf("message %d", i), fmt.Sprintf("key-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if status == domain.StatusQueued {
		return r
	}
	claimed, err := s.Claim(ctx, "worker_"+label, time.Minute)
	if err != nil || claimed.ID != r.ID {
		t.Fatalf("fixture claim: got=%s want=%s err=%v", claimed.ID, r.ID, err)
	}
	r = claimed
	ref := domain.ID("receipt_" + label)
	if err := s.PublishArtifact(ctx, persistence.Artifact{TenantID: r.TenantID, RunID: r.ID, ID: ref, Kind: "review_evidence", ObjectKey: string(r.TenantID) + "/" + string(r.ID) + "/receipt", SHA256: strings.Repeat("0", 64), ByteSize: 0}); err != nil {
		t.Fatal(err)
	}
	advance := func(event flow.Event) {
		event.ExpectedVersion, event.Owner, event.Epoch = r.State.Version, r.State.Lease.Owner, r.State.Lease.Epoch
		r, err = s.Advance(ctx, r.TenantID, r.ID, event)
		if err != nil {
			t.Fatal(err)
		}
	}
	// Heartbeat is deliberately not a reducer transition. The subsequent audit
	// must retain its actual renewed lease input rather than the prior snapshot.
	if _, err := s.Heartbeat(ctx, r.TenantID, r.ID, r.State.Lease.Owner, r.State.Lease.Epoch, 90*time.Second); err != nil {
		t.Fatal(err)
	}
	advance(flow.Event{Kind: flow.EventWorkspaceReady, WorkspaceRevision: 1, OutputRef: string(ref)})
	if status == domain.StatusRunning {
		return r
	}
	if status == domain.StatusCancelled || status == domain.StatusCancelRequested {
		r, err = s.Cancel(ctx, identity, r.ID)
		if err != nil {
			t.Fatal(err)
		}
	} else if status == domain.StatusFailed {
		advance(flow.Event{Kind: flow.EventFailed, Reason: "review terminal failure"})
	} else if status == domain.StatusBudgetExhausted {
		advance(flow.Event{Kind: flow.EventBudgetReached, Reason: "review budget stop"})
	} else {
		advance(flow.Event{Kind: flow.EventContextBuilt, OutputRef: string(ref)})
		advance(flow.Event{Kind: flow.EventModelCompleted, Complete: true, Finish: status == domain.StatusCompleted, OutputRef: string(ref)})
		if status == domain.StatusCompleted {
			advance(flow.Event{Kind: flow.EventVerificationCompleted, Verification: &flow.VerificationEvidence{Trusted: true, ReportRef: string(ref), WorkspaceRevision: 1, BaselineTargetFailed: true, TargetPassed: true, RegressionPassed: true}})
			advance(flow.Event{Kind: flow.EventFinalized, OutputRef: string(ref)})
		} else {
			args := json.RawMessage(`{"path":"a \u003c b \u0026 c.py"}`)
			hash := sha256.Sum256(args)
			advance(flow.Event{Kind: flow.EventToolsValidated, Complete: true, OutputRef: string(ref), Effects: []flow.Effect{{ID: domain.ID("op_" + label), Kind: "read_file", Args: args, ArgsHash: hex.EncodeToString(hash[:]), ExpectedRevision: 1, PolicyVersion: "review-v1", RequiresApproval: status == domain.StatusWaitingApproval, Status: flow.EffectPlanned}}})
			if status == domain.StatusNeedsReconciliation {
				advance(flow.Event{Kind: flow.EventEffectUncertain, Reason: "review uncertain receipt"})
			}
		}
	}
	if status == domain.StatusCancelled || status == domain.StatusFailed || status == domain.StatusBudgetExhausted {
		advance(flow.Event{Kind: flow.EventCancellationConfirmed, Stop: &flow.StopReceipt{Ref: string(ref), NoActiveOperations: true, WorkspaceRevision: 1}})
	}
	if r.State.Status != status {
		t.Fatalf("fixture status=%s want=%s", r.State.Status, status)
	}
	return r
}

func assertReviewTransitionReplay(t *testing.T, ctx context.Context, s *persistence.Store, r persistence.Run) {
	t.Helper()
	rows, err := s.Pool.Query(ctx, `SELECT version,body,input_state,input_event FROM run_snapshots WHERE tenant_id=$1 AND run_id=$2 ORDER BY version`, r.TenantID, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		var version uint64
		var body, prior, input json.RawMessage
		if err := rows.Scan(&version, &body, &prior, &input); err != nil {
			t.Fatal(err)
		}
		if version == 1 {
			if prior != nil || input != nil {
				t.Fatal("initial snapshot fabricated a reducer input")
			}
			continue
		}
		if len(prior) == 0 || len(input) == 0 {
			t.Fatalf("new transition v%d has no replay input; nullable legacy history must not count as replay evidence", version)
		}
		var state flow.State
		var event flow.Event
		if err := json.Unmarshal(prior, &state); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(input, &event); err != nil {
			t.Fatal(err)
		}
		replayed, _, err := flow.Transition(state, event)
		if err != nil {
			t.Fatalf("replay v%d: %v", version, err)
		}
		actual, err := json.Marshal(replayed)
		if err != nil {
			t.Fatal(err)
		}
		var compact bytes.Buffer
		if err := json.Compact(&compact, body); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(actual, compact.Bytes()) {
			t.Fatalf("replay v%d differs from committed snapshot\nreplayed=%s\ncommitted=%s", version, actual, compact.Bytes())
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if r.State.Version > 1 && count != int(r.State.Version-1) {
		t.Fatalf("replayed %d transitions for version %d", count, r.State.Version)
	}
}

func TestReviewTrimEventsPreservesActiveStatesAndReplayEvidence(t *testing.T) {
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
	p, err := s.CreateProject(ctx, "review_tenant", "retention review", "fixture", "python")
	if err != nil {
		t.Fatal(err)
	}
	all := []persistence.Run{}
	for _, status := range []domain.RunStatus{domain.StatusCompleted, domain.StatusFailed, domain.StatusCancelled, domain.StatusBudgetExhausted, domain.StatusRunning, domain.StatusWaitingApproval, domain.StatusNeedsReconciliation, domain.StatusCancelRequested} {
		all = append(all, retentionRun(t, ctx, s, p.ID, string(status), status))
	}
	recent := retentionRun(t, ctx, s, p.ID, "recent", domain.StatusCancelled)
	all = append(all, recent)
	queued := retentionRun(t, ctx, s, p.ID, "queued", domain.StatusQueued)
	all = append(all, queued)
	for _, r := range all {
		assertReviewTransitionReplay(t, ctx, s, r)
	}
	if _, err := s.Pool.Exec(ctx, `UPDATE runs SET updated_at=clock_timestamp()-interval '2 hours' WHERE id<>$1`, recent.ID); err != nil {
		t.Fatal(err)
	}
	beforeCounts := map[domain.ID]int64{}
	var expected int64
	for _, r := range all {
		var count int64
		if err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM run_events WHERE tenant_id=$1 AND run_id=$2`, r.TenantID, r.ID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		beforeCounts[r.ID] = count
		if r.State.Status.Terminal() && r.ID != recent.ID {
			expected += count - 2
		}
	}
	deleted, err := s.TrimEvents(ctx, time.Now().Add(-time.Hour), 2, 32)
	if err != nil || deleted != expected {
		t.Fatalf("trim deleted=%d want=%d err=%v", deleted, expected, err)
	}
	for _, r := range all {
		var count, first, retained int64
		if err := s.Pool.QueryRow(ctx, `SELECT count(*),min(seq) FROM run_events WHERE tenant_id=$1 AND run_id=$2`, r.TenantID, r.ID).Scan(&count, &first); err != nil {
			t.Fatal(err)
		}
		if err := s.Pool.QueryRow(ctx, `SELECT retained_from_seq FROM runs WHERE tenant_id=$1 AND id=$2`, r.TenantID, r.ID).Scan(&retained); err != nil {
			t.Fatal(err)
		}
		want := beforeCounts[r.ID]
		if r.State.Status.Terminal() && r.ID != recent.ID {
			want = 2
		}
		if count != want || first != retained {
			t.Fatalf("status=%s count=%d want=%d first=%d retained=%d", r.State.Status, count, want, first, retained)
		}
		assertReviewTransitionReplay(t, ctx, s, r)
	}
	if deleted, err := s.TrimEvents(ctx, time.Now().Add(-time.Hour), 2, 32); err != nil || deleted != 0 {
		t.Fatalf("repeated trim deleted=%d err=%v", deleted, err)
	}
	// Migration 7 deliberately leaves older history NULL. Emulate one such
	// prior transition and verify retention does not invent missing audit input.
	legacy := all[0]
	if _, err := s.Pool.Exec(ctx, `UPDATE run_snapshots SET input_state=NULL,input_event=NULL WHERE tenant_id=$1 AND run_id=$2 AND version=2`, legacy.TenantID, legacy.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.TrimEvents(ctx, time.Now().Add(-time.Hour), 1, 32); err != nil {
		t.Fatal(err)
	}
	var unknown bool
	if err := s.Pool.QueryRow(ctx, `SELECT input_state IS NULL AND input_event IS NULL FROM run_snapshots WHERE tenant_id=$1 AND run_id=$2 AND version=2`, legacy.TenantID, legacy.ID).Scan(&unknown); err != nil || !unknown {
		t.Fatalf("legacy audit uncertainty lost: %v", err)
	}
}
