package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	flow "github.com/JDinSeattle/forge-runtime/internal/runtime"
)

// Admission replies below are explicit fixtures, before delegating any runner
// work. The successful path uses the real SQLite/file engine with TestBackend;
// none of these tests claim an actual Docker capacity fault.
type capacityAdmissionRunner struct {
	runner.Service
	replies   []error
	prepares  atomic.Int64
	adopts    atomic.Int64
	fakeAdopt bool
}

func (r *capacityAdmissionRunner) PrepareWorkspace(ctx context.Context, request runner.PrepareRequest) (runner.Workspace, error) {
	n := int(r.prepares.Add(1))
	if n <= len(r.replies) {
		return runner.Workspace{}, r.replies[n-1]
	}
	return r.Service.PrepareWorkspace(ctx, request)
}

func (r *capacityAdmissionRunner) AdoptWorkspace(ctx context.Context, request runner.WorkspaceRequest) (runner.StopReceipt, error) {
	r.adopts.Add(1)
	if r.fakeAdopt {
		return runner.StopReceipt{}, nil
	}
	return r.Service.AdoptWorkspace(ctx, request)
}

func capacityCounts(t *testing.T, ctx context.Context, d *Driver, want int) {
	t.Helper()
	var active, slots int
	if err := d.Store.Pool.QueryRow(ctx, `SELECT t.active_count,r.reserved_slots FROM tenant_runtime t CROSS JOIN runners r WHERE t.tenant_id='tenant' AND r.id='runner'`).Scan(&active, &slots); err != nil {
		t.Fatal(err)
	}
	if active != want || slots != want {
		t.Fatalf("capacity active=%d slots=%d, want %d", active, slots, want)
	}
}

func capacityRetryTime(t *testing.T, ctx context.Context, d *Driver, r persistence.Run) time.Time {
	t.Helper()
	var notBefore time.Time
	var state, runnerID, workspaceID string
	var epoch uint64
	if err := d.Store.Pool.QueryRow(ctx, `SELECT r.not_before,a.state,a.runner_id,r.workspace_id,a.lease_epoch FROM runs r JOIN runner_allocations a ON a.tenant_id=r.tenant_id AND a.run_id=r.id WHERE r.tenant_id=$1 AND r.id=$2`, r.TenantID, r.ID).Scan(&notBefore, &state, &runnerID, &workspaceID, &epoch); err != nil {
		t.Fatal(err)
	}
	got, err := d.Store.GetRun(ctx, r.TenantID, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State.Status != domain.StatusQueued || got.State.Version != r.State.Version+1 || got.State.Lease.Owner != "" || !got.State.Lease.Until.IsZero() || got.State.Lease.Epoch != r.State.Lease.Epoch || len(got.Commands) != 0 || got.RunnerID != r.RunnerID || workspaceID != string(r.ID) || runnerID != r.RunnerID || state != "released" || epoch != r.State.Lease.Epoch {
		t.Fatalf("requeue changed identity or retained capacity: %+v allocation=%s/%s/%d workspace=%s", got, runnerID, state, epoch, workspaceID)
	}
	var input flow.Event
	var raw []byte
	if err = d.Store.Pool.QueryRow(ctx, `SELECT input_event FROM run_snapshots WHERE tenant_id=$1 AND run_id=$2 AND version=$3`, r.TenantID, r.ID, got.State.Version).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &input); err != nil {
		t.Fatal(err)
	}
	if input.Kind != flow.EventCapacityRejected || input.NotBefore == nil || !input.NotBefore.Equal(notBefore) || notBefore.Sub(input.At) != persistence.RunnerCapacityBackoff || input.ExpectedVersion != r.State.Version || input.Owner != r.State.Lease.Owner || input.Epoch != r.State.Lease.Epoch {
		t.Fatalf("retry is not bound to DB event/lease: %s", raw)
	}
	t.Logf("capacity_requeue snapshot=%s allocation_state=%s runner=%s workspace=%s", raw, state, runnerID, workspaceID)
	return notBefore
}

func awaitCapacityRetry(t *testing.T, ctx context.Context, d *Driver, retry time.Time) {
	t.Helper()
	for {
		now, err := d.Store.DatabaseTime(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !now.Before(retry) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestPrepareCapacityRequeuesAtomically(t *testing.T) {
	d, original, provider, workspacePath := repairSetup(t)
	d.LeaseDuration = 30 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rejector := &capacityAdmissionRunner{Service: d.Runner, replies: []error{fmt.Errorf("fixture slot pool full: %w", domain.ErrCapacity), domain.ErrCapacity}}
	d.Runner = rejector
	claimed, err := d.Store.Claim(ctx, "capacity-worker", d.LeaseDuration)
	if err != nil {
		t.Fatal(err)
	}
	capacityCounts(t, ctx, d, 1)
	if err = d.Drive(ctx, claimed); !errors.Is(err, errDeferred) {
		t.Fatalf("capacity rejection failed run: %v", err)
	}
	capacityCounts(t, ctx, d, 0)
	retry := capacityRetryTime(t, ctx, d, claimed)
	if _, err = os.Stat(workspacePath); !os.IsNotExist(err) {
		t.Fatalf("admission fixture performed workspace I/O: %v", err)
	}
	if provider.calls.Load() != 0 || rejector.adopts.Load() != 0 {
		t.Fatal("admission rejection invoked provider or adoption")
	}
	if _, err = d.Store.Claim(ctx, "too-early", d.LeaseDuration); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("not_before was ignored: %v", err)
	}
	// Another active run ensures duplicate release cannot hide behind zero or a
	// clamped counter. It remains leased throughout both rejected epochs.
	sentinel, _, err := d.Store.Submit(ctx, persistence.SubmitRequest{TenantID: original.TenantID, PrincipalID: original.PrincipalID, ProjectID: original.ProjectID, Task: "capacity accounting sentinel", BaseCommit: original.BaseCommit, Config: original.Config}, "capacity-sentinel")
	if err != nil {
		t.Fatal(err)
	}
	sentinel, err = d.Store.Claim(ctx, "sentinel-worker", d.LeaseDuration)
	if err != nil || sentinel.ID == original.ID {
		t.Fatalf("sentinel could not claim during backoff: %+v / %v", sentinel, err)
	}
	capacityCounts(t, ctx, d, 1)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Go(func() { errs <- d.Store.RequeueCapacity(ctx, claimed) })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if !errors.Is(err, domain.ErrFenced) {
			t.Fatalf("duplicate requeue was not fenced: %v", err)
		}
	}
	capacityCounts(t, ctx, d, 1)
	awaitCapacityRetry(t, ctx, d, retry)
	if _, err = d.Store.ClaimOnRunner(ctx, "other-runner", d.LeaseDuration, "different-runner"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("rejected workspace migrated to another runner: %v", err)
	}
	second, err := d.Store.ClaimOnRunner(ctx, "retry-worker", d.LeaseDuration, claimed.RunnerID)
	if err != nil || second.ID != original.ID || second.State.Lease.Epoch != claimed.State.Lease.Epoch+1 {
		t.Fatalf("retry did not reclaim the original run with a new epoch: %+v / %v", second, err)
	}
	capacityCounts(t, ctx, d, 2)
	if err = d.Drive(ctx, second); !errors.Is(err, errDeferred) {
		t.Fatal(err)
	}
	capacityCounts(t, ctx, d, 1)
	retry = capacityRetryTime(t, ctx, d, second)
	if err = d.Store.RequeueCapacity(ctx, second); !errors.Is(err, domain.ErrFenced) {
		t.Fatalf("second rejection replay: %v", err)
	}
	capacityCounts(t, ctx, d, 1)
	awaitCapacityRetry(t, ctx, d, retry)
	third, err := d.Store.Claim(ctx, "accepted-worker", d.LeaseDuration)
	if err != nil || third.ID != original.ID || third.State.Lease.Epoch != second.State.Lease.Epoch+1 {
		t.Fatalf("final admission claim: %+v / %v", third, err)
	}
	if err = d.Drive(ctx, third); err != nil {
		t.Fatal(err)
	}
	finished, err := d.Store.GetRun(ctx, original.TenantID, original.ID)
	if err != nil || finished.State.Status != domain.StatusCompleted || provider.calls.Load() != 3 || rejector.prepares.Load() != 3 {
		t.Fatalf("accepted retry did not complete once: %+v err=%v model_calls=%d prepares=%d", finished, err, provider.calls.Load(), rejector.prepares.Load())
	}
	capacityCounts(t, ctx, d, 1)
	events, err := d.Store.Events(ctx, original.TenantID, original.ID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	rejections := 0
	for n, event := range events {
		if event.Seq != uint64(n+1) {
			t.Fatal("durable event cursor gap")
		}
		if event.Type == "run.capacity_rejected" {
			rejections++
		}
	}
	if rejections != 2 {
		t.Fatalf("duplicate requeue emitted events: %d", rejections)
	}
	if _, err = d.Store.Cancel(ctx, persistence.Identity{TenantID: sentinel.TenantID, PrincipalID: sentinel.PrincipalID, Role: "developer"}, sentinel.ID); err != nil {
		t.Fatal(err)
	}
	if err = d.Drive(ctx, sentinel); err != nil {
		t.Fatal(err)
	}
	capacityCounts(t, ctx, d, 0)
	t.Logf("capacity_result run=%s terminal=%s epochs=1,2,3 rejections=%d model_calls=%d final_active=0 final_reserved_slots=0 events=%d", original.ID, finished.State.Status, rejections, provider.calls.Load(), len(events))
}

func TestPrepareCapacityDoesNotDiscardDurableIntent(t *testing.T) {
	d, _, _, _ := repairSetup(t)
	d.LeaseDuration = 30 * time.Second
	ctx := context.Background()
	claimed, err := d.Store.Claim(ctx, "intent-worker", d.leaseDuration())
	if err != nil {
		t.Fatal(err)
	}
	args := json.RawMessage(`{"target":true}`)
	hash := sha256.Sum256(args)
	intent := flow.Effect{ID: "initial-target-intent", Kind: "verify", Args: args, ArgsHash: hex.EncodeToString(hash[:]), ExpectedRevision: 1, DispatchEpoch: claimed.State.Lease.Epoch, PolicyVersion: "fixture-v1"}
	stored, err := d.Store.PlanSystemEffect(ctx, claimed, intent, -1)
	if err != nil {
		t.Fatal(err)
	}
	d.Runner = &capacityAdmissionRunner{Service: d.Runner, replies: []error{domain.ErrCapacity}}
	if err = d.Drive(ctx, claimed); !errors.Is(err, errDeferred) || !errors.Is(err, domain.ErrTransition) {
		t.Fatalf("capacity response discarded existing work: %v", err)
	}
	after, err := d.Store.GetRun(ctx, claimed.TenantID, claimed.ID)
	if err != nil || !reflect.DeepEqual(after.State, claimed.State) || !reflect.DeepEqual(after.Commands, claimed.Commands) {
		t.Fatalf("failed requeue mutated run: %+v / %v", after, err)
	}
	effects, err := d.Store.Effects(ctx, claimed.TenantID, claimed.ID, 0)
	if err != nil || len(effects) != 1 || !reflect.DeepEqual(effects[0].Effect, stored) {
		t.Fatalf("durable intent changed: %+v / %v", effects, err)
	}
	capacityCounts(t, ctx, d, 1)
	if _, err = d.Store.Advance(ctx, claimed.TenantID, claimed.ID, flow.Event{Kind: flow.EventCapacityRejected}); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("Advance bypassed capacity transaction: %v", err)
	}
}

func TestPrepareNonCapacityErrorsPreserveHandling(t *testing.T) {
	for _, code := range []error{domain.ErrInvalid, domain.ErrReconciliation} {
		t.Run(code.Error(), func(t *testing.T) {
			d, original, provider, _ := repairSetup(t)
			d.LeaseDuration = 30 * time.Second
			ctx := context.Background()
			d.Runner = &capacityAdmissionRunner{Service: d.Runner, replies: []error{code}}
			claimed, err := d.Store.Claim(ctx, "error-worker", d.leaseDuration())
			if err != nil {
				t.Fatal(err)
			}
			err = d.Drive(ctx, claimed)
			got, readErr := d.Store.GetRun(ctx, original.TenantID, original.ID)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if errors.Is(code, domain.ErrReconciliation) {
				if !errors.Is(err, errDeferred) || got.State.Status != domain.StatusRunning || got.State.Version != claimed.State.Version || got.RunnerID != claimed.RunnerID {
					t.Fatalf("unknown Prepare was moved/requeued: %+v / %v", got, err)
				}
				capacityCounts(t, ctx, d, 1)
			} else {
				if err != nil || got.State.Status != domain.StatusFailed {
					t.Fatalf("noncapacity error behavior changed: %+v / %v", got, err)
				}
				capacityCounts(t, ctx, d, 0)
			}
			if provider.calls.Load() != 0 {
				t.Fatal("failed Prepare invoked model")
			}
		})
	}
}

func TestPrepareCapacityAfterAdoptionCannotRequeue(t *testing.T) {
	d, _, _, _ := repairSetup(t)
	d.LeaseDuration = 30 * time.Second
	ctx := context.Background()
	claimed, err := d.Store.Claim(ctx, "adopt-worker", d.leaseDuration())
	if err != nil {
		t.Fatal(err)
	}
	r := &capacityAdmissionRunner{Service: d.Runner, replies: []error{domain.ErrFenced, domain.ErrCapacity}, fakeAdopt: true}
	d.Runner = r
	if _, err = d.execute(ctx, claimed, claimed.Commands[0]); !errors.Is(err, domain.ErrCapacity) || errors.Is(err, errDeferred) {
		t.Fatalf("post-adoption capacity was incorrectly requeued: %v", err)
	}
	after, err := d.Store.GetRun(ctx, claimed.TenantID, claimed.ID)
	if err != nil || !reflect.DeepEqual(after.State, claimed.State) || r.adopts.Load() != 1 {
		t.Fatalf("post-adoption run mutated: %+v / %v", after, err)
	}
	capacityCounts(t, ctx, d, 1)
}
