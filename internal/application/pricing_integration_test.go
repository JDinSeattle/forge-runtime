package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/provider"
	flow "github.com/JDinSeattle/forge-runtime/internal/runtime"
)

func freezeAtModelFault(t *testing.T, d *Driver, point string) persistence.Run {
	t.Helper()
	ctx := context.Background()
	d.Fault = func(got string) error {
		if got == point {
			return context.Canceled
		}
		return nil
	}
	claimed, err := d.Store.Claim(ctx, "before-crash", d.leaseDuration())
	if err != nil {
		t.Fatal(err)
	}
	if err = d.Drive(ctx, claimed); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected fault %s, got %v", point, err)
	}
	d.Fault = nil
	return reclaimPricingRun(t, d, claimed)
}
func reclaimPricingRun(t *testing.T, d *Driver, r persistence.Run) persistence.Run {
	t.Helper()
	ctx := context.Background()
	if _, err := d.Store.Pool.Exec(ctx, `UPDATE runs SET lease_until=clock_timestamp()-interval '1 second',not_before=clock_timestamp() WHERE tenant_id=$1 AND id=$2`, r.TenantID, r.ID); err != nil {
		t.Fatal(err)
	}
	recovered, err := d.Store.Claim(ctx, "after-crash", d.leaseDuration())
	if err != nil {
		t.Fatal(err)
	}
	return recovered
}
func TestAttemptPricingSurvivesCrashAndConfigChange(t *testing.T) {
	for _, point := range []string{"after_model_pricing_before_reservation", "after_model_result_before_transition", "after_model_settlement_before_transition"} {
		t.Run(point, func(t *testing.T) {
			d, _, p, _ := repairSetup(t)
			ctx := context.Background()
			spec := d.Models["fake/fake"]
			spec.PriceVersion = "original-v1"
			spec.InputPrice = 1_000_000
			spec.OutputPrice = 2_000_000
			d.Models["fake/fake"] = spec
			recovered := freezeAtModelFault(t, d, point)
			a, err := d.Store.LatestAttempt(ctx, recovered.TenantID, recovered.ID, recovered.State.StepSeq)
			if err != nil {
				t.Fatal(err)
			}
			originalDeadline := a.Deadline
			spec.PriceVersion = "changed-v2"
			spec.InputPrice = 100_000_000
			spec.OutputPrice = 200_000_000
			spec.RequestTimeout = time.Hour
			d.Models["fake/fake"] = spec
			if point != "after_model_pricing_before_reservation" {
				delete(d.Models, "fake/fake")
				delete(d.Providers, "fake")
			}
			event, err := d.callModel(ctx, recovered)
			if err != nil {
				t.Fatal(err)
			}
			if event.Kind != flow.EventModelCompleted || event.Cost != 200 || p.calls.Load() != 1 {
				t.Fatalf("event=%+v calls=%d", event, p.calls.Load())
			}
			a, err = d.Store.LatestAttempt(ctx, recovered.TenantID, recovered.ID, recovered.State.StepSeq)
			if err != nil {
				t.Fatal(err)
			}
			if !a.Deadline.Equal(originalDeadline) || a.PriceVersion != "original-v1" {
				t.Fatalf("attempt identity changed: %+v", a)
			}
			reservation, err := d.Quota.Get(ctx, string(recovered.TenantID), string(a.ID))
			if err != nil {
				t.Fatal(err)
			}
			if reservation.ActualCost == nil || *reservation.ActualCost != 200 || reservation.Cost != 10_240 {
				t.Fatalf("reservation=%+v", reservation)
			}
			// A second delivery of the completion settles exactly once and contributes
			// no new budget cost after the reducer has already observed the charge.
			recovered.State.Cost = event.Cost
			replay, err := d.completedModel(ctx, recovered, a)
			if err != nil || replay.Cost != 0 {
				t.Fatalf("settled replay=%+v err=%v", replay, err)
			}
			// Later definitive settlement can make the ledger smaller than the previous
			// conservative reducer charge. That cannot block a valid completion replay.
			recovered.State.Cost = 10_240
			replay, err = d.completedModel(ctx, recovered, a)
			if err != nil || replay.Cost != 0 {
				t.Fatalf("late lower ledger=%+v err=%v", replay, err)
			}
			if _, err = d.Store.Pool.Exec(ctx, `UPDATE model_attempts SET pricing=jsonb_set(pricing,'{price_version}','"tampered"') WHERE tenant_id=$1 AND attempt_id=$2`, a.TenantID, a.ID); err == nil {
				t.Fatal("pricing snapshot was mutable")
			}
		})
	}
}
func TestModelRetryDecisionSurvivesRecovery(t *testing.T) {
	d, _, p, _ := repairSetup(t)
	ctx := context.Background()
	r := freezeAtModelFault(t, d, "after_model_pricing_before_reservation")
	p.scripts[0] = provider.Script{Failure: &provider.Error{Kind: provider.ErrRateLimited, RetryAfter: 20 * time.Second}}
	if _, err := d.callModel(ctx, r); !errors.Is(err, errDeferred) {
		t.Fatalf("retry=%v", err)
	}
	a, err := d.Store.LatestAttempt(ctx, r.TenantID, r.ID, r.State.StepSeq)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := d.Store.AttemptFailure(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	if !policy.Retry || policy.Outcome != "failure" || time.Until(policy.NotBefore) < 18*time.Second {
		t.Fatalf("policy=%+v", policy)
	}
	r = reclaimPricingRun(t, d, r)
	if _, err = d.callModel(ctx, r); !errors.Is(err, errDeferred) {
		t.Fatalf("replayed retry=%v", err)
	}
	latest, err := d.Store.LatestAttempt(ctx, r.TenantID, r.ID, r.State.StepSeq)
	if err != nil {
		t.Fatal(err)
	}
	if latest.ID != a.ID || p.calls.Load() != 1 {
		t.Fatal("recovery bypassed persisted Retry-After")
	}
	again, err := d.Store.AttemptFailure(ctx, a)
	if err != nil || !again.NotBefore.Equal(policy.NotBefore) {
		t.Fatalf("policy resampled: %+v %v", again, err)
	}
	q, err := d.Quota.Snapshot(ctx, "fake")
	if err != nil || q.ConsecutiveFailures != 1 {
		t.Fatalf("breaker outcome duplicated: %+v %v", q, err)
	}
}
func TestPermanentModelFailureDoesNotRetryOrTripCircuit(t *testing.T) {
	d, _, p, _ := repairSetup(t)
	ctx := context.Background()
	r := freezeAtModelFault(t, d, "after_model_pricing_before_reservation")
	p.scripts[0] = provider.Script{Failure: &provider.Error{Kind: provider.ErrInvalidRequest}}
	if _, err := d.callModel(ctx, r); err == nil || errors.Is(err, errDeferred) {
		t.Fatalf("permanent failure=%v", err)
	}
	a, err := d.Store.LatestAttempt(ctx, r.TenantID, r.ID, r.State.StepSeq)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := d.Store.AttemptFailure(ctx, a)
	if err != nil || policy.Retry || policy.Outcome != "neutral" {
		t.Fatalf("policy=%+v err=%v", policy, err)
	}
	r = reclaimPricingRun(t, d, r)
	if _, err = d.callModel(ctx, r); err == nil || errors.Is(err, errDeferred) {
		t.Fatalf("permanent replay=%v", err)
	}
	if p.calls.Load() != 1 {
		t.Fatal("permanent failure was retried")
	}
	q, err := d.Quota.Snapshot(ctx, "fake")
	if err != nil || q.ConsecutiveFailures != 0 {
		t.Fatalf("local/permanent error tripped provider circuit: %+v %v", q, err)
	}
}

func TestModelRetryOverallBudgetCannotResetOnRecovery(t *testing.T) {
	d, _, p, _ := repairSetup(t)
	ctx := context.Background()
	r := freezeAtModelFault(t, d, "after_model_pricing_before_reservation")
	p.scripts[0] = provider.Script{Failure: &provider.Error{Kind: provider.ErrUnavailable}}
	if _, err := d.callModel(ctx, r); !errors.Is(err, errDeferred) {
		t.Fatalf("initial retry=%v", err)
	}
	a, err := d.Store.LatestAttempt(ctx, r.TenantID, r.ID, r.State.StepSeq)
	if err != nil {
		t.Fatal(err)
	}
	// Recovery can occur well after its persisted retry time. The total budget
	// remains anchored to the first attempt, including time spent off-worker.
	if _, err = d.Store.Pool.Exec(ctx, `UPDATE model_attempts SET started_at=clock_timestamp()-interval '3 minutes' WHERE tenant_id=$1 AND attempt_id=$2`, a.TenantID, a.ID); err != nil {
		t.Fatal(err)
	}
	r = reclaimPricingRun(t, d, r)
	if _, err = d.callModel(ctx, r); !errors.Is(err, domain.ErrCapacity) {
		t.Fatalf("expired total retry budget=%v", err)
	}
	if p.calls.Load() != 1 {
		t.Fatal("recovery reset the total retry budget")
	}
}

func TestModelAdmissionKeepsConservativeReducerBudget(t *testing.T) {
	d, _, p, _ := repairSetup(t)
	ctx := context.Background()
	spec := d.Models["fake/fake"]
	spec.InputPrice = 1_000_000
	spec.OutputPrice = 2_000_000
	d.Models["fake/fake"] = spec
	r := freezeAtModelFault(t, d, "after_model_pricing_before_reservation")
	// Simulate earlier unknown consumption subsequently settled to a smaller
	// ledger value. The reducer's previously consumed budget stays monotonic.
	r.State.Cost = r.State.Limits.MaxCost - 100
	event, err := d.callModel(ctx, r)
	if err != nil || event.Kind != flow.EventBudgetReached || p.calls.Load() != 0 {
		t.Fatalf("budget admission=%+v calls=%d err=%v", event, p.calls.Load(), err)
	}
}
