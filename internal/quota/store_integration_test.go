package quota

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/db"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func quotaFixture(t *testing.T) (*Store, string, string, string) {
	t.Helper()
	dsn := os.Getenv("FORGE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set FORGE_TEST_DATABASE_URL to isolated loopback /forge database")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || u.Path != "/forge" {
		t.Fatal("quota integration tests require loopback /forge database")
	}
	ctx := context.Background()
	if err = db.Migrate(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	tenant, run, group := "qt_"+uuid.NewString(), "qr_"+uuid.NewString(), "qg_"+uuid.NewString()
	for _, stmt := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO tenants(id,name) VALUES($1,'quota fixture')`, []any{tenant}},
		{`INSERT INTO projects(tenant_id,id,name,repo_source,repo_profile) VALUES($1,'project','quota','fixture','{}')`, []any{tenant}},
		{`INSERT INTO runs(tenant_id,id,project_id,principal_id,task,base_commit,state,version,snapshot,config_snapshot) VALUES($1,$2,'project','fixture','quota','base','queued',1,'{}','{}')`, []any{tenant, run}},
	} {
		if _, err = pool.Exec(ctx, stmt.sql, stmt.args...); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for _, table := range []string{"quota_reservations", "runs", "projects"} {
			if _, err := pool.Exec(context.Background(), "DELETE FROM "+table+" WHERE tenant_id=$1", tenant); err != nil {
				t.Error(err)
			}
		}
		if _, err := pool.Exec(context.Background(), `DELETE FROM provider_quotas WHERE credential_group=$1`, group); err != nil {
			t.Error(err)
		}
		if _, err := pool.Exec(context.Background(), `DELETE FROM tenants WHERE id=$1`, tenant); err != nil {
			t.Error(err)
		}
	})
	s := New(pool)
	if err = s.Configure(ctx, Config{CredentialGroup: group, MaxConcurrent: 4, MaxTokens: 10000, MaxCost: 10000, WindowDuration: time.Hour, FailureThreshold: 2, BreakerCooldown: time.Minute}); err != nil {
		t.Fatal(err)
	}
	return s, tenant, run, group
}

func TestSharedBudgetsAcrossTenantsAndIndependentPools(t *testing.T) {
	s, tenant, run, group := quotaFixture(t)
	other, otherTenant, otherRun, _ := quotaFixture(t)
	ctx := context.Background()
	if err := s.Configure(ctx, Config{CredentialGroup: group, MaxConcurrent: 4, MaxTokens: 201, MaxCost: 101, WindowDuration: time.Hour, FailureThreshold: 2, BreakerCooldown: time.Minute}); err != nil {
		t.Fatal(err)
	}
	first := quotaRequest(tenant, run, group, "first")
	if _, err := s.Reserve(ctx, first); err != nil {
		t.Fatal(err)
	}
	second := quotaRequest(otherTenant, otherRun, group, "second")
	second.InputTokens = 0
	second.MaxOutputTokens = 2
	second.MaxCost = 1
	if _, err := other.Reserve(ctx, second); !errors.Is(err, domain.ErrCapacity) {
		t.Fatalf("another tenant bypassed token budget: %v", err)
	}
	second.MaxOutputTokens = 1
	if _, err := other.Reserve(ctx, second); err != nil {
		t.Fatal(err)
	}
	if err := other.AbandonBeforeDispatch(ctx, otherTenant, first.AttemptID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant release=%v", err)
	}
	if err := s.AbandonBeforeDispatch(ctx, tenant, first.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := s.AbandonBeforeDispatch(ctx, tenant, first.AttemptID); err != nil {
		t.Fatal(err)
	}
	q, err := s.Snapshot(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	if q.ActiveRequests != 1 || q.ReservedTokens != 1 || q.ReservedCost != 1 || q.CommittedCost != 0 {
		t.Fatalf("release was not exact: %+v", q)
	}
	if err = s.Configure(ctx, Config{CredentialGroup: group, MaxConcurrent: 4, MaxTokens: 201, MaxCost: 1, WindowDuration: time.Hour, FailureThreshold: 2, BreakerCooldown: time.Minute}); err != nil {
		t.Fatal(err)
	}
	third := quotaRequest(tenant, run, group, "third")
	third.InputTokens = 0
	third.MaxOutputTokens = 1
	third.MaxCost = 1
	if _, err = s.Reserve(ctx, third); !errors.Is(err, domain.ErrCapacity) {
		t.Fatalf("cost budget bypassed: %v", err)
	}
}
func quotaRequest(tenant, run, group, id string) Request {
	return Request{TenantID: tenant, RunID: run, AttemptID: id, CredentialGroup: group, InputTokens: 100, MaxOutputTokens: 100, MaxCost: 100, PriceVersion: "fixture_v1", RequestDeadline: time.Now().Add(time.Minute)}
}

func TestGlobalAdmissionAndIdempotencyAcrossStoreInstances(t *testing.T) {
	s, tenant, run, group := quotaFixture(t)
	ctx := context.Background()
	var admitted atomic.Int32
	var wg sync.WaitGroup
	errs := make(chan error, 40)
	for n := range 40 {
		wg.Go(func() {
			other := New(s.Pool)
			_, err := other.Reserve(ctx, quotaRequest(tenant, run, group, fmt.Sprintf("attempt_%d", n)))
			if err == nil {
				admitted.Add(1)
			} else if !errors.Is(err, domain.ErrCapacity) {
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	q, err := s.Snapshot(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	if admitted.Load() != 4 || q.ActiveRequests != 4 || q.ReservedTokens != 800 || q.ReservedCost != 400 {
		t.Fatalf("admitted=%d snapshot=%+v", admitted.Load(), q)
	}
	var attempt string
	if err = s.Pool.QueryRow(ctx, `SELECT id FROM quota_reservations WHERE tenant_id=$1 LIMIT 1`, tenant).Scan(&attempt); err != nil {
		t.Fatal(err)
	}
	r, err := s.Get(ctx, tenant, attempt)
	if err != nil {
		t.Fatal(err)
	}
	req := quotaRequest(tenant, run, group, attempt)
	req.RequestDeadline = r.RequestDeadline
	if _, err = s.Reserve(ctx, req); err != nil {
		t.Fatalf("idempotent duplicate while full: %v", err)
	}
	req.MaxCost++
	if _, err = s.Reserve(ctx, req); !errors.Is(err, ErrConflict) {
		t.Fatalf("mismatched reservation=%v", err)
	}
	if _, err = s.Get(ctx, "other_tenant", attempt); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross tenant=%v", err)
	}
}

func TestSettlementExactlyOnceAndActualOverage(t *testing.T) {
	s, tenant, run, group := quotaFixture(t)
	ctx := context.Background()
	req := quotaRequest(tenant, run, group, "attempt")
	if _, err := s.Reserve(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkDispatched(ctx, tenant, req.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkDispatched(ctx, tenant, req.AttemptID); !errors.Is(err, ErrAlreadyDispatched) {
		t.Fatal("duplicate dispatch must require reconciliation", err)
	}
	if err := s.AbandonBeforeDispatch(ctx, tenant, req.AttemptID); !errors.Is(err, ErrAlreadyDispatched) {
		t.Fatal("dispatched request released as free", err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for range 20 {
		wg.Go(func() { errs <- s.Settle(ctx, tenant, req.AttemptID, Settlement{Tokens: 250, Cost: 150}) })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	q, err := s.Snapshot(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	if q.ActiveRequests != 0 || q.ReservedTokens != 0 || q.ReservedCost != 0 || q.CommittedTokens != 250 || q.CommittedCost != 150 {
		t.Fatalf("double settlement: %+v", q)
	}
	if err = s.Settle(ctx, tenant, req.AttemptID, Settlement{Tokens: 1, Cost: 1}); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed actuals accepted: %v", err)
	}
	if err = s.MarkUnknown(ctx, tenant, req.AttemptID); !errors.Is(err, ErrConflict) {
		t.Fatalf("settled was overwritten unknown: %v", err)
	}
}

func TestUnknownRetainsTokenAndCostAcrossExpiryAndWindowRotation(t *testing.T) {
	s, tenant, run, group := quotaFixture(t)
	ctx := context.Background()
	req := quotaRequest(tenant, run, group, "unknown")
	if _, err := s.Reserve(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkDispatched(ctx, tenant, req.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkUnknown(ctx, tenant, req.AttemptID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool.Exec(ctx, `UPDATE quota_reservations SET request_deadline=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND id=$2`, tenant, req.AttemptID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool.Exec(ctx, `UPDATE provider_quotas SET window_ends_at=clock_timestamp()-interval '1 second',committed_tokens=50,committed_microusd=10 WHERE credential_group=$1`, group); err != nil {
		t.Fatal(err)
	}
	for n := range 2 {
		count, err := s.ExpireRequestSlots(ctx, group)
		if err != nil {
			t.Fatal(err)
		}
		if count != 1-n {
			t.Fatal("slot was released more than once", count)
		}
	}
	q, err := s.Snapshot(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	if q.ActiveRequests != 0 || q.ReservedTokens != 200 || q.ReservedCost != 100 || q.CommittedTokens != 0 || q.CommittedCost != 0 || q.WindowGeneration != 2 {
		t.Fatalf("unknown reservation lost: %+v", q)
	}
	if err = s.Settle(ctx, tenant, req.AttemptID, Settlement{Tokens: 180, Cost: 90}); err != nil {
		t.Fatal(err)
	}
	q, err = s.Snapshot(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	if q.ActiveRequests != 0 || q.ReservedCost != 0 || q.CommittedCost != 90 {
		t.Fatalf("late settlement=%+v", q)
	}
}

func TestCircuitSharedSingleProbeAndCrashAfterSettlement(t *testing.T) {
	s, tenant, run, group := quotaFixture(t)
	ctx := context.Background()
	for n := range 2 {
		req := quotaRequest(tenant, run, group, fmt.Sprintf("failed_%d", n))
		if _, err := s.Reserve(ctx, req); err != nil {
			t.Fatal(err)
		}
		if err := s.MarkDispatched(ctx, tenant, req.AttemptID); err != nil {
			t.Fatal(err)
		}
		if err := s.RecordOutcome(ctx, tenant, req.AttemptID, "failure"); err != nil {
			t.Fatal(err)
		}
		if err := s.RecordOutcome(ctx, tenant, req.AttemptID, "failure"); err != nil {
			t.Fatal(err)
		}
		if err := s.Settle(ctx, tenant, req.AttemptID, Settlement{}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Reserve(ctx, quotaRequest(tenant, run, group, "denied")); !errors.Is(err, domain.ErrCapacity) {
		t.Fatalf("open breaker allowed reserve: %v", err)
	}
	if _, err := s.Pool.Exec(ctx, `UPDATE provider_quotas SET breaker_until=clock_timestamp()-interval '1 second' WHERE credential_group=$1`, group); err != nil {
		t.Fatal(err)
	}
	probe := quotaRequest(tenant, run, group, "probe")
	if _, err := s.Reserve(ctx, probe); err != nil {
		t.Fatal(err)
	}
	if _, err := New(s.Pool).Reserve(ctx, quotaRequest(tenant, run, group, "extra_probe")); !errors.Is(err, domain.ErrCapacity) {
		t.Fatal("multiple half-open probes admitted", err)
	}
	if err := s.MarkDispatched(ctx, tenant, probe.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := s.Settle(ctx, tenant, probe.AttemptID, Settlement{}); err != nil {
		t.Fatal(err)
	}
	// The process disappears here before RecordOutcome; slot is already released.
	if _, err := s.Pool.Exec(ctx, `UPDATE quota_reservations SET request_deadline=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND id=$2`, tenant, probe.AttemptID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ExpireRequestSlots(ctx, group); err != nil {
		t.Fatal(err)
	}
	q, err := s.Snapshot(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	if q.ProbeAttemptID != nil || q.BreakerUntil == nil {
		t.Fatalf("orphan probe not recovered: %+v", q)
	}
	if _, err = s.Pool.Exec(ctx, `UPDATE provider_quotas SET breaker_until=clock_timestamp()-interval '1 second' WHERE credential_group=$1`, group); err != nil {
		t.Fatal(err)
	}
	recovery := quotaRequest(tenant, run, group, "recovery")
	if _, err = s.Reserve(ctx, recovery); err != nil {
		t.Fatal(err)
	}
	if err = s.MarkDispatched(ctx, tenant, recovery.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err = s.RecordOutcome(ctx, tenant, recovery.AttemptID, "success"); err != nil {
		t.Fatal(err)
	}
	q, err = s.Snapshot(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	if q.BreakerUntil != nil || q.ProbeAttemptID != nil || q.ConsecutiveFailures != 0 {
		t.Fatalf("breaker did not heal: %+v", q)
	}
}
