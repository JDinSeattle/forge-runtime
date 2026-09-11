package persistence

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	flow "github.com/JDinSeattle/forge-runtime/internal/runtime"
	"github.com/JDinSeattle/forge-runtime/internal/telemetry"
	"github.com/jackc/pgx/v5"
)

type observationCommit struct {
	pgx.Tx
	failure error
}

func (t observationCommit) Commit(context.Context) error   { return t.failure }
func (t observationCommit) Rollback(context.Context) error { return nil }

func TestObservationOnlyAfterConfirmedCommit(t *testing.T) {
	ctx := context.Background()
	for _, name := range []string{"committed", "rollback", "failed_commit", "ambiguous_commit"} {
		t.Run(name, func(t *testing.T) {
			var calls int
			fake := observationCommit{}
			if name == "failed_commit" {
				fake.failure = pgx.ErrTxCommitRollback
			}
			if name == "ambiguous_commit" {
				fake.failure = errors.New("connection lost while awaiting COMMIT")
			}
			tx := &observedTx{Tx: fake, after: []func(){func() { calls++ }}}
			if name == "rollback" {
				_ = tx.Rollback(ctx)
			} else {
				_ = tx.Commit(ctx)
			}
			// Repeated cleanup/Commit cannot re-observe a cleared callback, including a
			// lost COMMIT acknowledgement. The database may have committed in that case;
			// metric omission is preferable to claiming an unconfirmed observation.
			_ = tx.Commit(ctx)
			_ = tx.Rollback(ctx)
			want := 0
			if name == "committed" {
				want = 1
			}
			if calls != want {
				t.Fatalf("callbacks=%d want=%d", calls, want)
			}
		})
	}
}

func TestTelemetryPostgresCommitRollbackAndReconciliation(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	m, err := telemetry.Setup(ctx, "forge-worker", "")
	if err != nil {
		t.Fatal(err)
	}
	defer m.Shutdown(ctx)
	s.Telemetry = m
	i, _, _ := fixture(t, s)
	counter := func(name string) float64 {
		fs, e := m.Registry.Gather()
		if e != nil {
			t.Fatal(e)
		}
		var sum float64
		for _, f := range fs {
			if f.GetName() == name {
				for _, v := range f.Metric {
					sum += v.GetCounter().GetValue()
				}
			}
		}
		return sum
	}
	for _, commit := range []bool{false, true} {
		tx, e := s.Tx(ctx, i.TenantID, pgx.TxOptions{})
		if e != nil {
			t.Fatal(e)
		}
		old := flow.State{Version: 1, Status: domain.StatusRunning}
		next := flow.State{Version: 2, Status: domain.StatusNeedsReconciliation}
		observeTransition(tx, old, next)
		observeTransition(tx, next, next) // Same-version replay contributes no observation.
		if counter("forge_runtime_reconciliation_total") != 0 {
			t.Fatal("observed uncommitted transition")
		}
		if commit {
			e = tx.Commit(ctx)
		} else {
			e = tx.Rollback(ctx)
		}
		if e != nil {
			t.Fatal(e)
		}
	}
	if counter("forge_runtime_reconciliation_total") != 1 || counter("forge_runtime_run_transitions_total") != 1 {
		t.Fatal("committed transition delta")
	}
	tx, err := s.Tx(ctx, i.TenantID, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	observeTransition(tx, flow.State{Version: 1, Status: domain.StatusRunning}, flow.State{Version: 2, Status: domain.StatusNeedsReconciliation})
	if _, err = tx.Exec(ctx, "SELECT 1/0"); err == nil {
		t.Fatal("expected aborted transaction")
	}
	if err = tx.Commit(ctx); err == nil {
		t.Fatal("aborted transaction committed")
	}
	if counter("forge_runtime_reconciliation_total") != 1 {
		t.Fatal("aborted transaction counted")
	}
}

func TestTelemetryImmediateDeferNeverCountsExpiration(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	m, err := telemetry.Setup(ctx, "forge-worker", "")
	if err != nil {
		t.Fatal(err)
	}
	defer m.Shutdown(ctx)
	s.Telemetry = m
	i, p, c := fixture(t, s)
	submitFixture(t, s, i, p, c, "defer")
	if err = s.RegisterRunner(ctx, "telemetry_runner", "private-fixture", 1); err != nil {
		t.Fatal(err)
	}
	var equal int
	for n := range 100 {
		r, e := s.ClaimOnRunner(ctx, "worker", time.Minute, "telemetry_runner")
		if e != nil {
			t.Fatal(e)
		}
		if n > 0 {
			fs, e := m.Registry.Gather()
			if e != nil {
				t.Fatal(e)
			}
			for _, f := range fs {
				if f.GetName() == "forge_runtime_lease_expirations_total" && f.Metric[0].GetCounter().GetValue() != 0 {
					t.Fatalf("active Defer counted as lease expiry after %d iterations (%d equal SQL timestamps)", n, equal)
				}
			}
		}
		if e = s.Defer(ctx, r, time.Now().Add(-time.Second), "fixture.deferred"); e != nil {
			t.Fatal(e)
		}
		var same bool
		if e = s.Pool.QueryRow(ctx, `SELECT runnable_at=lease_until FROM runs WHERE tenant_id=$1 AND id=$2`, i.TenantID, r.ID).Scan(&same); e != nil {
			t.Fatal(e)
		}
		if same {
			equal++
		}
	}
	t.Logf("100 immediate Defer cycles; %d equal-microsecond SQL timestamps; zero false expirations", equal)
}
