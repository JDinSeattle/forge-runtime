package persistence

import (
	"context"

	"github.com/JDinSeattle/forge-runtime/internal/dependency"
	flow "github.com/JDinSeattle/forge-runtime/internal/runtime"
	"github.com/JDinSeattle/forge-runtime/internal/telemetry"
	"github.com/jackc/pgx/v5"
)

// Callbacks are synchronous, bounded in-process observations AFTER Commit.
// They never perform network I/O. A process crash in this gap can omit metrics;
// these counters are not an exactly-once ledger or an alternative to PostgreSQL.
type observedTx struct {
	pgx.Tx
	telemetry *telemetry.Telemetry
	after     []func()
}

func (t *observedTx) Commit(ctx context.Context) error {
	callbacks := t.after
	t.after = nil
	if err := t.Tx.Commit(ctx); err != nil {
		return err
	}
	for _, f := range callbacks {
		f()
	}
	return nil
}
func (t *observedTx) Rollback(ctx context.Context) error { t.after = nil; return t.Tx.Rollback(ctx) }
func (s *Store) begin(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	tx, err := dependency.BeginTx(ctx, s.Pool, opts)
	if err != nil {
		return nil, err
	}
	if s.Telemetry == nil {
		return tx, nil
	}
	return &observedTx{Tx: tx, telemetry: s.Telemetry}, nil
}
func observeTransition(tx pgx.Tx, old, next flow.State) {
	if t, ok := tx.(*observedTx); ok && old.Version != next.Version {
		t.after = append(t.after, func() { t.telemetry.Transition(old.Status, next.Status, old.Version, next.Version) })
	}
}
func (s *Store) phase(ctx context.Context, name string, i telemetry.Identity) (context.Context, func(telemetry.Outcome)) {
	if s.Telemetry != nil {
		return s.Telemetry.StartPhase(ctx, name, i)
	}
	return ctx, func(telemetry.Outcome) {}
}

// ObserveScheduler reads global control-plane facts within one database budget.
// Every worker observes the same DB scope: dashboards use max, not sum, across
// worker scrape targets. Failed reads preserve the last successful gauges.
func (s *Store) ObserveScheduler(ctx context.Context) error {
	if s.Telemetry == nil {
		return nil
	}
	ctx, cancel := dependency.Database(ctx)
	defer cancel()
	var queued, active, reserved int64
	err := s.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM runs WHERE snapshot_hold_at IS NULL AND state IN ('queued','running','cancel_requested','needs_reconciliation') AND not_before<=clock_timestamp() AND (lease_until IS NULL OR lease_until<=clock_timestamp())),(SELECT count(*) FROM runner_allocations WHERE state!='released'),(SELECT coalesce(sum(reserved_microusd),0)::bigint FROM provider_quotas)`).Scan(&queued, &active, &reserved)
	if err != nil {
		return err
	}
	s.Telemetry.SetSchedulerSnapshot(queued, active)
	s.Telemetry.SetReservedBudget(reserved)
	return nil
}
