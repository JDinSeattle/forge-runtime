package persistence

import (
	"context"
	"errors"

	"github.com/JDinSeattle/forge-runtime/internal/dependency"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/jackc/pgx/v5"
)

type ModelRoute struct {
	Provider string `json:"provider"`
	Model    string `json:"model"`
}

func (c Config) PrimaryRoute() ModelRoute { return ModelRoute{Provider: c.Provider, Model: c.Model} }
func (a ModelAttempt) Route() ModelRoute  { return ModelRoute{Provider: a.Provider, Model: a.Model} }
func (r ModelRoute) Key() string          { return r.Provider + "/" + r.Model }

// FallbackFailure deliberately excludes local quota, consumer, protocol,
// cancellation, deadline and unknown prior-dispatch reconciliation failures.
func FallbackFailure(code string) bool {
	return code == "rate_limited" || code == "unavailable" || code == "stream_interrupted"
}

func latestRunAttempt(ctx context.Context, q querier, tenant, run domain.ID) (ModelAttempt, error) {
	return scanAttempt(q.QueryRow(ctx, `SELECT `+attemptColumns+` FROM model_attempts WHERE tenant_id=$1 AND run_id=$2 ORDER BY step_seq DESC,attempt DESC LIMIT 1`, tenant, run))
}

// ActiveModelRoute derives the route from immutable attempt identities. Once a
// fallback attempt is prepared, all following steps keep that route, including
// recovery before dispatch. The caller cannot bounce back to the primary.
func (s *Store) ActiveModelRoute(ctx context.Context, r Run) (ModelRoute, error) {
	ctx, cancel := dependency.Database(ctx)
	defer cancel()
	a, err := latestRunAttempt(ctx, s.Pool, r.TenantID, r.ID)
	if errors.Is(err, domain.ErrNotFound) {
		return r.Config.PrimaryRoute(), nil
	}
	if err != nil {
		return ModelRoute{}, err
	}
	route := a.Route()
	if route != r.Config.PrimaryRoute() && (r.Config.Fallback == nil || route != *r.Config.Fallback) {
		return ModelRoute{}, domain.ErrReconciliation
	}
	return route, nil
}

func validateModelRoute(ctx context.Context, tx pgx.Tx, r Run, latest ModelAttempt, route ModelRoute) (bool, error) {
	previous, err := latestRunAttempt(ctx, tx, r.TenantID, r.ID)
	active := r.Config.PrimaryRoute()
	if err == nil {
		active = previous.Route()
	} else if !errors.Is(err, domain.ErrNotFound) {
		return false, err
	}
	if active != r.Config.PrimaryRoute() && (r.Config.Fallback == nil || active != *r.Config.Fallback) {
		return false, domain.ErrReconciliation
	}
	if route == active {
		return false, nil
	}
	if active != r.Config.PrimaryRoute() || r.Config.Fallback == nil || route != *r.Config.Fallback || latest.Status != "failed" || latest.Route() != active || !FallbackFailure(latest.ErrorCode) {
		return false, domain.ErrConflict
	}
	// StageModel alone is not evidence that an inconsistent effect ledger is
	// closed. Pending approvals, remaining tools and unknown executions all block.
	if r.State.PendingEffect != nil || len(r.State.RemainingEffects) != 0 || r.State.Approval != nil {
		return false, domain.ErrReconciliation
	}
	var unsettled bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM effects e WHERE e.tenant_id=$1 AND e.run_id=$2
	 AND NOT((e.status IN ('succeeded','failed','cancelled') AND EXISTS(
	   SELECT 1 FROM artifacts receipt WHERE receipt.tenant_id=e.tenant_id AND receipt.run_id=e.run_id AND receipt.id=e.receipt_ref AND receipt.state='ready'))
	 OR (e.status='planned' AND e.ordinal>=0 AND e.step_seq<$3 AND EXISTS(
	   SELECT 1 FROM effects stopped JOIN artifacts receipt ON receipt.tenant_id=stopped.tenant_id AND receipt.run_id=stopped.run_id AND receipt.id=stopped.receipt_ref AND receipt.state='ready'
	   WHERE stopped.tenant_id=e.tenant_id AND stopped.run_id=e.run_id AND stopped.step_seq=e.step_seq AND stopped.ordinal>=0 AND stopped.ordinal<e.ordinal AND stopped.status IN ('failed','cancelled')))))`, r.TenantID, r.ID, r.State.StepSeq).Scan(&unsettled); err != nil {
		return false, err
	}
	if unsettled {
		return false, domain.ErrReconciliation
	}
	return true, nil
}
