package persistence

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/dependency"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	flow "github.com/JDinSeattle/forge-runtime/internal/runtime"
	"github.com/jackc/pgx/v5"
)

// BeginPricedAttempt commits pricing and request identity in the same transaction.
// Existing prepared/completed attempts return their stored snapshot and deadline,
// irrespective of the current configuration passed by a recovered worker.
func (s *Store) BeginPricedAttempt(ctx context.Context, r Run, requestRef, priceVersion string, timeout time.Duration, pricing json.RawMessage) (ModelAttempt, json.RawMessage, error) {
	ctx, cancel := dependency.Database(ctx)
	defer cancel()
	if timeout <= 0 || priceVersion == "" || len(pricing) > 32<<10 || !json.Valid(pricing) {
		return ModelAttempt{}, nil, domain.ErrInvalid
	}
	var metadata struct {
		Provider     string `json:"provider"`
		Model        string `json:"model"`
		PriceVersion string `json:"price_version"`
	}
	if json.Unmarshal(pricing, &metadata) != nil || metadata.Provider == "" || metadata.Model == "" || metadata.PriceVersion != priceVersion {
		return ModelAttempt{}, nil, domain.ErrInvalid
	}
	tx, err := s.Tx(ctx, r.TenantID, pgx.TxOptions{})
	if err != nil {
		return ModelAttempt{}, nil, err
	}
	defer tx.Rollback(ctx)
	current, err := getRun(ctx, tx, r.TenantID, r.ID, true)
	if err != nil {
		return ModelAttempt{}, nil, err
	}
	var now time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return ModelAttempt{}, nil, err
	}
	if current.State.Version != r.State.Version || current.State.Lease.Owner != r.State.Lease.Owner || current.State.Lease.Epoch != r.State.Lease.Epoch || !current.State.Lease.ValidAt(now) || current.State.Stage != domain.StageModel {
		return ModelAttempt{}, nil, domain.ErrFenced
	}
	latest, err := scanAttempt(tx.QueryRow(ctx, `SELECT `+attemptColumns+` FROM model_attempts WHERE tenant_id=$1 AND run_id=$2 AND step_seq=$3 ORDER BY attempt DESC LIMIT 1`, r.TenantID, r.ID, r.State.StepSeq))
	if err == nil && latest.Status != "failed" {
		var frozen json.RawMessage
		if err = tx.QueryRow(ctx, `SELECT pricing FROM model_attempts WHERE tenant_id=$1 AND attempt_id=$2`, r.TenantID, latest.ID).Scan(&frozen); err != nil {
			return latest, nil, err
		}
		if len(frozen) == 0 {
			return latest, nil, domain.ErrReconciliation
		}
		return latest, frozen, tx.Commit(ctx)
	}
	if err != nil && !errors.Is(err, domain.ErrNotFound) {
		return ModelAttempt{}, nil, err
	}
	if latest.Status == "failed" {
		var raw json.RawMessage
		if err = tx.QueryRow(ctx, `SELECT failure_policy FROM model_attempts WHERE tenant_id=$1 AND attempt_id=$2`, r.TenantID, latest.ID).Scan(&raw); err != nil {
			return ModelAttempt{}, nil, err
		}
		var policy AttemptFailurePolicy
		if json.Unmarshal(raw, &policy) != nil || !policy.Retry {
			return ModelAttempt{}, nil, domain.ErrReconciliation
		}
		if now.Before(policy.NotBefore) {
			return ModelAttempt{}, nil, domain.ErrCapacity
		}
		var first time.Time
		if err = tx.QueryRow(ctx, `SELECT min(started_at) FROM model_attempts WHERE tenant_id=$1 AND run_id=$2 AND step_seq=$3`, r.TenantID, r.ID, r.State.StepSeq).Scan(&first); err != nil {
			return ModelAttempt{}, nil, err
		}
		if !now.Before(first.Add(2 * time.Minute)) {
			return ModelAttempt{}, nil, domain.ErrCapacity
		}
	}
	route := ModelRoute{Provider: metadata.Provider, Model: metadata.Model}
	switching, err := validateModelRoute(ctx, tx, current, latest, route)
	if err != nil {
		return ModelAttempt{}, nil, err
	}
	number := latest.Number + 1
	if number > 3 {
		return ModelAttempt{}, nil, domain.ErrCapacity
	}
	if err = validateEvidence(ctx, tx, r.TenantID, r.ID, flow.Event{OutputRef: requestRef}); err != nil {
		return ModelAttempt{}, nil, err
	}
	deadline := now.Add(timeout)
	if deadline.After(r.State.Limits.Deadline) {
		deadline = r.State.Limits.Deadline
	}
	if !now.Before(deadline) {
		return ModelAttempt{}, nil, domain.ErrCapacity
	}
	a, err := scanAttempt(tx.QueryRow(ctx, `INSERT INTO model_attempts(tenant_id,run_id,step_seq,attempt,attempt_id,provider,model_id,status,request_ref,price_version,deadline,pricing) VALUES($1,$2,$3,$4,$5,$6,$7,'prepared',$8,$9,$10,$11) RETURNING `+attemptColumns, r.TenantID, r.ID, r.State.StepSeq, number, NewID("attempt"), route.Provider, route.Model, requestRef, priceVersion, deadline, pricing))
	if err != nil {
		return a, nil, err
	}
	if switching {
		body, _ := json.Marshal(map[string]any{"from_attempt_id": latest.ID, "to_attempt_id": a.ID, "from": latest.Route(), "to": route, "step_seq": a.StepSeq, "request_ref": requestRef, "reason": latest.ErrorCode, "policy": "single-fallback-v1"})
		if _, err = appendEvent(ctx, tx, r.TenantID, r.ID, "model.handoff_prepared", body); err != nil {
			return a, nil, err
		}
	}
	payload, _ := json.Marshal(map[string]any{"attempt_id": a.ID, "step_seq": a.StepSeq, "price_version": a.PriceVersion})
	if _, err = appendEvent(ctx, tx, r.TenantID, r.ID, "model.started", payload); err != nil {
		return a, nil, err
	}
	return a, pricing, tx.Commit(ctx)
}

func (s *Store) AttemptPricing(ctx context.Context, a ModelAttempt) (json.RawMessage, error) {
	ctx, cancel := dependency.Database(ctx)
	defer cancel()
	var raw json.RawMessage
	err := s.Pool.QueryRow(ctx, `SELECT pricing FROM model_attempts WHERE tenant_id=$1 AND run_id=$2 AND step_seq=$3 AND attempt_id=$4 AND price_version=$5`, a.TenantID, a.RunID, a.StepSeq, a.ID, a.PriceVersion).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err == nil && len(raw) == 0 {
		return nil, domain.ErrReconciliation
	}
	return raw, err
}

type AttemptFailurePolicy struct {
	Code      string    `json:"code"`
	Outcome   string    `json:"outcome"`
	Retry     bool      `json:"retry"`
	NotBefore time.Time `json:"not_before"`
}

func (s *Store) AttemptFailure(ctx context.Context, a ModelAttempt) (AttemptFailurePolicy, error) {
	ctx, cancel := dependency.Database(ctx)
	defer cancel()
	var raw json.RawMessage
	var policy AttemptFailurePolicy
	err := s.Pool.QueryRow(ctx, `SELECT failure_policy FROM model_attempts WHERE tenant_id=$1 AND run_id=$2 AND attempt_id=$3`, a.TenantID, a.RunID, a.ID).Scan(&raw)
	if err != nil {
		return policy, err
	}
	if len(raw) == 0 {
		return policy, domain.ErrReconciliation
	}
	if err = json.Unmarshal(raw, &policy); err != nil {
		return policy, domain.ErrReconciliation
	}
	return policy, nil
}
func (s *Store) FirstAttemptAt(ctx context.Context, a ModelAttempt) (time.Time, error) {
	ctx, cancel := dependency.Database(ctx)
	defer cancel()
	var at time.Time
	err := s.Pool.QueryRow(ctx, `SELECT min(started_at) FROM model_attempts WHERE tenant_id=$1 AND run_id=$2 AND step_seq=$3`, a.TenantID, a.RunID, a.StepSeq).Scan(&at)
	return at, err
}

// FinishFailedAttempt persists the retry decision with the failure transition so
// recovery cannot skip Retry-After or retry a permanent/local validation failure.
func (s *Store) FinishFailedAttempt(ctx context.Context, a ModelAttempt, policy AttemptFailurePolicy) error {
	ctx, cancel := dependency.Database(ctx)
	defer cancel()
	if policy.Code == "" || (policy.Outcome != "failure" && policy.Outcome != "neutral") || (policy.Retry && policy.NotBefore.IsZero()) {
		return domain.ErrInvalid
	}
	body, err := json.Marshal(policy)
	if err != nil {
		return err
	}
	tx, err := s.Tx(ctx, a.TenantID, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = getRun(ctx, tx, a.TenantID, a.RunID, true); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE model_attempts SET status='failed',error_code=$4,failure_policy=$5 WHERE tenant_id=$1 AND run_id=$2 AND attempt_id=$3 AND status='prepared'`, a.TenantID, a.RunID, a.ID, policy.Code, body)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		var matches bool
		if err = tx.QueryRow(ctx, `SELECT status='failed' AND failure_policy=$4::jsonb FROM model_attempts WHERE tenant_id=$1 AND run_id=$2 AND attempt_id=$3`, a.TenantID, a.RunID, a.ID, body).Scan(&matches); err != nil {
			return err
		}
		if !matches {
			return domain.ErrConflict
		}
	} else {
		event, _ := json.Marshal(map[string]any{"attempt_id": a.ID, "error_code": policy.Code, "provisional": true, "retry": policy.Retry, "not_before": policy.NotBefore})
		if _, err = appendEvent(ctx, tx, a.TenantID, a.RunID, "model.attempt_failed", event); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
