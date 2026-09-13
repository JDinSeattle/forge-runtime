package quota

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/dependency"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store requires a trusted worker pool with visibility across tenant reservation
// rows. Do not expose it through a tenant API connection or endpoint: credential
// groups, global sweeps, and breaker state are internal service responsibilities.
type Store struct{ Pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Store { return &Store{Pool: pool} }

func (s *Store) Configure(ctx context.Context, c Config) error {
	ctx, cancel := dependency.Database(ctx)
	defer cancel()
	if c.CredentialGroup == "" || len(c.CredentialGroup) > 128 || c.MaxConcurrent <= 0 || c.MaxTokens < 0 || c.MaxCost < 0 || c.WindowDuration < time.Millisecond || c.WindowDuration > 365*24*time.Hour || c.FailureThreshold <= 0 || c.BreakerCooldown < time.Millisecond || c.BreakerCooldown > 24*time.Hour {
		return ErrInvalid
	}
	_, err := s.Pool.Exec(ctx, `INSERT INTO provider_quotas(credential_group,max_concurrent,max_tokens,max_microusd,window_ends_at,window_duration_ms,failure_threshold,breaker_cooldown_ms)
	 VALUES($1,$2,$3,$4,clock_timestamp()+($5 * interval '1 millisecond'),$5,$6,$7)
	 ON CONFLICT(credential_group) DO UPDATE SET max_concurrent=EXCLUDED.max_concurrent,max_tokens=EXCLUDED.max_tokens,max_microusd=EXCLUDED.max_microusd,window_duration_ms=EXCLUDED.window_duration_ms,failure_threshold=EXCLUDED.failure_threshold,breaker_cooldown_ms=EXCLUDED.breaker_cooldown_ms`, c.CredentialGroup, c.MaxConcurrent, c.MaxTokens, c.MaxCost, c.WindowDuration.Milliseconds(), c.FailureThreshold, c.BreakerCooldown.Milliseconds())
	return err
}

const quotaColumns = `credential_group,max_concurrent,max_tokens,max_microusd,active_requests,reserved_tokens,committed_tokens,reserved_microusd,committed_microusd,window_ends_at,window_generation,breaker_until,consecutive_failures,probe_tenant_id,probe_reservation_id,window_duration_ms,failure_threshold,breaker_cooldown_ms`

func scanQuota(row pgx.Row) (Snapshot, error) {
	var q Snapshot
	err := row.Scan(&q.CredentialGroup, &q.MaxConcurrent, &q.MaxTokens, &q.MaxCost, &q.ActiveRequests, &q.ReservedTokens, &q.CommittedTokens, &q.ReservedCost, &q.CommittedCost, &q.WindowEndsAt, &q.WindowGeneration, &q.BreakerUntil, &q.ConsecutiveFailures, &q.ProbeTenantID, &q.ProbeAttemptID, &q.windowDurationMS, &q.failureThreshold, &q.breakerCooldownMS)
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrNotFound
	}
	return q, err
}

const reservationColumns = `tenant_id,run_id,id,credential_group,tokens,microusd,status,request_slot_released,request_deadline,dispatched_at,actual_tokens,actual_microusd,settlement_kind,outcome,window_generation,request_hash`

func scanReservation(row pgx.Row) (Reservation, error) {
	var r Reservation
	err := row.Scan(&r.TenantID, &r.RunID, &r.AttemptID, &r.CredentialGroup, &r.Tokens, &r.Cost, &r.Status, &r.SlotReleased, &r.RequestDeadline, &r.DispatchedAt, &r.ActualTokens, &r.ActualCost, &r.SettlementKind, &r.Outcome, &r.WindowGeneration, &r.requestHash)
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrNotFound
	}
	return r, err
}

func (s *Store) Get(ctx context.Context, tenant, id string) (Reservation, error) {
	ctx, cancel := dependency.Database(ctx)
	defer cancel()
	return scanReservation(s.Pool.QueryRow(ctx, `SELECT `+reservationColumns+` FROM quota_reservations WHERE tenant_id=$1 AND id=$2`, tenant, id))
}
func (s *Store) Snapshot(ctx context.Context, group string) (Snapshot, error) {
	ctx, cancel := dependency.Database(ctx)
	defer cancel()
	return scanQuota(s.Pool.QueryRow(ctx, `SELECT `+quotaColumns+` FROM provider_quotas WHERE credential_group=$1`, group))
}

func saveQuota(ctx context.Context, tx pgx.Tx, q Snapshot) error {
	_, err := tx.Exec(ctx, `UPDATE provider_quotas SET active_requests=$2,reserved_tokens=$3,committed_tokens=$4,reserved_microusd=$5,committed_microusd=$6,window_ends_at=$7,window_generation=$8,breaker_until=$9,consecutive_failures=$10,probe_tenant_id=$11,probe_reservation_id=$12 WHERE credential_group=$1`, q.CredentialGroup, q.ActiveRequests, q.ReservedTokens, q.CommittedTokens, q.ReservedCost, q.CommittedCost, q.WindowEndsAt, q.WindowGeneration, q.BreakerUntil, q.ConsecutiveFailures, q.ProbeTenantID, q.ProbeAttemptID)
	return err
}

// lockGroup is always acquired before any reservation row, across every mutation.
// A fresh database clock read after the lock avoids stale admission decisions
// when a contender spent time waiting for another worker's transaction.
func lockGroup(ctx context.Context, tx pgx.Tx, group string) (Snapshot, time.Time, error) {
	q, err := scanQuota(tx.QueryRow(ctx, `SELECT `+quotaColumns+` FROM provider_quotas WHERE credential_group=$1 FOR UPDATE`, group))
	if err != nil {
		return q, time.Time{}, err
	}
	var now time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return q, now, err
	}
	if !now.Before(q.WindowEndsAt) {
		q.CommittedCost = 0
		q.CommittedTokens = 0
		q.WindowGeneration++
		q.WindowEndsAt = now.Add(time.Duration(q.windowDurationMS) * time.Millisecond)
	}
	return q, now, nil
}

func expireSlots(ctx context.Context, tx pgx.Tx, q *Snapshot, now time.Time) (int, error) {
	rows, err := tx.Query(ctx, `UPDATE quota_reservations SET request_slot_released=true,status=CASE WHEN status='settled' THEN status ELSE 'unknown' END WHERE credential_group=$1 AND NOT request_slot_released AND request_deadline <= $2 RETURNING tenant_id,id`, q.CredentialGroup, now)
	if err != nil {
		return 0, err
	}
	count := 0
	for rows.Next() {
		var tenant, id string
		if err = rows.Scan(&tenant, &id); err != nil {
			rows.Close()
			return 0, err
		}
		count++
		if q.ProbeTenantID != nil && *q.ProbeTenantID == tenant && q.ProbeAttemptID != nil && *q.ProbeAttemptID == id {
			q.ProbeTenantID = nil
			q.ProbeAttemptID = nil
			until := now.Add(time.Duration(q.breakerCooldownMS) * time.Millisecond)
			q.BreakerUntil = &until
		}
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return 0, err
	}
	q.ActiveRequests -= count
	// A worker can crash after settling usage but before recording the circuit
	// outcome. That probe no longer owns a request slot, so recover it separately.
	if q.ProbeTenantID != nil && q.ProbeAttemptID != nil {
		var deadline time.Time
		if err = tx.QueryRow(ctx, `SELECT request_deadline FROM quota_reservations WHERE tenant_id=$1 AND id=$2`, *q.ProbeTenantID, *q.ProbeAttemptID).Scan(&deadline); err != nil {
			return 0, err
		}
		if !now.Before(deadline) {
			reopen(q, now)
		}
	}
	return count, nil
}

func (s *Store) Reserve(ctx context.Context, request Request) (Reservation, error) {
	ctx, cancel := dependency.Database(ctx)
	defer cancel()
	req, tokens, hash, err := request.normalized()
	if err != nil {
		return Reservation{}, err
	}
	tx, err := dependency.BeginTx(ctx, s.Pool, pgx.TxOptions{})
	if err != nil {
		return Reservation{}, err
	}
	defer tx.Rollback(ctx)
	q, now, err := lockGroup(ctx, tx, req.CredentialGroup)
	if err != nil {
		return Reservation{}, err
	}
	if _, err = expireSlots(ctx, tx, &q, now); err != nil {
		return Reservation{}, err
	}
	existing, err := scanReservation(tx.QueryRow(ctx, `SELECT `+reservationColumns+` FROM quota_reservations WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, req.TenantID, req.AttemptID))
	if err == nil {
		if existing.requestHash != hash {
			return Reservation{}, ErrConflict
		}
		if err = saveQuota(ctx, tx, q); err != nil {
			return Reservation{}, err
		}
		return existing, tx.Commit(ctx)
	}
	if !errors.Is(err, ErrNotFound) {
		return Reservation{}, err
	}
	// Maintenance effects are committed even when admission is denied so expired
	// request slots and circuit probes do not remain pinned by repeated failures.
	deny := func(reason string, retryAt time.Time) (Reservation, error) {
		if err := saveQuota(ctx, tx, q); err != nil {
			return Reservation{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return Reservation{}, err
		}
		return Reservation{}, &CapacityError{Reason: reason, RetryAt: retryAt}
	}
	if !now.Before(req.RequestDeadline) {
		return Reservation{}, fmt.Errorf("%w: request deadline expired", ErrInvalid)
	}
	if q.BreakerUntil != nil {
		if now.Before(*q.BreakerUntil) {
			return deny("circuit open", *q.BreakerUntil)
		}
		if q.ProbeAttemptID != nil {
			return deny("half-open probe in progress", time.Time{})
		}
	}
	if q.ActiveRequests >= q.MaxConcurrent {
		return deny("concurrent request limit", time.Time{})
	}
	// Subtraction avoids overflow and records debt from actual overages without
	// allowing additional reservations until a later window/configuration change.
	if q.CommittedTokens > q.MaxTokens || q.ReservedTokens > q.MaxTokens-q.CommittedTokens || tokens > q.MaxTokens-q.CommittedTokens-q.ReservedTokens {
		return deny("token budget", q.WindowEndsAt)
	}
	if q.CommittedCost > q.MaxCost || q.ReservedCost > q.MaxCost-q.CommittedCost || req.MaxCost > q.MaxCost-q.CommittedCost-q.ReservedCost {
		return deny("cost budget", q.WindowEndsAt)
	}
	row := tx.QueryRow(ctx, `INSERT INTO quota_reservations(tenant_id,id,run_id,credential_group,tokens,microusd,status,request_deadline,request_hash,window_generation) VALUES($1,$2,$3,$4,$5,$6,'reserved',$7,$8,$9) ON CONFLICT(tenant_id,id) DO NOTHING RETURNING `+reservationColumns, req.TenantID, req.AttemptID, req.RunID, req.CredentialGroup, tokens, req.MaxCost, req.RequestDeadline, hash, q.WindowGeneration)
	r, err := scanReservation(row)
	if errors.Is(err, ErrNotFound) {
		return Reservation{}, ErrConflict
	}
	if err != nil {
		return Reservation{}, err
	}
	q.ActiveRequests++
	q.ReservedTokens += tokens
	q.ReservedCost += req.MaxCost
	if q.BreakerUntil != nil {
		q.ProbeTenantID = &req.TenantID
		q.ProbeAttemptID = &req.AttemptID
	}
	if err = saveQuota(ctx, tx, q); err != nil {
		return Reservation{}, err
	}
	return r, tx.Commit(ctx)
}

type mutation func(pgx.Tx, *Snapshot, *Reservation, time.Time) error

func (s *Store) mutate(ctx context.Context, tenant, id string, fn mutation) error {
	ctx, cancel := dependency.Database(ctx)
	defer cancel()
	// Reservation group is immutable. Reading it before the transaction does not
	// grant a right to mutate; identity is checked again under both row locks.
	r, err := s.Get(ctx, tenant, id)
	if err != nil {
		return err
	}
	tx, err := dependency.BeginTx(ctx, s.Pool, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	q, now, err := lockGroup(ctx, tx, r.CredentialGroup)
	if err != nil {
		return err
	}
	r, err = scanReservation(tx.QueryRow(ctx, `SELECT `+reservationColumns+` FROM quota_reservations WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, tenant, id))
	if err != nil {
		return err
	}
	if err = fn(tx, &q, &r, now); err != nil {
		return err
	}
	if err = saveQuota(ctx, tx, q); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// MarkDispatched must commit before starting the HTTP request. An error stating
// AlreadyDispatched forbids resending that attempt, even if it might have crashed
// between this marker and the network write. Create a new attempt only through
// the application's bounded retry/reconciliation policy.
func (s *Store) MarkDispatched(ctx context.Context, tenant, id string) error {
	ctx, cancel := dependency.Database(ctx)
	defer cancel()
	return s.mutate(ctx, tenant, id, func(tx pgx.Tx, q *Snapshot, r *Reservation, now time.Time) error {
		if r.DispatchedAt != nil {
			return ErrAlreadyDispatched
		}
		if r.Status != "reserved" || r.SlotReleased || !now.Before(r.RequestDeadline) {
			return ErrConflict
		}
		_, err := tx.Exec(ctx, `UPDATE quota_reservations SET dispatched_at=$3 WHERE tenant_id=$1 AND id=$2`, tenant, id, now)
		return err
	})
}

func (s *Store) MarkUnknown(ctx context.Context, tenant, id string) error {
	ctx, cancel := dependency.Database(ctx)
	defer cancel()
	return s.mutate(ctx, tenant, id, func(tx pgx.Tx, q *Snapshot, r *Reservation, now time.Time) error {
		if r.Status == "settled" {
			return ErrConflict
		}
		_, err := tx.Exec(ctx, `UPDATE quota_reservations SET status='unknown' WHERE tenant_id=$1 AND id=$2`, tenant, id)
		return err
	})
}

// CompleteWithUnknownUsage requires a durably saved, complete provider response.
// Release only concurrency: unknown token/cost obligations stay reserved until
// definitive settlement. Interrupted or unconfirmed responses use MarkUnknown.
func (s *Store) CompleteWithUnknownUsage(ctx context.Context, tenant, id string) error {
	ctx, cancel := dependency.Database(ctx)
	defer cancel()
	return s.mutate(ctx, tenant, id, func(tx pgx.Tx, q *Snapshot, r *Reservation, now time.Time) error {
		if r.DispatchedAt == nil || r.Status == "settled" {
			return ErrConflict
		}
		if !r.SlotReleased {
			q.ActiveRequests--
		}
		_, err := tx.Exec(ctx, `UPDATE quota_reservations SET status='unknown',request_slot_released=true WHERE tenant_id=$1 AND id=$2`, tenant, id)
		return err
	})
}

func (s *Store) Settle(ctx context.Context, tenant, id string, actual Settlement) error {
	ctx, cancel := dependency.Database(ctx)
	defer cancel()
	if actual.Tokens < 0 || actual.Cost < 0 {
		return ErrInvalid
	}
	return s.settle(ctx, tenant, id, actual, "actual_usage")
}

// AbandonBeforeDispatch is the only zero-cost release without provider usage.
// A persisted dispatch marker makes this operation unsafe and therefore invalid.
func (s *Store) AbandonBeforeDispatch(ctx context.Context, tenant, id string) error {
	ctx, cancel := dependency.Database(ctx)
	defer cancel()
	return s.settle(ctx, tenant, id, Settlement{}, "not_dispatched")
}
func (s *Store) settle(ctx context.Context, tenant, id string, actual Settlement, kind string) error {
	ctx, cancel := dependency.Database(ctx)
	defer cancel()
	return s.mutate(ctx, tenant, id, func(tx pgx.Tx, q *Snapshot, r *Reservation, now time.Time) error {
		if r.Status == "settled" {
			if r.SettlementKind != nil && *r.SettlementKind == kind && r.ActualTokens != nil && *r.ActualTokens == actual.Tokens && r.ActualCost != nil && *r.ActualCost == actual.Cost {
				return nil
			}
			return ErrConflict
		}
		if kind == "not_dispatched" && r.DispatchedAt != nil {
			return ErrAlreadyDispatched
		}
		if kind == "actual_usage" && r.DispatchedAt == nil {
			return fmt.Errorf("%w: usage for undispatched request", ErrConflict)
		}
		if actual.Tokens > math.MaxInt64-q.CommittedTokens {
			return domain.ErrOverflow
		}
		cost, err := q.CommittedCost.Add(actual.Cost)
		if err != nil {
			return err
		}
		q.ReservedTokens -= r.Tokens
		q.ReservedCost -= r.Cost
		q.CommittedTokens += actual.Tokens
		q.CommittedCost = cost
		if !r.SlotReleased {
			q.ActiveRequests--
		}
		_, err = tx.Exec(ctx, `UPDATE quota_reservations SET status='settled',actual_tokens=$3,actual_microusd=$4,settlement_kind=$5,settled_at=$6,request_slot_released=true WHERE tenant_id=$1 AND id=$2`, tenant, id, actual.Tokens, actual.Cost, kind, now)
		if err != nil {
			return err
		}
		// An abandoned half-open probe has no model outcome, so allow another
		// probe after cooldown instead of leaving a settled ID pinned forever.
		if kind == "not_dispatched" && ownsProbe(*q, *r) {
			reopen(q, now)
		}
		return nil
	})
}

func ownsProbe(q Snapshot, r Reservation) bool {
	return q.ProbeTenantID != nil && *q.ProbeTenantID == r.TenantID && q.ProbeAttemptID != nil && *q.ProbeAttemptID == r.AttemptID
}
func reopen(q *Snapshot, now time.Time) {
	until := now.Add(time.Duration(q.breakerCooldownMS) * time.Millisecond)
	q.BreakerUntil = &until
	q.ProbeTenantID = nil
	q.ProbeAttemptID = nil
}

// RecordOutcome is exactly once per attempt and independent of usage billing.
// Use failure for transient credential-group/provider errors; permanent invalid
// inputs and caller cancellation are neutral. Success can have unknown billing.
func (s *Store) RecordOutcome(ctx context.Context, tenant, id, outcome string) error {
	ctx, cancel := dependency.Database(ctx)
	defer cancel()
	if outcome != "success" && outcome != "failure" && outcome != "neutral" {
		return ErrInvalid
	}
	return s.mutate(ctx, tenant, id, func(tx pgx.Tx, q *Snapshot, r *Reservation, now time.Time) error {
		if r.Outcome != nil {
			if *r.Outcome == outcome {
				return nil
			}
			return ErrConflict
		}
		if r.DispatchedAt == nil {
			return ErrConflict
		}
		probe := ownsProbe(*q, *r)
		switch outcome {
		case "success":
			if q.BreakerUntil == nil || probe {
				q.ConsecutiveFailures = 0
				q.BreakerUntil = nil
				q.ProbeTenantID = nil
				q.ProbeAttemptID = nil
			}
		case "failure":
			if q.ConsecutiveFailures < q.failureThreshold {
				q.ConsecutiveFailures++
			}
			if q.ConsecutiveFailures >= q.failureThreshold || probe {
				reopen(q, now)
			}
		case "neutral":
			if probe {
				reopen(q, now)
			}
		}
		_, err := tx.Exec(ctx, `UPDATE quota_reservations SET outcome=$3 WHERE tenant_id=$1 AND id=$2`, tenant, id, outcome)
		return err
	})
}

// ExpireRequestSlots releases only concurrency; token/cost reserves survive
// deadlines and window rotation until definitive usage or non-dispatch proof.
func (s *Store) ExpireRequestSlots(ctx context.Context, group string) (int, error) {
	ctx, cancel := dependency.Database(ctx)
	defer cancel()
	tx, err := dependency.BeginTx(ctx, s.Pool, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	q, now, err := lockGroup(ctx, tx, group)
	if err != nil {
		return 0, err
	}
	n, err := expireSlots(ctx, tx, &q, now)
	if err != nil {
		return 0, err
	}
	if err = saveQuota(ctx, tx, q); err != nil {
		return 0, err
	}
	return n, tx.Commit(ctx)
}
