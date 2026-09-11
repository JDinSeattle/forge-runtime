package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/provider"
	"github.com/JDinSeattle/forge-runtime/internal/quota"
	flow "github.com/JDinSeattle/forge-runtime/internal/runtime"
	"github.com/JDinSeattle/forge-runtime/internal/telemetry"
)

func (d *Driver) callModel(ctx context.Context, r persistence.Run) (flow.Event, error) {
	a, err := d.Store.LatestAttempt(ctx, r.TenantID, r.ID, r.State.StepSeq)
	if err != nil && !errors.Is(err, domain.ErrNotFound) {
		return flow.Event{}, err
	}
	if a.Status == "completed" {
		return d.completedModel(ctx, r, a)
	}
	if a.Status == "failed" {
		policy, err := d.Store.AttemptFailure(ctx, a)
		if err != nil {
			return flow.Event{}, err
		}
		if err = d.replayFailureOutcome(ctx, a, policy); err != nil {
			return flow.Event{}, err
		}
		if !policy.Retry || a.Number >= 3 {
			return flow.Event{}, fmt.Errorf("model attempt failed: %s", policy.Code)
		}
		_, now, err := d.Store.LeaseProof(ctx, r.TenantID, r.ID, r.State.Lease.Owner, r.State.Lease.Epoch)
		if err != nil {
			return flow.Event{}, err
		}
		first, firstErr := d.Store.FirstAttemptAt(ctx, a)
		if firstErr != nil {
			return flow.Event{}, firstErr
		}
		if !now.Before(first.Add(2 * time.Minute)) {
			return flow.Event{}, domain.ErrCapacity
		}
		if now.Before(policy.NotBefore) {
			return flow.Event{}, d.deferRun(ctx, r, policy.NotBefore, "quota.delayed")
		}
	}
	var frozen json.RawMessage
	if a.Status == "prepared" {
		frozen, err = d.Store.AttemptPricing(ctx, a)
	} else {
		spec, ok := d.Models[r.Config.Provider+"/"+r.Config.Model]
		if !ok {
			return flow.Event{}, domain.ErrInvalid
		}
		pricing, freezeErr := freezePricing(r.Config.Provider, r.Config.Model, spec)
		if freezeErr != nil {
			return flow.Event{}, freezeErr
		}
		frozen, err = json.Marshal(pricing)
		if err == nil {
			a, frozen, err = d.Store.BeginPricedAttempt(ctx, r, r.State.OutputRef, spec.PriceVersion, spec.RequestTimeout, frozen)
		}
	}
	if err != nil {
		return flow.Event{}, err
	}
	pricing, err := decodePricing(a, frozen)
	if err != nil {
		return flow.Event{}, err
	}
	if err = d.fault("after_model_pricing_before_reservation"); err != nil {
		return flow.Event{}, err
	}
	cost, err := pricing.reserveCost()
	if err != nil {
		return flow.Event{}, err
	}
	existing, resErr := d.Quota.Get(ctx, string(r.TenantID), string(a.ID))
	if resErr != nil && !errors.Is(resErr, quota.ErrNotFound) {
		return flow.Event{}, resErr
	}
	_, now, err := d.Store.LeaseProof(ctx, r.TenantID, r.ID, r.State.Lease.Owner, r.State.Lease.Epoch)
	if err != nil {
		return flow.Event{}, err
	}
	if !now.Before(a.Deadline) && existing.DispatchedAt == nil {
		if resErr == nil {
			if err = d.Quota.AbandonBeforeDispatch(ctx, string(r.TenantID), string(a.ID)); err != nil {
				return flow.Event{}, err
			}
		}
		return d.failModel(ctx, r, a, domain.ErrCapacity, "request_expired_before_dispatch", true, 0, "neutral")
	}
	if errors.Is(resErr, quota.ErrNotFound) {
		total, err := d.Store.RunCost(ctx, r.TenantID, r.ID)
		if err != nil {
			return flow.Event{}, err
		}
		if total < r.State.Cost {
			total = r.State.Cost
		}
		total, err = total.Add(cost)
		if err != nil {
			return flow.Event{}, err
		}
		if r.State.Limits.MaxCost > 0 && total > r.State.Limits.MaxCost {
			return flow.Event{Kind: flow.EventBudgetReached, Reason: "conservative model reservation would exceed run budget"}, nil
		}
	}
	// Always replay Reserve, even for existing rows, to verify the immutable
	// pricing/request/deadline hash before dispatch or settlement.
	reservation, err := d.Quota.Reserve(ctx, pricing.reservation(r, a, cost))
	var capacity *quota.CapacityError
	if errors.As(err, &capacity) {
		until := capacity.RetryAt
		if !until.After(now) {
			until = now.Add(time.Second)
		}
		if until.After(a.Deadline) {
			until = a.Deadline
		}
		return flow.Event{}, d.deferRun(ctx, r, until, "quota.delayed")
	}
	if err != nil {
		return flow.Event{}, err
	}
	if reservation.DispatchedAt != nil {
		if reservation.Status != "settled" {
			if err = d.Quota.MarkUnknown(ctx, string(r.TenantID), string(a.ID)); err != nil {
				return flow.Event{}, err
			}
		}
		if now.Before(a.Deadline) {
			return flow.Event{}, d.deferRun(ctx, r, a.Deadline, "quota.reconciliation_wait")
		}
		if _, err = d.Quota.ExpireRequestSlots(ctx, pricing.CredentialGroup); err != nil {
			return flow.Event{}, err
		}
		return d.failModel(ctx, r, a, domain.ErrReconciliation, "unconfirmed_previous_dispatch", true, 0, "neutral")
	}
	// Credentials are supplied outside the snapshot. A changed credential group
	// must not dispatch against an old group's frozen reservation.
	current, ok := d.Models[a.Provider+"/"+a.Model]
	if !ok || current.CredentialGroup != pricing.CredentialGroup {
		return flow.Event{}, domain.ErrReconciliation
	}
	p := d.Providers[a.Provider]
	if d.ProviderFactory != nil {
		p = d.ProviderFactory(r)
	}
	if p == nil {
		return flow.Event{}, domain.ErrInvalid
	}
	var input contextEnvelope
	if err = d.load(ctx, r, a.RequestRef, &input); err != nil {
		return flow.Event{}, err
	}
	req := provider.ModelRequest{RunID: string(r.ID), StepID: fmt.Sprint(a.StepSeq), AttemptID: string(a.ID), ModelID: a.Model, Messages: input.Messages, NativeState: input.NativeState, Tools: input.Tools, MaxOutputTokens: pricing.MaxOutputTokens, Deadline: a.Deadline}
	inputBound, boundErr := provider.InputTokenUpperBound(req)
	caps, capsErr := p.Capabilities(ctx, a.Model)
	if boundErr != nil || capsErr != nil || inputBound > pricing.ContextTokens || (caps.ContextWindow > 0 && (pricing.MaxOutputTokens > caps.ContextWindow || inputBound > caps.ContextWindow-pricing.MaxOutputTokens)) {
		if err = d.Quota.AbandonBeforeDispatch(ctx, string(r.TenantID), string(a.ID)); err != nil {
			return flow.Event{}, err
		}
		return d.failModel(ctx, r, a, domain.ErrCapacity, "input_exceeds_frozen_context_bound", false, 0, "neutral")
	}
	if _, _, err = d.Store.LeaseProof(ctx, r.TenantID, r.ID, r.State.Lease.Owner, r.State.Lease.Epoch); err != nil {
		return flow.Event{}, err
	}
	if err = d.Quota.MarkDispatched(ctx, string(r.TenantID), string(a.ID)); err != nil {
		return flow.Event{}, err
	}
	if err = d.fault("after_model_dispatch_marker"); err != nil {
		return flow.Event{}, err
	}
	modelctx, cancel := context.WithDeadline(ctx, a.Deadline)
	defer cancel()
	sink := newSink(modelctx, cancel, d, r, a)
	defer sink.close()
	done := make(chan struct{})
	defer close(done)
	go func() {
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-modelctx.Done():
				return
			case <-ticker.C:
				current, err := d.Store.GetRun(modelctx, r.TenantID, r.ID)
				if err != nil || current.State.Status == domain.StatusCancelRequested || current.State.Lease.Epoch != r.State.Lease.Epoch {
					cancel()
					return
				}
			}
		}
	}()
	modelEnd := func(telemetry.ModelObservation) {}
	streamCtx := modelctx
	if d.Telemetry != nil {
		streamCtx, modelEnd = d.Telemetry.StartModel(modelctx, a.Provider)
	}
	turn, streamErr := p.Stream(streamCtx, req, sink.emit)
	observation := telemetry.ModelObservation{Outcome: telemetry.Success}
	if streamErr != nil {
		observation.Outcome = telemetry.Unknown
	} else if turn.Usage.Final {
		observation.InputTokens = telemetry.TokenCount{Value: turn.Usage.Input.Value, Known: turn.Usage.Input.Known}
		observation.OutputTokens = telemetry.TokenCount{Value: turn.Usage.Output.Value, Known: turn.Usage.Output.Known}
		observation.CacheReadTokens = telemetry.TokenCount{Value: turn.Usage.CacheRead.Value, Known: turn.Usage.CacheRead.Known}
		observation.CacheWriteTokens = telemetry.TokenCount{Value: turn.Usage.CacheWrite.Value, Known: turn.Usage.CacheWrite.Known}
	}
	modelEnd(observation)
	// A consumer failure takes precedence over a provider wrapping the cancelled
	// context; a database sink error says nothing about provider health.
	if flushErr := sink.close(); flushErr != nil {
		streamErr = &provider.Error{Kind: provider.ErrConsumer, Detail: "event persistence failed", Cause: flushErr}
	}
	if streamErr != nil {
		if err = d.Quota.MarkUnknown(ctx, string(r.TenantID), string(a.ID)); err != nil {
			return flow.Event{}, err
		}
		current, err := d.Store.GetRun(ctx, r.TenantID, r.ID)
		if err != nil {
			return flow.Event{}, err
		}
		code, outcome, retryable, retryAfter := classifyModelFailure(streamErr)
		cancelled := current.State.Status == domain.StatusCancelRequested
		if cancelled {
			code = "cancelled"
			outcome = "neutral"
			retryable = false
		}
		event, err := d.failModel(ctx, r, a, streamErr, code, retryable, retryAfter, outcome)
		if cancelled && err != nil && !errors.Is(err, errDeferred) {
			return flow.Event{}, errReload
		}
		return event, err
	}
	if turn.RunID != string(r.ID) || turn.AttemptID != string(a.ID) || turn.StepID != fmt.Sprint(a.StepSeq) {
		return d.failModel(ctx, r, a, domain.ErrInvalid, "response_identity_mismatch", false, 0, "neutral")
	}
	rawRef, err := d.put(ctx, r, "model_response", turn)
	if err != nil {
		return flow.Event{}, err
	}
	usage, _ := json.Marshal(turn.Usage)
	if err = d.Store.CompleteAttempt(ctx, a, rawRef, turn.ProviderRequestID, usage); err != nil {
		return flow.Event{}, err
	}
	if err = d.fault("after_model_result_before_transition"); err != nil {
		return flow.Event{}, err
	}
	a.RawRef = rawRef
	a.Status = "completed"
	return d.completedModel(ctx, r, a)
}

func (p attemptPricing) reservation(r persistence.Run, a persistence.ModelAttempt, cost domain.Money) quota.Request {
	return quota.Request{TenantID: string(r.TenantID), RunID: string(r.ID), AttemptID: string(a.ID), CredentialGroup: p.CredentialGroup, InputTokens: p.ContextTokens, MaxOutputTokens: p.MaxOutputTokens, MaxCost: cost, PriceVersion: p.PriceVersion, RequestDeadline: a.Deadline}
}

func classifyModelFailure(err error) (code, outcome string, retryable bool, retryAfter time.Duration) {
	if errors.Is(err, context.Canceled) {
		return "cancelled", "neutral", false, 0
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline", "neutral", false, 0
	}
	var failure *provider.Error
	if !errors.As(err, &failure) {
		return "local_failure", "neutral", false, 0
	}
	outcome = "neutral"
	if failure.Retryable() {
		outcome = "failure"
	}
	return string(failure.Kind), outcome, failure.Retryable(), failure.RetryAfter
}

func (d *Driver) replayFailureOutcome(ctx context.Context, a persistence.ModelAttempt, policy persistence.AttemptFailurePolicy) error {
	reservation, err := d.Quota.Get(ctx, string(a.TenantID), string(a.ID))
	if errors.Is(err, quota.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if reservation.DispatchedAt == nil {
		return d.Quota.AbandonBeforeDispatch(ctx, string(a.TenantID), string(a.ID))
	}
	if reservation.Status != "settled" {
		if err = d.Quota.MarkUnknown(ctx, string(a.TenantID), string(a.ID)); err != nil {
			return err
		}
	}
	return d.Quota.RecordOutcome(ctx, string(a.TenantID), string(a.ID), policy.Outcome)
}

func (d *Driver) failModel(ctx context.Context, r persistence.Run, a persistence.ModelAttempt, cause error, code string, retryable bool, retryAfter time.Duration, outcome string) (flow.Event, error) {
	first, err := d.Store.FirstAttemptAt(ctx, a)
	if err != nil {
		return flow.Event{}, err
	}
	now, err := d.Store.DatabaseTime(ctx)
	if err != nil {
		return flow.Event{}, err
	}
	decision, err := (quota.RetryPolicy{MaxAttempts: 3, BaseDelay: time.Second, MaxDelay: 30 * time.Second, TotalBudget: 2 * time.Minute}).Next(quota.RetryInput{Now: now, FirstAttemptAt: first, Deadline: r.State.Limits.Deadline, CompletedAttempts: a.Number, Retryable: retryable, RetryAfter: retryAfter, Jitter: rand.Float64()})
	if err != nil {
		return flow.Event{}, err
	}
	policy := persistence.AttemptFailurePolicy{Code: code, Outcome: outcome, Retry: decision.Retry, NotBefore: decision.NotBefore}
	if err = d.Store.FinishFailedAttempt(ctx, a, policy); err != nil {
		return flow.Event{}, err
	}
	if err = d.replayFailureOutcome(ctx, a, policy); err != nil {
		return flow.Event{}, err
	}
	if decision.Retry {
		return flow.Event{}, d.deferRun(ctx, r, decision.NotBefore, "quota.delayed")
	}
	return flow.Event{}, cause
}

func (d *Driver) completedModel(ctx context.Context, r persistence.Run, a persistence.ModelAttempt) (flow.Event, error) {
	frozen, err := d.Store.AttemptPricing(ctx, a)
	if err != nil {
		return flow.Event{}, err
	}
	pricing, err := decodePricing(a, frozen)
	if err != nil {
		return flow.Event{}, err
	}
	var turn provider.ModelTurn
	if err = d.load(ctx, r, a.RawRef, &turn); err != nil {
		return flow.Event{}, err
	}
	if turn.RunID != string(r.ID) || turn.AttemptID != string(a.ID) || turn.StepID != fmt.Sprint(a.StepSeq) {
		return flow.Event{}, domain.ErrConflict
	}
	cost, err := pricing.reserveCost()
	if err != nil {
		return flow.Event{}, err
	}
	// A completed attempt must already have a dispatch reservation. Never create
	// a new charge while recovering damaged/legacy completion evidence.
	if _, err = d.Quota.Get(ctx, string(r.TenantID), string(a.ID)); err != nil {
		return flow.Event{}, err
	}
	reservation, err := d.Quota.Reserve(ctx, pricing.reservation(r, a, cost))
	if err != nil {
		return flow.Event{}, err
	}
	if reservation.DispatchedAt == nil {
		return flow.Event{}, domain.ErrReconciliation
	}
	settlement, known, err := pricing.priceUsage(turn.Usage)
	if err != nil {
		return flow.Event{}, err
	}
	if known {
		err = d.Quota.Settle(ctx, string(r.TenantID), string(a.ID), settlement)
	} else if reservation.Status != "settled" {
		err = d.Quota.MarkUnknown(ctx, string(r.TenantID), string(a.ID))
	}
	if err != nil {
		return flow.Event{}, err
	}
	if err = d.Quota.RecordOutcome(ctx, string(r.TenantID), string(a.ID), "success"); err != nil {
		return flow.Event{}, err
	}
	if err = d.fault("after_model_settlement_before_transition"); err != nil {
		return flow.Event{}, err
	}
	total, err := d.Store.RunCost(ctx, r.TenantID, r.ID)
	if err != nil {
		return flow.Event{}, err
	}
	// Reducer cost is a monotonic conservative budget. Late definitive billing
	// may reduce the separate ledger without producing a negative event charge.
	delta := domain.Money(0)
	if total > r.State.Cost {
		delta = total - r.State.Cost
	}
	finish := len(turn.ToolCalls) == 0
	for _, call := range turn.ToolCalls {
		if call.Name == "finish" {
			if len(turn.ToolCalls) != 1 {
				return flow.Event{}, domain.ErrInvalid
			}
			finish = true
		}
	}
	return flow.Event{Kind: flow.EventModelCompleted, Complete: true, Finish: finish, OutputRef: a.RawRef, Cost: delta}, nil
}

type streamSink struct {
	mu      sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
	d       *Driver
	r       persistence.Run
	a       persistence.ModelAttempt
	pending string
	err     error
	done    chan struct{}
	once    sync.Once
	wg      sync.WaitGroup
}

func newSink(ctx context.Context, cancel context.CancelFunc, d *Driver, r persistence.Run, a persistence.ModelAttempt) *streamSink {
	s := &streamSink{ctx: ctx, cancel: cancel, d: d, r: r, a: a, done: make(chan struct{})}
	s.wg.Go(func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-s.done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.mu.Lock()
				s.flush()
				s.mu.Unlock()
			}
		}
	})
	return s
}
func (s *streamSink) flush() {
	if s.err != nil || s.pending == "" {
		return
	}
	body, _ := json.Marshal(map[string]any{"attempt_id": s.a.ID, "provisional": true, "delta": s.pending})
	ctx, cancel := context.WithTimeout(s.ctx, 3*time.Second)
	defer cancel()
	s.err = s.d.Store.AppendWorkerEvent(ctx, s.r, "text.delta", body)
	s.pending = ""
	if s.err != nil {
		s.cancel()
	}
}
func (s *streamSink) emit(e provider.ModelEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	if e.Type == provider.EventTextDelta {
		for len(e.Delta) > 0 {
			size := min(16<<10-len(s.pending), len(e.Delta))
			s.pending += e.Delta[:size]
			e.Delta = e.Delta[size:]
			if len(s.pending) >= 16<<10 {
				s.flush()
				if s.err != nil {
					return s.err
				}
			}
		}
	}
	return nil
}
func (s *streamSink) close() error {
	s.once.Do(func() { close(s.done); s.wg.Wait(); s.mu.Lock(); s.flush(); s.mu.Unlock() })
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}
