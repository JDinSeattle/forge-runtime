package quota

import (
	"fmt"
	"math"
	"time"
)

type RetryPolicy struct {
	MaxAttempts                      int
	BaseDelay, MaxDelay, TotalBudget time.Duration
}
type RetryInput struct {
	Now               time.Time
	FirstAttemptAt    time.Time
	Deadline          time.Time
	CompletedAttempts int
	Retryable         bool
	RetryAfter        time.Duration
	// Jitter is a caller-supplied random value in [0,1]. Persist the resulting
	// NotBefore once; recovery must not resample or sleep inside a worker lease.
	Jitter float64
}
type RetryDecision struct {
	Retry     bool
	NotBefore time.Time
	Reason    string
}

func (p RetryPolicy) Next(in RetryInput) (RetryDecision, error) {
	if p.MaxAttempts <= 0 || p.BaseDelay <= 0 || p.MaxDelay < p.BaseDelay || p.TotalBudget <= 0 || in.CompletedAttempts <= 0 || in.Now.IsZero() || in.FirstAttemptAt.IsZero() || in.Jitter < 0 || in.Jitter > 1 || math.IsNaN(in.Jitter) || in.RetryAfter < 0 {
		return RetryDecision{}, fmt.Errorf("%w: retry policy or attempt", ErrInvalid)
	}
	if !in.Retryable {
		return RetryDecision{Reason: "permanent failure"}, nil
	}
	if in.CompletedAttempts >= p.MaxAttempts {
		return RetryDecision{Reason: "attempt budget exhausted"}, nil
	}
	cap := p.BaseDelay
	for n := 1; n < in.CompletedAttempts && cap < p.MaxDelay; n++ {
		if cap > p.MaxDelay/2 {
			cap = p.MaxDelay
		} else {
			cap *= 2
		}
	}
	delay := time.Duration(float64(cap) * in.Jitter)
	if delay < 0 || delay > cap {
		delay = cap
	}
	if in.RetryAfter > delay {
		delay = in.RetryAfter
	}
	until := in.FirstAttemptAt.Add(p.TotalBudget)
	if !in.Deadline.IsZero() && in.Deadline.Before(until) {
		until = in.Deadline
	}
	next := in.Now.Add(delay)
	if !next.Before(until) {
		return RetryDecision{Reason: "retry would exceed deadline or total budget"}, nil
	}
	return RetryDecision{Retry: true, NotBefore: next, Reason: "transient provider failure"}, nil
}
