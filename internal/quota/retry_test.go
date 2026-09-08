package quota

import (
	"math"
	"testing"
	"time"
)

func TestRetryBudgetAndRetryAfter(t *testing.T) {
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	p := RetryPolicy{MaxAttempts: 4, BaseDelay: time.Second, MaxDelay: 8 * time.Second, TotalBudget: time.Minute}
	in := RetryInput{Now: now, FirstAttemptAt: now, CompletedAttempts: 2, Retryable: true, Jitter: 0.25, RetryAfter: 3 * time.Second}
	got, err := p.Next(in)
	if err != nil || !got.Retry || !got.NotBefore.Equal(now.Add(3*time.Second)) {
		t.Fatalf("decision=%+v err=%v", got, err)
	}
	in.RetryAfter = 0
	got, err = p.Next(in)
	if err != nil || !got.NotBefore.Equal(now.Add(500*time.Millisecond)) {
		t.Fatalf("jitter=%+v err=%v", got, err)
	}
	in.CompletedAttempts = 4
	got, err = p.Next(in)
	if err != nil || got.Retry {
		t.Fatalf("attempt budget=%+v %v", got, err)
	}
	in.CompletedAttempts = 1
	in.RetryAfter = time.Hour
	got, err = p.Next(in)
	if err != nil || got.Retry {
		t.Fatalf("retry after must not be shortened=%+v %v", got, err)
	}
	in.RetryAfter = 0
	in.Deadline = now
	got, err = p.Next(in)
	if err != nil || got.Retry {
		t.Fatalf("expired=%+v %v", got, err)
	}
	in.Deadline = time.Time{}
	in.Retryable = false
	got, err = p.Next(in)
	if err != nil || got.Retry {
		t.Fatalf("permanent=%+v %v", got, err)
	}
	in.Jitter = math.NaN()
	if _, err = p.Next(in); err == nil {
		t.Fatal("NaN jitter accepted")
	}
}

func TestReservationHashBindsPriceAndDeadline(t *testing.T) {
	r := Request{TenantID: "tenant", RunID: "run", AttemptID: "attempt", CredentialGroup: "group", InputTokens: 3, MaxOutputTokens: 5, MaxCost: 10, PriceVersion: "price_v1", RequestDeadline: time.Now().Add(time.Minute)}
	_, tokens, hash, err := r.normalized()
	if err != nil || tokens != 8 {
		t.Fatal(err, tokens)
	}
	r.PriceVersion = "price_v2"
	_, _, next, err := r.normalized()
	if err != nil || hash == next {
		t.Fatal("price change did not change idempotency identity")
	}
	r.InputTokens = math.MaxInt64
	if _, _, _, err = r.normalized(); err == nil {
		t.Fatal("overflow accepted")
	}
}
