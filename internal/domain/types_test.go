package domain

import (
	"errors"
	"math"
	"math/big"
	"testing"
	"time"
)

func TestMoneyDoesNotUnderReserveOrOverflow(t *testing.T) {
	for _, tc := range []struct {
		tokens int64
		rate   Money
		want   Money
	}{
		{0, USD, 0}, {1, 1, 1}, {1_000_001, 1, 2},
		{1_000_000, 5 * USD, 5 * USD}, {math.MaxInt64, 1, 9_223_372_036_855},
		{999_999, math.MaxInt64, 9_223_362_813_482_738_953},
	} {
		got, err := Cost(tc.tokens, tc.rate)
		if err != nil || got != tc.want {
			t.Errorf("Cost(%d,%d) = %d, %v; want %d", tc.tokens, tc.rate, got, err, tc.want)
		}
	}
	if _, err := Cost(math.MaxInt64, USD+1); !errors.Is(err, ErrOverflow) {
		t.Fatalf("overflow: %v", err)
	}
	if _, err := Money(math.MaxInt64).Add(1); !errors.Is(err, ErrOverflow) {
		t.Fatalf("addition overflow: %v", err)
	}
	if _, err := Money(-1).Add(1); !errors.Is(err, ErrInvalid) {
		t.Fatalf("negative amount: %v", err)
	}
}

func TestLeaseExpiryBoundary(t *testing.T) {
	now := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	lease := Lease{Owner: "worker", Epoch: 1, Until: now}
	if lease.ValidAt(now) || lease.ValidAt(now.Add(time.Nanosecond)) || lease.ValidAt(time.Time{}) {
		t.Fatal("expired/zero clock accepted")
	}
	if !lease.ValidAt(now.Add(-time.Nanosecond)) {
		t.Fatal("unexpired lease rejected")
	}
	lease.Owner = " "
	if lease.ValidAt(now.Add(-time.Nanosecond)) {
		t.Fatal("empty principal owns lease")
	}
}

func TestIDCannotBecomeFilesystemPath(t *testing.T) {
	for _, id := range []ID{"", "../run", "tenant/run", "run\x00id", "run name", "a.b", "é"} {
		if id.Validate() == nil {
			t.Errorf("accepted unsafe ID %q", id)
		}
	}
	for _, id := range []ID{"run-123", "tenant_1", "a"} {
		if err := id.Validate(); err != nil {
			t.Error(err)
		}
	}
}

// The arbitrary-precision oracle is independent of the overflow-avoiding
// integer implementation and exercises large values where naive products fail.
func FuzzCostMatchesExactCeiling(f *testing.F) {
	f.Add(int64(1), int64(1))
	f.Add(int64(math.MaxInt64), int64(1))
	f.Add(int64(999_999), int64(math.MaxInt64))
	f.Add(int64(math.MaxInt64), int64(math.MaxInt64))
	f.Fuzz(func(t *testing.T, tokens, rate int64) {
		got, err := Cost(tokens, Money(rate))
		if tokens < 0 || rate < 0 {
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("negative input accepted: %v", err)
			}
			return
		}
		want := new(big.Int).Mul(big.NewInt(tokens), big.NewInt(rate))
		want.Add(want, big.NewInt(999_999)).Div(want, big.NewInt(1_000_000))
		if !want.IsInt64() {
			if !errors.Is(err, ErrOverflow) {
				t.Fatalf("overflow not rejected: %v", err)
			}
			return
		}
		if err != nil || int64(got) != want.Int64() {
			t.Fatalf("got %d, %v; want %s", got, err, want)
		}
	})
}
