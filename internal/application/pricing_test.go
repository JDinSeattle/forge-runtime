package application

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/provider"
)

func money(v domain.Money) *domain.Money { return &v }
func pricingFixture(name string) attemptPricing {
	return attemptPricing{SchemaVersion: 1, Provider: name, Model: "fixture", ModelSpec: ModelSpec{CredentialGroup: "group", PriceVersion: "test-v1", ExactPricing: true, InputPrice: 1_000_000, OutputPrice: 2_000_000, CacheReadPrice: money(250_000), CacheWrite5mPrice: money(1_250_000), CacheWrite1hPrice: money(1_250_000), ContextTokens: 1000, MaxOutputTokens: 100, RequestTimeout: time.Second}}
}
func measured(v int64) provider.TokenCount { return provider.TokenCount{Value: v, Known: true} }
func usage(in, out, read, write int64) provider.Usage {
	return provider.Usage{Final: true, Input: measured(in), Output: measured(out), CacheRead: measured(read), CacheWrite: measured(write)}
}
func TestProviderSpecificPricing(t *testing.T) {
	tests := []struct {
		name    string
		pricing attemptPricing
		usage   provider.Usage
		tokens  int64
		cost    domain.Money
		known   bool
		err     error
	}{
		{"deepseek Responses cached input is inclusive", pricingFixture("deepseek"), usage(100, 50, 40, 0), 150, 170, true, nil},
		{"openai cached input is inclusive", pricingFixture("openai"), usage(100, 50, 40, 0), 150, 170, true, nil},
		{"anthropic cache input is additive", pricingFixture("anthropic"), usage(100, 50, 40, 20), 210, 235, true, nil},
		{"measured zero", pricingFixture("openai"), usage(0, 0, 0, 0), 0, 0, true, nil},
		{"cache cannot exceed inclusive input", pricingFixture("openai"), usage(10, 1, 11, 0), 0, 0, false, domain.ErrInvalid},
		{"input output overflow", pricingFixture("openai"), usage(math.MaxInt64, 1, 0, 0), 0, 0, false, domain.ErrOverflow},
		{"additive cache overflow", pricingFixture("anthropic"), usage(math.MaxInt64-2, 1, 2, 0), 0, 0, false, domain.ErrOverflow},
		{"negative measured cache", pricingFixture("openai"), usage(1, 1, -1, 0), 0, 0, false, domain.ErrInvalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, known, err := tt.pricing.priceUsage(tt.usage)
			if !errors.Is(err, tt.err) || known != tt.known || got.Tokens != tt.tokens || got.Cost != tt.cost {
				t.Fatalf("got %+v known=%v err=%v", got, known, err)
			}
		})
	}
	p := pricingFixture("anthropic")
	p.CacheWrite1hPrice = money(2_000_000)
	if _, known, err := p.priceUsage(usage(100, 50, 40, 20)); err != nil || known {
		t.Fatalf("ambiguous TTL priced: known=%v err=%v", known, err)
	}
	if _, known, err := p.priceUsage(usage(100, 50, 40, 0)); err != nil || !known {
		t.Fatalf("zero writes are unambiguous: known=%v err=%v", known, err)
	}
	p.ExactPricing = false
	if _, known, err := p.priceUsage(usage(100, 50, 40, 0)); err != nil || known {
		t.Fatal("reservation ceiling treated as an exact price")
	}
	u := usage(0, 0, 0, 0)
	u.Input.Known = false
	if _, known, err := pricingFixture("fake").priceUsage(u); err != nil || known {
		t.Fatal("unknown input treated as zero")
	}
	u = usage(1, 1, 0, 0)
	u.Final = false
	if _, known, _ := pricingFixture("fake").priceUsage(u); known {
		t.Fatal("partial usage priced")
	}
}
func TestPricingReserveBoundsAndRounding(t *testing.T) {
	p := pricingFixture("anthropic")
	p.CacheWrite1hPrice = money(3_000_000)
	if cost, err := p.reserveCost(); err != nil || cost != 3200 {
		t.Fatalf("reserve=%d err=%v", cost, err)
	}
	if cost, err := priceTerms([]pricedTokens{{1, 1}, {1, 1}}); err != nil || cost != 1 {
		t.Fatalf("must round once after exact sum: %d %v", cost, err)
	}
	if _, err := priceTerms([]pricedTokens{{math.MaxInt64, domain.Money(math.MaxInt64)}}); !errors.Is(err, domain.ErrOverflow) {
		t.Fatalf("overflow=%v", err)
	}
	frozen, err := freezePricing("anthropic", "fixture", p.ModelSpec)
	if err != nil {
		t.Fatal(err)
	}
	*p.CacheWrite1hPrice = 7
	if *frozen.CacheWrite1hPrice != 3_000_000 {
		t.Fatal("snapshot retained mutable rate pointer")
	}
}
func TestModelFailureCircuitClassification(t *testing.T) {
	cases := []struct {
		name    string
		err     error
		outcome string
		retry   bool
	}{
		{"user cancellation", context.Canceled, "neutral", false},
		{"request deadline", context.DeadlineExceeded, "neutral", false},
		{"database sink", errors.New("database unavailable"), "neutral", false},
		{"consumer wrapper", &provider.Error{Kind: provider.ErrConsumer, Cause: errors.New("database failed")}, "neutral", false},
		{"validation", &provider.Error{Kind: provider.ErrInvalidRequest}, "neutral", false},
		{"protocol", &provider.Error{Kind: provider.ErrProtocol}, "neutral", false},
		{"authentication", &provider.Error{Kind: provider.ErrAuthentication}, "neutral", false},
		{"cancelled transport", &provider.Error{Kind: provider.ErrInterrupted, Cause: context.Canceled}, "neutral", false},
		{"overloaded", &provider.Error{Kind: provider.ErrUnavailable}, "failure", true},
		{"rate limit", &provider.Error{Kind: provider.ErrRateLimited, RetryAfter: 15 * time.Second}, "failure", true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			_, outcome, retry, _ := classifyModelFailure(tt.err)
			if outcome != tt.outcome || retry != tt.retry {
				t.Fatalf("outcome=%s retry=%v", outcome, retry)
			}
		})
	}
}

func TestDeepSeekPeakBoundAndUnknownUsage(t *testing.T) {
	spec := ModelSpec{CredentialGroup: "deepseek-eval", PriceVersion: "deepseek-flash-peak-20260912", ExactPricing: false, InputPrice: 300000, OutputPrice: 1200000, CacheReadPrice: money(6000), ContextTokens: 32768, MaxOutputTokens: 8192, RequestTimeout: time.Minute}
	p, err := freezePricing("deepseek", "deepseek-v4-flash", spec)
	if err != nil {
		t.Fatal(err)
	}
	if reserved, err := p.reserveCost(); err != nil || reserved != 19661 {
		t.Fatalf("reserve %d %v", reserved, err)
	}
	// Peak/off-peak billing time is not proven by a local dispatch clock. Even a
	// measured complete usage is not an invoice at this conservative peak quote.
	if _, known, err := p.priceUsage(usage(100, 50, 40, 0)); err != nil || known {
		t.Fatal("peak ceiling treated as exact billing")
	}
	p.ExactPricing = true // hypothetical immutable, applicable quote: arithmetic only
	for _, u := range []provider.Usage{{}, {Final: true, Input: measured(100), Output: measured(50)}, {Input: measured(100), Output: measured(50), CacheRead: measured(40)}} {
		if _, known, err := p.priceUsage(u); err != nil || known {
			t.Fatal("unknown/partial counter treated as free")
		}
	}
	if _, known, err := p.priceUsage(usage(1, 1, 2, 0)); err == nil || known {
		t.Fatal("impossible inclusive cache accepted")
	}
}
