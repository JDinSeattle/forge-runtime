package application

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/provider"
	"github.com/JDinSeattle/forge-runtime/internal/quota"
)

// attemptPricing is a versioned quote from operator configuration, not a vendor
// invoice. Monetary arithmetic is exact at the configured rate, rounded upward
// once to a microdollar. Missing prices or ambiguous cache classes stay unknown.
type attemptPricing struct {
	SchemaVersion int    `json:"schema_version"`
	Provider      string `json:"provider"`
	Model         string `json:"model"`
	ModelSpec
}

func freezePricing(providerName, model string, spec ModelSpec) (attemptPricing, error) {
	p := attemptPricing{SchemaVersion: 1, Provider: providerName, Model: model, ModelSpec: spec}
	if providerName == "fake" {
		p.ExactPricing = true
	}
	if err := p.validate(); err != nil {
		return p, err
	}
	// Pointer values must be copied: a mutable configuration object is not the
	// quote snapshot that the request will persist and dispatch against.
	for _, field := range []**domain.Money{&p.CacheReadPrice, &p.CacheWrite5mPrice, &p.CacheWrite1hPrice} {
		if *field != nil {
			value := **field
			*field = &value
		}
	}
	return p, nil
}
func (p attemptPricing) validate() error {
	if p.SchemaVersion != 1 || p.Model == "" || p.PriceVersion == "" || p.CredentialGroup == "" || p.ContextTokens <= 0 || p.MaxOutputTokens <= 0 || p.ContextTokens > math.MaxInt64-p.MaxOutputTokens || p.RequestTimeout <= 0 || p.InputPrice < 0 || p.OutputPrice < 0 {
		return domain.ErrInvalid
	}
	if p.Provider != "fake" && p.Provider != "openai" && p.Provider != "anthropic" {
		return domain.ErrInvalid
	}
	if p.Provider != "fake" && !p.ExactPricing && (p.InputPrice == 0 || p.OutputPrice == 0) {
		return fmt.Errorf("%w: unknown pricing needs positive conservative input/output bounds", domain.ErrInvalid)
	}
	for _, rate := range []*domain.Money{p.CacheReadPrice, p.CacheWrite5mPrice, p.CacheWrite1hPrice} {
		if rate != nil && *rate < 0 {
			return domain.ErrInvalid
		}
	}
	if p.Provider == "anthropic" && p.ExactPricing && (p.CacheReadPrice == nil || p.CacheWrite5mPrice == nil || p.CacheWrite1hPrice == nil) {
		return fmt.Errorf("%w: exact Anthropic pricing needs rates for all cache classes; otherwise configure conservative bounds", domain.ErrInvalid)
	}
	return nil
}
func decodePricing(a persistence.ModelAttempt, raw json.RawMessage) (attemptPricing, error) {
	var p attemptPricing
	if json.Unmarshal(raw, &p) != nil {
		return p, domain.ErrReconciliation
	}
	if p.Provider != a.Provider || p.Model != a.Model || p.PriceVersion != a.PriceVersion {
		return p, domain.ErrConflict
	}
	return p, p.validate()
}
func (p attemptPricing) reserveCost() (domain.Money, error) {
	inputRate := p.InputPrice
	for _, rate := range []*domain.Money{p.CacheReadPrice, p.CacheWrite5mPrice, p.CacheWrite1hPrice} {
		if rate != nil && *rate > inputRate {
			inputRate = *rate
		}
	}
	return priceTerms([]pricedTokens{{p.ContextTokens, inputRate}, {p.MaxOutputTokens, p.OutputPrice}})
}

type pricedTokens struct {
	tokens int64
	rate   domain.Money
}

func priceTerms(terms []pricedTokens) (domain.Money, error) {
	var sum big.Int
	for _, term := range terms {
		if term.tokens < 0 || term.rate < 0 {
			return 0, domain.ErrInvalid
		}
		var product big.Int
		product.Mul(big.NewInt(term.tokens), big.NewInt(int64(term.rate)))
		sum.Add(&sum, &product)
	}
	sum.Add(&sum, big.NewInt(999999))
	sum.Div(&sum, big.NewInt(1000000))
	if !sum.IsInt64() {
		return 0, domain.ErrOverflow
	}
	return domain.Money(sum.Int64()), nil
}
func addTokens(values ...int64) (int64, error) {
	var total int64
	for _, v := range values {
		if v < 0 {
			return 0, domain.ErrInvalid
		}
		if v > math.MaxInt64-total {
			return 0, domain.ErrOverflow
		}
		total += v
	}
	return total, nil
}

func (p attemptPricing) priceUsage(u provider.Usage) (quota.Settlement, bool, error) {
	for _, counter := range []provider.TokenCount{u.Input, u.Output, u.CacheRead, u.CacheWrite} {
		if counter.Known && counter.Value < 0 {
			return quota.Settlement{}, false, domain.ErrInvalid
		}
	}
	if !u.Final || !u.Input.Known || !u.Output.Known || !p.ExactPricing {
		return quota.Settlement{}, false, nil
	}
	input, output := u.Input.Value, u.Output.Value
	terms := []pricedTokens{{output, p.OutputPrice}}
	total, err := addTokens(input, output)
	if err != nil {
		return quota.Settlement{}, false, err
	}
	switch p.Provider {
	case "fake":
		terms = append(terms, pricedTokens{input, p.InputPrice})
	case "openai":
		// OpenAI input_tokens already includes cached_tokens. Subtract cache reads
		// from base input rather than charging them a second time.
		if !u.CacheRead.Known {
			return quota.Settlement{}, false, nil
		}
		if u.CacheRead.Value > input {
			return quota.Settlement{}, false, domain.ErrInvalid
		}
		if u.CacheRead.Value > 0 && p.CacheReadPrice == nil {
			return quota.Settlement{}, false, nil
		}
		terms = append(terms, pricedTokens{input - u.CacheRead.Value, p.InputPrice})
		if u.CacheRead.Value > 0 {
			terms = append(terms, pricedTokens{u.CacheRead.Value, *p.CacheReadPrice})
		}
	case "anthropic":
		// Anthropic input_tokens excludes additive cache-read/cache-write counts.
		if !u.CacheRead.Known || !u.CacheWrite.Known {
			return quota.Settlement{}, false, nil
		}
		total, err = addTokens(input, u.CacheRead.Value, u.CacheWrite.Value, output)
		if err != nil {
			return quota.Settlement{}, false, err
		}
		terms = append(terms, pricedTokens{input, p.InputPrice})
		if u.CacheRead.Value > 0 {
			if p.CacheReadPrice == nil {
				return quota.Settlement{}, false, nil
			}
			terms = append(terms, pricedTokens{u.CacheRead.Value, *p.CacheReadPrice})
		}
		if u.CacheWrite.Value > 0 {
			// The initial adapter has an aggregate cache creation count, not its
			// 5-minute/1-hour TTL split. Equal configured rates are unambiguous;
			// differing or missing rates cannot produce a definitive estimate.
			if p.CacheWrite5mPrice == nil || p.CacheWrite1hPrice == nil || *p.CacheWrite5mPrice != *p.CacheWrite1hPrice {
				return quota.Settlement{}, false, nil
			}
			terms = append(terms, pricedTokens{u.CacheWrite.Value, *p.CacheWrite5mPrice})
		}
	default:
		return quota.Settlement{}, false, domain.ErrInvalid
	}
	cost, err := priceTerms(terms)
	return quota.Settlement{Tokens: total, Cost: cost}, err == nil, err
}
