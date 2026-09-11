package persistence

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestFallbackConfigBoundsAndOmission(t *testing.T) {
	cfg := Config{Provider: "fake", Model: "primary", MaxModelRounds: 3, MaxToolCalls: 10, MaxCost: 100, MaxRuntimeSeconds: 30}
	raw, _ := json.Marshal(cfg)
	if strings.Contains(string(raw), "fallback") {
		t.Fatal("absent new policy changed legacy idempotency request encoding")
	}
	for _, scenario := range []string{"valid", "same_route", "empty_provider", "empty_model", "unbounded_cost"} {
		t.Run(scenario, func(t *testing.T) {
			c := cfg
			c.Fallback = &ModelRoute{Provider: "fake", Model: "backup"}
			switch scenario {
			case "same_route":
				c.Fallback.Model = c.Model
			case "empty_provider":
				c.Fallback.Provider = ""
			case "empty_model":
				c.Fallback.Model = ""
			case "unbounded_cost":
				c.MaxCost = 0
			}
			if err := c.Validate(); (err == nil) != (scenario == "valid") {
				t.Fatalf("validation=%v", err)
			}
		})
	}
}
