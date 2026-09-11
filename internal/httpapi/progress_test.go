package httpapi

import (
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"testing"
)

func TestProgressBudgetOnlyReducesConfiguredLimit(t *testing.T) {
	c := persistence.Config{Provider: "fake", Model: "fake", MaxModelRounds: 8, MaxToolCalls: 20, MaxRuntimeSeconds: 60}
	got, err := (Budget{}).apply(c)
	if err != nil || got.MaxNoProgressBatches != 0 {
		t.Fatal("omitted budget changed request before idempotency", got, err)
	}
	for _, value := range []uint64{0, 1, 3, 4, 21} {
		got, err = (Budget{MaxNoProgressBatches: &value}).apply(c)
		if value == 1 || value == 3 {
			if err != nil || got.MaxNoProgressBatches != value {
				t.Fatal(value, got, err)
			}
		} else if err == nil {
			t.Fatal("invalid progress override accepted", value)
		}
	}
	c.MaxNoProgressBatches = 5
	value := uint64(4)
	if _, err = (Budget{MaxNoProgressBatches: &value}).apply(c); err != nil {
		t.Fatal(err)
	}
}
