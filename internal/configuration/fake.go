package configuration

import (
	"context"
	"fmt"
	"strconv"

	"github.com/JDinSeattle/forge-runtime/internal/provider"
)

// ScriptedProvider indexes immutable fixtures by durable step ID. Concurrent
// workers and restarts cannot consume a shared mutable script cursor.
type ScriptedProvider struct{ Scripts []provider.Script }

func (p ScriptedProvider) Capabilities(ctx context.Context, id string) (provider.Capabilities, error) {
	return provider.NewFake().Capabilities(ctx, id)
}
func (p ScriptedProvider) Stream(ctx context.Context, r provider.ModelRequest, emit func(provider.ModelEvent) error) (provider.ModelTurn, error) {
	n, err := strconv.Atoi(r.StepID)
	if err != nil || n < 1 || n > len(p.Scripts) {
		return provider.ModelTurn{}, fmt.Errorf("fixture has no durable step %s", r.StepID)
	}
	return provider.NewFake(p.Scripts[n-1]).Stream(ctx, r, emit)
}
