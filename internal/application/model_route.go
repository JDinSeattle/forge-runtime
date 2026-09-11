package application

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/provider"
)

func (d *Driver) selectedProvider(r persistence.Run, route persistence.ModelRoute) provider.Provider {
	if d.ProviderFactory != nil {
		// The factory gets the selected dispatch identity without mutating the
		// persisted submission configuration or the caller's Run.
		r.Config.Provider, r.Config.Model = route.Provider, route.Model
		return d.ProviderFactory(r)
	}
	return d.Providers[route.Provider]
}

// portableHandoff strips opaque state and rebinds public call IDs. IDs produced
// by one vendor need not meet another vendor's syntax; their pairing must stay
// exact. Only completely closed tool batches can be migrated.
func portableHandoff(input contextEnvelope) (contextEnvelope, error) {
	messages := input.Messages
	if input.NativeState != nil {
		messages = input.PortableMessages
		if len(messages) == 0 {
			return contextEnvelope{}, domain.ErrReconciliation
		}
	}
	out := input
	out.NativeState, out.PortableMessages = nil, nil
	out.Messages = make([]provider.Message, 0, len(messages))
	pending := map[string]string{}
	n := 0
	for _, message := range messages {
		m := message
		switch m.Role {
		case "system", "user", "assistant", "tool":
		default:
			return contextEnvelope{}, domain.ErrInvalid
		}
		if m.Role != "tool" && len(pending) != 0 {
			return contextEnvelope{}, domain.ErrReconciliation
		}
		if len(m.ToolCalls) != 0 {
			if m.Role != "assistant" {
				return contextEnvelope{}, domain.ErrInvalid
			}
			m.ToolCalls = append([]provider.ToolCall(nil), m.ToolCalls...)
			for i, call := range m.ToolCalls {
				if call.ID == "" || pending[call.ID] != "" {
					return contextEnvelope{}, domain.ErrReconciliation
				}
				n++
				id := fmt.Sprintf("handoff_%d", n)
				pending[call.ID] = id
				m.ToolCalls[i].ID = id
			}
		}
		if m.Role == "tool" {
			id, ok := pending[m.ToolCallID]
			if !ok {
				return contextEnvelope{}, domain.ErrReconciliation
			}
			delete(pending, m.ToolCallID)
			m.ToolCallID = id
		}
		out.Messages = append(out.Messages, m)
	}
	if len(pending) != 0 || len(out.Messages) == 0 {
		return contextEnvelope{}, domain.ErrReconciliation
	}
	return out, nil
}

// fallbackContext returns an eligible candidate using the already committed
// request watermark. It neither consumes a new message nor advances the reducer.
// Incompatible destinations leave the ordinary bounded primary retry intact.
func (d *Driver) fallbackContext(ctx context.Context, r persistence.Run, a persistence.ModelAttempt, active persistence.ModelRoute) (persistence.ModelRoute, *contextEnvelope, error) {
	if r.Config.Fallback == nil || active != r.Config.PrimaryRoute() || a.Status != "failed" || !persistence.FallbackFailure(a.ErrorCode) {
		return active, nil, nil
	}
	target := *r.Config.Fallback
	spec, ok := d.Models[target.Key()]
	p := d.selectedProvider(r, target)
	if !ok || p == nil {
		return active, nil, nil
	}
	if _, err := freezePricing(target.Provider, target.Model, spec); err != nil {
		return active, nil, nil
	}
	caps, err := p.Capabilities(ctx, target.Model)
	if err != nil {
		if ctx.Err() != nil {
			return active, nil, ctx.Err()
		}
		return active, nil, nil
	}
	if !caps.ToolCalling || caps.ContextWindow <= 0 || caps.MaxOutputTokens <= 0 || spec.MaxOutputTokens > caps.MaxOutputTokens || spec.MaxOutputTokens >= caps.ContextWindow {
		return active, nil, nil
	}
	var original contextEnvelope
	if err = d.load(ctx, r, a.RequestRef, &original); err != nil {
		return active, nil, err
	}
	input, err := portableHandoff(original)
	if err != nil {
		return active, nil, err
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return active, nil, err
	}
	if len(raw) > 512<<10 {
		return active, nil, nil
	}
	bound, err := provider.InputTokenUpperBound(provider.ModelRequest{Messages: input.Messages, Tools: input.Tools})
	if err != nil {
		return active, nil, err
	}
	if bound > spec.ContextTokens || bound > caps.ContextWindow-spec.MaxOutputTokens {
		return active, nil, nil
	}
	return target, &input, nil
}
