package provider

import (
	"context"
	"sync"
)

// Script is a complete attempt; chunks permit deterministic split-JSON tests.
// Failure happens after the listed chunks; tools are still withheld on failure.
type Script struct {
	Chunks       []Chunk
	Usage        Usage
	FinishReason string
	Failure      error
}
type Chunk struct {
	Kind   string
	CallID string
	Name   string
	Delta  string
}

type FakeProvider struct {
	Registry Registry
	Limits   Limits
	mu       sync.Mutex
	scripts  []Script
	requests []ModelRequest
}

func NewFake(scripts ...Script) *FakeProvider {
	return &FakeProvider{Registry: Registry{"fake": {ToolCalling: true, MaxOutputTokens: 8192}}, scripts: append([]Script(nil), scripts...)}
}
func (p *FakeProvider) Capabilities(ctx context.Context, model string) (Capabilities, error) {
	return p.Registry.Lookup(ctx, model)
}
func (p *FakeProvider) Requests() []ModelRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ModelRequest(nil), p.requests...)
}
func (p *FakeProvider) Stream(ctx context.Context, req ModelRequest, emit func(ModelEvent) error) (ModelTurn, error) {
	caps, err := p.Capabilities(ctx, req.ModelID)
	if err != nil {
		return ModelTurn{}, err
	}
	if err = validateRequest(req, caps, "fake"); err != nil {
		return ModelTurn{}, err
	}
	if !req.Deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, req.Deadline)
		defer cancel()
	}
	a, err := newAssembly(ctx, req, p.Limits, emit)
	if err != nil {
		return ModelTurn{}, err
	}
	if err = a.event(EventAttemptStarted, "", "", "", nil, ""); err != nil {
		return a.fail(err)
	}
	p.mu.Lock()
	if len(p.scripts) == 0 {
		p.mu.Unlock()
		return a.fail(&Error{Kind: ErrInvalidRequest, Detail: "fake script exhausted"})
	}
	script := p.scripts[0]
	p.scripts = p.scripts[1:]
	p.requests = append(p.requests, req)
	p.mu.Unlock()
	for _, c := range script.Chunks {
		if err = ctx.Err(); err != nil {
			return a.fail(err)
		}
		switch c.Kind {
		case "text":
			err = a.textDelta(c.Delta)
		case "tool_start":
			err = a.begin(c.CallID, c.Name)
		case "tool_delta":
			err = a.arguments(c.CallID, c.Delta)
		case "tool_end":
			err = a.end(c.CallID)
		default:
			err = &Error{Kind: ErrProtocol, Detail: "unknown scripted chunk"}
		}
		if err != nil {
			return a.fail(err)
		}
	}
	a.usage = script.Usage
	if script.Failure != nil {
		return a.fail(script.Failure)
	}
	if script.FinishReason == "" {
		return a.fail(&Error{Kind: ErrInterrupted, Detail: "fake stream has no terminal event"})
	}
	turn, err := a.finish(script.FinishReason, nil, "")
	if err != nil {
		return a.fail(err)
	}
	return turn, nil
}
