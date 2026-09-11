// Package provider isolates model wire protocols from the durable runtime.
// Stream returns executable tool calls only after the entire attempt completes.
// The caller must persist that turn before authorizing or executing any call.
package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type Provider interface {
	Capabilities(context.Context, string) (Capabilities, error)
	Stream(context.Context, ModelRequest, func(ModelEvent) error) (ModelTurn, error)
}

// Capabilities are operator configured for an exact model ID. Unknown models do
// not inherit assumptions from a family-name prefix. Advanced capabilities are
// recorded for routing; adapters reject required features not implemented here.
type Capabilities struct {
	ToolCalling      bool  `json:"tool_calling"`
	StructuredOutput bool  `json:"structured_output"`
	NativeCompaction bool  `json:"native_compaction"`
	ToolSearch       bool  `json:"tool_search"`
	NativeAsyncTools bool  `json:"native_async_tools"`
	ContextWindow    int64 `json:"context_window"`
	MaxOutputTokens  int64 `json:"max_output_tokens"`
}

type Registry map[string]Capabilities

func (r Registry) Lookup(ctx context.Context, model string) (Capabilities, error) {
	if err := ctx.Err(); err != nil {
		return Capabilities{}, classify(err)
	}
	caps, ok := r[model]
	if !ok {
		return Capabilities{}, &Error{Kind: ErrUnsupported, Detail: "unregistered model"}
	}
	return caps, nil
}

func cloneRegistry(r Registry) Registry {
	out := Registry{}
	for id, caps := range r {
		out[id] = caps
	}
	return out
}

type Message struct {
	Role       string     `json:"role"`
	Text       string     `json:"text,omitempty"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
	IsError    bool       `json:"is_error,omitempty"`
}

type Tool struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Schema      json.RawMessage `json:"schema"`
}

type ToolCall struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

// NativeState is deliberately opaque outside the owning adapter. Raw preserves
// the native response, including fields this runtime does not understand. The
// caller stores it as a protected artifact, not in user-visible event payloads.
// When supplied to Stream, Messages must contain only new messages since this
// state; otherwise use the complete canonical history with NativeState=nil.
type NativeState struct {
	Provider   string          `json:"provider"`
	ResponseID string          `json:"response_id"`
	Raw        json.RawMessage `json:"raw"`
}

type ModelRequest struct {
	RunID           string       `json:"run_id"`
	StepID          string       `json:"step_id"`
	AttemptID       string       `json:"attempt_id"`
	ModelID         string       `json:"model_id"`
	Messages        []Message    `json:"messages"`
	NativeState     *NativeState `json:"native_state,omitempty"`
	Tools           []Tool       `json:"tools,omitempty"`
	MaxOutputTokens int64        `json:"max_output_tokens"`
	Deadline        time.Time    `json:"deadline"`
}

// TokenCount distinguishes a measured zero from missing billing information.
type TokenCount struct {
	Value int64 `json:"value"`
	Known bool  `json:"known"`
}
type Usage struct {
	// Final is true only after a complete, validated provider response. Observed
	// counters in failed attempts are lower-bound evidence, never final billing.
	Final      bool       `json:"final"`
	Input      TokenCount `json:"input"`
	Output     TokenCount `json:"output"`
	CacheRead  TokenCount `json:"cache_read"`
	CacheWrite TokenCount `json:"cache_write"`
}

type ModelTurn struct {
	RunID             string       `json:"run_id"`
	StepID            string       `json:"step_id"`
	AttemptID         string       `json:"attempt_id"`
	Text              string       `json:"text"`
	ToolCalls         []ToolCall   `json:"tool_calls,omitempty"`
	Usage             Usage        `json:"usage"`
	FinishReason      string       `json:"finish_reason"`
	NativeState       *NativeState `json:"native_state,omitempty"`
	ProviderRequestID string       `json:"provider_request_id,omitempty"`
}

type EventType string

const (
	EventAttemptStarted   EventType = "model.attempt_started"
	EventTextDelta        EventType = "model.text_delta"
	EventToolDelta        EventType = "model.tool_arguments_delta"
	EventUsage            EventType = "model.usage"
	EventAttemptCompleted EventType = "model.attempt_completed"
	EventAttemptFailed    EventType = "model.attempt_failed"
)

// ModelEvent never includes an executable ToolCall. Deltas are provisional and
// scoped to AttemptID; consumers must discard/mark them on attempt_failed.
type ModelEvent struct {
	Type        EventType `json:"type"`
	RunID       string    `json:"run_id"`
	StepID      string    `json:"step_id"`
	AttemptID   string    `json:"attempt_id"`
	Sequence    int64     `json:"sequence"`
	Provisional bool      `json:"provisional"`
	Delta       string    `json:"delta,omitempty"`
	CallID      string    `json:"call_id,omitempty"`
	ToolName    string    `json:"tool_name,omitempty"`
	Usage       *Usage    `json:"usage,omitempty"`
	ErrorKind   ErrorKind `json:"error_kind,omitempty"`
}

type Limits struct {
	MaxCalls         int
	MaxArgumentBytes int
	MaxJSONDepth     int
	MaxTextBytes     int
	MaxNativeBytes   int
}

func (l Limits) normalized() Limits {
	if l.MaxCalls <= 0 {
		l.MaxCalls = 32
	}
	if l.MaxArgumentBytes <= 0 {
		l.MaxArgumentBytes = 64 << 10
	}
	if l.MaxJSONDepth <= 0 {
		l.MaxJSONDepth = 32
	}
	if l.MaxTextBytes <= 0 {
		l.MaxTextBytes = 1 << 20
	}
	if l.MaxNativeBytes <= 0 {
		l.MaxNativeBytes = 4 << 20
	}
	return l
}

func validateRequest(req ModelRequest, caps Capabilities, providerName string) error {
	if req.RunID == "" || req.StepID == "" || req.AttemptID == "" || req.ModelID == "" {
		return &Error{Kind: ErrInvalidRequest, Detail: "run, step, attempt, and model IDs are required"}
	}
	if req.MaxOutputTokens <= 0 || (caps.MaxOutputTokens > 0 && req.MaxOutputTokens > caps.MaxOutputTokens) {
		return &Error{Kind: ErrInvalidRequest, Detail: "invalid output token limit"}
	}
	if len(req.Tools) > 0 && !caps.ToolCalling {
		return &Error{Kind: ErrUnsupported, Detail: "model does not support tool calling"}
	}
	if req.NativeState != nil && req.NativeState.Provider != providerName {
		return &Error{Kind: ErrUnsupported, Detail: "cross-provider native state is not portable"}
	}
	for _, m := range req.Messages {
		switch m.Role {
		case "system", "user", "assistant", "tool":
		default:
			return &Error{Kind: ErrInvalidRequest, Detail: fmt.Sprintf("unsupported message role %q", m.Role)}
		}
		if m.Role == "tool" && m.ToolCallID == "" {
			return &Error{Kind: ErrInvalidRequest, Detail: "tool result needs a call ID"}
		}
		if len(m.ToolCalls) > 0 && m.Role != "assistant" {
			return &Error{Kind: ErrInvalidRequest, Detail: "only assistant messages can contain tool calls"}
		}
	}
	return nil
}
