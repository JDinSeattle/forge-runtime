package provider

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	anthropic "github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/anthropics/anthropic-sdk-go/packages/param"
)

type Anthropic struct {
	client   anthropic.Client
	registry Registry
	limits   Limits
}

func NewAnthropic(registry Registry, limits Limits, options ...option.RequestOption) *Anthropic {
	options = append(options, option.WithMaxRetries(0), option.WithMiddleware(boundResponse(limits.normalized().MaxNativeBytes)))
	return &Anthropic{client: anthropic.NewClient(options...), registry: cloneRegistry(registry), limits: limits.normalized()}
}
func (p *Anthropic) Capabilities(ctx context.Context, model string) (Capabilities, error) {
	return p.registry.Lookup(ctx, model)
}

type anthropicState struct {
	Messages []json.RawMessage `json:"messages"`
	System   []json.RawMessage `json:"system,omitempty"`
	Response json.RawMessage   `json:"response"`
	Events   []json.RawMessage `json:"events"`
}
type anthropicUsage struct {
	Input      *int64 `json:"input_tokens"`
	Output     *int64 `json:"output_tokens"`
	CacheRead  *int64 `json:"cache_read_input_tokens"`
	CacheWrite *int64 `json:"cache_creation_input_tokens"`
}
type anthropicWire struct {
	Type         string                     `json:"type"`
	Index        int                        `json:"index"`
	Message      json.RawMessage            `json:"message"`
	ContentBlock map[string]json.RawMessage `json:"content_block"`
	Delta        struct {
		Type        string          `json:"type"`
		Text        string          `json:"text"`
		PartialJSON string          `json:"partial_json"`
		Thinking    string          `json:"thinking"`
		Signature   string          `json:"signature"`
		StopReason  string          `json:"stop_reason"`
		Citation    json.RawMessage `json:"citation"`
	} `json:"delta"`
	Usage anthropicUsage `json:"usage"`
}
type anthropicBlock struct {
	raw              map[string]json.RawMessage
	kind, id         string
	closed, hasDelta bool
}

func (p *Anthropic) Stream(ctx context.Context, req ModelRequest, emit func(ModelEvent) error) (ModelTurn, error) {
	caps, err := p.Capabilities(ctx, req.ModelID)
	if err != nil {
		return ModelTurn{}, err
	}
	if err = validateRequest(req, caps, "anthropic"); err != nil {
		return ModelTurn{}, err
	}
	if !req.Deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, req.Deadline)
		defer cancel()
	}
	a, err := newAssembly(ctx, req, p.limits, emit)
	if err != nil {
		return ModelTurn{}, err
	}
	params, state, err := p.params(req)
	if err != nil {
		return ModelTurn{}, err
	}
	if err = a.event(EventAttemptStarted, "", "", "", nil, ""); err != nil {
		return a.fail(err)
	}
	var response *http.Response
	stream := p.client.Messages.NewStreaming(ctx, params, option.WithMaxRetries(0), option.WithResponseInto(&response), option.WithHeader("X-Client-Request-Id", req.AttemptID))
	defer stream.Close()
	blocks := []anthropicBlock{}
	var message map[string]json.RawMessage
	var responseID, reason string
	completed, started := false, false
	for stream.Next() {
		raw := stream.Current().RawJSON()
		if err = a.recordNative(raw); err != nil {
			return a.fail(err)
		}
		state.Events = append(state.Events, json.RawMessage(raw))
		var e anthropicWire
		if err = json.Unmarshal([]byte(raw), &e); err != nil {
			return a.fail(&Error{Kind: ErrProtocol, Detail: "invalid Anthropic stream event"})
		}
		if completed {
			return a.fail(&Error{Kind: ErrProtocol, Detail: "event after message_stop"})
		}
		if !started && e.Type != "message_start" && e.Type != "ping" {
			return a.fail(&Error{Kind: ErrProtocol, Detail: "message_start must precede content"})
		}
		switch e.Type {
		case "message_start":
			if started {
				return a.fail(&Error{Kind: ErrProtocol, Detail: "duplicate message_start"})
			}
			started = true
			if err = json.Unmarshal(e.Message, &message); err != nil {
				return a.fail(&Error{Kind: ErrProtocol, Detail: "invalid message_start"})
			}
			responseID = rawString(message["id"])
			if responseID == "" {
				return a.fail(&Error{Kind: ErrProtocol, Detail: "missing native message ID"})
			}
			var usage anthropicUsage
			_ = json.Unmarshal(message["usage"], &usage)
			a.usage = Usage{Input: token(usage.Input), CacheRead: token(usage.CacheRead), CacheWrite: token(usage.CacheWrite)}
		case "content_block_start":
			if e.Index != len(blocks) {
				return a.fail(&Error{Kind: ErrProtocol, Detail: "nonsequential or duplicate content block"})
			}
			b := anthropicBlock{raw: e.ContentBlock, kind: rawString(e.ContentBlock["type"]), id: rawString(e.ContentBlock["id"])}
			switch b.kind {
			case "text":
				err = a.textDelta(rawString(b.raw["text"]))
			case "tool_use":
				err = a.begin(b.id, rawString(b.raw["name"]))
			case "thinking", "redacted_thinking":
			default:
				err = &Error{Kind: ErrUnsupported, Detail: "unsupported native content block"}
			}
			blocks = append(blocks, b)
		case "content_block_delta":
			if e.Index < 0 || e.Index >= len(blocks) || blocks[e.Index].closed {
				return a.fail(&Error{Kind: ErrProtocol, Detail: "delta outside an open content block"})
			}
			b := &blocks[e.Index]
			switch e.Delta.Type {
			case "text_delta":
				if b.kind != "text" {
					err = &Error{Kind: ErrProtocol, Detail: "text delta in non-text block"}
					break
				}
				appendRawString(b.raw, "text", e.Delta.Text)
				err = a.textDelta(e.Delta.Text)
			case "input_json_delta":
				if b.kind != "tool_use" {
					err = &Error{Kind: ErrProtocol, Detail: "argument delta in non-tool block"}
					break
				}
				if !b.hasDelta && len(b.raw["input"]) > 0 && string(b.raw["input"]) != "{}" {
					err = &Error{Kind: ErrProtocol, Detail: "nonempty initial tool input with streamed deltas"}
					break
				}
				b.hasDelta = true
				err = a.arguments(b.id, e.Delta.PartialJSON)
			case "thinking_delta":
				if b.kind != "thinking" {
					err = &Error{Kind: ErrProtocol, Detail: "thinking delta in wrong block"}
					break
				}
				appendRawString(b.raw, "thinking", e.Delta.Thinking)
			case "signature_delta":
				if b.kind != "thinking" {
					err = &Error{Kind: ErrProtocol, Detail: "signature delta in wrong block"}
					break
				}
				appendRawString(b.raw, "signature", e.Delta.Signature)
			case "citations_delta":
				if b.kind != "text" {
					err = &Error{Kind: ErrProtocol, Detail: "citation delta in wrong block"}
					break
				}
				var citations []json.RawMessage
				_ = json.Unmarshal(b.raw["citations"], &citations)
				citations = append(citations, e.Delta.Citation)
				b.raw["citations"], err = json.Marshal(citations)
			default:
				err = &Error{Kind: ErrUnsupported, Detail: "unsupported native content delta"}
			}
		case "content_block_stop":
			if e.Index < 0 || e.Index >= len(blocks) || blocks[e.Index].closed {
				return a.fail(&Error{Kind: ErrProtocol, Detail: "unknown or duplicate content block stop"})
			}
			b := &blocks[e.Index]
			b.closed = true
			if b.kind == "tool_use" {
				if !b.hasDelta {
					err = a.arguments(b.id, string(b.raw["input"]))
				}
				if err == nil {
					err = a.end(b.id)
				}
				if err == nil {
					b.raw["input"] = json.RawMessage(a.calls[b.id].args.String())
				}
			}
		case "message_delta":
			reason = e.Delta.StopReason
			if e.Usage.Input != nil {
				a.usage.Input = token(e.Usage.Input)
			}
			if e.Usage.Output != nil {
				a.usage.Output = token(e.Usage.Output)
			}
			if e.Usage.CacheRead != nil {
				a.usage.CacheRead = token(e.Usage.CacheRead)
			}
			if e.Usage.CacheWrite != nil {
				a.usage.CacheWrite = token(e.Usage.CacheWrite)
			}
		case "message_stop":
			if reason != "end_turn" && reason != "tool_use" && reason != "stop_sequence" {
				err = &Error{Kind: ErrInterrupted, Detail: "message stopped without a complete turn"}
				break
			}
			if (reason == "tool_use") != (len(a.calls) > 0) {
				err = &Error{Kind: ErrProtocol, Detail: "stop reason disagrees with tool calls"}
				break
			}
			for _, b := range blocks {
				if !b.closed {
					err = &Error{Kind: ErrInterrupted, Detail: "content block did not close"}
					break
				}
			}
			completed = err == nil
		case "ping":
		default:
			err = &Error{Kind: ErrUnsupported, Detail: "unsupported native stream event"}
		}
		if err != nil {
			return a.fail(err)
		}
	}
	if err = stream.Err(); err != nil {
		return a.fail(anthropicError(err))
	}
	if !completed {
		return a.fail(&Error{Kind: ErrInterrupted, Detail: "missing message_stop event"})
	}
	content := make([]map[string]json.RawMessage, len(blocks))
	for i, b := range blocks {
		content[i] = b.raw
	}
	message["content"], _ = json.Marshal(content)
	message["stop_reason"], _ = json.Marshal(reason)
	state.Response, _ = json.Marshal(message)
	assistant, _ := json.Marshal(map[string]any{"role": "assistant", "content": content})
	state.Messages = append(state.Messages, assistant)
	raw, err := json.Marshal(state)
	if err != nil {
		return a.fail(err)
	}
	if len(raw) > p.limits.MaxNativeBytes {
		return a.fail(&Error{Kind: ErrLimit, Detail: "native continuation state too large"})
	}
	requestID := ""
	if response != nil {
		requestID = response.Header.Get("request-id")
	}
	turn, err := a.finish(reason, &NativeState{Provider: "anthropic", ResponseID: responseID, Raw: raw}, requestID)
	if err != nil {
		return a.fail(err)
	}
	return turn, nil
}

func (p *Anthropic) params(req ModelRequest) (anthropic.MessageNewParams, anthropicState, error) {
	params := anthropic.MessageNewParams{Model: anthropic.Model(req.ModelID), MaxTokens: req.MaxOutputTokens}
	state := anthropicState{}
	if req.NativeState != nil {
		if len(req.NativeState.Raw) > p.limits.MaxNativeBytes {
			return params, state, &Error{Kind: ErrLimit, Detail: "native input state too large"}
		}
		if json.Unmarshal(req.NativeState.Raw, &state) != nil {
			return params, state, &Error{Kind: ErrInvalidRequest, Detail: "invalid Anthropic native state"}
		}
		var message map[string]json.RawMessage
		if json.Unmarshal(state.Response, &message) != nil || rawString(message["id"]) == "" || rawString(message["id"]) != req.NativeState.ResponseID {
			return params, state, &Error{Kind: ErrInvalidRequest, Detail: "invalid native message identity"}
		}
		state.Events = nil
	}
	newSystem := []json.RawMessage{}
	for _, m := range req.Messages {
		if m.Role == "system" {
			raw, _ := json.Marshal(map[string]string{"type": "text", "text": m.Text})
			newSystem = append(newSystem, raw)
			continue
		}
		role := m.Role
		content := []any{}
		if role == "tool" {
			role = "user"
			content = append(content, map[string]any{"type": "tool_result", "tool_use_id": m.ToolCallID, "content": m.Text, "is_error": m.IsError})
		} else {
			if m.Text != "" {
				content = append(content, map[string]any{"type": "text", "text": m.Text})
			}
			for _, call := range m.ToolCalls {
				content = append(content, map[string]any{"type": "tool_use", "id": call.ID, "name": call.Name, "input": call.Arguments})
			}
		}
		raw, err := json.Marshal(map[string]any{"role": role, "content": content})
		if err != nil {
			return params, state, &Error{Kind: ErrInvalidRequest, Detail: "invalid canonical message", Cause: err}
		}
		state.Messages = append(state.Messages, raw)
	}
	if len(newSystem) > 0 {
		state.System = newSystem
	}
	for _, raw := range state.System {
		params.System = append(params.System, param.Override[anthropic.TextBlockParam](raw))
	}
	for _, raw := range state.Messages {
		params.Messages = append(params.Messages, param.Override[anthropic.MessageParam](raw))
	}
	for _, tool := range req.Tools {
		params.Tools = append(params.Tools, anthropic.ToolUnionParam{OfTool: &anthropic.ToolParam{Name: tool.Name, Description: anthropic.String(tool.Description), InputSchema: param.Override[anthropic.ToolInputSchemaParam](tool.Schema)}})
	}
	return params, state, nil
}

func rawString(raw json.RawMessage) string { var s string; _ = json.Unmarshal(raw, &s); return s }
func appendRawString(fields map[string]json.RawMessage, key, value string) {
	fields[key], _ = json.Marshal(rawString(fields[key]) + value)
}
func anthropicError(err error) *Error {
	var api *anthropic.Error
	if errors.As(err, &api) {
		headers := http.Header{}
		if api.Response != nil {
			headers = api.Response.Header
		}
		mapped := HTTPError(api.StatusCode, headers, time.Now())
		// Streaming failures arrive after HTTP 200; classify the native error type,
		// not the already successful handshake status.
		if api.StatusCode >= 200 && api.StatusCode < 300 {
			switch string(api.Type()) {
			case "overloaded_error", "api_error":
				mapped.Kind = ErrUnavailable
			case "rate_limit_error":
				mapped.Kind = ErrRateLimited
			case "authentication_error", "permission_error":
				mapped.Kind = ErrAuthentication
			default:
				mapped.Kind = ErrInterrupted
			}
			mapped.Detail = "provider terminated stream"
		}
		return mapped
	}
	return classify(err)
}
