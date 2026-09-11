package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

type noRemoteSchemas struct{}

func (noRemoteSchemas) Load(url string) (any, error) {
	return nil, fmt.Errorf("remote schema loading disabled")
}

type pendingCall struct {
	name string
	args strings.Builder
	done bool
}
type assembly struct {
	ctx         context.Context
	req         ModelRequest
	limits      Limits
	emit        func(ModelEvent) error
	seq         int64
	calls       map[string]*pendingCall
	order       []string
	schemas     map[string]*jsonschema.Schema
	text        strings.Builder
	usage       Usage
	nativeBytes int
}

func newAssembly(ctx context.Context, req ModelRequest, limits Limits, emit func(ModelEvent) error) (*assembly, error) {
	a := &assembly{ctx: ctx, req: req, limits: limits.normalized(), emit: emit, calls: map[string]*pendingCall{}, schemas: map[string]*jsonschema.Schema{}}
	for _, tool := range req.Tools {
		if tool.Name == "" || len(tool.Name) > 128 || a.schemas[tool.Name] != nil {
			return nil, &Error{Kind: ErrInvalidRequest, Detail: "invalid or duplicate tool name"}
		}
		if len(tool.Schema) > a.limits.MaxArgumentBytes {
			return nil, &Error{Kind: ErrLimit, Detail: "tool schema too large"}
		}
		value, err := decodeObject(tool.Schema, 64)
		if err != nil {
			return nil, &Error{Kind: ErrInvalidRequest, Detail: "invalid tool schema", Cause: err}
		}
		compiler := jsonschema.NewCompiler()
		compiler.UseLoader(noRemoteSchemas{})
		const resource = "https://forge.invalid/tool.json"
		if err = compiler.AddResource(resource, value); err != nil {
			return nil, &Error{Kind: ErrInvalidRequest, Detail: "invalid tool schema", Cause: err}
		}
		schema, err := compiler.Compile(resource)
		if err != nil {
			return nil, &Error{Kind: ErrInvalidRequest, Detail: "tool schema compilation failed", Cause: err}
		}
		a.schemas[tool.Name] = schema
	}
	return a, nil
}

func (a *assembly) event(kind EventType, delta, callID, name string, usage *Usage, errorKind ErrorKind) error {
	if kind != EventAttemptFailed {
		if err := a.ctx.Err(); err != nil {
			return classify(err)
		}
	}
	a.seq++
	if a.emit == nil {
		return nil
	}
	if err := a.emit(ModelEvent{Type: kind, RunID: a.req.RunID, StepID: a.req.StepID, AttemptID: a.req.AttemptID, Sequence: a.seq, Provisional: kind == EventTextDelta || kind == EventToolDelta, Delta: delta, CallID: callID, ToolName: name, Usage: usage, ErrorKind: errorKind}); err != nil {
		return &Error{Kind: ErrConsumer, Detail: "event consumer rejected output", Cause: err}
	}
	return nil
}

func (a *assembly) textDelta(delta string) error {
	if len(delta) > a.limits.MaxTextBytes-a.text.Len() {
		return &Error{Kind: ErrLimit, Detail: "visible text too large"}
	}
	a.text.WriteString(delta)
	return a.event(EventTextDelta, delta, "", "", nil, "")
}
func (a *assembly) begin(id, name string) error {
	if id == "" || len(id) > 256 || name == "" {
		return &Error{Kind: ErrProtocol, Detail: "missing tool identity"}
	}
	if _, ok := a.calls[id]; ok {
		return &Error{Kind: ErrProtocol, Detail: "duplicate tool call ID"}
	}
	if a.schemas[name] == nil {
		return &Error{Kind: ErrProtocol, Detail: "model called an unregistered tool"}
	}
	if len(a.calls) >= a.limits.MaxCalls {
		return &Error{Kind: ErrLimit, Detail: "too many tool calls"}
	}
	a.calls[id] = &pendingCall{name: name}
	a.order = append(a.order, id)
	return nil
}
func (a *assembly) arguments(id, delta string) error {
	call := a.calls[id]
	if call == nil || call.done {
		return &Error{Kind: ErrProtocol, Detail: "arguments outside an open tool call"}
	}
	if len(delta) > a.limits.MaxArgumentBytes-call.args.Len() {
		return &Error{Kind: ErrLimit, Detail: "tool arguments too large"}
	}
	call.args.WriteString(delta)
	return a.event(EventToolDelta, delta, id, call.name, nil, "")
}
func (a *assembly) end(id string) error {
	call := a.calls[id]
	if call == nil || call.done {
		return &Error{Kind: ErrProtocol, Detail: "duplicate or unknown tool completion"}
	}
	value, err := decodeObject([]byte(call.args.String()), a.limits.MaxJSONDepth)
	if err != nil {
		return &Error{Kind: ErrProtocol, Detail: "invalid tool argument JSON", Cause: err}
	}
	if err := a.schemas[call.name].Validate(value); err != nil {
		return &Error{Kind: ErrProtocol, Detail: "tool arguments fail schema", Cause: err}
	}
	call.done = true
	return nil
}

// confirm requires exact arguments from the final provider record; mismatched
// snapshots cannot quietly override the stream observed by the application.
func (a *assembly) confirm(id, name, args string) error {
	call := a.calls[id]
	if call == nil || call.name != name || call.args.String() != args {
		return &Error{Kind: ErrProtocol, Detail: "final tool call differs from streamed call"}
	}
	return nil
}
func (a *assembly) recordNative(raw string) error {
	if len(raw) > a.limits.MaxNativeBytes-a.nativeBytes {
		return &Error{Kind: ErrLimit, Detail: "native response too large"}
	}
	a.nativeBytes += len(raw)
	return nil
}
func (a *assembly) finish(reason string, state *NativeState, requestID string) (ModelTurn, error) {
	for _, count := range []TokenCount{a.usage.Input, a.usage.Output, a.usage.CacheRead, a.usage.CacheWrite} {
		if count.Known && count.Value < 0 {
			return ModelTurn{}, &Error{Kind: ErrProtocol, Detail: "negative provider usage"}
		}
	}
	a.usage.Final = true
	turn := ModelTurn{RunID: a.req.RunID, StepID: a.req.StepID, AttemptID: a.req.AttemptID, Text: a.text.String(), Usage: a.usage, FinishReason: reason, NativeState: state, ProviderRequestID: requestID}
	for _, id := range a.order {
		call := a.calls[id]
		if !call.done {
			return ModelTurn{}, &Error{Kind: ErrInterrupted, Detail: "tool call was not completed"}
		}
		turn.ToolCalls = append(turn.ToolCalls, ToolCall{ID: id, Name: call.name, Arguments: json.RawMessage(call.args.String())})
	}
	if err := a.event(EventAttemptCompleted, "", "", "", &turn.Usage, ""); err != nil {
		return ModelTurn{}, err
	}
	return turn, nil
}
func (a *assembly) fail(err error) (ModelTurn, error) {
	p := classify(err)
	a.usage.Final = false
	for _, count := range []*TokenCount{&a.usage.Input, &a.usage.Output, &a.usage.CacheRead, &a.usage.CacheWrite} {
		if count.Value < 0 {
			*count = TokenCount{}
		}
	}
	_ = a.event(EventAttemptFailed, "", "", "", &a.usage, p.Kind)
	// Partial text and calls remain provisional only in events. Never return an
	// executable partial turn, even when the server sent a tool completion event.
	return ModelTurn{RunID: a.req.RunID, StepID: a.req.StepID, AttemptID: a.req.AttemptID, Usage: a.usage}, p
}

// decodeObject rejects duplicate keys and excessive nesting before schema
// validation. json.Unmarshal alone silently accepts ambiguous duplicate keys.
func decodeObject(raw []byte, maxDepth int) (any, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var read func(int) (any, error)
	read = func(depth int) (any, error) {
		if depth > maxDepth {
			return nil, fmt.Errorf("JSON nesting limit exceeded")
		}
		tok, err := d.Token()
		if err != nil {
			return nil, err
		}
		delim, ok := tok.(json.Delim)
		if !ok {
			return tok, nil
		}
		switch delim {
		case '{':
			m := map[string]any{}
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return nil, err
				}
				k, ok := key.(string)
				if !ok {
					return nil, fmt.Errorf("invalid key")
				}
				if _, exists := m[k]; exists {
					return nil, fmt.Errorf("duplicate key")
				}
				v, err := read(depth + 1)
				if err != nil {
					return nil, err
				}
				m[k] = v
			}
			close, err := d.Token()
			if err != nil || close != json.Delim('}') {
				return nil, fmt.Errorf("invalid object")
			}
			return m, nil
		case '[':
			values := []any{}
			for d.More() {
				v, err := read(depth + 1)
				if err != nil {
					return nil, err
				}
				values = append(values, v)
			}
			close, err := d.Token()
			if err != nil || close != json.Delim(']') {
				return nil, fmt.Errorf("invalid array")
			}
			return values, nil
		default:
			return nil, fmt.Errorf("unexpected delimiter")
		}
	}
	value, err := read(1)
	if err != nil {
		return nil, err
	}
	if _, ok := value.(map[string]any); !ok {
		return nil, fmt.Errorf("root must be object")
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, fmt.Errorf("trailing JSON")
	}
	return value, nil
}
