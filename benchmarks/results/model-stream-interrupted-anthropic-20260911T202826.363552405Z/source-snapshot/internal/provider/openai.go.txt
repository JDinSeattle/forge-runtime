package provider

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	"github.com/openai/openai-go/v3/packages/param"
	"github.com/openai/openai-go/v3/responses"
	"github.com/openai/openai-go/v3/shared"
)

type OpenAI struct {
	client   openai.Client
	registry Registry
	limits   Limits
}

func NewOpenAI(registry Registry, limits Limits, options ...option.RequestOption) *OpenAI {
	options = append(options, option.WithMaxRetries(0), option.WithMiddleware(boundResponse(limits.normalized().MaxNativeBytes)))
	return &OpenAI{client: openai.NewClient(options...), registry: cloneRegistry(registry), limits: limits.normalized()}
}
func (p *OpenAI) Capabilities(ctx context.Context, model string) (Capabilities, error) {
	return p.registry.Lookup(ctx, model)
}

type openAIState struct {
	Input    []json.RawMessage `json:"input"`
	Response json.RawMessage   `json:"response"`
}
type openAIOutput struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	CallID    string `json:"call_id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}
type openAIResponse struct {
	ID     string            `json:"id"`
	Status string            `json:"status"`
	Output []json.RawMessage `json:"output"`
	Usage  struct {
		Input   *int64 `json:"input_tokens"`
		Output  *int64 `json:"output_tokens"`
		Details struct {
			Cache *int64 `json:"cached_tokens"`
		} `json:"input_tokens_details"`
	} `json:"usage"`
}

func (p *OpenAI) Stream(ctx context.Context, req ModelRequest, emit func(ModelEvent) error) (ModelTurn, error) {
	caps, err := p.Capabilities(ctx, req.ModelID)
	if err != nil {
		return ModelTurn{}, err
	}
	if err = validateRequest(req, caps, "openai"); err != nil {
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
	params, input, err := p.params(req)
	if err != nil {
		return ModelTurn{}, err
	}
	if err = a.event(EventAttemptStarted, "", "", "", nil, ""); err != nil {
		return a.fail(err)
	}
	var response *http.Response
	stream := p.client.Responses.NewStreaming(ctx, params, option.WithMaxRetries(0), option.WithResponseInto(&response), option.WithHeader("X-Client-Request-Id", req.AttemptID))
	defer stream.Close()
	itemCalls := map[string]string{}
	var finalRaw json.RawMessage
	var responseID string
	completed := false
	for stream.Next() {
		e := stream.Current()
		if err = a.recordNative(e.RawJSON()); err != nil {
			return a.fail(err)
		}
		if completed {
			return a.fail(&Error{Kind: ErrProtocol, Detail: "event after response completion"})
		}
		switch e.Type {
		case "response.created":
			responseID = e.Response.ID
		case "response.output_text.delta":
			err = a.textDelta(e.Delta)
		case "response.output_item.added":
			if e.Item.Type == "function_call" {
				item := e.Item.AsFunctionCall()
				if _, exists := itemCalls[e.Item.ID]; exists {
					return a.fail(&Error{Kind: ErrProtocol, Detail: "duplicate output item"})
				}
				itemCalls[e.Item.ID] = e.Item.CallID
				err = a.begin(e.Item.CallID, e.Item.Name)
				if err == nil && item.Arguments != "" {
					err = a.arguments(e.Item.CallID, item.Arguments)
				}
			}
		case "response.function_call_arguments.delta":
			err = a.arguments(itemCalls[e.ItemID], e.Delta)
		case "response.function_call_arguments.done":
			id := itemCalls[e.ItemID]
			call := a.calls[id]
			if call == nil || (e.Name != "" && e.Name != call.name) {
				err = &Error{Kind: ErrProtocol, Detail: "tool name changed in argument completion"}
				break
			}
			err = a.confirm(id, call.name, e.Arguments)
			if err == nil {
				err = a.end(id)
			}
		case "response.output_item.done":
			if e.Item.Type == "function_call" {
				err = a.confirm(e.Item.CallID, e.Item.Name, e.Item.AsFunctionCall().Arguments)
			}
		case "response.completed":
			var final openAIResponse
			finalRaw = json.RawMessage(e.Response.RawJSON())
			if err = json.Unmarshal(finalRaw, &final); err != nil {
				err = &Error{Kind: ErrProtocol, Detail: "invalid completed response"}
				break
			}
			if final.ID == "" || (responseID != "" && responseID != final.ID) || final.Status != "completed" {
				err = &Error{Kind: ErrProtocol, Detail: "response identity or status mismatch"}
				break
			}
			responseID = final.ID
			count := 0
			seen := map[string]bool{}
			for _, raw := range final.Output {
				var output openAIOutput
				if err = json.Unmarshal(raw, &output); err != nil {
					break
				}
				switch output.Type {
				case "function_call":
					if seen[output.CallID] {
						err = &Error{Kind: ErrProtocol, Detail: "duplicate final tool call"}
						break
					}
					seen[output.CallID] = true
					count++
					err = a.confirm(output.CallID, output.Name, output.Arguments)
				case "message", "reasoning", "compaction":
				default:
					err = &Error{Kind: ErrUnsupported, Detail: "unsupported native output item"}
				}
				if err != nil {
					break
				}
			}
			if err == nil && count != len(a.calls) {
				err = &Error{Kind: ErrProtocol, Detail: "completed response omitted tool calls"}
			}
			a.usage = Usage{Input: token(final.Usage.Input), Output: token(final.Usage.Output), CacheRead: token(final.Usage.Details.Cache)}
			completed = err == nil
		case "response.failed", "response.incomplete", "error":
			err = &Error{Kind: ErrInterrupted, Detail: "provider did not complete response"}
		}
		if err != nil {
			return a.fail(err)
		}
	}
	if err = stream.Err(); err != nil {
		return a.fail(openAIError(err))
	}
	if !completed {
		return a.fail(&Error{Kind: ErrInterrupted, Detail: "missing response.completed event"})
	}
	raw, err := json.Marshal(openAIState{Input: input, Response: finalRaw})
	if err != nil {
		return a.fail(err)
	}
	if len(raw) > p.limits.MaxNativeBytes {
		return a.fail(&Error{Kind: ErrLimit, Detail: "native continuation state too large"})
	}
	requestID := ""
	if response != nil {
		requestID = response.Header.Get("x-request-id")
	}
	turn, err := a.finish("completed", &NativeState{Provider: "openai", ResponseID: responseID, Raw: raw}, requestID)
	if err != nil {
		return a.fail(err)
	}
	return turn, nil
}

func (p *OpenAI) params(req ModelRequest) (responses.ResponseNewParams, []json.RawMessage, error) {
	params := responses.ResponseNewParams{Model: shared.ResponsesModel(req.ModelID), MaxOutputTokens: openai.Int(req.MaxOutputTokens), Store: openai.Bool(false), Include: []responses.ResponseIncludable{"reasoning.encrypted_content"}}
	input := []json.RawMessage{}
	if req.NativeState != nil {
		if len(req.NativeState.Raw) > p.limits.MaxNativeBytes {
			return params, nil, &Error{Kind: ErrLimit, Detail: "native input state too large"}
		}
		var state openAIState
		var response openAIResponse
		if json.Unmarshal(req.NativeState.Raw, &state) != nil || json.Unmarshal(state.Response, &response) != nil || response.ID == "" || response.ID != req.NativeState.ResponseID {
			return params, nil, &Error{Kind: ErrInvalidRequest, Detail: "invalid OpenAI native state"}
		}
		input = append(input, state.Input...)
		input = append(input, response.Output...)
	}
	add := func(v any) { raw, _ := json.Marshal(v); input = append(input, raw) }
	for _, m := range req.Messages {
		if m.Role == "tool" {
			add(map[string]any{"type": "function_call_output", "call_id": m.ToolCallID, "output": m.Text})
			continue
		}
		if m.Text != "" {
			add(map[string]any{"role": m.Role, "content": m.Text})
		}
		for _, call := range m.ToolCalls {
			add(map[string]any{"type": "function_call", "call_id": call.ID, "name": call.Name, "arguments": string(call.Arguments)})
		}
	}
	for _, raw := range input {
		params.Input.OfInputItemList = append(params.Input.OfInputItemList, param.Override[responses.ResponseInputItemUnionParam](raw))
	}
	for _, tool := range req.Tools {
		var schema map[string]any
		if err := json.Unmarshal(tool.Schema, &schema); err != nil {
			return params, nil, &Error{Kind: ErrInvalidRequest, Detail: "invalid tool schema"}
		}
		params.Tools = append(params.Tools, responses.ToolUnionParam{OfFunction: &responses.FunctionToolParam{Name: tool.Name, Description: openai.String(tool.Description), Parameters: schema, Strict: openai.Bool(false)}})
	}
	return params, input, nil
}

func token(value *int64) TokenCount {
	if value == nil {
		return TokenCount{}
	}
	return TokenCount{Value: *value, Known: true}
}
func openAIError(err error) *Error {
	var api *openai.Error
	if errors.As(err, &api) {
		headers := http.Header{}
		if api.Response != nil {
			headers = api.Response.Header
		}
		return HTTPError(api.StatusCode, headers, time.Now())
	}
	return classify(err)
}
