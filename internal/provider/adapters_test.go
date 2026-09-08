package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	anthropicoption "github.com/anthropics/anthropic-sdk-go/option"
	openaioption "github.com/openai/openai-go/v3/option"
)

type wireEvent map[string]any

func writeEvents(t *testing.T, w http.ResponseWriter, events []wireEvent) {
	t.Helper()
	w.Header().Set("Content-Type", "text/event-stream")
	for _, e := range events {
		raw, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", e["type"], raw); err != nil {
			return
		}
		w.(http.Flusher).Flush()
	}
}

func openAIEvents() []wireEvent {
	call := func(id, name, args string) map[string]any {
		return map[string]any{"id": "item_" + id, "type": "function_call", "call_id": id, "name": name, "arguments": args, "status": "completed", "future_metadata": "retain-me"}
	}
	a, b := call("a", "read_file", `{"path":"a.go"}`), call("b", "read_file", `{"path":"b.go"}`)
	return []wireEvent{
		{"type": "response.created", "response": map[string]any{"id": "resp_1", "status": "in_progress"}},
		{"type": "response.output_text.delta", "delta": "Inspecting."},
		{"type": "response.output_item.added", "output_index": 0, "item": call("a", "read_file", "")},
		{"type": "response.output_item.added", "output_index": 1, "item": call("b", "read_file", "")},
		{"type": "response.function_call_arguments.delta", "item_id": "item_a", "delta": `{"path":"`},
		{"type": "response.function_call_arguments.delta", "item_id": "item_b", "delta": `{"path":"b.go"}`},
		{"type": "response.function_call_arguments.done", "item_id": "item_b", "arguments": b["arguments"], "name": "read_file"},
		{"type": "response.output_item.done", "output_index": 1, "item": b},
		{"type": "response.function_call_arguments.delta", "item_id": "item_a", "delta": `a.go"}`},
		{"type": "response.function_call_arguments.done", "item_id": "item_a", "arguments": a["arguments"], "name": "read_file"},
		{"type": "response.output_item.done", "output_index": 0, "item": a},
		{"type": "response.completed", "response": map[string]any{"id": "resp_1", "status": "completed", "output": []any{a, b, map[string]any{"id": "reason_1", "type": "reasoning", "encrypted_content": "opaque-encrypted", "summary": []any{}, "future_field": "untouched"}}, "usage": map[string]any{"input_tokens": 12, "output_tokens": 0, "input_tokens_details": map[string]any{"cached_tokens": 0}}, "future_response_field": "kept"}},
	}
}

func TestOpenAIResponsesContractAndNativeContinuation(t *testing.T) {
	var mu sync.Mutex
	var bodies []map[string]any
	var headers []http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/responses" {
			t.Errorf("path=%s", r.URL.Path)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		mu.Lock()
		bodies = append(bodies, body)
		headers = append(headers, r.Header.Clone())
		mu.Unlock()
		w.Header().Set("x-request-id", "provider_req_1")
		writeEvents(t, w, openAIEvents())
	}))
	defer server.Close()
	p := NewOpenAI(testRegistry("test-model"), Limits{}, openaioption.WithBaseURL(server.URL), openaioption.WithAPIKey("test-key"))
	req := request("test-model")
	var events []ModelEvent
	turn, err := p.Stream(context.Background(), req, func(e ModelEvent) error { events = append(events, e); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if turn.ProviderRequestID != "provider_req_1" || len(turn.ToolCalls) != 2 || turn.Text != "Inspecting." {
		t.Fatalf("turn=%+v", turn)
	}
	if !turn.Usage.Output.Known || turn.Usage.Output.Value != 0 || turn.Usage.CacheWrite.Known {
		t.Fatalf("usage=%+v", turn.Usage)
	}
	if !strings.Contains(string(turn.NativeState.Raw), "future_response_field") {
		t.Fatal("raw response lost unknown native field")
	}
	req.NativeState = turn.NativeState
	req.AttemptID = "attempt_2"
	req.Messages = []Message{{Role: "tool", ToolCallID: "a", Text: "file a"}, {Role: "tool", ToolCallID: "b", Text: "file b"}}
	if _, err = p.Stream(context.Background(), req, nil); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 2 || bodies[0]["stream"] != true || bodies[0]["store"] != false {
		t.Fatalf("bodies=%+v", bodies)
	}
	if headers[0].Get("X-Client-Request-Id") != "attempt_1" || headers[1].Get("X-Client-Request-Id") != "attempt_2" {
		t.Fatal("attempt correlation missing")
	}
	input := bodies[1]["input"].([]any)
	if len(input) != 6 {
		t.Fatalf("native continuation input=%+v", input)
	}
	if input[3].(map[string]any)["encrypted_content"] != "opaque-encrypted" || input[3].(map[string]any)["future_field"] != "untouched" {
		t.Fatal("native reasoning was reconstructed or dropped")
	}
	if input[4].(map[string]any)["call_id"] != "a" || input[4].(map[string]any)["type"] != "function_call_output" {
		t.Fatal("tool output correlation missing")
	}
	tool := bodies[0]["tools"].([]any)[0].(map[string]any)
	if tool["parameters"].(map[string]any)["additionalProperties"] != false {
		t.Fatal("schema changed on wire")
	}
	if events[len(events)-1].Type != EventAttemptCompleted {
		t.Fatal("no completed event")
	}
}

func anthropicEvents() []wireEvent {
	return []wireEvent{
		{"type": "message_start", "message": map[string]any{"id": "msg_1", "type": "message", "role": "assistant", "model": "test-model", "content": []any{}, "usage": map[string]any{"input_tokens": 12, "output_tokens": 1, "cache_read_input_tokens": 0}}},
		{"type": "content_block_start", "index": 0, "content_block": map[string]any{"type": "thinking", "thinking": "", "signature": "", "future_field": "keep"}},
		{"type": "content_block_delta", "index": 0, "delta": map[string]any{"type": "thinking_delta", "thinking": "private reasoning"}},
		{"type": "content_block_delta", "index": 0, "delta": map[string]any{"type": "signature_delta", "signature": "opaque-signature"}},
		{"type": "content_block_stop", "index": 0},
		{"type": "content_block_start", "index": 1, "content_block": map[string]any{"type": "tool_use", "id": "a", "name": "read_file", "input": map[string]any{}}},
		{"type": "content_block_start", "index": 2, "content_block": map[string]any{"type": "tool_use", "id": "b", "name": "read_file", "input": map[string]any{}}},
		{"type": "content_block_delta", "index": 1, "delta": map[string]any{"type": "input_json_delta", "partial_json": `{"path":"`}},
		{"type": "content_block_delta", "index": 2, "delta": map[string]any{"type": "input_json_delta", "partial_json": `{"path":"b.go"}`}},
		{"type": "content_block_stop", "index": 2},
		{"type": "content_block_delta", "index": 1, "delta": map[string]any{"type": "input_json_delta", "partial_json": `a.go"}`}},
		{"type": "content_block_stop", "index": 1},
		{"type": "content_block_start", "index": 3, "content_block": map[string]any{"type": "text", "text": ""}},
		{"type": "content_block_delta", "index": 3, "delta": map[string]any{"type": "text_delta", "text": "Inspecting."}},
		{"type": "content_block_stop", "index": 3},
		{"type": "message_delta", "delta": map[string]any{"stop_reason": "tool_use", "stop_sequence": nil}, "usage": map[string]any{"output_tokens": 19}},
		{"type": "message_stop"},
	}
}

func TestAnthropicMessagesContractAndNativeContinuation(t *testing.T) {
	var mu sync.Mutex
	var bodies []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/messages" {
			t.Errorf("path=%s", r.URL.Path)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		mu.Lock()
		bodies = append(bodies, body)
		mu.Unlock()
		w.Header().Set("request-id", "anthropic_req_1")
		writeEvents(t, w, anthropicEvents())
	}))
	defer server.Close()
	p := NewAnthropic(testRegistry("test-model"), Limits{}, anthropicoption.WithBaseURL(server.URL), anthropicoption.WithAPIKey("test-key"))
	req := request("test-model")
	req.Messages = append([]Message{{Role: "system", Text: "Preserve tests."}}, req.Messages...)
	turn, err := p.Stream(context.Background(), req, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(turn.ToolCalls) != 2 || turn.Text != "Inspecting." || turn.ProviderRequestID != "anthropic_req_1" {
		t.Fatalf("turn=%+v", turn)
	}
	if !turn.Usage.Input.Known || turn.Usage.Input.Value != 12 || !turn.Usage.Output.Known || turn.Usage.Output.Value != 19 || turn.Usage.CacheWrite.Known {
		t.Fatalf("usage=%+v", turn.Usage)
	}
	req.NativeState = turn.NativeState
	req.AttemptID = "attempt_2"
	req.Messages = []Message{{Role: "tool", ToolCallID: "a", Text: "file a"}, {Role: "tool", ToolCallID: "b", Text: "file b", IsError: true}}
	if _, err = p.Stream(context.Background(), req, nil); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 2 {
		t.Fatal(len(bodies))
	}
	messages := bodies[1]["messages"].([]any)
	if len(messages) != 4 {
		t.Fatalf("history=%+v", messages)
	}
	thinking := messages[1].(map[string]any)["content"].([]any)[0].(map[string]any)
	if thinking["signature"] != "opaque-signature" || thinking["future_field"] != "keep" {
		t.Fatalf("native block metadata lost: %+v", thinking)
	}
	result := messages[3].(map[string]any)["content"].([]any)[0].(map[string]any)
	if result["tool_use_id"] != "b" || result["is_error"] != true {
		t.Fatalf("tool result=%+v", result)
	}
	if bodies[1]["system"].([]any)[0].(map[string]any)["text"] != "Preserve tests." {
		t.Fatal("system instructions lost on continuation")
	}
	schema := bodies[0]["tools"].([]any)[0].(map[string]any)["input_schema"].(map[string]any)
	if schema["additionalProperties"] != false {
		t.Fatal("schema constraint lost")
	}
}

func TestAdaptersAbortAfterToolCompletionWithoutTerminalEvent(t *testing.T) {
	for _, name := range []string{"openai", "anthropic"} {
		t.Run(name, func(t *testing.T) {
			events := openAIEvents()
			if name == "anthropic" {
				events = anthropicEvents()
			}
			events = events[:len(events)-1]
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { writeEvents(t, w, events) }))
			defer server.Close()
			var p Provider
			if name == "openai" {
				p = NewOpenAI(testRegistry("test-model"), Limits{}, openaioption.WithBaseURL(server.URL), openaioption.WithAPIKey("test"))
			} else {
				p = NewAnthropic(testRegistry("test-model"), Limits{}, anthropicoption.WithBaseURL(server.URL), anthropicoption.WithAPIKey("test"))
			}
			var got []ModelEvent
			turn, err := p.Stream(context.Background(), request("test-model"), func(e ModelEvent) error { got = append(got, e); return nil })
			errKind(t, err, ErrInterrupted)
			if len(turn.ToolCalls) > 0 || turn.NativeState != nil {
				t.Fatal("incomplete attempt exposed executable calls or continuation")
			}
			if got[len(got)-1].Type != EventAttemptFailed {
				t.Fatal("no attempt_failed")
			}
		})
	}
}

func TestAdaptersDisableSDKRetriesAndRespectHTTPErrorMetadata(t *testing.T) {
	for _, name := range []string{"openai", "anthropic"} {
		t.Run(name, func(t *testing.T) {
			var count atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", "13")
				w.Header().Set("x-request-id", "safe-id")
				w.Header().Set("request-id", "safe-id")
				w.WriteHeader(http.StatusTooManyRequests)
				fmt.Fprint(w, `{"type":"error","error":{"type":"rate_limit_error","message":"secret remote body"}}`)
			}))
			defer server.Close()
			var p Provider
			if name == "openai" {
				p = NewOpenAI(testRegistry("test-model"), Limits{}, openaioption.WithBaseURL(server.URL), openaioption.WithAPIKey("test"), openaioption.WithMaxRetries(3))
			} else {
				p = NewAnthropic(testRegistry("test-model"), Limits{}, anthropicoption.WithBaseURL(server.URL), anthropicoption.WithAPIKey("test"), anthropicoption.WithMaxRetries(3))
			}
			_, err := p.Stream(context.Background(), request("test-model"), nil)
			errKind(t, err, ErrRateLimited)
			e := err.(*Error)
			if count.Load() != 1 || e.RetryAfter != 13*time.Second || e.RequestID != "safe-id" {
				t.Fatalf("count=%d error=%+v", count.Load(), e)
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatal("remote body leaked")
			}
		})
	}
}

func TestOpenAIFinalToolMismatchRejected(t *testing.T) {
	events := openAIEvents()
	events[len(events)-1]["response"].(map[string]any)["output"].([]any)[0].(map[string]any)["name"] = "run_command"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { writeEvents(t, w, events) }))
	defer server.Close()
	p := NewOpenAI(testRegistry("test-model"), Limits{}, openaioption.WithBaseURL(server.URL), openaioption.WithAPIKey("test"))
	turn, err := p.Stream(context.Background(), request("test-model"), nil)
	errKind(t, err, ErrProtocol)
	if len(turn.ToolCalls) > 0 {
		t.Fatal("mismatched call escaped")
	}
}

func TestAnthropicOverloadInsideSuccessfulHTTPStream(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeEvents(t, w, []wireEvent{{"type": "error", "error": map[string]any{"type": "overloaded_error", "message": "private server details"}}})
	}))
	defer server.Close()
	p := NewAnthropic(testRegistry("test-model"), Limits{}, anthropicoption.WithBaseURL(server.URL), anthropicoption.WithAPIKey("test"))
	_, err := p.Stream(context.Background(), request("test-model"), nil)
	errKind(t, err, ErrUnavailable)
	if strings.Contains(err.Error(), "private") {
		t.Fatal("native error leaked")
	}
}

func TestAdaptersBoundToolArgumentsAtWireBoundary(t *testing.T) {
	for _, name := range []string{"openai", "anthropic"} {
		t.Run(name, func(t *testing.T) {
			events := openAIEvents()
			if name == "openai" {
				events[4]["delta"] = strings.Repeat("x", 1025)
			} else {
				events = anthropicEvents()
				events[7]["delta"].(map[string]any)["partial_json"] = strings.Repeat("x", 1025)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { writeEvents(t, w, events) }))
			defer server.Close()
			var p Provider
			if name == "openai" {
				p = NewOpenAI(testRegistry("test-model"), Limits{MaxArgumentBytes: 1024}, openaioption.WithBaseURL(server.URL), openaioption.WithAPIKey("test"))
			} else {
				p = NewAnthropic(testRegistry("test-model"), Limits{MaxArgumentBytes: 1024}, anthropicoption.WithBaseURL(server.URL), anthropicoption.WithAPIKey("test"))
			}
			turn, err := p.Stream(context.Background(), request("test-model"), nil)
			errKind(t, err, ErrLimit)
			if len(turn.ToolCalls) > 0 {
				t.Fatal("oversized tool escaped")
			}
		})
	}
}
