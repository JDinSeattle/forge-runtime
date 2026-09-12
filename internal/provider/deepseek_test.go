package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	openaioption "github.com/openai/openai-go/v3/option"
)

// All DeepSeek HTTP fixtures use an in-memory RoundTripper: no listener, DNS,
// external request, credential lookup, paid model, or executable tool exists.
type deepSeekTransport func(*http.Request) (*http.Response, error)

func (f deepSeekTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func deepSeekEvents() []wireEvent {
	events := openAIEvents()
	for i := range events {
		events[i]["sequence_number"] = i
	}
	final := events[len(events)-1]["response"].(map[string]any)
	final["model"] = "deepseek-v4.1-flash-fixture"
	final["usage"] = map[string]any{"input_tokens": 12, "output_tokens": 8, "input_tokens_details": map[string]any{"cached_tokens": 4}, "output_tokens_details": map[string]any{"reasoning_tokens": 2}}
	output := final["output"].([]any)
	output[2] = map[string]any{"type": "reasoning", "id": "reason_1", "content": []any{map[string]any{"type": "reasoning_text", "text": "fixture-private-reasoning"}}, "future_field": "untouched"}
	final["output"] = append(output, map[string]any{"type": "message", "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": "Inspecting."}}})
	return events
}
func deepSeekBody(events []wireEvent) string {
	var wire strings.Builder
	for _, event := range events {
		raw, _ := json.Marshal(event)
		fmt.Fprintf(&wire, "event: %s\ndata: %s\n\n", event["type"], raw)
	}
	return wire.String()
}
func deepSeekFixture(t *testing.T, limits Limits, rt deepSeekTransport) *DeepSeek {
	t.Helper()
	p, err := newDeepSeek(testRegistry("deepseek-v4-flash"), limits, "memory-only-credential-sentinel", &http.Client{Transport: rt})
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func deepSeekResponse(r *http.Request, body string) *http.Response {
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}, "X-Request-Id": []string{"deepseek-fixture-request"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}
}

func TestDeepSeekResponsesIdentityAndStatelessContinuation(t *testing.T) {
	// A provider-owned ResponseService must not inherit any OPENAI_* value.
	t.Setenv("OPENAI_BASE_URL", "https://wrong.invalid/")
	t.Setenv("OPENAI_API_KEY", "wrong-openai-credential")
	t.Setenv("OPENAI_CUSTOM_HEADERS", "Authorization: wrong-custom-auth\nX-Wrong: forbidden")
	t.Setenv("OPENAI_ORG_ID", "wrong-org")
	t.Setenv("OPENAI_PROJECT_ID", "wrong-project")
	var bodies []map[string]any
	p := deepSeekFixture(t, Limits{}, func(r *http.Request) (*http.Response, error) {
		if r.URL.String() != DeepSeekBaseURL+"responses" || r.Method != "POST" {
			t.Fatal("wrong native endpoint")
		}
		if r.Header.Get("Authorization") != "Bearer memory-only-credential-sentinel" || r.Header.Get("X-Wrong") != "" || r.Header.Get("OpenAI-Organization") != "" || r.Header.Get("OpenAI-Project") != "" {
			t.Fatal("cross-provider credential contamination")
		}
		if r.Header.Get("X-Client-Request-Id") != "attempt_1" {
			t.Fatal("attempt correlation missing")
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, body)
		return deepSeekResponse(r, deepSeekBody(deepSeekEvents())), nil
	})
	var events []ModelEvent
	req := request("deepseek-v4-flash")
	turn, err := p.Stream(context.Background(), req, func(e ModelEvent) error { events = append(events, e); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if turn.NativeState.Provider != "deepseek" || turn.ProviderModel != "deepseek-v4.1-flash-fixture" || turn.ProviderRequestID != "deepseek-fixture-request" || len(turn.ToolCalls) != 2 || turn.Text != "Inspecting." || !turn.Usage.Final || turn.Usage.Input.Value != 12 || turn.Usage.CacheRead.Value != 4 || turn.Usage.Output.Value != 8 {
		t.Fatalf("invalid final turn: %+v", turn)
	}
	for _, e := range events {
		if strings.Contains(e.Delta, "fixture-private-reasoning") {
			t.Fatal("private reasoning leaked to public events")
		}
	}
	if !strings.Contains(string(turn.NativeState.Raw), "fixture-private-reasoning") || !strings.Contains(string(turn.NativeState.Raw), "future_field") {
		t.Fatal("native opaque fields lost")
	}
	req.NativeState = turn.NativeState
	req.Messages = []Message{{Role: "tool", ToolCallID: "a", Text: "first result"}, {Role: "tool", ToolCallID: "b", Text: "second result"}}
	if _, err = p.Stream(context.Background(), req, nil); err != nil {
		t.Fatal(err)
	}
	for _, b := range bodies {
		if b["model"] != "deepseek-v4-flash" || b["stream"] != true || b["max_output_tokens"] != float64(1024) || b["reasoning"].(map[string]any)["effort"] != "none" {
			t.Fatal("unsupported request contract")
		}
		for _, key := range []string{"previous_response_id", "store", "include", "parallel_tool_calls", "metadata", "conversation"} {
			if _, ok := b[key]; ok {
				t.Fatalf("unsupported parameter %s", key)
			}
		}
		for _, tool := range b["tools"].([]any) {
			if tool.(map[string]any)["type"] != "function" {
				t.Fatal("native built-in tool escaped")
			}
		}
	}
	raw, _ := json.Marshal(bodies[1]["input"])
	if strings.Count(string(raw), "first result") != 1 || !strings.Contains(string(raw), "fixture-private-reasoning") || !strings.Contains(string(raw), "Inspect the failing test") {
		t.Fatal("stateless continuation lost or duplicated history")
	}
	req.NativeState.Provider = "openai"
	_, err = p.Stream(context.Background(), req, nil)
	errKind(t, err, ErrUnsupported)
	if len(bodies) != 2 {
		t.Fatal("cross-provider native state dispatched")
	}
}

func TestDeepSeekStreamRejectsPartialAndAmbiguousTurns(t *testing.T) {
	cases := []struct {
		name   string
		mutate func([]wireEvent) []wireEvent
		kind   ErrorKind
	}{
		{"eof_before_final", func(e []wireEvent) []wireEvent { return e[:len(e)-1] }, ErrInterrupted},
		{"missing_sequence", func(e []wireEvent) []wireEvent { delete(e[1], "sequence_number"); return e }, ErrProtocol},
		{"duplicate_sequence", func(e []wireEvent) []wireEvent { e[2]["sequence_number"] = 1; return e }, ErrProtocol},
		{"fractional_sequence", func(e []wireEvent) []wireEvent { e[2]["sequence_number"] = 1.5; return e }, ErrProtocol},
		{"missing_created_id", func(e []wireEvent) []wireEvent { delete(e[0]["response"].(map[string]any), "id"); return e }, ErrProtocol},
		{"missing_created", func(e []wireEvent) []wireEvent { return e[1:] }, ErrProtocol},
		{"duplicate_created", func(e []wireEvent) []wireEvent { e[1]["type"] = "response.created"; return e }, ErrProtocol},
		{"final_wrong_response", func(e []wireEvent) []wireEvent { e[len(e)-1]["response"].(map[string]any)["id"] = "other"; return e }, ErrProtocol},
		{"final_wrong_tool", func(e []wireEvent) []wireEvent {
			e[len(e)-1]["response"].(map[string]any)["output"].([]any)[0].(map[string]any)["arguments"] = `{"path":"evil.go"}`
			return e
		}, ErrProtocol},
		{"final_missing_model", func(e []wireEvent) []wireEvent { delete(e[len(e)-1]["response"].(map[string]any), "model"); return e }, ErrProtocol},
		{"final_wrong_text", func(e []wireEvent) []wireEvent { e[1]["delta"] = "different"; return e }, ErrProtocol},
		{"cache_larger_than_input", func(e []wireEvent) []wireEvent {
			e[len(e)-1]["response"].(map[string]any)["usage"].(map[string]any)["input_tokens"] = 1
			return e
		}, ErrProtocol},
		{"unsupported_custom", func(e []wireEvent) []wireEvent { e[2]["item"].(map[string]any)["type"] = "custom_tool_call"; return e }, ErrUnsupported},
		{"event_after_complete", func(e []wireEvent) []wireEvent {
			return append(e, wireEvent{"type": "response.in_progress", "sequence_number": 100})
		}, ErrProtocol},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := deepSeekFixture(t, Limits{}, func(r *http.Request) (*http.Response, error) {
				return deepSeekResponse(r, deepSeekBody(tc.mutate(deepSeekEvents()))), nil
			})
			var events []ModelEvent
			turn, err := p.Stream(context.Background(), request("deepseek-v4-flash"), func(e ModelEvent) error { events = append(events, e); return nil })
			errKind(t, err, tc.kind)
			if len(turn.ToolCalls) != 0 || turn.Text != "" || turn.NativeState != nil || turn.Usage.Final {
				t.Fatal("executable partial turn escaped")
			}
			if events[len(events)-1].Type != EventAttemptFailed {
				t.Fatal("failed boundary missing")
			}
		})
	}
}

func TestDeepSeekIncompleteUsageRemainsLowerBound(t *testing.T) {
	for _, kind := range []string{"response.incomplete", "response.failed"} {
		t.Run(kind, func(t *testing.T) {
			events := deepSeekEvents()
			last := events[len(events)-1]
			last["type"] = kind
			last["response"].(map[string]any)["status"] = strings.TrimPrefix(kind, "response.")
			p := deepSeekFixture(t, Limits{}, func(r *http.Request) (*http.Response, error) { return deepSeekResponse(r, deepSeekBody(events)), nil })
			turn, err := p.Stream(context.Background(), request("deepseek-v4-flash"), nil)
			errKind(t, err, ErrInterrupted)
			if turn.Usage.Final || !turn.Usage.Input.Known || turn.Usage.Input.Value != 12 || len(turn.ToolCalls) != 0 {
				t.Fatal("partial usage lost or promoted to final")
			}
		})
	}
	for _, missing := range []string{"usage", "cached_tokens"} {
		t.Run(missing, func(t *testing.T) {
			events := deepSeekEvents()
			final := events[len(events)-1]["response"].(map[string]any)
			if missing == "usage" {
				delete(final, "usage")
			} else {
				delete(final["usage"].(map[string]any)["input_tokens_details"].(map[string]any), "cached_tokens")
			}
			p := deepSeekFixture(t, Limits{}, func(r *http.Request) (*http.Response, error) { return deepSeekResponse(r, deepSeekBody(events)), nil })
			turn, err := p.Stream(context.Background(), request("deepseek-v4-flash"), nil)
			if err != nil || !turn.Usage.Final || turn.Usage.CacheRead.Known || (missing == "usage" && turn.Usage.Input.Known) {
				t.Fatalf("missing counters synthesized: %+v %v", turn.Usage, err)
			}
		})
	}
}

func TestDeepSeekNoHiddenRetriesDeadlineOrConsumerLeak(t *testing.T) {
	for _, status := range []int{401, 429, 500} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			calls := 0
			p := deepSeekFixture(t, Limits{}, func(r *http.Request) (*http.Response, error) {
				calls++
				return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}, "Retry-After": []string{"3"}}, Body: io.NopCloser(strings.NewReader(`{"error":{"message":"sensitive remote body"}}`)), Request: r}, nil
			})
			_, err := p.Stream(context.Background(), request("deepseek-v4-flash"), nil)
			if calls != 1 || err == nil || strings.Contains(err.Error(), "sensitive") {
				t.Fatalf("hidden retry or unsafe error: %d %v", calls, err)
			}
			var pe *Error
			if !errors.As(err, &pe) || pe.StatusCode != status {
				t.Fatal("status lost")
			}
		})
	}
	p := deepSeekFixture(t, Limits{}, func(r *http.Request) (*http.Response, error) { <-r.Context().Done(); return nil, r.Context().Err() })
	req := request("deepseek-v4-flash")
	req.Deadline = time.Now().Add(10 * time.Millisecond)
	_, err := p.Stream(context.Background(), req, nil)
	errKind(t, err, ErrDeadline)
	p = deepSeekFixture(t, Limits{}, func(r *http.Request) (*http.Response, error) {
		return deepSeekResponse(r, deepSeekBody(deepSeekEvents())), nil
	})
	turn, err := p.Stream(context.Background(), request("deepseek-v4-flash"), func(e ModelEvent) error {
		if e.Type == EventTextDelta {
			return errors.New("rejected")
		}
		return nil
	})
	errKind(t, err, ErrConsumer)
	if len(turn.ToolCalls) != 0 {
		t.Fatal("consumer failure released tools")
	}
	for _, limits := range []Limits{{MaxTextBytes: 2}, {MaxNativeBytes: 100}, {MaxArgumentBytes: 1}} {
		p = deepSeekFixture(t, limits, func(r *http.Request) (*http.Response, error) {
			return deepSeekResponse(r, deepSeekBody(deepSeekEvents())), nil
		})
		_, err = p.Stream(context.Background(), request("deepseek-v4-flash"), nil)
		errKind(t, err, ErrLimit)
	}
}

func TestResponsesSharedAssemblyPreservesOpenAIContract(t *testing.T) {
	var body map[string]any
	client := &http.Client{Transport: deepSeekTransport(func(r *http.Request) (*http.Response, error) {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		return deepSeekResponse(r, deepSeekBody(openAIEvents())), nil
	})}
	p := NewOpenAI(testRegistry("test-model"), Limits{}, openaioption.WithHTTPClient(client), openaioption.WithAPIKey("memory-only-openai-sentinel"))
	turn, err := p.Stream(context.Background(), request("test-model"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if turn.NativeState.Provider != "openai" || len(turn.ToolCalls) != 2 || body["store"] != false || body["include"].([]any)[0] != "reasoning.encrypted_content" || body["reasoning"] != nil || turn.ProviderModel != "" {
		t.Fatal("OpenAI contract changed")
	}
}

func TestDeepSeekContradictoryFinalItemStatus(t *testing.T) {
	for _, index := range []int{0, 3} {
		for _, status := range []any{"in_progress", "incomplete", nil, 7, ""} {
			t.Run(fmt.Sprintf("item_%d_status_%v", index, status), func(t *testing.T) {
				events := deepSeekEvents()
				events[len(events)-1]["response"].(map[string]any)["output"].([]any)[index].(map[string]any)["status"] = status
				p := deepSeekFixture(t, Limits{}, func(r *http.Request) (*http.Response, error) { return deepSeekResponse(r, deepSeekBody(events)), nil })
				turn, err := p.Stream(context.Background(), request("deepseek-v4-flash"), nil)
				errKind(t, err, ErrProtocol)
				if len(turn.ToolCalls) != 0 || turn.Usage.Final || turn.NativeState != nil {
					t.Fatal("contradictory final item released tools")
				}
			})
		}
	}
}
