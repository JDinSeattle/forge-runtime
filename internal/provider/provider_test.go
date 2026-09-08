package provider

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

func request(model string) ModelRequest {
	return ModelRequest{RunID: "run_1", StepID: "step_1", AttemptID: "attempt_1", ModelID: model, MaxOutputTokens: 1024, Messages: []Message{{Role: "user", Text: "Inspect the failing test"}}, Tools: []Tool{{Name: "read_file", Description: "Read a repository file", Schema: json.RawMessage(`{"type":"object","properties":{"path":{"type":"string","minLength":1}},"required":["path"],"additionalProperties":false}`)}}}
}
func testRegistry(model string) Registry {
	return Registry{model: {ToolCalling: true, MaxOutputTokens: 8192}}
}
func errKind(t *testing.T, err error, want ErrorKind) {
	t.Helper()
	var p *Error
	if !errors.As(err, &p) || p.Kind != want {
		t.Fatalf("error=%v; want kind %s", err, want)
	}
}

func TestParallelAssemblyDoesNotReleasePartialCalls(t *testing.T) {
	req := request("fake")
	fake := NewFake(Script{Chunks: []Chunk{{Kind: "text", Delta: "Inspecting"}, {Kind: "tool_start", CallID: "a", Name: "read_file"}, {Kind: "tool_start", CallID: "b", Name: "read_file"}, {Kind: "tool_delta", CallID: "a", Delta: `{"path":"`}, {Kind: "tool_delta", CallID: "b", Delta: `{"path":"b.go"}`}, {Kind: "tool_end", CallID: "b"}, {Kind: "tool_delta", CallID: "a", Delta: `a.go"}`}, {Kind: "tool_end", CallID: "a"}}, Usage: Usage{Input: TokenCount{Value: 0, Known: true}}, FinishReason: "tool_use"})
	var events []ModelEvent
	turn, err := fake.Stream(context.Background(), req, func(e ModelEvent) error { events = append(events, e); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(turn.ToolCalls) != 2 || turn.ToolCalls[0].ID != "a" || string(turn.ToolCalls[1].Arguments) != `{"path":"b.go"}` {
		t.Fatalf("calls=%+v", turn.ToolCalls)
	}
	if !turn.Usage.Final || !turn.Usage.Input.Known || turn.Usage.Output.Known {
		t.Fatalf("unknown usage collapsed: %+v", turn.Usage)
	}
	for i, e := range events {
		if e.Sequence != int64(i+1) || e.AttemptID != req.AttemptID || e.RunID != req.RunID {
			t.Fatalf("event identity=%+v", e)
		}
		if (e.Type == EventTextDelta || e.Type == EventToolDelta) && !e.Provisional {
			t.Fatalf("nonprovisional delta %+v", e)
		}
	}
	if events[len(events)-1].Type != EventAttemptCompleted {
		t.Fatal("missing completion")
	}
}

func TestFailedAttemptWithCompletedToolReturnsNoExecutableCall(t *testing.T) {
	fake := NewFake(Script{Chunks: []Chunk{{Kind: "tool_start", CallID: "a", Name: "read_file"}, {Kind: "tool_delta", CallID: "a", Delta: `{"path":"x.go"}`}, {Kind: "tool_end", CallID: "a"}}, Failure: errors.New("connection reset")})
	var events []ModelEvent
	turn, err := fake.Stream(context.Background(), request("fake"), func(e ModelEvent) error { events = append(events, e); return nil })
	errKind(t, err, ErrInterrupted)
	if len(turn.ToolCalls) != 0 || turn.Text != "" || turn.Usage.Final {
		t.Fatalf("partial turn escaped: %+v", turn)
	}
	if events[len(events)-1].Type != EventAttemptFailed {
		t.Fatal("no failed-attempt marker")
	}
}

func TestArgumentBoundsAndSchema(t *testing.T) {
	cases := []struct {
		name, args string
		limits     Limits
		kind       ErrorKind
	}{
		{"syntax", `{"path":`, Limits{}, ErrProtocol},
		{"duplicate_key", `{"path":"a","path":"b"}`, Limits{}, ErrProtocol},
		{"required", `{}`, Limits{}, ErrProtocol},
		{"wrong_type", `{"path":123}`, Limits{}, ErrProtocol},
		{"additional", `{"path":"x","command":"rm"}`, Limits{}, ErrProtocol},
		{"depth", `{"path":{"a":{"b":{}}}}`, Limits{MaxJSONDepth: 3}, ErrProtocol},
		{"size", `{"path":"` + strings.Repeat("x", 200) + `"}`, Limits{MaxArgumentBytes: 180}, ErrLimit},
		{"trailing", `{"path":"x"}{}`, Limits{}, ErrProtocol},
		{"array", `["x"]`, Limits{}, ErrProtocol},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := NewFake(Script{Chunks: []Chunk{{Kind: "tool_start", CallID: "a", Name: "read_file"}, {Kind: "tool_delta", CallID: "a", Delta: tc.args}, {Kind: "tool_end", CallID: "a"}}, FinishReason: "tool_use"})
			fake.Limits = tc.limits
			turn, err := fake.Stream(context.Background(), request("fake"), nil)
			errKind(t, err, tc.kind)
			if len(turn.ToolCalls) != 0 {
				t.Fatal("unsafe arguments released")
			}
		})
	}
}

func TestCallLimitUnknownToolAndDuplicateID(t *testing.T) {
	for _, tc := range []struct {
		name   string
		chunks []Chunk
		limits Limits
		kind   ErrorKind
	}{
		{"limit", []Chunk{{Kind: "tool_start", CallID: "a", Name: "read_file"}, {Kind: "tool_start", CallID: "b", Name: "read_file"}}, Limits{MaxCalls: 1}, ErrLimit},
		{"unknown", []Chunk{{Kind: "tool_start", CallID: "a", Name: "unregistered"}}, Limits{}, ErrProtocol},
		{"duplicate", []Chunk{{Kind: "tool_start", CallID: "a", Name: "read_file"}, {Kind: "tool_start", CallID: "a", Name: "read_file"}}, Limits{}, ErrProtocol},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := NewFake(Script{Chunks: tc.chunks, FinishReason: "tool_use"})
			fake.Limits = tc.limits
			_, err := fake.Stream(context.Background(), request("fake"), nil)
			errKind(t, err, tc.kind)
		})
	}
}

func TestSchemaRemoteReferencesRejectedWithoutHTTP(t *testing.T) {
	req := request("fake")
	req.Tools[0].Schema = json.RawMessage(`{"$ref":"http://127.0.0.1:1/private"}`)
	_, err := NewFake().Stream(context.Background(), req, nil)
	errKind(t, err, ErrInvalidRequest)
}

func TestConsumerCancellationAndModelCapabilityValidation(t *testing.T) {
	fake := NewFake(Script{Chunks: []Chunk{{Kind: "text", Delta: "x"}}, FinishReason: "end_turn"})
	_, err := fake.Stream(context.Background(), request("fake"), func(e ModelEvent) error {
		if e.Type == EventTextDelta {
			return errors.New("storage unavailable")
		}
		return nil
	})
	errKind(t, err, ErrConsumer)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = fake.Stream(ctx, request("fake"), nil)
	errKind(t, err, ErrCancelled)
	_, err = fake.Stream(context.Background(), request("unknown"), nil)
	errKind(t, err, ErrUnsupported)
	req := request("fake")
	req.NativeState = &NativeState{Provider: "other"}
	_, err = fake.Stream(context.Background(), req, nil)
	errKind(t, err, ErrUnsupported)
}

func TestRetryAfterAndHTTPMapping(t *testing.T) {
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	if got := ParseRetryAfter(now.Add(5*time.Second).Format(http.TimeFormat), now); got != 5*time.Second {
		t.Fatal(got)
	}
	for _, tc := range []struct {
		value string
		want  time.Duration
	}{{"3", 3 * time.Second}, {"-1", 0}, {"nonsense", 0}, {"9223372036854775807", time.Duration(1<<63 - 1)}} {
		if got := ParseRetryAfter(tc.value, now); got != tc.want {
			t.Fatalf("%s => %v", tc.value, got)
		}
	}
	for _, tc := range []struct {
		status int
		kind   ErrorKind
		retry  bool
	}{{400, ErrInvalidRequest, false}, {401, ErrAuthentication, false}, {403, ErrAuthentication, false}, {404, ErrInvalidRequest, false}, {408, ErrUnavailable, true}, {429, ErrRateLimited, true}, {503, ErrUnavailable, true}} {
		e := HTTPError(tc.status, http.Header{"Retry-After": []string{"7"}, "X-Request-Id": []string{"req-safe"}}, now)
		if e.Kind != tc.kind || e.Retryable() != tc.retry || e.RetryAfter != 7*time.Second || e.RequestID != "req-safe" {
			t.Fatalf("%d: %+v", tc.status, e)
		}
	}
}
