package application

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/JDinSeattle/forge-runtime/internal/provider"
)

func TestPortableHandoffClosedHistory(t *testing.T) {
	input := contextEnvelope{AppliedMessageSeq: 7, Summary: "committed summary", Tools: toolSchemas(),
		NativeState: &provider.NativeState{Provider: "openai", ResponseID: "opaque-id", Raw: json.RawMessage(`{"opaque":"sentinel"}`)},
		Messages:    []provider.Message{{Role: "tool", ToolCallID: "foreign/id", Text: "latest result"}},
		PortableMessages: []provider.Message{{Role: "system", Text: "instructions"}, {Role: "user", Text: "original task"},
			{Role: "assistant", ToolCalls: []provider.ToolCall{{ID: "foreign/id", Name: "read_file", Arguments: json.RawMessage(`{"path":"a"}`)}, {ID: "b", Name: "read_file", Arguments: json.RawMessage(`{"path":"b"}`)}}},
			{Role: "tool", ToolCallID: "b", Text: "b result"}, {Role: "tool", ToolCallID: "foreign/id", Text: "a result"},
			{Role: "assistant", ToolCalls: []provider.ToolCall{{ID: "foreign/id", Name: "finish", Arguments: json.RawMessage(`{}`)}}},
			{Role: "tool", ToolCallID: "foreign/id", Text: "verification failed"}, {Role: "user", Text: "message seven"}}}
	before, _ := json.Marshal(input)
	got, err := portableHandoff(input)
	if err != nil || got.NativeState != nil || got.PortableMessages != nil || got.AppliedMessageSeq != 7 || got.Summary != input.Summary || !reflect.DeepEqual(got.Tools, input.Tools) {
		t.Fatalf("handoff=%+v err=%v", got, err)
	}
	if got.Messages[2].ToolCalls[0].ID != "handoff_1" || got.Messages[3].ToolCallID != "handoff_2" || got.Messages[4].ToolCallID != "handoff_1" || got.Messages[6].ToolCallID != "handoff_3" {
		t.Fatal("call pairing changed across batches or reordered results")
	}
	after, _ := json.Marshal(input)
	if string(before) != string(after) {
		t.Fatal("frozen source context mutated")
	}
	raw, _ := json.Marshal(got)
	if strings.Contains(string(raw), "opaque") || strings.Contains(string(raw), "sentinel") {
		t.Fatal("foreign opaque state leaked into portable context")
	}
}

func TestPortableHandoffRejectsOpenOrAmbiguousTools(t *testing.T) {
	call := provider.Message{Role: "assistant", ToolCalls: []provider.ToolCall{{ID: "a", Name: "read_file", Arguments: json.RawMessage(`{}`)}}}
	result := provider.Message{Role: "tool", ToolCallID: "a", Text: "a"}
	for name, messages := range map[string][]provider.Message{
		"missing_result": {call}, "orphan_result": {result}, "duplicate_result": {call, result, result},
		"message_before_close":    {call, {Role: "user", Text: "new"}, result},
		"next_batch_before_close": {call, call, result},
		"duplicate_call":          {{Role: "assistant", ToolCalls: []provider.ToolCall{call.ToolCalls[0], call.ToolCalls[0]}}, result},
		"empty":                   {},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := portableHandoff(contextEnvelope{Messages: messages}); err == nil {
				t.Fatal("open/ambiguous history accepted")
			}
		})
	}
	if _, err := portableHandoff(contextEnvelope{NativeState: &provider.NativeState{Provider: "openai"}, Messages: []provider.Message{{Role: "user", Text: "only delta"}}}); err == nil {
		t.Fatal("legacy native delta mistaken for portable history")
	}
}
