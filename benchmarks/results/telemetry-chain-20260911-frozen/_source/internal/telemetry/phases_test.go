package telemetry

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.38.0"
	"go.opentelemetry.io/otel/trace"
)

func TestBoundedPhaseLifecycleAndActualLinks(t *testing.T) {
	m, ex := recording(t)
	ctx, end := m.StartPhase(context.Background(), "enqueue", Identity{Run: "private-run"})
	previous := Traceparent(ctx)
	end(Success)
	ctx, end = m.StartPhase(context.Background(), "claim", Identity{Run: "private-run"})
	Link(ctx, previous, "lease_takeover")
	Link(ctx, "invalid", "lease_takeover")
	Link(ctx, previous, "secret-label")
	Event(ctx, "context_missing")
	Event(ctx, "private-body")
	end(Success)
	end(Failed)
	_, end = m.StartPhase(context.Background(), "private-phase", Identity{Phase: "secret-phase"})
	end(Unknown)
	ctx, rpcEnd := m.StartRPC(context.Background(), "secret-method", false)
	rpcEnd(Success)
	rpcEnd(Failed)
	_, runnerEnd := m.StartRunner(ctx, Identity{Operation: "private-operation"}, "private-command")
	runnerEnd()
	runnerEnd()
	now := time.Now()
	m.Queue(context.Background(), Identity{}, time.Time{}, now)
	m.Queue(context.Background(), Identity{}, now, now.Add(-time.Second))
	m.Transition(domain.StatusRunning, domain.StatusFailed, 4, 4)
	spans := ex.GetSpans()
	if len(spans) != 5 || len(spans[1].Links) != 1 || spans[1].Links[0].SpanContext.SpanID() != spans[0].SpanContext.SpanID() {
		t.Fatalf("invalid real links or lifecycle: %+v", spans)
	}
	for _, s := range spans {
		if s.InstrumentationScope.SchemaURL != semconv.SchemaURL {
			t.Fatal("schema drift")
		}
	}
	raw, _ := json.Marshal(spans)
	for _, secret := range []string{"secret-label", "private-body", "private-phase", "secret-phase", "secret-method", "private-command"} {
		if strings.Contains(string(raw), secret) {
			t.Fatalf("unbounded label %s", secret)
		}
	}
}

func TestPinnedGenAIUsageKnownAndUnknown(t *testing.T) {
	m, ex := recording(t)
	for _, known := range []bool{false, true} {
		ctx, end := m.StartModel(context.Background(), "anthropic")
		ModelFields(ctx, "operator-model", 2)
		trace.SpanFromContext(ctx).SetAttributes(attribute.String("fixture", "safe"))
		end(ModelObservation{Outcome: Success, InputTokens: TokenCount{Known: true, Value: 3}, OutputTokens: TokenCount{Known: true, Value: 0}, SemanticInput: TokenCount{Known: known, Value: 13}})
	}
	for n, s := range ex.GetSpans() {
		attrs := map[string]any{}
		for _, a := range s.Attributes {
			attrs[string(a.Key)] = a.Value.AsInterface()
		}
		if attrs["gen_ai.provider.name"] != "anthropic" || attrs["gen_ai.operation.name"] != "chat" || attrs["gen_ai.request.model"] != "operator-model" || attrs["gen_ai.usage.output_tokens"] != int64(0) {
			t.Fatalf("semantic fields: %+v", attrs)
		}
		_, has := attrs["gen_ai.usage.input_tokens"]
		if has != (n == 1) {
			t.Fatalf("unknown semantic input must be omitted: %+v", attrs)
		}
		if n == 1 && attrs["gen_ai.usage.input_tokens"] != int64(13) {
			t.Fatal("semantic input value")
		}
	}
}
