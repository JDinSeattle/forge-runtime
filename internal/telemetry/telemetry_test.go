package telemetry

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func recording(t *testing.T) (*Telemetry, *tracetest.InMemoryExporter) {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	telemetry := newTelemetry(tp)
	t.Cleanup(func() {
		if err := telemetry.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return telemetry, exporter
}

func TestQueueTraceContinuityAndSSEWriter(t *testing.T) {
	m, exporter := recording(t)
	const parent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	var saved string
	r := chi.NewRouter()
	r.Use(m.Middleware)
	r.Get("/v1/runs/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		saved = Traceparent(r.Context())
		if _, ok := w.(http.Flusher); !ok {
			t.Error("middleware hid Flusher")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("id: 1\ndata: {}\n\n"))
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Error(err)
		}
	})
	req := httptest.NewRequest("GET", "/v1/runs/secret-run-123/events?api_key=secret-query", nil)
	req.Header.Set("Traceparent", parent)
	req.Header.Set("Authorization", "Bearer secret-token")
	req.Header.Set("Baggage", "secret-baggage=payload")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if !w.Flushed || saved == "" {
		t.Fatal("SSE flush or captured traceparent missing")
	}
	worker := ContextFromTraceparent(context.Background(), saved)
	ctx, endRun := m.StartRun(worker)
	ctx, endModel := m.StartModel(ctx, "openai")
	endModel(ModelObservation{Outcome: Success, InputTokens: TokenCount{Value: 8, Known: true}, OutputTokens: TokenCount{Value: 0, Known: true}})
	header := http.Header{"Authorization": []string{"Bearer worker-secret"}, "Baggage": []string{"secret"}, "Tracestate": []string{"secret"}}
	InjectHTTP(ctx, header)
	if header.Get("Baggage") != "" || header.Get("Tracestate") != "" || header.Get("Traceparent") == "" {
		t.Fatal("unsafe trace propagation")
	}
	if header.Get("Authorization") != "Bearer worker-secret" {
		t.Fatal("propagator changed explicit outbound authentication")
	}
	endRun(domain.StatusCompleted)
	spans := exporter.GetSpans()
	if len(spans) != 3 {
		t.Fatalf("got %d spans", len(spans))
	}
	for _, span := range spans {
		if span.SpanContext.TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" {
			t.Fatalf("trace broken: %s", span.Name)
		}
	}
	if spans[2].Parent.SpanID() != spans[0].SpanContext.SpanID() {
		t.Fatal("worker run is not a child of submission/request trace")
	}
	data, _ := json.Marshal(spans)
	if strings.Contains(string(data), "secret") {
		t.Fatalf("sensitive input leaked into spans: %s", data)
	}
	metrics := httptest.NewRecorder()
	m.Handler().ServeHTTP(metrics, httptest.NewRequest("GET", "/metrics", nil))
	if strings.Contains(metrics.Body.String(), "secret") {
		t.Fatal("sensitive input leaked into metric labels")
	}
	if got := metricValue(m.httpRequests.WithLabelValues("/v1/runs/{id}/events", "GET", "2xx")); got != 1 {
		t.Fatalf("route label count %v", got)
	}
}

func TestUntrustedDimensionsCollapseAndEndIsConcurrentSafe(t *testing.T) {
	m, exporter := recording(t)
	ctx, endRun := m.StartRun(context.Background())
	ctx, endModel := m.StartModel(ctx, "secret-model-custom-name")
	_, endEffect := m.StartEffect(ctx, "secret-shell-command")
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			endModel(ModelObservation{Outcome: Outcome("secret-error"), InputTokens: TokenCount{Value: -1, Known: true}, OutputTokens: TokenCount{Value: 0, Known: true}, CostKnown: true, CostMicroUSD: -1})
			endEffect(Outcome("secret-operation-error"))
			endRun(domain.RunStatus("secret-status"))
		})
	}
	wg.Wait()
	if metricValue(m.runInflight) != 0 || metricValue(m.modelInflight.WithLabelValues("other")) != 0 || metricValue(m.effectInflight.WithLabelValues("other")) != 0 {
		t.Fatal("inflight gauge was not decremented exactly once")
	}
	if metricValue(m.modelAttempts.WithLabelValues("other", "other")) != 1 || metricValue(m.effectOperations.WithLabelValues("other", "other")) != 1 {
		t.Fatal("duplicate end inflated counters")
	}
	if metricValue(m.modelUnknownUsage.WithLabelValues("other", "input")) != 1 {
		t.Fatal("negative usage was accepted")
	}
	if metricValue(m.modelTokens.WithLabelValues("other", "output")) != 0 || metricValue(m.modelUnknownUsage.WithLabelValues("other", "output")) != 0 {
		t.Fatal("known zero was treated as unknown")
	}
	if len(exporter.GetSpans()) != 3 {
		t.Fatal("duplicate span completion")
	}
	data, _ := json.Marshal(exporter.GetSpans())
	if strings.Contains(string(data), "secret") {
		t.Fatal("untrusted dimension leaked into spans")
	}
}

func TestUnmatchedPathsNeverBecomeLabels(t *testing.T) {
	m, exporter := recording(t)
	r := chi.NewRouter()
	r.Use(m.Middleware)
	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	for range 200 {
		req := httptest.NewRequest("SECRET_METHOD", "/secret-unmatched-path?password=secret", nil)
		r.ServeHTTP(httptest.NewRecorder(), req)
	}
	if got := metricValue(m.httpRequests.WithLabelValues("unmatched", "OTHER", "4xx")); got != 200 {
		t.Fatalf("count %v", got)
	}
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == "forge_runtime_http_requests_total" && len(family.Metric) != 1 {
			t.Fatal("unbounded HTTP metric series")
		}
	}
	data, _ := json.Marshal(exporter.GetSpans())
	if strings.Contains(string(data), "secret") || strings.Contains(string(data), "SECRET_METHOD") {
		t.Fatal("unmatched request leaked into traces")
	}
}

func TestPanicEndsSpanWithoutRecordingPayload(t *testing.T) {
	m, exporter := recording(t)
	h := m.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("secret-panic-message") }))
	func() {
		defer func() {
			if recover() == nil {
				t.Error("middleware swallowed panic")
			}
		}()
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/secret", nil))
	}()
	if metricValue(m.httpInflight) != 0 || metricValue(m.httpRequests.WithLabelValues("unmatched", "GET", "5xx")) != 1 {
		t.Fatal("panic accounting incorrect")
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatal("panic span missing")
	}
	data, _ := json.Marshal(spans)
	if strings.Contains(string(data), "secret") {
		t.Fatal("panic payload leaked")
	}
}

func TestSetupIsolationAndInvalidTraceparent(t *testing.T) {
	for _, endpoint := range []string{"https://secret:password@example.com/v1/traces", "https://example.com/v1/traces?secret=token", "file:///tmp/collector", "https://example.com/#secret"} {
		if _, err := Setup(context.Background(), "forge-api", endpoint); err == nil {
			t.Fatalf("accepted endpoint %q", endpoint)
		}
	}
	for range 2 {
		m, err := Setup(context.Background(), "forge-api", "")
		if err != nil {
			t.Fatal(err)
		}
		ctx, done := m.StartRun(context.Background())
		done(domain.StatusWaitingApproval)
		if !trace.SpanContextFromContext(ctx).IsValid() {
			t.Fatal("disabled export lost trace context")
		}
		if err := m.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	for _, value := range []string{"secret-token", strings.Repeat("x", 100000), "00-00000000000000000000000000000000-0000000000000000-01", "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"} {
		if trace.SpanContextFromContext(ContextFromTraceparent(context.Background(), value)).IsValid() {
			t.Fatal("invalid traceparent accepted")
		}
	}
}

func metricValue(metric prometheus.Metric) float64 {
	var value dto.Metric
	if err := metric.Write(&value); err != nil {
		panic(err)
	}
	if value.Counter != nil {
		return value.Counter.GetValue()
	}
	return value.Gauge.GetValue()
}

func TestOperationalSnapshotsAndSSECleanup(t *testing.T) {
	m, _ := recording(t)
	m.SetSchedulerSnapshot(10, 2)
	m.SetSchedulerSnapshot(-1, 0)
	m.SetReservedBudget(1250000)
	m.SetReservedBudget(-1)
	if metricValue(m.queueDepth) != 10 || metricValue(m.activeRuns) != 2 || metricValue(m.reservedBudget) != 1.25 {
		t.Fatal("invalid snapshot overwrote last successful observation")
	}
	end := m.StartSSE()
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() { end(SSESlowSubscriber) })
	}
	wg.Wait()
	if metricValue(m.sseConnections) != 0 || metricValue(m.sseDisconnects.WithLabelValues("slow_subscriber")) != 1 {
		t.Fatal("SSE cleanup was not exactly once")
	}
	m.RecordRunTransition(domain.StatusRunning, domain.StatusNeedsReconciliation)
	m.RecordRunTransition(domain.StatusNeedsReconciliation, domain.StatusNeedsReconciliation)
	if metricValue(m.reconciliations) != 1 {
		t.Fatal("unchanged status counted as new reconciliation")
	}
}
