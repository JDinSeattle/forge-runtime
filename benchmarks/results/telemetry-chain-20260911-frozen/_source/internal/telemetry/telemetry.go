// Package telemetry records operational metadata only. Its public hooks do not
// accept prompts, messages, credentials, tool arguments, or arbitrary errors.
// Each instance owns its registry and tracer provider; no global OTel or
// Prometheus state is replaced.
package telemetry

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.38.0"
	"go.opentelemetry.io/otel/trace"
)

type Telemetry struct {
	Registry           *prometheus.Registry
	provider           *sdktrace.TracerProvider
	tracer             trace.Tracer
	httpRequests       *prometheus.CounterVec
	httpDuration       *prometheus.HistogramVec
	httpInflight       prometheus.Gauge
	runDrives          *prometheus.CounterVec
	runDuration        *prometheus.HistogramVec
	runTransitions     *prometheus.CounterVec
	runInflight        prometheus.Gauge
	modelAttempts      *prometheus.CounterVec
	modelDuration      *prometheus.HistogramVec
	modelTokens        *prometheus.CounterVec
	modelUnknownUsage  *prometheus.CounterVec
	modelCost          *prometheus.CounterVec
	modelInflight      *prometheus.GaugeVec
	effectOperations   *prometheus.CounterVec
	effectDuration     *prometheus.HistogramVec
	runnerDuration     *prometheus.HistogramVec
	effectInflight     *prometheus.GaugeVec
	queueDepth         prometheus.Gauge
	activeRuns         prometheus.Gauge
	dispatchLatency    prometheus.Histogram
	leaseExpirations   prometheus.Counter
	reconciliations    prometheus.Counter
	providerRateLimits *prometheus.CounterVec
	reservedBudget     prometheus.Gauge
	sseConnections     prometheus.Gauge
	sseDisconnects     *prometheus.CounterVec
	artifactBytes      *prometheus.CounterVec
}

var servicePattern = regexp.MustCompile(`^[a-z][a-z0-9._-]{0,63}$`)

// Setup configures a local metrics registry and an optional OTLP HTTP trace
// exporter. endpointURL is an operator-controlled absolute URL, normally
// http://127.0.0.1:4318/v1/traces. Empty disables export but preserves trace IDs
// and metrics. The exporter uses a bounded non-blocking queue; collector failure
// must never block task execution. Call Shutdown after stopping request intake.
func Setup(ctx context.Context, serviceName, endpointURL string) (*Telemetry, error) {
	if !servicePattern.MatchString(serviceName) {
		return nil, fmt.Errorf("invalid telemetry service name")
	}
	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName(serviceName))),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	}
	if endpointURL != "" {
		u, err := url.Parse(endpointURL)
		if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return nil, fmt.Errorf("invalid OTLP endpoint URL")
		}
		if u.Path == "" || u.Path == "/" {
			u.Path = "/v1/traces"
		}
		exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(u.String()), otlptracehttp.WithTimeout(3*time.Second))
		if err != nil {
			return nil, fmt.Errorf("initialize OTLP trace exporter: %w", err)
		}
		opts = append(opts, sdktrace.WithBatcher(exporter,
			sdktrace.WithMaxQueueSize(2048), sdktrace.WithMaxExportBatchSize(256),
			sdktrace.WithBatchTimeout(time.Second), sdktrace.WithExportTimeout(4*time.Second)))
	}
	return newTelemetry(sdktrace.NewTracerProvider(opts...)), nil
}

func newTelemetry(provider *sdktrace.TracerProvider) *Telemetry {
	t := &Telemetry{Registry: prometheus.NewRegistry(), provider: provider, tracer: provider.Tracer("github.com/JDinSeattle/forge-runtime/internal/telemetry", trace.WithSchemaURL(semconv.SchemaURL))}
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		v := prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "forge", Subsystem: "runtime", Name: name, Help: help}, labels)
		t.Registry.MustRegister(v)
		return v
	}
	histogram := func(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
		v := prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "forge", Subsystem: "runtime", Name: name, Help: help, Buckets: buckets}, labels)
		t.Registry.MustRegister(v)
		return v
	}
	gauge := func(name, help string) prometheus.Gauge {
		v := prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "forge", Subsystem: "runtime", Name: name, Help: help})
		t.Registry.MustRegister(v)
		return v
	}
	gaugeVec := func(name, help string, labels ...string) *prometheus.GaugeVec {
		v := prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "forge", Subsystem: "runtime", Name: name, Help: help}, labels)
		t.Registry.MustRegister(v)
		return v
	}
	t.httpRequests = counter("http_requests_total", "Completed HTTP requests by route template, method, and status class.", "route", "method", "status_class")
	t.httpDuration = histogram("http_request_duration_seconds", "HTTP request duration, including the full lifetime of SSE streams.", []float64{.005, .025, .1, .5, 1, 5, 30, 120, 600}, "route", "method")
	t.httpInflight = gauge("http_inflight", "HTTP requests currently being served.")
	t.runDrives = counter("run_drives_total", "Worker drive invocations by status at exit; a resumed run may have several drives.", "status")
	t.runDuration = histogram("run_drive_duration_seconds", "Worker drive duration, excluding time waiting between claims.", []float64{.1, 1, 5, 30, 120, 600, 1800}, "status")
	t.runTransitions = counter("run_transitions_total", "Successful newly committed run transitions observed by this process; commit-to-observation crashes can omit samples.", "from", "to")
	t.runInflight = gauge("run_drives_inflight", "Worker drive invocations currently active in this process.")
	t.modelAttempts = counter("model_attempts_total", "Completed Provider.Stream invocations; not a transport HTTP request count or authoritative billing ledger.", "provider", "outcome")
	t.modelDuration = histogram("model_request_duration_seconds", "Provider request duration.", []float64{.1, .5, 1, 5, 15, 30, 60, 120}, "provider", "outcome")
	t.modelTokens = counter("model_tokens_total", "Known final token usage observed from provider responses.", "provider", "kind")
	t.modelUnknownUsage = counter("model_unknown_usage_total", "Requests for which a usage dimension was not known.", "provider", "kind")
	t.modelCost = counter("model_observed_cost_usd_total", "Known observed provider cost; database ledger remains authoritative.", "provider")
	t.modelInflight = gaugeVec("model_requests_inflight", "Provider requests currently active.", "provider")
	t.effectOperations = counter("effect_operations_total", "Completed instrumented runner operations by kind and outcome.", "kind", "outcome")
	t.effectDuration = histogram("effect_operation_duration_seconds", "Runner operation duration, including receipt inspection.", []float64{.001, .01, .1, 1, 5, 30, 120, 600}, "kind", "outcome")
	t.runnerDuration = histogram("runner_operation_duration_seconds", "New runner execution observation duration; Inspect/replay do not add samples, and observation end is not durable success.", []float64{.001, .01, .1, 1, 5, 30, 120, 600}, "kind")
	t.effectInflight = gaugeVec("effect_operations_inflight", "Runner operations currently active.", "kind")
	t.queueDepth = gauge("queue_depth", "Runnable waiting runs in the latest successful global database snapshot; use max across duplicate worker scrapes.")
	t.activeRuns = gauge("active_runs", "Nonreleased run allocations, including uncertain allocations, in the latest global database snapshot; use max across worker scrapes.")
	t.dispatchLatency = prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "forge", Subsystem: "runtime", Name: "dispatch_latency_seconds", Help: "Time between runnable admission and a successful claim.", Buckets: []float64{.001, .01, .1, .5, 1, 5, 30, 120}})
	t.leaseExpirations = prometheus.NewCounter(prometheus.CounterOpts{Namespace: "forge", Subsystem: "runtime", Name: "lease_expirations_total", Help: "Observed expired leases at recovery; database state remains authoritative."})
	t.reconciliations = prometheus.NewCounter(prometheus.CounterOpts{Namespace: "forge", Subsystem: "runtime", Name: "reconciliation_total", Help: "Observed committed transitions into needs_reconciliation."})
	t.Registry.MustRegister(t.dispatchLatency, t.leaseExpirations, t.reconciliations)
	t.providerRateLimits = counter("provider_rate_limits_total", "Observed provider rate-limit responses.", "provider")
	t.reservedBudget = gauge("reserved_budget_usd", "Unsettled budget reservations in the latest successful ledger snapshot.")
	t.sseConnections = gauge("sse_connections", "Event streams currently connected to this API process.")
	t.sseDisconnects = counter("sse_disconnects_total", "Closed event streams by bounded reason; slow_subscriber identifies queue overflow.", "reason")
	t.artifactBytes = counter("artifact_bytes_total", "Newly committed READY artifact bytes observed by this process; idempotent publications do not add samples.", "kind")
	t.Registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return t
}

func (t *Telemetry) Handler() http.Handler {
	return promhttp.HandlerFor(t.Registry, promhttp.HandlerOpts{Timeout: 5 * time.Second})
}

// Shutdown flushes traces within the caller's deadline. It is safe to call more
// than once. Metrics are scrape based and require no shutdown action.
func (t *Telemetry) Shutdown(ctx context.Context) error { return t.provider.Shutdown(ctx) }
