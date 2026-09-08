package telemetry

import (
	"context"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Traceparent returns the W3C traceparent value suitable for runs.traceparent.
// No baggage, tracestate, request ID, or authentication data is persisted here.
func Traceparent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// ContextFromTraceparent restores the submission trace after a worker claim.
// Invalid and oversized input is ignored by the W3C parser. Only version 00's
// bounded traceparent form is accepted, so future extensions require review.
func ContextFromTraceparent(ctx context.Context, value string) context.Context {
	if len(value) != 55 || value[:3] != "00-" {
		return ctx
	}
	return propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{"traceparent": value})
}

// InjectHTTP propagates only traceparent. It never forwards request headers or
// baggage from the incoming HTTP request to a provider or runner.
func InjectHTTP(ctx context.Context, header http.Header) {
	header.Del("Tracestate")
	header.Del("Baggage")
	if value := Traceparent(ctx); value != "" {
		header.Set("Traceparent", value)
	} else {
		header.Del("Traceparent")
	}
}

// Middleware must be installed with chi.Router.Use, before route registration.
// It records the matched template after dispatch, never URL.Path, query strings,
// headers, request/response bodies, principal IDs, or error strings. The wrapper
// preserves Flusher and ResponseController.Unwrap for SSE and write deadlines.
func (t *Telemetry) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := ContextFromTraceparent(r.Context(), r.Header.Get("Traceparent"))
		method := methodLabel(r.Method)
		ctx, span := t.tracer.Start(ctx, "HTTP "+method, trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(attribute.String("http.request.method", method)))
		wrapped := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		t.httpInflight.Inc()
		started := time.Now()
		panicking := true
		defer func() {
			status := wrapped.Status()
			if status == 0 {
				status = http.StatusOK
			}
			if panicking {
				status = http.StatusInternalServerError
			}
			route := "unmatched"
			if rc := chi.RouteContext(r.Context()); rc != nil {
				route = routeLabel(rc.RoutePattern())
			}
			t.httpInflight.Dec()
			t.httpRequests.WithLabelValues(route, method, statusClass(status)).Inc()
			t.httpDuration.WithLabelValues(route, method).Observe(time.Since(started).Seconds())
			span.SetName(method + " " + route)
			span.SetAttributes(attribute.String("http.route", route), attribute.Int("http.response.status_code", status))
			if status >= 500 {
				span.SetStatus(codes.Error, "HTTP server error")
			}
			span.End()
		}()
		next.ServeHTTP(wrapped, r.WithContext(ctx))
		panicking = false
	})
}

func methodLabel(method string) string {
	switch method {
	case "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS":
		return method
	default:
		return "OTHER"
	}
}
func statusClass(status int) string {
	switch {
	case status >= 100 && status < 200:
		return "1xx"
	case status >= 200 && status < 300:
		return "2xx"
	case status >= 300 && status < 400:
		return "3xx"
	case status >= 400 && status < 500:
		return "4xx"
	case status >= 500 && status < 600:
		return "5xx"
	default:
		return "other"
	}
}
func routeLabel(route string) string {
	switch route {
	case "/healthz", "/readyz", "/metrics", "/v1/projects", "/v1/projects/{id}/runs", "/v1/runs/{id}", "/v1/runs/{id}/snapshot", "/v1/runs/{id}/events", "/v1/runs/{id}/cancel", "/v1/runs/{id}/resume", "/v1/runs/{id}/messages", "/v1/approvals/{id}", "/v1/approvals/{id}/decision", "/v1/runs/{id}/artifacts", "/v1/artifacts/{id}":
		return route
	default:
		return "unmatched"
	}
}
