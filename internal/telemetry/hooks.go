package telemetry

import (
	"context"
	"sync"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.38.0"
	"go.opentelemetry.io/otel/trace"
)

// Outcome is deliberately finite. Unrecognized values map to "other" before
// reaching either metrics or traces.
type Outcome string

const (
	Success        Outcome = "success"
	Failed         Outcome = "failed"
	TransientError Outcome = "transient_error"
	PermanentError Outcome = "permanent_error"
	Cancelled      Outcome = "cancelled"
	Fenced         Outcome = "fenced"
	Unknown        Outcome = "unknown"
)

type TokenCount struct {
	Value int64
	Known bool
}

type ModelObservation struct {
	Outcome                                                      Outcome
	InputTokens, OutputTokens, CacheReadTokens, CacheWriteTokens TokenCount
	CostMicroUSD                                                 int64
	CostKnown                                                    bool
	SemanticInput                                                TokenCount
}

func outcomeLabel(value Outcome) string {
	switch value {
	case Success, Failed, TransientError, PermanentError, Cancelled, Fenced, Unknown:
		return string(value)
	default:
		return "other"
	}
}
func providerLabel(value string) string {
	switch value {
	case "openai", "anthropic", "deepseek", "fake":
		return value
	default:
		return "other"
	}
}
func effectLabel(value string) string {
	switch value {
	case "list_files", "read_file", "search", "apply_patch", "run_command", "get_diff", "finish", "verify", "verify_target", "verify_regression", "baseline", "stop", "adopt", "prepare", "release":
		return value
	default:
		return "other"
	}
}
func statusLabel(value domain.RunStatus) string {
	if value.Valid() {
		return string(value)
	}
	return "other"
}

// StartRun instruments one drive invocation, which may exit while a run is
// waiting for approval or reclaim. End must receive the most recently known
// persisted status; it does not imply durable completion. End is idempotent.
func (t *Telemetry) StartRun(ctx context.Context) (context.Context, func(domain.RunStatus)) {
	ctx, span := t.tracer.Start(ctx, "forge.run.drive", trace.WithSpanKind(trace.SpanKindInternal))
	t.runInflight.Inc()
	started := time.Now()
	var once sync.Once
	return ctx, func(status domain.RunStatus) {
		once.Do(func() {
			label := statusLabel(status)
			t.runInflight.Dec()
			t.runDrives.WithLabelValues(label).Inc()
			t.runDuration.WithLabelValues(label).Observe(time.Since(started).Seconds())
			span.SetAttributes(attribute.String("forge.run.status", label))
			if status == domain.StatusFailed {
				span.SetStatus(codes.Error, "run failed")
			}
			span.End()
		})
	}
}

// RecordRunTransition is called only after a successful database commit. These
// process metrics are for operations, not exactly-once business accounting.
func (t *Telemetry) RecordRunTransition(from, to domain.RunStatus) {
	t.runTransitions.WithLabelValues(statusLabel(from), statusLabel(to)).Inc()
	if from != to && to == domain.StatusNeedsReconciliation {
		t.reconciliations.Inc()
	}
}

// StartModel surrounds only a provider dispatch, not replay of a saved result.
// Known token/cost values must come from the final normalized response. Unknown
// values remain distinguishable from zero. End is safe in multiple cleanup paths.
func (t *Telemetry) StartModel(ctx context.Context, provider string) (context.Context, func(ModelObservation)) {
	p := providerLabel(provider)
	ctx, span := t.tracer.Start(ctx, "forge.model.request", trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(semconv.GenAIProviderNameKey.String(p), semconv.GenAIOperationNameChat))
	t.modelInflight.WithLabelValues(p).Inc()
	started := time.Now()
	var once sync.Once
	return ctx, func(o ModelObservation) {
		once.Do(func() {
			outcome := outcomeLabel(o.Outcome)
			t.modelInflight.WithLabelValues(p).Dec()
			t.modelAttempts.WithLabelValues(p, outcome).Inc()
			t.modelDuration.WithLabelValues(p, outcome).Observe(time.Since(started).Seconds())
			span.SetAttributes(attribute.String("forge.outcome", outcome))
			for _, usage := range []struct {
				kind  string
				count TokenCount
			}{{"input", o.InputTokens}, {"output", o.OutputTokens}, {"cache_read", o.CacheReadTokens}, {"cache_write", o.CacheWriteTokens}} {
				if usage.count.Known && usage.count.Value >= 0 {
					t.modelTokens.WithLabelValues(p, usage.kind).Add(float64(usage.count.Value))
					span.SetAttributes(attribute.Int64("forge.usage."+usage.kind+"_tokens", usage.count.Value))

					if usage.kind == "output" {
						span.SetAttributes(semconv.GenAIUsageOutputTokensKey.Int64(usage.count.Value))
					}
				} else {
					t.modelUnknownUsage.WithLabelValues(p, usage.kind).Inc()
				}
			}
			if o.SemanticInput.Known && o.SemanticInput.Value >= 0 {
				span.SetAttributes(semconv.GenAIUsageInputTokensKey.Int64(o.SemanticInput.Value))
			}
			if o.CostKnown && o.CostMicroUSD >= 0 {
				t.modelCost.WithLabelValues(p).Add(float64(o.CostMicroUSD) / 1e6)
			}
			setOutcomeStatus(span, outcome)
			span.End()
		})
	}
}

func (t *Telemetry) StartEffect(ctx context.Context, kind string) (context.Context, func(Outcome)) {
	k := effectLabel(kind)
	ctx, span := t.tracer.Start(ctx, "forge.effect.execute", trace.WithSpanKind(trace.SpanKindInternal), trace.WithAttributes(attribute.String("forge.effect.kind", k)))
	t.effectInflight.WithLabelValues(k).Inc()
	started := time.Now()
	var once sync.Once
	return ctx, func(outcome Outcome) {
		once.Do(func() {
			o := outcomeLabel(outcome)
			t.effectInflight.WithLabelValues(k).Dec()
			t.effectOperations.WithLabelValues(k, o).Inc()
			t.effectDuration.WithLabelValues(k, o).Observe(time.Since(started).Seconds())
			span.SetAttributes(attribute.String("forge.outcome", o))
			setOutcomeStatus(span, o)
			span.End()
		})
	}
}

func setOutcomeStatus(span trace.Span, outcome string) {
	switch outcome {
	case "failed", "transient_error", "permanent_error", "unknown", "other":
		span.SetStatus(codes.Error, "operation "+outcome)
	}
}
