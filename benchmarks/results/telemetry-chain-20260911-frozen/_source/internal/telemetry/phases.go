package telemetry

import (
	"context"
	"sync"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Identity is trace-only metadata. None of these values becomes a metric label.
// Inputs are application identities, never prompts, command arguments or grants.
type Identity struct {
	Tenant, Run, Operation, Attempt string
	Step, Version, Epoch, Revision  uint64
	Phase                           string
}

func (i Identity) attrs() []attribute.KeyValue {
	a := []attribute.KeyValue{attribute.String("forge.tenant_id", i.Tenant), attribute.String("forge.run_id", i.Run), attribute.Int64("forge.step.seq", int64(i.Step)), attribute.Int64("forge.state.version", int64(i.Version)), attribute.Int64("forge.lease.epoch", int64(i.Epoch)), attribute.Int64("forge.workspace.revision", int64(i.Revision))}
	if i.Operation != "" {
		a = append(a, attribute.String("forge.operation.id", i.Operation))
	}
	if i.Attempt != "" {
		a = append(a, attribute.String("forge.attempt.id", i.Attempt))
	}
	switch i.Phase {
	case "initial_target", "final_target", "regression", "final_diff":
		a = append(a, attribute.String("forge.verification.phase", i.Phase))
	}
	return a
}
func SpanIdentity(ctx context.Context, i Identity) {
	trace.SpanFromContext(ctx).SetAttributes(i.attrs()...)
}
func Link(ctx context.Context, previous, relationship string) {
	switch relationship {
	case "run_retry", "provider_retry", "lease_takeover", "claim_continuation":
	default:
		return
	}
	sc := trace.SpanContextFromContext(ContextFromTraceparent(context.Background(), previous))
	if sc.IsValid() {
		trace.SpanFromContext(ctx).AddLink(trace.Link{SpanContext: sc, Attributes: []attribute.KeyValue{attribute.String("forge.relationship", relationship)}})
	}
}
func Event(ctx context.Context, name string) {
	switch name {
	case "committed", "deduplicated", "result_replayed", "receipt_replayed", "deferred", "reload", "context_missing", "fenced":
		trace.SpanFromContext(ctx).AddEvent("forge." + name)
	}
}

// StartPhase accepts only fixed instrumentation phase names.
func (t *Telemetry) StartPhase(ctx context.Context, name string, i Identity) (context.Context, func(Outcome)) {
	switch name {
	case "enqueue", "claim", "step", "verification", "model_attempt":
	default:
		name = "other"
	}
	ctx, span := t.tracer.Start(ctx, "forge.run."+name, trace.WithAttributes(i.attrs()...))
	var once sync.Once
	return ctx, func(o Outcome) {
		once.Do(func() {
			v := outcomeLabel(o)
			span.SetAttributes(attribute.String("forge.outcome", v))
			setOutcomeStatus(span, v)
			span.End()
		})
	}
}

// Queue records a completed admission interval only after its successful claim.
// DB timestamps avoid worker clock skew. Missing historical admission is omitted.
func (t *Telemetry) Queue(ctx context.Context, i Identity, from, claimed time.Time) {
	if from.IsZero() || claimed.Before(from) {
		return
	}
	_, span := t.tracer.Start(ctx, "forge.run.queue", trace.WithTimestamp(from), trace.WithAttributes(i.attrs()...))
	span.End(trace.WithTimestamp(claimed))
	t.ObserveDispatchLatency(claimed.Sub(from))
}
func (t *Telemetry) StartRPC(ctx context.Context, method string, server bool) (context.Context, func(Outcome)) {
	switch method {
	case "PrepareWorkspace", "AdoptWorkspace", "StartOperation", "InspectOperation", "CancelOperation", "StopWorkspace", "SealSnapshot", "ReleaseWorkspace":
	default:
		method = "other"
	}
	kind := trace.SpanKindClient
	name := "forge.runner.client/" + method
	if server {
		kind = trace.SpanKindServer
		name = "forge.runner.server/" + method
	}
	ctx, span := t.tracer.Start(ctx, name, trace.WithSpanKind(kind), trace.WithAttributes(attribute.String("rpc.system.name", "grpc"), attribute.String("rpc.method", method)))
	var once sync.Once
	return ctx, func(o Outcome) {
		once.Do(func() {
			span.SetAttributes(attribute.String("forge.outcome", outcomeLabel(o)))
			setOutcomeStatus(span, outcomeLabel(o))
			span.End()
		})
	}
}

// StartRunner measures a newly admitted execution invocation, never Inspect or
// repeated Start. Ending observation is not a durable success assertion.
func (t *Telemetry) StartRunner(ctx context.Context, i Identity, kind string) (context.Context, func()) {
	k := effectLabel(kind)
	ctx, span := t.tracer.Start(ctx, "forge.runner.operation", trace.WithAttributes(append(i.attrs(), attribute.String("forge.effect.kind", k))...))
	start := time.Now()
	var once sync.Once
	return ctx, func() {
		once.Do(func() { t.runnerDuration.WithLabelValues(k).Observe(time.Since(start).Seconds()); span.End() })
	}
}
func (t *Telemetry) Transition(from, to domain.RunStatus, oldVersion, newVersion uint64) {
	if oldVersion != newVersion {
		t.RecordRunTransition(from, to)
	}
}

func ModelFields(ctx context.Context, model string, number int) {
	trace.SpanFromContext(ctx).SetAttributes(attribute.String("gen_ai.request.model", model), attribute.Int("forge.attempt.number", number))
}
