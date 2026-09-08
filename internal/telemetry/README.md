# Runtime telemetry

`Setup(ctx, "forge-api", endpointURL)` creates an instance with an isolated
Prometheus registry and optional OTLP HTTP trace export. An empty endpoint keeps
metrics and trace propagation enabled without sending traces. The usual local
endpoint is `http://127.0.0.1:4318/v1/traces`. No global providers are replaced.

API integration:

```go
observability, err := telemetry.Setup(ctx, "forge-api", cfg.OTLPEndpoint)
// Handle err; shutdown with a bounded context during graceful shutdown.
r.Use(observability.Middleware) // inside the Chi router, before routes
r.Handle("/metrics", observability.Handler())
// Persist telemetry.Traceparent(request.Context()) in runs.traceparent.
```

Worker integration:

```go
ctx = telemetry.ContextFromTraceparent(ctx, run.Traceparent)
ctx, endRun := observability.StartRun(ctx)
defer func() { endRun(latestPersistedStatus) }()

ctx, endModel := observability.StartModel(ctx, providerName)
// Dispatch once, then endModel(telemetry.ModelObservation{...}).
// Instrument actual requests, not replay of a saved completed attempt.

ctx, endEffect := observability.StartEffect(ctx, effectKind)
// Start/inspect the same operation, then endEffect(telemetry.Success).
```

All end hooks are concurrency safe and count once. They accept finite outcomes,
known numeric usage, and whitelisted categories. Unknown providers, effects,
statuses, HTTP methods, and routes collapse to bounded fallback labels. Add
new operator-defined categories explicitly in the package rather than placing
model IDs, run IDs, tenant IDs, task paths, or request IDs into metric labels.

HTTP traces contain method, a fixed route template, and status. The middleware
preserves SSE flushing and `http.ResponseController` unwrapping. It never records
URL paths/queries, bodies, headers, panic contents, or arbitrary errors. Trace
propagation carries only W3C `traceparent`; baggage and tracestate are excluded.

Run duration measures a worker drive, not the whole queued/approval lifecycle.
Transition metrics are observations of committed state, and provider usage
metrics are operational estimates; PostgreSQL remains the authoritative source
for billing and exactly-once run accounting. Known zero usage is distinct from
missing usage. Durations use seconds, observed cost uses USD, and token counts
are counts. The exporter queue is bounded to 2,048 spans and non-blocking, with
256 spans per batch and bounded export deadlines. Collector outages can drop
traces without blocking run execution.

Additional numeric hooks are `SetSchedulerSnapshot(queued, active)`,
`ObserveDispatchLatency(delay)`, `RecordLeaseExpiration()`,
`RecordProviderRateLimit(provider)`, `SetReservedBudget(microUSD)`, and
`ObserveArtifact(kind, bytes)`. Supply scheduler/ledger gauges only after a
successful aggregate read; a failed read must preserve the last observation.
`RecordRunTransition` increments reconciliation count on transitions into that
status. `StartSSE()` returns an idempotent cleanup function taking a bounded
`SSECloseReason`; `SSESlowSubscriber` separates queue-overflow disconnects.
These hooks require integration at the actual commit/dispatch/subscription
boundaries; registering the metrics alone does not produce business evidence.

`go test -race ./internal/telemetry` covers persisted parent continuity, SSE,
concurrent cleanup, bounded labels, secret exclusion, and panic cleanup using
an in-memory OTel exporter. It does not assert connectivity to a real collector.
