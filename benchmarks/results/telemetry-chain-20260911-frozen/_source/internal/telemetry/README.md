# Runtime telemetry

`Setup` owns a process-local Prometheus registry and OTel provider; it never
replaces globals. API, worker and runner entrypoints install it on their real
Store/Driver/gRPC/Engine objects. Set `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` to an
operator-controlled OTLP HTTP URL. Empty disables export while retaining context
and metrics. Runner metrics use numeric loopback `FORGE_METRICS_LISTEN` (default
`127.0.0.1:8097`); API and worker retain their existing metrics listeners.

## Trace boundaries and recovery

| Span | Production boundary | Context and completion |
|---|---|---|
| HTTP route | Chi middleware | Only method, route template, status; retains SSE flush behavior |
| `forge.run.enqueue` | `Store.Submit` | Context committed in `runs.traceparent`; idempotent hit uses actual existing run ID and never replaces its persisted context |
| `forge.run.queue` | Successful `Store.Claim` | Retrospective interval from max(runnable admission, finite not_before, old SQL lease expiry) to database claim time; no sample for missing historical admission |
| `forge.run.claim` | Claim transaction | `last_claim_traceparent` committed atomically; failed claims have no dispatch sample |
| `forge.run.drive` / `.step` | `Driver.Drive` / each durable command iteration | Drive restores committed claim context; step surrounds command execution and `Advance` commit, including reload/defer exits |
| `forge.run.model_attempt` | New `BeginPricedAttempt` reservation | Actual context committed with attempt; existing attempts return their original context |
| `forge.model.request` | One `Provider.Stream` invocation | Restores attempt context; saved response replay makes no new request observation |
| `forge.runner.client/Method` / `.server/Method` | gRPC unary interceptors | Only W3C traceparent propagated; baggage and tracestate removed; finite method allowlist covers all eight runner RPCs |
| `forge.runner.operation` | Newly admitted Engine execution goroutine | Only SpanContext copied into independent operation context; RPC cancellation does not cancel the durable job; duplicate Start/Inspect creates no new execution observation |
| `forge.run.verification` | `systemOperation` through durable receipt settlement | Phase initial_target/final_target/regression/final_diff; success means receipt settlement succeeded, not that the target test passed |

Trace-only identity includes tenant/run, step, state version, lease epoch,
workspace revision and operation/attempt ID where available. No prompts, tool
arguments, file paths, grant/token headers, arbitrary errors, or baggage enter
spans. `run_retry` links the new enqueue to its actual parent-run enqueue;
`provider_retry` links new attempt to the prior attempt; `lease_takeover` and
`claim_continuation` link actual persisted claim contexts. The adoption RPC is a
child of the resumed drive, whose claim links to the predecessor claim.

Migration 12 adds nullable admission/claim/attempt context, and the explicit
`lease_yielded` observation flag. `Defer` sets it in the existing transaction;
Claim clears it atomically. It changes no lease authority, fencing, reducer
snapshot or billing rule. Historical NULL context is not fabricated. Historical
runs without admission metadata omit queue and lease-expiry observations. The
SQLite journal does not persist trace context: recovery inspection uses the new
RPC context and emits `forge.context_missing` when the operation is not live;
it does not reconstruct an original operation span or link after runner restart.

## Metric mapping

All names below carry prefix `forge_runtime_`. Labels are finite allowlists;
run/tenant/attempt/model/path/prompt-hash values never become default labels.

| Requirement | Metric / source |
|---|---|
| queue_depth | `queue_depth`: runnable waiting candidates from successful global SQL snapshot; includes capacity-blocked candidates |
| active_runs | `active_runs`: nonreleased allocations, including uncertain ownership; not just running workers |
| dispatch_latency | `dispatch_latency_seconds`: successful claims with known admission only |
| lease_expirations | `lease_expirations_total`: committed takeover of expired, non-yielded lease with known admission |
| reconciliation_count | `reconciliation_total`: successful committed transition into needs_reconciliation; same-status transitions do not add |
| provider_requests | `model_attempts_total`: completed Provider.Stream invocations; an adapter can fail its start callback before sending HTTP, so this is not vendor HTTP request count |
| provider_rate_limits | `provider_rate_limits_total`: explicitly typed rate-limit returns from that invocation; replay does not add |
| reserved_budget | `reserved_budget_usd`: successful global reservation-ledger snapshot |
| SSE connections / slow disconnects | existing `sse_connections` and `sse_disconnects_total{reason="slow_subscriber"}` admission/cleanup hooks |
| runner_operation_duration | `runner_operation_duration_seconds`: new Engine invocation lifetime; observation ending does not assert durable success |
| artifact_bytes | `artifact_bytes_total`: newly committed READY artifact rows only; repeat publication adds zero |

`run_transitions_total` and artifact counters execute bounded in-process
callbacks after confirmed commit. Rollback and ambiguous/failed COMMIT produce
no observations. A process dying after database commit but before the callback
can omit a sample. Restart resets process counters. They are operational
observations, never exactly-once accounting or durable completion evidence.
Global queue/allocation/budget gauges duplicate the same database scope on each
worker: use **max across worker scrape targets**, not sum; a failed aggregate
read retains the last successful observation. The database remains authoritative.
The older `effect_operation_duration_seconds` covers worker inspection/replay
latency; it is distinct from the new runner execution-duration histogram.

## Fixed GenAI semantic conventions

The Go OTel SDK remains pinned by `go.mod` to v1.46.0. Resource and tracer scope
use the locally installed module's **semconv/v1.38.0**, schema URL
`https://opentelemetry.io/schemas/1.38.0`. This is an explicit compatibility pin,
not a statement that future convention versions have identical meanings.

| Attribute | Mapping |
|---|---|
| `gen_ai.provider.name` | allowlisted active provider: openai/anthropic/fake/other |
| `gen_ai.operation.name` | chat |
| `gen_ai.request.model` | frozen attempt's operator-selected model; trace only |
| `gen_ai.usage.input_tokens` | OpenAI/fake native input total; Anthropic fresh input + cache read + cache write only when all three are known, nonnegative and sum without overflow |
| `gen_ai.usage.output_tokens` | known nonnegative final output total; zero is retained |
| `forge.usage.*_tokens` | original normalized provider-native dimensions; unknown dimensions omitted and counted separately |

Token attributes require a successful final normalized result. Unknown usage
never becomes zero. The optional generic observed-cost hook remains separate
from authoritative quota billing; the Driver does not invent a billed amount
from a trace. `forge.*`, RPC and runtime attributes are explicitly application
extensions, not claims that every field is a GenAI convention.

The exporter uses a nonblocking 2,048-span queue, 256-span batches and bounded
HTTP/export deadlines; outages can drop spans. Shutdown is bounded by the
caller's deadline. E43 tests the real exporter under blocked HTTP delivery and
separately validates a production-component graph against an independent
Collector; see `docs/telemetry-chain-evidence.md` for precise evidence limits.
