package telemetry

import (
	"sync"
	"time"
)

// SetSchedulerSnapshot publishes one successful database read, aggregated over
// the operator's scope. Failed reads must not overwrite the last values with
// zero. There are intentionally no tenant, worker, or runner labels.
func (t *Telemetry) SetSchedulerSnapshot(queued, active int64) {
	if queued < 0 || active < 0 {
		return
	}
	t.queueDepth.Set(float64(queued))
	t.activeRuns.Set(float64(active))
}

// ObserveDispatchLatency measures delay since runnable admission (not the run's
// original creation time if it has been resumed from approval).
func (t *Telemetry) ObserveDispatchLatency(delay time.Duration) {
	if delay >= 0 {
		t.dispatchLatency.Observe(delay.Seconds())
	}
}

func (t *Telemetry) RecordLeaseExpiration() { t.leaseExpirations.Inc() }
func (t *Telemetry) RecordProviderRateLimit(provider string) {
	t.providerRateLimits.WithLabelValues(providerLabel(provider)).Inc()
}

// SetReservedBudget publishes an aggregate of outstanding ledger reservations.
// Unknown obligations remain reserved and must be included in the supplied sum.
func (t *Telemetry) SetReservedBudget(microUSD int64) {
	if microUSD >= 0 {
		t.reservedBudget.Set(float64(microUSD) / 1e6)
	}
}

type SSECloseReason string

const (
	SSEFinished       SSECloseReason = "finished"
	SSEClientClosed   SSECloseReason = "client_closed"
	SSESlowSubscriber SSECloseReason = "slow_subscriber"
	SSEReset          SSECloseReason = "reset_required"
	SSEUnauthorized   SSECloseReason = "unauthorized"
	SSEShutdown       SSECloseReason = "shutdown"
	SSEFailed         SSECloseReason = "failed"
)

// StartSSE counts a successfully admitted subscription. End is idempotent and
// accepts only a bounded reason, never a transport error or client identifier.
func (t *Telemetry) StartSSE() func(SSECloseReason) {
	t.sseConnections.Inc()
	var once sync.Once
	return func(reason SSECloseReason) {
		once.Do(func() {
			label := "other"
			switch reason {
			case SSEFinished, SSEClientClosed, SSESlowSubscriber, SSEReset, SSEUnauthorized, SSEShutdown, SSEFailed:
				label = string(reason)
			}
			t.sseConnections.Dec()
			t.sseDisconnects.WithLabelValues(label).Inc()
		})
	}
}

func (t *Telemetry) ObserveArtifact(kind string, size int64) {
	if size < 0 {
		return
	}
	switch kind {
	case "model_request", "model_result", "model_response", "context", "tool_output", "tool_batch", "operation_receipt", "baseline", "verification", "verification_report", "patch_manifest", "patch", "stop_receipt", "snapshot":
	default:
		kind = "other"
	}
	t.artifactBytes.WithLabelValues(kind).Add(float64(size))
}
