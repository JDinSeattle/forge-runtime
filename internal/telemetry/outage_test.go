package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// This exercises Setup's real bounded OTLP HTTP exporter queue. It is not a
// business recovery test: collector backpressure must not block hook completion.
func TestCollectorBackpressureKeepsObservationsBounded(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case started <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer collector.Close()
	defer close(release)
	m, err := Setup(context.Background(), "forge-worker", collector.URL+"/v1/traces")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = m.Shutdown(ctx)
	}()
	for range 256 {
		_, end := m.StartPhase(context.Background(), "step", Identity{})
		end(Success)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("real exporter never reached collector")
	}
	start := time.Now()
	for range 4096 {
		_, end := m.StartModel(context.Background(), "fake")
		end(ModelObservation{Outcome: Success})
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("full exporter queue blocked observation loop: %v", elapsed)
	}
	if metricValue(m.modelInflight.WithLabelValues("fake")) != 0 || metricValue(m.modelAttempts.WithLabelValues("fake", "success")) != 4096 {
		t.Fatal("collector outage changed local metrics")
	}
}
