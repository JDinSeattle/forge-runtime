package runner

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
)

// These are deterministic journal/engine tests. The backend never executes a
// process; actual SIGTERM and Docker recovery have separate opt-in acceptance.
func TestShutdownFencesQueuedAdmissionAndPreservesJobForSuccessor(t *testing.T) {
	var starts, cancels atomic.Int64
	var stopped atomic.Bool
	started := make(chan struct{})
	backend := &sandbox.TestBackend{
		StartFunc: func(_ context.Context, s sandbox.JobSpec) (sandbox.Job, error) {
			starts.Add(1)
			close(started)
			return sandbox.Job{ID: s.ID, Started: true, Running: true}, nil
		},
		InspectFunc: func(_ context.Context, id string) (sandbox.Job, error) {
			return sandbox.Job{ID: id, Started: true, Running: !stopped.Load()}, nil
		},
		CancelFunc: func(_ context.Context, id string) (sandbox.Job, error) {
			cancels.Add(1)
			return sandbox.Job{ID: id, Started: true, Interrupted: true}, nil
		},
	}
	e, config, r := fixture(t, nil, backend)
	first := request(r, "inflight", "run_command", CommandArgs{Command: []string{"fixture-long-command"}}, 1)
	if _, err := e.StartOperation(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("fixture backend did not start")
	}
	queued := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		_, err := e.StartOperation(ctx, request(r, "queued", "run_command", CommandArgs{Command: []string{"must-not-start"}}, 1))
		queued <- err
	}()
	// The first operation owns the workspace lock for its execution lifetime.
	select {
	case err := <-queued:
		t.Fatalf("second operation did not wait for the workspace: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	e.BeginShutdown()
	e.BeginShutdown() // Repeated lifecycle notification is harmless.
	select {
	case err := <-queued:
		if err == nil {
			t.Fatal("queued operation was admitted during shutdown")
		}
	case <-ctx.Done():
		t.Fatal("queued handler did not drain after shutdown")
	}
	if starts.Load() != 1 || cancels.Load() != 0 {
		t.Fatalf("shutdown dispatched or cancelled a job: starts=%d cancels=%d", starts.Load(), cancels.Load())
	}
	// BeginShutdown must not close the journal before the RPC server has drained.
	original, err := e.journal.operation(ctx, first.OperationID)
	if err != nil || original.Status != Unknown || original.CancelRequested || original.Receipt.SHA256 != "" {
		t.Fatalf("in-flight outcome was not preserved as unknown: %+v / %v", original, err)
	}
	var count int
	if err := e.journal.db.QueryRowContext(ctx, "SELECT count(*) FROM operations").Scan(&count); err != nil || count != 1 {
		t.Fatalf("shutdown created another reservation: %d / %v", count, err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	// An external completion becomes observable after the old process has gone.
	// The successor must inspect the original ID instead of issuing another Start.
	stopped.Store(true)
	successor, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer successor.Close()
	recovered := waitOperation(t, successor, r, first.OperationID, true)
	if recovered.Status != Succeeded || recovered.JobID != original.JobID || recovered.Request.OperationID != first.OperationID || recovered.CancelRequested || recovered.Receipt.SHA256 == "" || starts.Load() != 1 || cancels.Load() != 0 {
		t.Fatalf("successor failed to reconcile the same job: %+v starts=%d cancels=%d", recovered, starts.Load(), cancels.Load())
	}
}
