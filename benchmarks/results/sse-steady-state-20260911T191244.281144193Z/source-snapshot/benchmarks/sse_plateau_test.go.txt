package benchmarks

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

type sustainedCycle struct {
	churnCycle
	FirstCursor    uint64 `json:"first_cursor"`
	FinalCursor    uint64 `json:"final_cursor"`
	Published      int    `json:"published_events"`
	Delivered      int    `json:"delivered_events"`
	ExactSequences bool   `json:"all_client_sequences_exact"`
}

func TestSSESustainedChurnEvidence(t *testing.T) {
	runSSESustainedChurn(t, false)
}

func TestSSESteadyStateEvidence(t *testing.T) {
	runSSESustainedChurn(t, true)
}

func runSSESustainedChurn(t *testing.T, steady bool) {
	if os.Getenv("FORGE_RUN_SUSTAINED_SSE") != "1" {
		t.Skip("set FORGE_RUN_SUSTAINED_SSE=1 and FORGE_TEST_DATABASE_URL")
	}
	const perRound, eventsPerRound = 100, 10
	rounds, warmupRounds := 24, 3
	if steady {
		rounds, warmupRounds = 72, 128
	}
	const cycleDuration = 5 * time.Second
	ctx, end := context.WithTimeout(context.Background(), time.Duration(rounds)*5*time.Second+180*time.Second)
	defer end()
	started := time.Now()
	kind := "sse-sustained-churn"
	if steady {
		kind = "sse-steady-state"
	}
	out := sustainedOutput(t, started, kind)
	f := sustainedSetup(t, ctx)
	var rowsTrace *sseRowBatchTrace
	var rowBytes uintptr
	if steady {
		rowsTrace, rowBytes = traceSSEAuthenticationRows(t, ctx, f)
	}
	h := sustainedServer(f, 0)
	defer h.server.Close()
	transport := h.transport(false, 0)
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	tick := 0
	cursor := f.run.CoveredSeq
	// Preallocation keeps the evidence harness from introducing a growing series
	// of retained per-delivery samples into the memory plateau measurement.
	results := make([]sustainedCycle, rounds)
	closedSamples := make([]sseResources, rounds)
	cycle := func(round int, duration time.Duration) (sustainedCycle, error) {
		begin := time.Now()
		if _, err := f.owner.Heartbeat(ctx, f.run.TenantID, f.run.ID, f.run.State.Lease.Owner, f.run.State.Lease.Epoch, 5*time.Minute); err != nil {
			return sustainedCycle{}, err
		}
		beforeStarts, beforeReturns := h.starts.Load(), h.returns.Load()
		result := sustainedCycle{churnCycle: churnCycle{Round: round, Expected: perRound}, FirstCursor: cursor}
		readersCtx, closeReaders := context.WithCancel(ctx)
		ready := make(chan error, perRound)
		readers := make([]sustainedReader, perRound)
		var wg sync.WaitGroup
		for i := range readers {
			wg.Go(func() { f.read(readersCtx, client, h.server.URL, result.FirstCursor, i, false, &readers[i], ready) })
		}
		var problem error
		if err := sustainedReady(ctx, perRound, ready); err != nil {
			problem = err
		} else {
			result.Connected = perRound
		}
		result.OpenResources = sseMeasure(fmt.Sprintf("round_%d_open", round))
		result.TCPAtOpen = h.activeTCP.Load()
		result.HandlersAtOpen = h.activeHandlers.Load()
		if problem == nil {
			for range eventsPerRound {
				if duration > 0 {
					timer := time.NewTimer(200 * time.Millisecond)
					select {
					case <-ctx.Done():
						timer.Stop()
						problem = ctx.Err()
					case <-timer.C:
					}
				}
				if problem != nil {
					break
				}
				tick++
				if err := f.append(ctx, tick, 1024); err != nil {
					problem = err
					break
				}
				result.Published++
				cursor++
			}
		}
		if problem == nil && !sustainedWait(ctx, 3*time.Second, func() bool {
			for i := range readers {
				if readers[i].delivered.Load() != eventsPerRound {
					return false
				}
			}
			return true
		}) {
			problem = fmt.Errorf("healthy streams failed to receive every committed event")
		}
		// Hold real requests open for most of each fixed cycle, then leave a quiet
		// interval. The experiment has a minimum measured duration, not 24 rapid loops.
		if problem == nil && duration > 0 {
			timer := time.NewTimer(max(time.Duration(0), time.Until(begin.Add(4*time.Second))))
			select {
			case <-ctx.Done():
				timer.Stop()
				problem = ctx.Err()
			case <-timer.C:
			}
		}
		closeReaders()
		wg.Wait()
		transport.CloseIdleConnections()
		cleaned := sustainedWait(ctx, 3*time.Second, func() bool { return h.activeTCP.Load() == 0 && h.activeHandlers.Load() == 0 })
		if !cleaned && problem == nil {
			problem = fmt.Errorf("TCP or handler did not close")
		}
		result.ExactSequences = true
		result.FinalCursor = cursor
		for i := range readers {
			r := &readers[i]
			result.Delivered += r.Count
			if r.Error != "" {
				result.ClientErrors = append(result.ClientErrors, r.Error)
			}
			result.ExactSequences = result.ExactSequences && r.Error == "" && r.Count == eventsPerRound && r.LastSeq == cursor
		}
		result.TCPAfterClose = h.activeTCP.Load()
		result.HandlersAfterClose = h.activeHandlers.Load()
		result.HandlerStarts = h.starts.Load() - beforeStarts
		result.HandlerReturns = h.returns.Load() - beforeReturns
		// Socket observations are useful for backpressure reports, but retaining a
		// per-connection list would itself manufacture memory growth in this test.
		h.socketsMu.Lock()
		h.sockets = h.sockets[:0]
		h.socketsMu.Unlock()
		if duration > 0 {
			timer := time.NewTimer(max(time.Duration(0), time.Until(begin.Add(duration))))
			select {
			case <-ctx.Done():
				timer.Stop()
				if problem == nil {
					problem = ctx.Err()
				}
			case <-timer.C:
			}
		}
		runtime.GC()
		result.ClosedResources = sseMeasure(fmt.Sprintf("round_%d_closed_post_gc", round))
		result.DurationSeconds = time.Since(begin).Seconds()
		result.Passed = problem == nil && result.Connected == perRound && result.TCPAtOpen == perRound && result.HandlersAtOpen == perRound && result.TCPAfterClose == 0 && result.HandlersAfterClose == 0 && result.HandlerStarts == perRound && result.HandlerReturns == perRound && result.Published == eventsPerRound && result.Delivered == perRound*eventsPerRound && result.ExactSequences
		return result, problem
	}
	actualWarmup := 0
	for round := range warmupRounds {
		result, err := cycle(-round-1, 0)
		if err != nil || !result.Passed {
			t.Fatalf("warmup cycle failed: %+v %v", result, err)
		}
		actualWarmup++
		if steady && actualWarmup >= 3 && rowsTrace.minimum() >= 256 && rowsTrace.connections() == int(f.api.Pool.Config().MaxConns) {
			break
		}
	}
	if steady && (rowsTrace.minimum() < 256 || rowsTrace.connections() != int(f.api.Pool.Config().MaxConns)) {
		t.Fatal("prewarm did not exercise every API connection for at least 256 authenticated pool rows")
	}
	var warmedRows map[uint32]int
	if steady {
		warmedRows = rowsTrace.snapshot()
	}
	sustainedProfile(t, out, "warmup")
	runtime.GC()
	baseline := sseMeasure("warmed_services_and_profile_writer")
	if baseline.FDs < 0 {
		t.Fatal("Linux FD evidence unavailable")
	}
	measureStart := time.Now()
	passed := true
	completed := 0
	for round := range rounds {
		result, err := cycle(round+1, cycleDuration)
		result.GoroutineDelta = result.ClosedResources.Goroutines - baseline.Goroutines
		result.FDDelta = result.ClosedResources.FDs - baseline.FDs
		result.Passed = result.Passed && result.GoroutineDelta <= 8 && result.FDDelta <= 4
		results[round] = result
		closedSamples[round] = result.ClosedResources
		completed++
		passed = passed && result.Passed
		t.Logf("round=%d heap=%d goroutines=%d fds=%d delivered=%d closed=%v passed=%v", round+1, result.ClosedResources.HeapAlloc, result.ClosedResources.Goroutines, result.ClosedResources.FDs, result.Delivered, result.TCPAfterClose == 0 && result.HandlersAfterClose == 0, result.Passed)
		if round == 5 || round == 11 || steady && (round == 35 || round == 59) {
			sustainedProfile(t, out, fmt.Sprintf("round-%02d", round+1))
		}
		if err != nil {
			t.Logf("cycle_error=%v", err)
			break
		}
	}
	measureSeconds := time.Since(measureStart).Seconds()
	sustainedProfile(t, out, "final")
	results = results[:completed]
	closedSamples = closedSamples[:completed]
	plateau := assessSSEPlateau(closedSamples)
	var steadyBounds map[string]any
	if steady {
		plateau, steadyBounds = assessSSESteadyState(closedSamples, rowsTrace, warmedRows, rowBytes, int(f.api.Pool.Config().MaxConns))
	}
	var durable, gaps int
	if err := f.owner.Pool.QueryRow(ctx, `SELECT count(*) FROM run_events WHERE type='benchmark.sustained'`).Scan(&durable); err != nil {
		t.Fatal(err)
	}
	if err := f.owner.Pool.QueryRow(ctx, `SELECT count(*) FROM (SELECT run_id FROM run_events GROUP BY run_id HAVING min(seq)<>1 OR count(*)<>max(seq)) g`).Scan(&gaps); err != nil {
		t.Fatal(err)
	}
	// Compare the API snapshot with the final durable sequence boundary.
	finalRun, err := f.api.GetRun(ctx, f.run.TenantID, f.run.ID)
	cursorMatches := err == nil && finalRun.CoveredSeq == cursor
	passed = passed && completed == rounds && measureSeconds >= float64(rounds)*cycleDuration.Seconds() && plateau.Passed && durable == (actualWarmup+rounds)*eventsPerRound && gaps == 0 && cursorMatches && f.slowQueues.Load() == 0
	manifest := sustainedManifest(t)
	data, err := os.ReadFile(filepath.Join("..", "benchmarks/sse_plateau_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	manifest["source_sha256"].(map[string]string)["benchmarks/sse_plateau_test.go"] = hex.EncodeToString(sum[:])
	if steady {
		extra, err := os.ReadFile(filepath.Join("..", "benchmarks/sse_steady_test.go"))
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(extra)
		manifest["source_sha256"].(map[string]string)["benchmarks/sse_steady_test.go"] = hex.EncodeToString(digest[:])
	}
	report := map[string]any{"schema_version": 1, "timestamp_utc": started.UTC(), "scope": "Sustained actual TCP/RLS SSE publish/open/cancel lifecycles and post-GC heap plateau over a minimum two-minute measured window", "manifest": manifest, "database": f.database, "configuration": map[string]any{"rounds": rounds, "connections_per_round": perRound, "warmup_rounds": actualWarmup, "events_per_round": eventsPerRound, "payload_bytes": 1024, "minimum_cycle_seconds": cycleDuration.Seconds(), "minimum_measurement_seconds": rounds * 5, "publish_interval_ms": 200, "open_hold_seconds": 4, "goroutine_allowance": 8, "fd_allowance": 4, "socket_buffers": "unmodified defaults", "heap_window_samples": 6, "slope_window": "last half of closed-round samples"}, "baseline": baseline, "cycles": results, "post_gc_heap_plateau": plateau, "steady_state_bounds": steadyBounds, "measured_seconds": measureSeconds, "invariants": map[string]any{"passed": passed, "completed_rounds": completed, "measured_connections": completed * perRound, "durable_events_including_warmup": durable, "durable_sequence_gaps": gaps, "final_cursor_matches": cursorMatches, "slow_queue_overflows": f.slowQueues.Load()}, "profiles": []string{"warmup-heap.pprof", "warmup-goroutine.pprof", "round-06-heap.pprof", "round-06-goroutine.pprof", "round-12-heap.pprof", "round-12-goroutine.pprof", "final-heap.pprof", "final-goroutine.pprof"}, "limitations": []string{"API, TCP clients and publisher share one process; the PostgreSQL service is separate. Measurements include the harness and runtime caches.", "Three full-size warmup cycles precede 24 measured five-second cycles. The result constrains this finite workload, not every future workload or infinite lifetime.", "Post-GC HeapAlloc must have last-six minus first-six mean growth <=1 MiB and last-half OLS slope <=8 KiB/s. These predeclared tolerances do not mean exactly zero retained allocations.", "Profiles are saved after warmup, rounds 6 and 12 and final. HeapInuse, FDs and goroutines are retained alongside HeapAlloc; no forced memory return to the operating system is assumed.", "Per-delivery samples and unbounded per-connection records are not retained during measurement, to avoid manufacturing a harness-owned memory trend.", "No models, effects, containers, retention deletion or service outage occurs. The separate backpressure experiment proves real slow-client transport closure."}}
	if steady {
		report["configuration"].(map[string]any)["heap_window_samples"] = 12
		report["scope"] = "Six-minute steady-state SSE churn after every API connection performs at least 256 auth queries; bounded pgx row-batch retention and stricter heap oracle"
		report["profiles"] = []string{"warmup-heap.pprof", "warmup-goroutine.pprof", "round-06-heap.pprof", "round-06-goroutine.pprof", "round-12-heap.pprof", "round-12-goroutine.pprof", "round-36-heap.pprof", "round-36-goroutine.pprof", "round-60-heap.pprof", "round-60-goroutine.pprof", "final-heap.pprof", "final-goroutine.pprof"}
		report["limitations"] = []string{"Finite local process-level measurement; API, clients and publisher share the process and host may run other work.", "Prewarm stops only after all 16 fixed API connections have at least 256 auth QueryRow calls. No production buffer or pool bound is increased.", "72 five-second measured cycles yield a minimum six-minute interval, 7200 new TCP connections, and multiple 128-row pool batch replacements; per-connection counters verify the minimum separately.", "The stricter oracle requires first/last 12-sample mean growth <=256 KiB, last-half positive slope <=1 KiB/s, and full-window heap range <=1536 KiB. Values remain reported even on failure.", "The 128-row pgx batch cap is inspected in the pinned dependency source. Reported baseRows size uses reflect.Type.Size on an actual closed raw QueryRow; its cleared context/args are source-inspected, not inferred from counters.", "No model, effect, container or outage occurs. A finite bound does not imply universal absence of leaks."}
	}
	sustainedWriteReport(t, out, report)
	t.Logf("passed=%v duration=%.3fs growth=%.0fB slope=%.1fB/s completed=%d", passed, measureSeconds, plateau.WindowGrowthBytes, plateau.LastHalfSlopeBytesPerSecond, completed)
	if !passed {
		t.Error("sustained SSE lifecycle/heap plateau acceptance failed; inspect report and profiles")
	}
}
