package benchmarks

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/eventstream"
	"github.com/JDinSeattle/forge-runtime/internal/httpapi"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/testutil"
)

type churnCycle struct {
	Round              int          `json:"round"`
	Expected           int          `json:"expected_connections"`
	Connected          int          `json:"connected"`
	ClientErrors       []string     `json:"client_errors,omitempty"`
	OpenResources      sseResources `json:"open_resources"`
	ClosedResources    sseResources `json:"closed_resources"`
	TCPAtOpen          int64        `json:"tcp_at_open"`
	HandlersAtOpen     int64        `json:"handlers_at_open"`
	TCPAfterClose      int64        `json:"tcp_after_close"`
	HandlersAfterClose int64        `json:"handlers_after_close"`
	HandlerStarts      int64        `json:"handler_starts"`
	HandlerReturns     int64        `json:"handler_returns"`
	GoroutineDelta     int          `json:"goroutine_delta"`
	FDDelta            int          `json:"fd_delta"`
	DurationSeconds    float64      `json:"duration_seconds"`
	Passed             bool         `json:"passed"`
}

// TestSSEConnectionChurnEvidence measures five bounded connection lifecycles.
// It asserts TCP and handler cleanup independently from process resource counts.
func TestSSEConnectionChurnEvidence(t *testing.T) {
	if os.Getenv("FORGE_RUN_BENCHMARK") != "1" {
		t.Skip("set FORGE_RUN_BENCHMARK=1 and FORGE_TEST_DATABASE_URL")
	}
	const rounds, perRound = 5, 50
	const goroutineAllowance, fdAllowance = 8, 4
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	started := time.Now()
	owner := testutil.Database(t)
	if err := owner.BootstrapTenant(ctx, "churn_tenant", "churn_viewer", "viewer"); err != nil {
		t.Fatal(err)
	}
	if err := owner.RegisterRunner(ctx, "churn_runner", "unix:///not-dispatched.sock", 1); err != nil {
		t.Fatal(err)
	}
	project, err := owner.CreateProject(ctx, "churn_tenant", "SSE churn fixture", "no-source-dispatched", "no-profile-dispatched")
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = owner.Submit(ctx, persistence.SubmitRequest{TenantID: "churn_tenant", PrincipalID: "churn_viewer", ProjectID: project.ID, Task: "Synthetic active run for finite SSE connection churn", BaseCommit: "no-checkout", Config: persistence.Config{Provider: "fake", Model: "not-dispatched", MaxModelRounds: 1, MaxToolCalls: 1, MaxRuntimeSeconds: 300}}, "churn-run")
	if err != nil {
		t.Fatal(err)
	}
	run, err := owner.ClaimOnRunner(ctx, "churn-publisher", 5*time.Minute, "churn_runner")
	if err != nil {
		t.Fatal(err)
	}
	token, err := owner.IssueToken(ctx, "churn_viewer", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	apiStore := sseNonOwnerPool(t, ctx, owner)
	sseWarmPool(t, ctx, owner.Pool)
	sseWarmPool(t, ctx, apiStore.Pool)
	var databaseVersion string
	if err := owner.Pool.QueryRow(ctx, `SELECT version()`).Scan(&databaseVersion); err != nil {
		t.Fatal(err)
	}
	manager := eventstream.New(ctx, apiStore, eventstream.Config{})
	defer manager.Close()
	api := (&httpapi.Server{Store: apiStore, Streams: manager}).Handler()
	var tcpActive, handlersActive, handlerStarts, handlerReturns atomic.Int64
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlersActive.Add(1)
		handlerStarts.Add(1)
		defer func() {
			handlerReturns.Add(1)
			handlersActive.Add(-1)
		}()
		api.ServeHTTP(w, r)
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			tcpActive.Add(1)
		case http.StateClosed, http.StateHijacked:
			tcpActive.Add(-1)
		}
	}
	server.Start()
	defer server.Close()
	transport := &http.Transport{MaxIdleConns: perRound, MaxIdleConnsPerHost: perRound, ForceAttemptHTTP2: false}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	cycle := func(round, count int) (churnCycle, error) {
		begin := time.Now()
		beforeStarts, beforeReturns := handlerStarts.Load(), handlerReturns.Load()
		result := churnCycle{Round: round, Expected: count}
		requestCtx, closeRequests := context.WithCancel(ctx)
		defer closeRequests()
		ready := make(chan error, count)
		var readers sync.WaitGroup
		for range count {
			readers.Go(func() {
				req, _ := http.NewRequestWithContext(requestCtx, "GET", server.URL+"/v1/runs/"+string(run.ID)+"/events", nil)
				req.Header.Set("Authorization", "Bearer "+token)
				req.Header.Set("X-Forge-Tenant", string(run.TenantID))
				req.Header.Set("Last-Event-ID", strconv.FormatUint(run.CoveredSeq, 10))
				response, err := client.Do(req)
				if err != nil {
					ready <- err
					return
				}
				defer response.Body.Close()
				if response.StatusCode != http.StatusOK {
					ready <- fmt.Errorf("HTTP %d", response.StatusCode)
					return
				}
				ready <- nil
				_, _ = io.Copy(io.Discard, response.Body)
			})
		}
		startup, stopStartup := context.WithTimeout(ctx, 4*time.Second)
		defer stopStartup()
	readyLoop:
		for range count {
			select {
			case err := <-ready:
				if err != nil {
					result.ClientErrors = append(result.ClientErrors, err.Error())
				} else {
					result.Connected++
				}
			case <-startup.Done():
				result.ClientErrors = append(result.ClientErrors, "subscriptions did not become ready within four seconds")
				break readyLoop
			}
		}
		result.OpenResources = sseMeasure(fmt.Sprintf("round_%d_open", round))
		result.TCPAtOpen = tcpActive.Load()
		result.HandlersAtOpen = handlersActive.Load()
		closeRequests()
		readers.Wait()
		transport.CloseIdleConnections()
		// Read client completion and real server ConnState separately: a canceled
		// client alone does not demonstrate that its server handler has returned.
		deadline := time.Now().Add(2 * time.Second)
		for (tcpActive.Load() != 0 || handlersActive.Load() != 0) && time.Now().Before(deadline) && ctx.Err() == nil {
			time.Sleep(10 * time.Millisecond)
		}
		time.Sleep(50 * time.Millisecond)
		runtime.GC()
		result.ClosedResources = sseMeasure(fmt.Sprintf("round_%d_closed", round))
		result.TCPAfterClose = tcpActive.Load()
		result.HandlersAfterClose = handlersActive.Load()
		result.HandlerStarts = handlerStarts.Load() - beforeStarts
		result.HandlerReturns = handlerReturns.Load() - beforeReturns
		result.DurationSeconds = time.Since(begin).Seconds()
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		return result, nil
	}
	// Warm one full HTTP/auth/RLS/subscription lifecycle before the baseline;
	// the measured five rounds still create 250 additional TCP connections.
	warmup, err := cycle(0, 1)
	if err != nil || warmup.Connected != 1 || warmup.TCPAfterClose != 0 || warmup.HandlersAfterClose != 0 {
		t.Fatalf("warmup lifecycle did not close: %+v %v", warmup, err)
	}
	baseline := warmup.ClosedResources
	if baseline.FDs < 0 {
		t.Fatal("Linux /proc/self/fd is required for FD evidence")
	}
	results := make([]churnCycle, 0, rounds)
	passed := true
	for round := 1; round <= rounds; round++ {
		result, err := cycle(round, perRound)
		result.GoroutineDelta = result.ClosedResources.Goroutines - baseline.Goroutines
		result.FDDelta = result.ClosedResources.FDs - baseline.FDs
		result.Passed = err == nil && result.Connected == perRound && len(result.ClientErrors) == 0 && result.TCPAtOpen == perRound && result.HandlersAtOpen == perRound && result.TCPAfterClose == 0 && result.HandlersAfterClose == 0 && result.HandlerStarts == perRound && result.HandlerReturns == perRound && result.GoroutineDelta <= goroutineAllowance && result.FDDelta <= fdAllowance
		results = append(results, result)
		passed = passed && result.Passed
		if err != nil {
			break
		}
	}
	passed = passed && len(results) == rounds
	output := filepath.Join("results", "local", "churn-"+started.UTC().Format("20060102T150405.000000000Z"))
	if err := os.MkdirAll(output, 0755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"heap", "goroutine"} {
		file, err := os.Create(filepath.Join(output, name+".pprof"))
		if err != nil {
			t.Fatal(err)
		}
		err = pprof.Lookup(name).WriteTo(file, 0)
		closeErr := file.Close()
		if err != nil || closeErr != nil {
			t.Fatalf("write %s profile: %v %v", name, err, closeErr)
		}
	}
	sources := map[string]string{}
	for _, path := range []string{"benchmarks/churn_test.go", "benchmarks/sse_test.go", "go.mod", "internal/httpapi/sse.go", "internal/httpapi/server.go", "internal/eventstream/hub.go", "internal/persistence/identity.go", "internal/persistence/store.go"} {
		b, err := os.ReadFile(filepath.Join("..", path))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(b)
		sources[path] = hex.EncodeToString(sum[:])
	}
	report := map[string]any{"schema_version": 1, "timestamp_utc": started.UTC(), "scope": "Five finite actual TCP HTTP/1.1 SSE open/cancel cycles using real PostgreSQL auth/RLS API reads", "machine": machine(), "database_version": databaseVersion, "configuration": map[string]any{"rounds": rounds, "connections_per_round": perRound, "warmup_connections": 1, "writer_pool_max": owner.Pool.Config().MaxConns, "api_pool_max": apiStore.Pool.Config().MaxConns, "max_elapsed_seconds": 25, "server_cleanup_grace_seconds": 2, "post_close_settle_ms": 50, "max_goroutines_above_baseline": goroutineAllowance, "max_fds_above_baseline": fdAllowance}, "warmup": warmup, "baseline": baseline, "rounds": results, "passed": passed, "actual_elapsed_seconds": time.Since(started).Seconds(), "profiles": []string{"heap.pprof", "goroutine.pprof"}, "source_sha256": sources, "limitations": []string{"Five rounds of 50 connections is finite lifecycle evidence, not proof of long-term absence of leaks.", "API and clients share one Go process; heap, goroutines and FDs include both and the harness. PostgreSQL runs separately.", "The baseline follows warmed database pools and one complete SSE lifecycle. Listener and pools remain open for every measurement.", "FD/goroutine assertions allow fixed small process-level slack above baseline, but require zero actual TCP connections and zero SSE handlers after every round.", "Heap is measured after GC but is reported without a leak assertion; runtime caches and the harness can retain allocations.", "No event publication, long-lived slow-client backpressure, reconnect replay, models, effects or containers are exercised."}}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(output, "report.json"), append(encoded, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
	abs, _ := filepath.Abs(output)
	t.Logf("evidence=%s passed=%v baseline=%+v", abs, passed, baseline)
	for _, round := range results {
		t.Logf("round=%d connected=%d TCP_after=%d handlers_after=%d goroutines=%d(delta=%+d) fds=%d(delta=%+d) heap=%d passed=%v", round.Round, round.Connected, round.TCPAfterClose, round.HandlersAfterClose, round.ClosedResources.Goroutines, round.GoroutineDelta, round.ClosedResources.FDs, round.FDDelta, round.ClosedResources.HeapAlloc, round.Passed)
	}
	if !passed {
		t.Errorf("finite SSE lifecycle/resource bound failed; inspect %s", abs)
	}
}
