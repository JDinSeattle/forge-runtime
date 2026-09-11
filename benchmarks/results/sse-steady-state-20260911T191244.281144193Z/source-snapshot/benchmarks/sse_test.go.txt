package benchmarks

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/eventstream"
	"github.com/JDinSeattle/forge-runtime/internal/httpapi"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/testutil"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type sseSample struct {
	Client     int       `json:"client"`
	Run        int       `json:"run_index"`
	Tick       int       `json:"tick"`
	Seq        uint64    `json:"seq"`
	CreatedAt  time.Time `json:"created_at"`
	ReceivedAt time.Time `json:"received_at"`
	LatencyMS  float64   `json:"latency_ms"`
}
type sseClient struct {
	Index      int         `json:"index"`
	Run        int         `json:"run_index"`
	Slow       bool        `json:"intentionally_not_reading"`
	Connected  bool        `json:"connected"`
	Received   int         `json:"received"`
	Duplicates int         `json:"duplicates"`
	Gaps       int         `json:"gaps"`
	Error      string      `json:"error,omitempty"`
	Samples    []sseSample `json:"samples,omitempty"`
}
type ssePublish struct {
	Run                int       `json:"run_index"`
	Tick               int       `json:"tick"`
	ScheduledAt        time.Time `json:"scheduled_at"`
	CommittedAt        time.Time `json:"committed_at"`
	ScheduleLatenessMS float64   `json:"schedule_lateness_ms"`
	AppendMS           float64   `json:"append_ms"`
	Error              string    `json:"error,omitempty"`
}
type sseResources struct {
	Stage      string    `json:"stage"`
	At         time.Time `json:"at"`
	HeapAlloc  uint64    `json:"heap_alloc_bytes"`
	HeapInuse  uint64    `json:"heap_inuse_bytes"`
	Goroutines int       `json:"goroutines"`
	FDs        int       `json:"fds"`
}

func sseMeasure(stage string) sseResources {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fds := -1
	if entries, err := os.ReadDir("/proc/self/fd"); err == nil {
		fds = len(entries)
	}
	return sseResources{Stage: stage, At: time.Now(), HeapAlloc: m.HeapAlloc, HeapInuse: m.HeapInuse, Goroutines: runtime.NumGoroutine(), FDs: fds}
}

func sseNonOwnerPool(t *testing.T, ctx context.Context, owner *persistence.Store) *persistence.Store {
	t.Helper()
	role := pgx.Identifier{strings.ToLower(string(persistence.NewID("sse_api")))}.Sanitize()
	var schema string
	if err := owner.Pool.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Pool.Exec(ctx, `CREATE ROLE `+role+` NOLOGIN NOSUPERUSER NOBYPASSRLS NOINHERIT`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		// This unique role only receives privileges in this test's private schema.
		if _, err := owner.Pool.Exec(cleanup, `DROP OWNED BY `+role); err != nil {
			t.Errorf("revoke own benchmark role grants: %v", err)
		}
		if _, err := owner.Pool.Exec(cleanup, `DROP ROLE `+role); err != nil {
			t.Errorf("drop own benchmark role: %v", err)
		}
	})
	for _, query := range []string{`GRANT USAGE ON SCHEMA ` + pgx.Identifier{schema}.Sanitize() + ` TO ` + role, `GRANT SELECT ON ALL TABLES IN SCHEMA ` + pgx.Identifier{schema}.Sanitize() + ` TO ` + role} {
		if _, err := owner.Pool.Exec(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	cfg := owner.Pool.Config()
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, `SET ROLE `+role)
		return err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	api := &persistence.Store{Pool: pool}
	if err := api.CheckAPIRole(ctx); err != nil {
		t.Fatal(err)
	}
	return api
}

func sseWarmPool(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	connections := make([]*pgxpool.Conn, 0, pool.Config().MaxConns)
	defer func() {
		for _, c := range connections {
			c.Release()
		}
	}()
	for range int(pool.Config().MaxConns) {
		c, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		connections = append(connections, c)
	}
}

// TestSSEFanoutEvidence is an opt-in, fixed 12-second experiment. A real TCP
// server uses the actual API/SSE handler and an actual non-owner RLS pool.
// Publishers use real committed AppendWorkerEvent transactions; no models,
// runner operations, containers, or artificial handler delays are involved.
func TestSSEFanoutEvidence(t *testing.T) {
	if os.Getenv("FORGE_RUN_BENCHMARK") != "1" {
		t.Skip("set FORGE_RUN_BENCHMARK=1 and FORGE_TEST_DATABASE_URL")
	}
	const runCount, perRun, slowPerRun, ticks, rate = 20, 10, 1, 60, 5
	const seed uint64 = 20260908
	const payloadBytes = 1024
	const window = 12 * time.Second
	const interval = time.Second / rate
	const clientsCount = runCount * perRun
	const healthyCount = runCount * (perRun - slowPerRun)
	ctx, end := context.WithTimeout(context.Background(), 70*time.Second)
	defer end()
	owner := testutil.Database(t)
	started := time.Now()
	var dbVersion, fsync, synchronousCommit string
	if err := owner.Pool.QueryRow(ctx, `SELECT version(),current_setting('fsync'),current_setting('synchronous_commit')`).Scan(&dbVersion, &fsync, &synchronousCommit); err != nil {
		t.Fatal(err)
	}
	if err := owner.BootstrapTenant(ctx, "sse_tenant", "sse_viewer", "viewer"); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Pool.Exec(ctx, `UPDATE tenant_runtime SET max_active=$1`, runCount); err != nil {
		t.Fatal(err)
	}
	if err := owner.RegisterRunner(ctx, "sse_runner", "unix:///not-dispatched.sock", runCount); err != nil {
		t.Fatal(err)
	}
	project, err := owner.CreateProject(ctx, "sse_tenant", "SSE load fixture", "no-source-dispatched", "no-profile-dispatched")
	if err != nil {
		t.Fatal(err)
	}
	runs := make([]persistence.Run, runCount)
	for i := range runs {
		r, _, err := owner.Submit(ctx, persistence.SubmitRequest{TenantID: "sse_tenant", PrincipalID: "sse_viewer", ProjectID: project.ID, Task: "Synthetic active run for persisted SSE events only", BaseCommit: "no-checkout", Config: persistence.Config{Provider: "fake", Model: "not-dispatched", MaxModelRounds: 1, MaxToolCalls: 1, MaxRuntimeSeconds: 300}}, fmt.Sprintf("sse-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		claimed, err := owner.ClaimOnRunner(ctx, fmt.Sprintf("sse-publisher-%d", i), 5*time.Minute, "sse_runner")
		if err != nil || claimed.ID != r.ID {
			t.Fatalf("synthetic active claim: %v", err)
		}
		runs[i] = claimed
	}
	token, err := owner.IssueToken(ctx, "sse_viewer", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	apiStore := sseNonOwnerPool(t, ctx, owner)
	sseWarmPool(t, ctx, owner.Pool)
	sseWarmPool(t, ctx, apiStore.Pool)
	var queueSlow, slowServerClosed atomic.Int64
	manager := eventstream.New(ctx, apiStore, eventstream.Config{Slow: func() { queueSlow.Add(1) }})
	defer manager.Close()
	api := (&httpapi.Server{Store: apiStore, Streams: manager}).Handler()
	clients := make([]sseClient, clientsCount)
	random := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	for run := range runCount {
		slow := random.IntN(perRun)
		for j := range perRun {
			i := run*perRun + j
			clients[i] = sseClient{Index: i, Run: run, Slow: j == slow}
		}
	}
	// Immutable classification avoids reading client-owned result fields from
	// handler goroutines while their stream readers are recording samples.
	slowClients := make([]bool, clientsCount)
	for i := range clients {
		slowClients[i] = clients[i].Slow
	}
	var harnessClosing atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i, _ := strconv.Atoi(r.Header.Get("X-Benchmark-Client"))
		defer func() {
			if i >= 0 && i < len(slowClients) && slowClients[i] && !harnessClosing.Load() {
				slowServerClosed.Add(1)
			}
		}()
		api.ServeHTTP(w, r)
	}))
	defer server.Close()
	transport := &http.Transport{MaxIdleConns: clientsCount, MaxIdleConnsPerHost: clientsCount, ForceAttemptHTTP2: false}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	clientCtx, closeClients := context.WithCancel(ctx)
	defer closeClients()
	var readerWG sync.WaitGroup
	ready := make(chan error, clientsCount)
	caughtUp := make(chan int, healthyCount)
	resources := []sseResources{}
	runtime.GC()
	resources = append(resources, sseMeasure("warm_services_before_connections"))
	for i := range clients {
		readerWG.Go(func() {
			result := &clients[i]
			run := runs[result.Run]
			req, _ := http.NewRequestWithContext(clientCtx, "GET", server.URL+"/v1/runs/"+string(run.ID)+"/events", nil)
			req.Header.Set("Authorization", "Bearer "+token)
			req.Header.Set("X-Forge-Tenant", string(run.TenantID))
			req.Header.Set("Last-Event-ID", strconv.FormatUint(run.CoveredSeq, 10))
			req.Header.Set("X-Benchmark-Client", strconv.Itoa(i))
			response, err := client.Do(req)
			if err != nil {
				result.Error = err.Error()
				ready <- err
				return
			}
			defer response.Body.Close()
			if response.StatusCode != 200 {
				body, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
				result.Error = fmt.Sprintf("HTTP %d: %s", response.StatusCode, body)
				ready <- fmt.Errorf("client %d: %s", i, result.Error)
				return
			}
			result.Connected = true
			ready <- nil
			if result.Slow {
				<-clientCtx.Done()
				return
			}
			scanner := bufio.NewScanner(response.Body)
			scanner.Buffer(make([]byte, 4096), 64<<10)
			last := run.CoveredSeq
			var eventID uint64
			var data []byte
			for scanner.Scan() {
				line := scanner.Text()
				if strings.HasPrefix(line, "id: ") {
					eventID, _ = strconv.ParseUint(strings.TrimPrefix(line, "id: "), 10, 64)
					continue
				}
				if strings.HasPrefix(line, "data: ") {
					data = append(data[:0], strings.TrimPrefix(line, "data: ")...)
					continue
				}
				if line != "" || len(data) == 0 {
					continue
				}
				received := time.Now()
				var event persistence.Event
				if err := json.Unmarshal(data, &event); err != nil {
					result.Error = err.Error()
					return
				}
				data = nil
				if event.RunID != run.ID || event.Type != "benchmark.sample" || event.Seq != eventID {
					result.Error = "event identity/type mismatch"
					return
				}
				if event.Seq <= last {
					result.Duplicates++
					continue
				}
				if event.Seq != last+1 {
					result.Gaps++
					result.Error = fmt.Sprintf("sequence gap: %d to %d", last, event.Seq)
					return
				}
				var payload struct {
					Tick int `json:"tick"`
				}
				if err := json.Unmarshal(event.Payload, &payload); err != nil {
					result.Error = err.Error()
					return
				}
				if payload.Tick != result.Received+1 {
					result.Error = "payload tick mismatch"
					return
				}
				last = event.Seq
				result.Received++
				result.Samples = append(result.Samples, sseSample{Client: i, Run: result.Run, Tick: payload.Tick, Seq: event.Seq, CreatedAt: event.CreatedAt, ReceivedAt: received, LatencyMS: received.Sub(event.CreatedAt).Seconds() * 1000})
				if result.Received == ticks {
					caughtUp <- i
				}
			}
			if clientCtx.Err() == nil {
				if err := scanner.Err(); err != nil {
					result.Error = err.Error()
				} else if result.Received < ticks {
					result.Error = "stream ended before final committed event"
				}
			}
		})
	}
	startupTimer := time.NewTimer(15 * time.Second)
	defer startupTimer.Stop()
	for range clientsCount {
		select {
		case err := <-ready:
			if err != nil {
				closeClients()
				readerWG.Wait()
				t.Fatal(err)
			}
		case <-startupTimer.C:
			closeClients()
			readerWG.Wait()
			t.Fatal("200 TCP SSE subscriptions did not start within 15 seconds")
		}
	}
	resources = append(resources, sseMeasure("all_200_connections_ready"))
	producerStart := time.Now()
	var resourceMu sync.Mutex
	sampleCtx, stopSampling := context.WithCancel(ctx)
	var sampleWG sync.WaitGroup
	sampleWG.Go(func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-sampleCtx.Done():
				return
			case <-ticker.C:
				r := sseMeasure("during_publication")
				resourceMu.Lock()
				resources = append(resources, r)
				resourceMu.Unlock()
			}
		}
	})
	publication := make([][]ssePublish, runCount)
	var producerWG sync.WaitGroup
	for i := range runs {
		producerWG.Go(func() {
			for tick := 1; tick <= ticks; tick++ {
				due := producerStart.Add(time.Duration(tick) * interval)
				timer := time.NewTimer(max(time.Duration(0), time.Until(due)))
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
				// PostgreSQL JSONB's two-field text representation adds three
				// spaces to compact JSON. Assert actual stored size below.
				payload := map[string]any{"tick": tick, "chunk": ""}
				base, _ := json.Marshal(payload)
				payload["chunk"] = strings.Repeat("x", payloadBytes-len(base)-3)
				raw, _ := json.Marshal(payload)
				at := time.Now()
				err := owner.AppendWorkerEvent(ctx, runs[i], "benchmark.sample", raw)
				committed := time.Now()
				sample := ssePublish{Run: i, Tick: tick, ScheduledAt: due, CommittedAt: committed, ScheduleLatenessMS: at.Sub(due).Seconds() * 1000, AppendMS: committed.Sub(at).Seconds() * 1000}
				if err != nil {
					sample.Error = err.Error()
				}
				publication[i] = append(publication[i], sample)
				if err != nil {
					return
				}
			}
		})
	}
	producerWG.Wait()
	producerWall := time.Since(producerStart)
	stopSampling()
	sampleWG.Wait()
	catchupTimer := time.NewTimer(5 * time.Second)
	completed := 0
catchup:
	for completed < healthyCount {
		select {
		case <-caughtUp:
			completed++
		case <-catchupTimer.C:
			break catchup
		}
	}
	catchupTimer.Stop()
	resources = append(resources, sseMeasure("after_publication_and_healthy_catchup"))
	closedBeforeHarness := slowServerClosed.Load()
	queueSlowBeforeHarness := queueSlow.Load()
	harnessClosing.Store(true)
	closeClients()
	readerWG.Wait()
	transport.CloseIdleConnections()
	// Let the real handler/subscription cleanup run; keep warm pools and the
	// idle HTTP listener alive so the before/after process readings are comparable.
	time.Sleep(250 * time.Millisecond)
	runtime.GC()
	resources = append(resources, sseMeasure("warm_services_after_connections_closed"))
	var durable, minBytes, maxBytes, eventGaps int
	if err := owner.Pool.QueryRow(ctx, `SELECT count(*),coalesce(min(octet_length(payload::text)),0),coalesce(max(octet_length(payload::text)),0) FROM run_events WHERE type='benchmark.sample'`).Scan(&durable, &minBytes, &maxBytes); err != nil {
		t.Fatal(err)
	}
	if err := owner.Pool.QueryRow(ctx, `SELECT count(*) FROM (SELECT run_id FROM run_events GROUP BY run_id HAVING min(seq)<>1 OR count(*)<>max(seq)) g`).Scan(&eventGaps); err != nil {
		t.Fatal(err)
	}
	latencies := []float64{}
	var gaps, duplicates, failedClients, publishErrors int
	for _, c := range clients {
		gaps += c.Gaps
		duplicates += c.Duplicates
		if !c.Slow {
			if c.Error != "" || c.Received != ticks {
				failedClients++
			}
			for _, s := range c.Samples {
				latencies = append(latencies, s.LatencyMS)
			}
		}
	}
	for _, run := range publication {
		for _, p := range run {
			if p.Error != "" {
				publishErrors++
			}
		}
	}
	latency := sseDistribution(latencies)
	passed := durable == runCount*ticks && minBytes == payloadBytes && maxBytes == payloadBytes && eventGaps == 0 && gaps == 0 && duplicates == 0 && failedClients == 0 && publishErrors == 0 && latency.Count == healthyCount*ticks
	sources := map[string]string{}
	for _, path := range []string{"benchmarks/sse_test.go", "go.mod", "internal/httpapi/sse.go", "internal/httpapi/server.go", "internal/eventstream/hub.go", "internal/persistence/store.go", "internal/persistence/execution.go", "db/queries/events.sql", "internal/persistence/sqlgen/events.sql.go"} {
		b, err := os.ReadFile(filepath.Join("..", path))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(b)
		sources[path] = hex.EncodeToString(sum[:])
	}
	report := map[string]any{"schema_version": 1, "timestamp_utc": started.UTC(), "scope": "Real PostgreSQL append transactions + actual TCP HTTP/1.1 API/SSE fanout under a non-owner NOBYPASSRLS role; synthetic active runs only", "seed": seed, "machine": machine(), "database": map[string]any{"version": dbVersion, "fsync": fsync, "synchronous_commit": synchronousCommit, "writer_pool_max": owner.Pool.Config().MaxConns, "api_pool_max": apiStore.Pool.Config().MaxConns, "writer_acquire_seconds": owner.Pool.Stat().AcquireDuration().Seconds(), "api_acquire_seconds": apiStore.Pool.Stat().AcquireDuration().Seconds()}, "configuration": map[string]any{"runs": runCount, "connections": clientsCount, "healthy": healthyCount, "intentionally_slow": runCount * slowPerRun, "events_per_run": ticks, "per_run_hz": rate, "scheduled_window_seconds": window.Seconds(), "actual_publish_seconds": producerWall.Seconds(), "actual_persisted_events_per_second": float64(durable) / producerWall.Seconds(), "stored_payload_bytes": payloadBytes, "default_hub_poll_ms": 100, "default_subscriber_queue": 128, "default_hub_history": 512, "catchup_grace_seconds": 5, "http_transport": "TCP HTTP/1.1 loopback; default socket buffers; no artificial handler delay"}, "invariants": map[string]any{"passed": passed, "durable_sample_events": durable, "durable_sequence_gaps": eventGaps, "stored_payload_min_bytes": minBytes, "stored_payload_max_bytes": maxBytes, "healthy_clients_caught_up": completed, "healthy_clients_incomplete_or_failed": failedClients, "healthy_sequence_gaps": gaps, "healthy_duplicates": duplicates, "healthy_unique_deliveries": len(latencies), "publish_errors": publishErrors, "p95_under_1000ms": latency.P95MS < 1000}, "healthy_latency": latency, "slow_observation": map[string]any{"server_returns_before_harness_close": closedBeforeHarness, "hub_queue_overflows": queueSlowBeforeHarness, "interpretation": "Non-reading bodies may remain buffered by TCP during this 12-second 60-KiB-per-run workload. Zero disconnects does not prove slow-client disconnection; no buffers/queues/timeouts were reduced to force it."}, "resources": resources, "clients": clients, "publication_samples": publication, "source_sha256": sources, "limitations": []string{"One 12-second sample on a shared development machine; no sustained-capacity or production SLO claim.", "API, clients and publisher goroutines share one Go process. Heap/goroutine/FD readings include the harness and retained raw latency samples; PostgreSQL is a separate process.", "Latency is database event created_at to client receive, includes commit/poll/transport, and assumes the local PostgreSQL/container and Go process share the host clock.", "Client subscriptions are established before publication. No reconnect churn or retention deletion is induced in this sample; separate regression tests cover those protocols.", "Healthy clients must receive every sequence through the final scheduled event within a five-second grace; no-loss refers only to this finite committed set.", "Slow clients do not read response bodies after headers. TCP buffers may absorb all data; report observed early handler returns and queue-overflow counts without inferring cause or forced isolation.", "No model, runner operation, real repair, HTTP mutation RPS, or notification-delivery benchmark is performed. Polling remains the source of live events."}}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join("results", "local", "sse-"+started.UTC().Format("20060102T150405.000000000Z")+".json")
	if err := os.MkdirAll(filepath.Dir(out), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(out, append(encoded, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
	abs, _ := filepath.Abs(out)
	t.Logf("evidence=%s", abs)
	t.Logf("healthy=%d/%d deliveries=%d p95=%.3fms p99=%.3fms durable=%d payload=%d..%dB slow_early_returns=%d queue_overflows=%d", completed, healthyCount, len(latencies), latency.P95MS, latency.P99MS, durable, minBytes, maxBytes, closedBeforeHarness, queueSlowBeforeHarness)
	t.Logf("resources before=%+v after=%+v", resources[0], resources[len(resources)-1])
	if !passed {
		t.Errorf("SSE correctness/workload invariant failed; inspect %s", abs)
	}
	if latency.Count > 0 && latency.P95MS >= 1000 {
		t.Errorf("SDE sample target p95<1s not met: %.3fms", latency.P95MS)
	}
}

func sseDistribution(values []float64) distribution {
	if len(values) == 0 {
		return distribution{}
	}
	sort.Float64s(values)
	q := func(p float64) float64 { return values[max(0, int(math.Ceil(p*float64(len(values))))-1)] }
	return distribution{Count: len(values), MinMS: values[0], P50MS: q(.5), P95MS: q(.95), P99MS: q(.99), MaxMS: values[len(values)-1]}
}
