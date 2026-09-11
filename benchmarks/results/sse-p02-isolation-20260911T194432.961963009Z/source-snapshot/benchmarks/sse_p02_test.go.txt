package benchmarks

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/eventstream"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/telemetry"
	"github.com/JDinSeattle/forge-runtime/internal/testutil"
	"github.com/jackc/pgx/v5"
)

// This is the exact P02 traffic shape. Server sockets, queue/history sizes,
// polling and write deadlines are production defaults. Only deliberately slow
// clients choose a bounded receive window; the earlier default-client baseline
// remains a separate, unchanged experiment.
const p02Runs, p02PerRun, p02HealthyPerRun = 20, 10, 9
const p02Rate, p02MaxTicks, p02PostCloseTicks, p02PositiveHintTicks = 5, 7500, 50, 10

type p02Raw struct {
	mu      sync.Mutex
	file    *os.File
	zip     *gzip.Writer
	encoder *json.Encoder
	err     error
	count   int
}

func p02OpenRaw(t *testing.T, filename string) *p02Raw {
	t.Helper()
	f, err := os.Create(filename)
	if err != nil {
		t.Fatal(err)
	}
	z, err := gzip.NewWriterLevel(f, gzip.BestSpeed)
	if err != nil {
		f.Close()
		t.Fatal(err)
	}
	return &p02Raw{file: f, zip: z, encoder: json.NewEncoder(z)}
}
func (w *p02Raw) write(value any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err == nil {
		w.err = w.encoder.Encode(value)
		if w.err == nil {
			w.count++
		}
	}
}
func (w *p02Raw) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.zip.Close(); w.err == nil {
		w.err = err
	}
	if err := w.file.Close(); w.err == nil {
		w.err = err
	}
	return w.err
}

type p02Client struct {
	Index        int    `json:"client"`
	Run          int    `json:"run_index"`
	Slow         bool   `json:"slow"`
	Connected    bool   `json:"connected"`
	InitialSeq   uint64 `json:"initial_seq"`
	LastSeq      uint64 `json:"last_seq"`
	Count        int    `json:"received"`
	Error        string `json:"error,omitempty"`
	DrainedBytes int64  `json:"slow_bytes_drained_after_server_close,omitempty"`
	DrainError   string `json:"slow_drain_error,omitempty"`
	seen         atomic.Int64
	latencies    []float64
	response     *http.Response
}

type p02Delivery struct {
	Client     int     `json:"client"`
	Run        int     `json:"run"`
	Tick       int     `json:"tick"`
	Seq        uint64  `json:"seq"`
	CreatedNS  int64   `json:"created_unix_ns"`
	ReceivedNS int64   `json:"received_unix_ns"`
	LatencyMS  float64 `json:"latency_ms"`
}

func p02Read(ctx context.Context, client *http.Client, base, token string, run persistence.Run, result *p02Client, raw *p02Raw, ready chan<- error) {
	req, _ := http.NewRequestWithContext(ctx, "GET", base+"/v1/runs/"+string(run.ID)+"/events", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Forge-Tenant", string(run.TenantID))
	req.Header.Set("Last-Event-ID", strconv.FormatUint(run.CoveredSeq, 10))
	if result.Slow {
		req.Header.Set("X-Benchmark-Slow", "1")
	}
	resp, err := client.Do(req)
	if err != nil {
		result.Error = err.Error()
		ready <- err
		return
	}
	if resp.StatusCode != 200 {
		resp.Body.Close()
		result.Error = fmt.Sprintf("HTTP %d", resp.StatusCode)
		ready <- fmt.Errorf("%s", result.Error)
		return
	}
	result.Connected = true
	result.InitialSeq = run.CoveredSeq
	result.LastSeq = run.CoveredSeq
	if result.Slow {
		result.response = resp
		ready <- nil
		return
	}
	defer resp.Body.Close()
	ready <- nil
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 4096), 64<<10)
	var id uint64
	var body []byte
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "id: ") {
			id, err = strconv.ParseUint(strings.TrimPrefix(line, "id: "), 10, 64)
			if err != nil {
				result.Error = err.Error()
				return
			}
			continue
		}
		if strings.HasPrefix(line, "data: ") {
			body = append(body[:0], strings.TrimPrefix(line, "data: ")...)
			continue
		}
		if line != "" || len(body) == 0 {
			continue
		}
		received := time.Now()
		var event persistence.Event
		if err = json.Unmarshal(body, &event); err != nil {
			result.Error = err.Error()
			return
		}
		body = body[:0]
		var payload struct {
			Tick int `json:"tick"`
		}
		if err = json.Unmarshal(event.Payload, &payload); err != nil {
			result.Error = err.Error()
			return
		}
		if event.Type != "benchmark.p02" || event.RunID != run.ID || event.Seq != id || event.Seq != result.LastSeq+1 || payload.Tick != result.Count+1 {
			result.Error = "event identity, sequence or payload tick mismatch"
			return
		}
		result.LastSeq = event.Seq
		result.Count++
		latency := received.Sub(event.CreatedAt).Seconds() * 1000
		result.latencies = append(result.latencies, latency)
		raw.write(p02Delivery{Client: result.Index, Run: result.Run, Tick: payload.Tick, Seq: event.Seq, CreatedNS: event.CreatedAt.UnixNano(), ReceivedNS: received.UnixNano(), LatencyMS: latency})
		result.seen.Store(int64(result.Count))
	}
	if ctx.Err() == nil {
		if scanner.Err() != nil {
			result.Error = scanner.Err().Error()
		} else {
			result.Error = "server ended healthy stream before final harness cancellation"
		}
	}
}

func p02Setup(t *testing.T, ctx context.Context) (*sustainedFixture, []persistence.Run) {
	t.Helper()
	f := &sustainedFixture{owner: testutil.Database(t)}
	if err := f.owner.BootstrapTenant(ctx, "p02_tenant", "p02_viewer", "viewer"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.owner.Pool.Exec(ctx, `UPDATE tenant_runtime SET max_active=$1`, p02Runs); err != nil {
		t.Fatal(err)
	}
	if err := f.owner.RegisterRunner(ctx, "p02_runner", "unix:///never-dispatched.sock", p02Runs); err != nil {
		t.Fatal(err)
	}
	project, err := f.owner.CreateProject(ctx, "p02_tenant", "Exact P02 actual TCP experiment", "no-checkout", "no-executor")
	if err != nil {
		t.Fatal(err)
	}
	runs := make([]persistence.Run, p02Runs)
	for i := range runs {
		r, _, err := f.owner.Submit(ctx, persistence.SubmitRequest{TenantID: "p02_tenant", PrincipalID: "p02_viewer", ProjectID: project.ID, Task: "Synthetic active event publisher only", BaseCommit: "no-checkout", Config: persistence.Config{Provider: "fake", Model: "not-called", MaxModelRounds: 1, MaxToolCalls: 1, MaxRuntimeSeconds: 1800}}, fmt.Sprintf("p02-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		claimed, err := f.owner.ClaimOnRunner(ctx, fmt.Sprintf("p02-publisher-%d", i), 5*time.Minute, "p02_runner")
		if err != nil || claimed.ID != r.ID {
			t.Fatalf("claim: %v", err)
		}
		runs[i] = claimed
	}
	f.run = runs[0]
	f.token, err = f.owner.IssueToken(ctx, "p02_viewer", 2*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	f.api = sseNonOwnerPool(t, ctx, f.owner)
	sseWarmPool(t, ctx, f.owner.Pool)
	sseWarmPool(t, ctx, f.api.Pool)
	f.metrics, err = telemetry.Setup(ctx, "p02-benchmark", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.metrics.Shutdown(context.Background()) })
	f.manager = eventstream.New(ctx, f.api, eventstream.Config{Slow: func() { f.slowQueues.Add(1) }})
	t.Cleanup(f.manager.Close)
	var version, fsync, commit, schema string
	if err = f.owner.Pool.QueryRow(ctx, `SELECT version(),current_setting('fsync'),current_setting('synchronous_commit'),current_schema()`).Scan(&version, &fsync, &commit, &schema); err != nil {
		t.Fatal(err)
	}
	f.database = map[string]any{"version": version, "fsync": fsync, "synchronous_commit": commit, "private_schema": schema, "owner_pool_max": f.owner.Pool.Config().MaxConns, "api_pool_max": f.api.Pool.Config().MaxConns, "api_role": "nonowner NOSUPERUSER NOBYPASSRLS NOINHERIT"}
	return f, runs
}

// TestSSEP02IsolationEvidence may take 25 minutes. It commits the exact traffic
// shape until ALL twenty slow TCP connections close, then commits fifty more
// events per run. Healthy readers must reach the actual final committed cursor.
func TestSSEP02IsolationEvidence(t *testing.T) {
	if os.Getenv("FORGE_RUN_P02_SSE") != "1" {
		t.Skip("set FORGE_RUN_P02_SSE=1 and FORGE_TEST_DATABASE_URL")
	}
	ctx, end := context.WithTimeout(context.Background(), 27*time.Minute)
	defer end()
	started := time.Now()
	out := sustainedOutput(t, started, "sse-p02-isolation")
	f, runs := p02Setup(t, ctx)
	h := sustainedServer(f, 0)
	defer h.server.Close()
	normalTransport := h.transport(false, 0)
	defer normalTransport.CloseIdleConnections()
	slowTransport := h.transport(true, 4096)
	defer slowTransport.CloseIdleConnections()
	normalClient, slowClient := &http.Client{Transport: normalTransport}, &http.Client{Transport: slowTransport}
	deliveries := p02OpenRaw(t, filepath.Join(out, "deliveries.jsonl.gz"))
	publications := p02OpenRaw(t, filepath.Join(out, "publication.jsonl.gz"))
	readCtx, closeReaders := context.WithCancel(ctx)
	defer closeReaders()
	clients := make([]p02Client, p02Runs*p02PerRun)
	ready := make(chan error, len(clients))
	var readers sync.WaitGroup
	for i := range clients {
		c := &clients[i]
		c.Index = i
		c.Run = i / p02PerRun
		c.Slow = i%p02PerRun == p02PerRun-1
		client := normalClient
		if c.Slow {
			client = slowClient
		} else {
			c.latencies = make([]float64, 0, p02MaxTicks)
		}
		readers.Go(func() { p02Read(readCtx, client, h.server.URL, f.token, runs[c.Run], c, deliveries, ready) })
	}
	if err := sustainedReady(ctx, len(clients), ready); err != nil {
		closeReaders()
		readers.Wait()
		t.Fatal(err)
	}
	if h.activeTCP.Load() != 200 || h.activeHandlers.Load() != 200 {
		closeReaders()
		readers.Wait()
		t.Fatal("expected exactly 200 active TCP/handlers before publication")
	}

	// Production event fanout is polling-only and AppendWorkerEvent emits no
	// wake hint. This additional, private fixture channel supplies a positive
	// NOTIFY control then deliberately loses hints by rolling back their own
	// transactions AFTER the business event has independently committed. A real
	// listener proves loss; no live channel or production setting is changed.
	channel := f.database["private_schema"].(string) + "_p02"
	listener, err := pgx.ConnectConfig(ctx, f.owner.Pool.Config().ConnConfig.Copy())
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close(context.Background())
	if _, err = listener.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	listenCtx, stopListen := context.WithCancel(ctx)
	defer stopListen()
	var notifications, droppedDelivered atomic.Int64
	var listenErr string
	var listenWG sync.WaitGroup
	listenWG.Go(func() {
		for {
			n, err := listener.WaitForNotification(listenCtx)
			if err != nil {
				if listenCtx.Err() == nil {
					listenErr = err.Error()
				}
				return
			}
			notifications.Add(1)
			if strings.HasPrefix(n.Payload, "dropped:") {
				droppedDelivered.Add(1)
			}
		}
	})
	producerStart := time.Now()
	ticks, lastSlowClosedTick := 0, 0
	var publishErrors, committedHints, rolledBackHints atomic.Int64
	var maximumScheduleLatenessNS atomic.Int64
	progress := make([]map[string]any, 0, 30)
	var problems []string
	for tick := 1; tick <= p02MaxTicks; tick++ {
		due := producerStart.Add(time.Duration(tick) * time.Second / p02Rate)
		timer := time.NewTimer(max(time.Duration(0), time.Until(due)))
		select {
		case <-ctx.Done():
			timer.Stop()
			problems = append(problems, ctx.Err().Error())
		case <-timer.C:
		}
		if ctx.Err() != nil {
			break
		}
		if tick == 1 || (tick-1)%300 == 0 {
			for _, r := range runs {
				if _, err = f.owner.Heartbeat(ctx, r.TenantID, r.ID, r.State.Lease.Owner, r.State.Lease.Epoch, 5*time.Minute); err != nil {
					problems = append(problems, "heartbeat: "+err.Error())
				}
			}
			if len(problems) > 0 {
				break
			}
		}
		var batch sync.WaitGroup
		for i := range runs {
			batch.Go(func() {
				payload := map[string]any{"tick": tick, "chunk": ""}
				base, _ := json.Marshal(payload)
				payload["chunk"] = strings.Repeat("x", 1024-len(base)-3)
				raw, _ := json.Marshal(payload)
				at := time.Now()
				late := at.Sub(due).Nanoseconds()
				for old := maximumScheduleLatenessNS.Load(); late > old; old = maximumScheduleLatenessNS.Load() {
					if maximumScheduleLatenessNS.CompareAndSwap(old, late) {
						break
					}
				}
				err := f.owner.AppendWorkerEvent(ctx, runs[i], "benchmark.p02", raw)
				committed := time.Now()
				p := ssePublish{Run: i, Tick: tick, ScheduledAt: due, CommittedAt: committed, ScheduleLatenessMS: at.Sub(due).Seconds() * 1000, AppendMS: committed.Sub(at).Seconds() * 1000}
				if err != nil {
					p.Error = err.Error()
					publishErrors.Add(1)
				}
				publications.write(p)
				if err != nil {
					return
				}
				if tick <= p02PositiveHintTicks {
					_, err = f.owner.Pool.Exec(ctx, `SELECT pg_notify($1,$2)`, channel, fmt.Sprintf("positive:%d:%d", i, tick))
					if err == nil {
						committedHints.Add(1)
					}
				} else {
					var tx pgx.Tx
					tx, err = f.owner.Pool.Begin(ctx)
					if err == nil {
						_, err = tx.Exec(ctx, `SELECT pg_notify($1,$2)`, channel, fmt.Sprintf("dropped:%d:%d", i, tick))
						rollbackErr := tx.Rollback(ctx)
						if err == nil {
							err = rollbackErr
						}
						if err == nil {
							rolledBackHints.Add(1)
						}
					}
				}
				if err != nil {
					publishErrors.Add(1)
				}
			})
		}
		batch.Wait()
		ticks = tick
		if publishErrors.Load() > 0 {
			problems = append(problems, "event publication or hint fixture failed")
			break
		}
		if tick == p02PositiveHintTicks && !sustainedWait(ctx, 5*time.Second, func() bool { return notifications.Load() == p02Runs*p02PositiveHintTicks }) {
			problems = append(problems, "positive NOTIFY control was not observed")
			break
		}
		if h.slowTCPClosed.Load() == p02Runs && lastSlowClosedTick == 0 {
			lastSlowClosedTick = tick
			t.Logf("all twenty slow TCP connections closed at tick=%d elapsed=%.3fs", tick, time.Since(producerStart).Seconds())
		}
		if tick%300 == 0 || lastSlowClosedTick == tick {
			row := map[string]any{"tick": tick, "elapsed_seconds": time.Since(producerStart).Seconds(), "slow_tcp_closed": h.slowTCPClosed.Load(), "slow_handler_returns": h.slowReturns.Load(), "slow_metric": f.slowMetric(), "resources": sseMeasure("publication")}
			progress = append(progress, row)
			t.Logf("tick=%d/%d slow_tcp=%d slow_handlers=%d", tick, p02MaxTicks, h.slowTCPClosed.Load(), h.slowReturns.Load())
		}
		if lastSlowClosedTick > 0 && tick-lastSlowClosedTick >= p02PostCloseTicks {
			break
		}
	}
	producerSeconds := time.Since(producerStart).Seconds()
	caughtUp := sustainedWait(ctx, 10*time.Second, func() bool {
		for i := range clients {
			if !clients[i].Slow && clients[i].seen.Load() != int64(ticks) {
				return false
			}
		}
		return true
	})
	slowTCP, slowHandlers, slowMetric := h.slowTCPClosed.Load(), h.slowReturns.Load(), f.slowMetric()
	stopListen()
	listenWG.Wait()
	// Drain only after observing ALL real server-side slow connections closed.
	// It cannot help the server make progress or manufacture the required close.
	if slowTCP == p02Runs {
		var drain sync.WaitGroup
		for i := range clients {
			c := &clients[i]
			if c.Slow && c.response != nil {
				drain.Go(func() {
					n, drainErr := io.Copy(io.Discard, c.response.Body)
					c.DrainedBytes = n
					if drainErr != nil {
						c.DrainError = drainErr.Error()
					}
					c.response.Body.Close()
				})
			}
		}
		drain.Wait()
	}
	// Reconnect the former slow clients with normal receive windows and the
	// original Last-Event-ID. Every missed event must replay from SQL through
	// the final cursor, including history older than the bounded hub ring.
	replayRaw := p02OpenRaw(t, filepath.Join(out, "slow-resume.jsonl.gz"))
	resumed := make([]p02Client, p02Runs)
	replayedExactly := 0
	replaySeconds := 0.0
	if slowTCP == p02Runs && caughtUp {
		replayStart := time.Now()
		replayCtx, closeReplay := context.WithCancel(ctx)
		replayReady := make(chan error, p02Runs)
		var replayWG sync.WaitGroup
		for i := range resumed {
			c := &resumed[i]
			c.Index, c.Run = i*p02PerRun+p02PerRun-1, i
			c.latencies = make([]float64, 0, ticks)
			replayWG.Go(func() {
				p02Read(replayCtx, normalClient, h.server.URL, f.token, runs[c.Run], c, replayRaw, replayReady)
			})
		}
		if err := sustainedReady(ctx, p02Runs, replayReady); err != nil {
			problems = append(problems, "slow client reconnect: "+err.Error())
		} else if !sustainedWait(ctx, 30*time.Second, func() bool {
			for i := range resumed {
				if resumed[i].seen.Load() != int64(ticks) {
					return false
				}
			}
			return true
		}) {
			problems = append(problems, "slow client durable replay did not reach final cursor")
		}
		closeReplay()
		replayWG.Wait()
		replaySeconds = time.Since(replayStart).Seconds()
		for i := range resumed {
			c := &resumed[i]
			if c.Connected && c.Error == "" && c.Count == ticks && c.LastSeq == c.InitialSeq+uint64(ticks) {
				replayedExactly++
			}
		}
	}
	if err := replayRaw.close(); err != nil {
		problems = append(problems, "replay raw writer: "+err.Error())
	}
	h.closing.Store(true)
	closeReaders()
	readers.Wait()
	for i := range clients {
		if clients[i].Slow && clients[i].response != nil {
			clients[i].response.Body.Close()
		}
	}
	normalTransport.CloseIdleConnections()
	slowTransport.CloseIdleConnections()
	cleaned := sustainedWait(ctx, 5*time.Second, func() bool { return h.activeTCP.Load() == 0 && h.activeHandlers.Load() == 0 })
	if err = deliveries.close(); err != nil {
		problems = append(problems, "delivery raw writer: "+err.Error())
	}
	if err = publications.close(); err != nil {
		problems = append(problems, "publication raw writer: "+err.Error())
	}
	var durable, minBytes, maxBytes, gaps int
	if err = f.owner.Pool.QueryRow(ctx, `SELECT count(*),coalesce(min(octet_length(payload::text)),0),coalesce(max(octet_length(payload::text)),0) FROM run_events WHERE type='benchmark.p02'`).Scan(&durable, &minBytes, &maxBytes); err != nil {
		problems = append(problems, err.Error())
	}
	if err = f.owner.Pool.QueryRow(ctx, `SELECT count(*) FROM (SELECT run_id FROM run_events GROUP BY run_id HAVING min(seq)<>1 OR count(*)<>max(seq)) g`).Scan(&gaps); err != nil {
		problems = append(problems, err.Error())
	}
	latencies := make([]float64, 0, p02Runs*p02HealthyPerRun*ticks)
	healthyExact := 0
	for i := range clients {
		c := &clients[i]
		if !c.Slow {
			if c.Error == "" && c.Count == ticks && c.LastSeq == c.InitialSeq+uint64(ticks) {
				healthyExact++
			}
			latencies = append(latencies, c.latencies...)
		}
	}
	latency := sseDistribution(latencies)
	cursorMatch := true
	for _, r := range runs {
		latest, err := f.api.GetRun(ctx, r.TenantID, r.ID)
		if err != nil || latest.CoveredSeq != r.CoveredSeq+uint64(ticks) {
			cursorMatch = false
		}
	}
	passed := ticks > p02PositiveHintTicks && durable == p02Runs*ticks && minBytes == 1024 && maxBytes == 1024 && gaps == 0 && caughtUp && healthyExact == 180 && latency.Count == 180*ticks && latency.P95MS < 1000 && slowTCP == 20 && slowHandlers == 20 && slowMetric == 20 && lastSlowClosedTick > 0 && ticks-lastSlowClosedTick >= p02PostCloseTicks && notifications.Load() == 200 && committedHints.Load() == 200 && rolledBackHints.Load() == int64(p02Runs*(ticks-p02PositiveHintTicks)) && droppedDelivered.Load() == 0 && listenErr == "" && publishErrors.Load() == 0 && cleaned && cursorMatch && len(problems) == 0 && replayedExactly == p02Runs && replayRaw.count == p02Runs*ticks && maximumScheduleLatenessNS.Load() < int64(time.Second) && producerSeconds <= float64(ticks)/p02Rate+1
	sustainedProfile(t, out, "final")
	manifest := sustainedManifest(t)
	for _, source := range []string{"benchmarks/sse_p02_test.go", "internal/persistence/execution.go", "internal/persistence/store.go", "internal/telemetry/http.go"} {
		data, err := os.ReadFile(filepath.Join("..", source))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		manifest["source_sha256"].(map[string]string)[source] = hex.EncodeToString(sum[:])
	}
	report := map[string]any{"schema_version": 1, "timestamp_utc": started.UTC(), "scope": "Exact P02 actual TCP/RLS/default-server traffic; client-only bounded slow receive window; optional private NOTIFY hints deliberately lost after independent durable event commit", "manifest": manifest, "database": f.database, "configuration": map[string]any{"runs": 20, "connections": 200, "healthy": 180, "slow": 20, "per_run_hz": 5, "stored_payload_bytes": 1024, "actual_ticks_per_run": ticks, "max_ticks_per_run": p02MaxTicks, "max_scheduled_seconds": 1500, "actual_publish_seconds": producerSeconds, "post_all_slow_close_ticks_required": p02PostCloseTicks, "server_socket_buffers": "unmodified defaults", "healthy_client_socket_buffers": "unmodified defaults", "slow_client_requested_receive_bytes": 4096, "production_poll_ms": 100, "production_queue_size": 128, "production_history_size": 512, "production_write_deadline_seconds": 5, "catchup_grace_seconds": 10, "max_schedule_lateness_ms": float64(maximumScheduleLatenessNS.Load()) / 1e6, "schedule_lateness_limit_ms": 1000, "actual_events_per_run_per_second": float64(ticks) / producerSeconds}, "invariants": map[string]any{"passed": passed, "durable_events": durable, "durable_gaps": gaps, "payload_min_bytes": minBytes, "payload_max_bytes": maxBytes, "healthy_clients_at_exact_final_cursor": healthyExact, "healthy_unique_deliveries": latency.Count, "all_api_cursors_match": cursorMatch, "publish_errors": publishErrors.Load(), "tcp_after_cleanup": h.activeTCP.Load(), "handlers_after_cleanup": h.activeHandlers.Load()}, "healthy_latency": latency, "slow_observation": map[string]any{"handler_returns_before_harness_close": slowHandlers, "tcp_closes_before_harness_close": slowTCP, "slow_subscriber_metric": slowMetric, "queue_overflows": f.slowQueues.Load(), "all_slow_closed_observed_at_tick": lastSlowClosedTick, "further_committed_events_per_run": ticks - lastSlowClosedTick}, "notification_fault": map[string]any{"fixture_private_channel": channel, "positive_hints_committed": committedHints.Load(), "notifications_received": notifications.Load(), "hint_transactions_rolled_back_after_event_commit": rolledBackHints.Load(), "dropped_hints_unexpectedly_received": droppedDelivered.Load(), "listener_error": listenErr, "production_semantics": "Manager uses 100ms durable polling, with no LISTEN consumer; AppendWorkerEvent emits no hint. Fixture-only NOTIFY positive control and rollback demonstrate missing optional hints, without inventing a production notification optimization."}, "slow_reconnect": map[string]any{"clients_at_exact_final_cursor": replayedExactly, "replayed_events": replayRaw.count, "duration_seconds": replaySeconds, "original_cursor_used": true, "replay_grace_seconds": 30, "clients": resumed, "raw_file": "slow-resume.jsonl.gz"}, "clients": clients, "progress": progress, "sockets": h.sockets, "raw_deliveries_file": "deliveries.jsonl.gz", "raw_publication_file": "publication.jsonl.gz", "raw_delivery_count": deliveries.count, "raw_publication_count": publications.count, "problems": problems, "limitations": []string{"Synthetic active runs and real durable events; no provider, executor, container, repair or external billing.", "Exactly nine healthy and one slow client per run. Only slow clients set SO_RCVBUF=4096 before connect; actual kernel sizes are recorded. Server socket buffers remain default; earlier all-default client baseline is separate.", "Raw gzip JSONL retains every healthy delivery and publication timestamp. Required completeness is the final durable cursor, not merely delivery within a partial time window.", "API, publishers, clients and raw evidence writer share the process on a shared host. This is a finite local workload, not a global SLO or memory plateau experiment.", "A private-channel notification fault is explicit fixture behavior; production fanout is already polling-only. There is no claim that a production LISTEN optimization was disconnected.", "No combined database outage or concurrent retention deletion occurs in this experiment; those protocol boundaries require their own matching evidence."}}
	sustainedWriteReport(t, out, report)
	t.Logf("passed=%v ticks=%d elapsed=%.3fs healthy=%d deliveries=%d p95=%.3fms slow_tcp=%d dropped_hints=%d", passed, ticks, producerSeconds, healthyExact, latency.Count, latency.P95MS, slowTCP, rolledBackHints.Load())
	if !passed {
		t.Error("exact P02 isolation/notification-loss acceptance failed; retain report and raw samples")
	}
}
