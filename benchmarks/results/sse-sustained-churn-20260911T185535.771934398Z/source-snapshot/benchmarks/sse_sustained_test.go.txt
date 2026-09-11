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
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/eventstream"
	"github.com/JDinSeattle/forge-runtime/internal/httpapi"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/telemetry"
	"github.com/JDinSeattle/forge-runtime/internal/testutil"
	"golang.org/x/sys/unix"
)

type sustainedFixture struct {
	owner, api *persistence.Store
	run        persistence.Run
	token      string
	manager    *eventstream.Manager
	metrics    *telemetry.Telemetry
	slowQueues atomic.Int64
	database   map[string]any
}

func sustainedSetup(t *testing.T, ctx context.Context) *sustainedFixture {
	t.Helper()
	f := &sustainedFixture{owner: testutil.Database(t)}
	for _, err := range []error{
		f.owner.BootstrapTenant(ctx, "sustained_tenant", "sustained_viewer", "viewer"),
		f.owner.RegisterRunner(ctx, "sustained_runner", "unix:///never-dispatched.sock", 1),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	project, err := f.owner.CreateProject(ctx, "sustained_tenant", "Actual TCP SSE fixture", "no-checkout", "no-executor")
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = f.owner.Submit(ctx, persistence.SubmitRequest{TenantID: "sustained_tenant", PrincipalID: "sustained_viewer", ProjectID: project.ID, Task: "Persisted synthetic SSE events only", BaseCommit: "no-checkout", Config: persistence.Config{Provider: "fake", Model: "not-called", MaxModelRounds: 1, MaxToolCalls: 1, MaxRuntimeSeconds: 900}}, "sustained")
	if err != nil {
		t.Fatal(err)
	}
	f.run, err = f.owner.ClaimOnRunner(ctx, "sustained-publisher", 5*time.Minute, "sustained_runner")
	if err != nil {
		t.Fatalf("claim synthetic event publisher: %v", err)
	}
	f.token, err = f.owner.IssueToken(ctx, "sustained_viewer", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	f.api = sseNonOwnerPool(t, ctx, f.owner)
	sseWarmPool(t, ctx, f.owner.Pool)
	sseWarmPool(t, ctx, f.api.Pool)
	f.metrics, err = telemetry.Setup(ctx, "sse-benchmark", "")
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
	return f
}

func (f *sustainedFixture) append(ctx context.Context, tick, payloadBytes int) error {
	payload := map[string]any{"tick": tick, "chunk": ""}
	base, _ := json.Marshal(payload)
	payload["chunk"] = strings.Repeat("x", payloadBytes-len(base)-3)
	raw, _ := json.Marshal(payload)
	return f.owner.AppendWorkerEvent(ctx, f.run, "benchmark.sustained", raw)
}

// Socket sizes are observed from the real descriptors. Default mode does not
// set either buffer; Linux can autotune after the recorded accept/dial sample.
type sustainedSocket struct {
	Side         string `json:"side"`
	Slow         bool   `json:"slow"`
	SendBytes    int    `json:"send_bytes_at_open"`
	ReceiveBytes int    `json:"receive_bytes_at_open"`
}

func socketSizes(c net.Conn) (int, int, error) {
	tcp, ok := c.(*net.TCPConn)
	if !ok {
		return 0, 0, fmt.Errorf("not TCP")
	}
	raw, err := tcp.SyscallConn()
	if err != nil {
		return 0, 0, err
	}
	var send, recv int
	var inner error
	err = raw.Control(func(fd uintptr) {
		send, inner = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_SNDBUF)
		if inner == nil {
			recv, inner = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_RCVBUF)
		}
	})
	if err != nil {
		return 0, 0, err
	}
	return send, recv, inner
}

type sustainedListener struct {
	net.Listener
	writeBuffer int
	record      func(sustainedSocket)
}

func (l sustainedListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if l.writeBuffer > 0 {
		if err = c.(*net.TCPConn).SetWriteBuffer(l.writeBuffer); err != nil {
			c.Close()
			return nil, err
		}
	}
	send, recv, err := socketSizes(c)
	if err != nil {
		c.Close()
		return nil, err
	}
	l.record(sustainedSocket{Side: "server", SendBytes: send, ReceiveBytes: recv})
	return c, nil
}

type sustainedConnection struct{ slow atomic.Bool }
type sustainedConnKey struct{}
type sustainedHTTP struct {
	server                                     *httptest.Server
	activeTCP, activeHandlers, starts, returns atomic.Int64
	slowReturns, slowTCPClosed                 atomic.Int64
	firstSlowReturn, firstSlowTCPClosed        atomic.Int64
	socketsMu                                  sync.Mutex
	sockets                                    []sustainedSocket
	connections                                sync.Map
	closing                                    atomic.Bool
}

func sustainedServer(f *sustainedFixture, writeBuffer int) *sustainedHTTP {
	h := &sustainedHTTP{sockets: make([]sustainedSocket, 0, 128)}
	api := (&httpapi.Server{Store: f.api, Streams: f.manager, Telemetry: f.metrics}).Handler()
	h.server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.activeHandlers.Add(1)
		h.starts.Add(1)
		slow := r.Header.Get("X-Benchmark-Slow") == "1"
		if record, ok := r.Context().Value(sustainedConnKey{}).(*sustainedConnection); ok {
			record.slow.Store(slow)
		}
		defer func() {
			h.activeHandlers.Add(-1)
			h.returns.Add(1)
			if slow && !h.closing.Load() {
				h.slowReturns.Add(1)
				h.firstSlowReturn.CompareAndSwap(0, time.Now().UnixNano())
			}
		}()
		api.ServeHTTP(w, r)
	}))
	h.server.Config.ConnContext = func(ctx context.Context, c net.Conn) context.Context {
		record := &sustainedConnection{}
		h.connections.Store(c, record)
		return context.WithValue(ctx, sustainedConnKey{}, record)
	}
	h.server.Config.ConnState = func(c net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			h.activeTCP.Add(1)
		case http.StateClosed, http.StateHijacked:
			h.activeTCP.Add(-1)
			if v, ok := h.connections.LoadAndDelete(c); ok && v.(*sustainedConnection).slow.Load() && !h.closing.Load() {
				h.slowTCPClosed.Add(1)
				h.firstSlowTCPClosed.CompareAndSwap(0, time.Now().UnixNano())
			}
		}
	}
	h.server.Listener = sustainedListener{Listener: h.server.Listener, writeBuffer: writeBuffer, record: h.recordSocket}
	h.server.Start()
	return h
}
func (h *sustainedHTTP) recordSocket(s sustainedSocket) {
	h.socketsMu.Lock()
	h.sockets = append(h.sockets, s)
	h.socketsMu.Unlock()
}
func (h *sustainedHTTP) transport(slow bool, receiveBuffer int) *http.Transport {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	if receiveBuffer > 0 {
		dialer.Control = func(_, _ string, raw syscall.RawConn) error {
			var inner error
			err := raw.Control(func(fd uintptr) { inner = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_RCVBUF, receiveBuffer) })
			if err != nil {
				return err
			}
			return inner
		}
	}
	return &http.Transport{ForceAttemptHTTP2: false, MaxIdleConns: 200, MaxIdleConnsPerHost: 200, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		c, err := dialer.DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		send, recv, err := socketSizes(c)
		if err != nil {
			c.Close()
			return nil, err
		}
		h.recordSocket(sustainedSocket{Side: "client", Slow: slow, SendBytes: send, ReceiveBytes: recv})
		return c, nil
	}}
}
func (f *sustainedFixture) request(ctx context.Context, base string, after uint64, slow bool) *http.Request {
	r, _ := http.NewRequestWithContext(ctx, "GET", base+"/v1/runs/"+string(f.run.ID)+"/events", nil)
	r.Header.Set("Authorization", "Bearer "+f.token)
	r.Header.Set("X-Forge-Tenant", string(f.run.TenantID))
	r.Header.Set("Last-Event-ID", strconv.FormatUint(after, 10))
	if slow {
		r.Header.Set("X-Benchmark-Slow", "1")
	}
	return r
}

type sustainedReader struct {
	delivered atomic.Int64
	LastSeq   uint64      `json:"last_seq"`
	Count     int         `json:"count"`
	Error     string      `json:"error,omitempty"`
	Samples   []sseSample `json:"samples,omitempty"`
}

func (f *sustainedFixture) read(ctx context.Context, c *http.Client, base string, after uint64, index int, capture bool, result *sustainedReader, ready chan<- error) {
	response, err := c.Do(f.request(ctx, base, after, false))
	if err != nil {
		result.Error = err.Error()
		ready <- err
		return
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		err = fmt.Errorf("HTTP %d", response.StatusCode)
		result.Error = err.Error()
		ready <- err
		return
	}
	ready <- nil
	result.LastSeq = after
	scanner := bufio.NewScanner(response.Body)
	scanner.Buffer(make([]byte, 4096), 256<<10)
	var id uint64
	var raw []byte
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "id: ") {
			id, err = strconv.ParseUint(strings.TrimPrefix(line, "id: "), 10, 64)
			if err != nil {
				result.Error = "invalid SSE id"
				return
			}
			continue
		}
		if strings.HasPrefix(line, "data: ") {
			raw = append(raw[:0], strings.TrimPrefix(line, "data: ")...)
			continue
		}
		if line != "" || len(raw) == 0 {
			continue
		}
		var event persistence.Event
		if err = json.Unmarshal(raw, &event); err != nil {
			result.Error = err.Error()
			return
		}
		raw = raw[:0]
		if event.RunID != f.run.ID || event.Type != "benchmark.sustained" || event.Seq != id || event.Seq != result.LastSeq+1 {
			result.Error = fmt.Sprintf("identity/sequence mismatch: last=%d event=%d frame=%d", result.LastSeq, event.Seq, id)
			return
		}
		result.LastSeq = event.Seq
		result.Count++
		if capture {
			at := time.Now()
			result.Samples = append(result.Samples, sseSample{Client: index, Seq: event.Seq, CreatedAt: event.CreatedAt, ReceivedAt: at, LatencyMS: at.Sub(event.CreatedAt).Seconds() * 1000})
		}
		result.delivered.Store(int64(result.Count))
	}
	if ctx.Err() == nil {
		if err = scanner.Err(); err != nil {
			result.Error = err.Error()
		} else {
			result.Error = "stream closed before harness cancellation"
		}
	}
}
func sustainedWait(ctx context.Context, duration time.Duration, condition func() bool) bool {
	deadline := time.NewTimer(duration)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-deadline.C:
			return condition()
		case <-ticker.C:
		}
	}
}
func sustainedReady(ctx context.Context, n int, ready <-chan error) error {
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	for range n {
		select {
		case err := <-ready:
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return fmt.Errorf("SSE subscriptions not ready")
		}
	}
	return nil
}
func sustainedOutput(t *testing.T, started time.Time, kind string) string {
	t.Helper()
	out := filepath.Join("results", "local", kind+"-"+started.UTC().Format("20060102T150405.000000000Z"))
	if err := os.MkdirAll(out, 0755); err != nil {
		t.Fatal(err)
	}
	return out
}
func sustainedProfile(t *testing.T, out, prefix string) {
	t.Helper()
	for _, kind := range []string{"heap", "goroutine"} {
		p, err := os.Create(filepath.Join(out, prefix+"-"+kind+".pprof"))
		if err != nil {
			t.Fatal(err)
		}
		err = pprof.Lookup(kind).WriteTo(p, 0)
		closeErr := p.Close()
		if err != nil || closeErr != nil {
			t.Fatalf("profile: %v %v", err, closeErr)
		}
	}
}
func sustainedManifest(t *testing.T) map[string]any {
	t.Helper()
	sources := map[string]string{}
	for _, path := range []string{"benchmarks/sse_sustained_test.go", "benchmarks/sse_test.go", "internal/httpapi/sse.go", "internal/eventstream/hub.go", "go.mod"} {
		data, err := os.ReadFile(filepath.Join("..", path))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		sources[path] = hex.EncodeToString(sum[:])
	}
	commit, _ := exec.Command("git", "rev-parse", "HEAD").Output()
	status, _ := exec.Command("git", "status", "--porcelain").Output()
	return map[string]any{"machine": machine(), "git_base_commit": strings.TrimSpace(string(commit)), "working_tree_dirty": len(status) > 0, "source_sha256": sources}
}
func sustainedWriteReport(t *testing.T, out string, report any) {
	t.Helper()
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(out, "report.json"), append(data, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
	abs, _ := filepath.Abs(out)
	t.Logf("evidence=%s", abs)
}
func (f *sustainedFixture) slowMetric() float64 {
	families, _ := f.metrics.Registry.Gather()
	for _, family := range families {
		if family.GetName() == "forge_runtime_sse_disconnects_total" {
			for _, metric := range family.Metric {
				for _, label := range metric.Label {
					if label.GetValue() == "slow_subscriber" {
						return metric.GetCounter().GetValue()
					}
				}
			}
		}
	}
	return 0
}

// The heavier workload fills actual TCP windows using normal API writes. It
// deliberately differs from the 200-client 1-KiB baseline, which stays intact.
func TestSSEBackpressureEvidence(t *testing.T) {
	if os.Getenv("FORGE_RUN_SUSTAINED_SSE") != "1" {
		t.Skip("set FORGE_RUN_SUSTAINED_SSE=1 and FORGE_TEST_DATABASE_URL")
	}
	for _, mode := range []struct {
		name                       string
		writeBuffer, receiveBuffer int
	}{{"default_socket", 0, 0}, {"constrained_socket", 4096, 4096}} {
		t.Run(mode.name, func(t *testing.T) {
			const healthy, ticks, rate, payloadBytes = 4, 1000, 50, 16 << 10
			ctx, end := context.WithTimeout(context.Background(), 45*time.Second)
			defer end()
			started := time.Now()
			f := sustainedSetup(t, ctx)
			h := sustainedServer(f, mode.writeBuffer)
			defer h.server.Close()
			normalTransport := h.transport(false, 0)
			defer normalTransport.CloseIdleConnections()
			normalClient := &http.Client{Transport: normalTransport}
			slowTransport := h.transport(true, mode.receiveBuffer)
			defer slowTransport.CloseIdleConnections()
			slowClient := &http.Client{Transport: slowTransport}
			readersCtx, closeReaders := context.WithCancel(ctx)
			defer closeReaders()
			readers := make([]sustainedReader, healthy)
			ready := make(chan error, healthy)
			var wg sync.WaitGroup
			for i := range readers {
				readers[i].Samples = make([]sseSample, 0, ticks)
				wg.Go(func() { f.read(readersCtx, normalClient, h.server.URL, f.run.CoveredSeq, i, true, &readers[i], ready) })
			}
			if err := sustainedReady(ctx, healthy, ready); err != nil {
				closeReaders()
				wg.Wait()
				t.Fatal(err)
			}
			slowResponse, err := slowClient.Do(f.request(readersCtx, h.server.URL, f.run.CoveredSeq, true))
			if err != nil {
				closeReaders()
				wg.Wait()
				t.Fatal(err)
			}
			defer slowResponse.Body.Close()
			if slowResponse.StatusCode != 200 {
				closeReaders()
				wg.Wait()
				t.Fatalf("slow HTTP %d", slowResponse.StatusCode)
			}
			publications := make([]ssePublish, 0, ticks)
			resources := make([]sseResources, 0, 24)
			runtime.GC()
			resources = append(resources, sseMeasure("all_connections_ready"))
			publishStart := time.Now()
			var publishErr error
			postDisconnectEvents := 0
			for tick := 1; tick <= ticks; tick++ {
				due := publishStart.Add(time.Duration(tick) * time.Second / rate)
				timer := time.NewTimer(max(time.Duration(0), time.Until(due)))
				select {
				case <-ctx.Done():
					timer.Stop()
					publishErr = ctx.Err()
				case <-timer.C:
				}
				if publishErr != nil {
					break
				}
				at := time.Now()
				publishErr = f.append(ctx, tick, payloadBytes)
				done := time.Now()
				sample := ssePublish{Tick: tick, ScheduledAt: due, CommittedAt: done, ScheduleLatenessMS: at.Sub(due).Seconds() * 1000, AppendMS: done.Sub(at).Seconds() * 1000}
				if publishErr != nil {
					sample.Error = publishErr.Error()
				}
				publications = append(publications, sample)
				if h.slowTCPClosed.Load() > 0 {
					postDisconnectEvents++
				}
				if tick%rate == 0 {
					resources = append(resources, sseMeasure(fmt.Sprintf("publication_second_%d", tick/rate)))
				}
				if publishErr != nil {
					break
				}
			}
			publishSeconds := time.Since(publishStart).Seconds()
			healthyCaughtUp := sustainedWait(ctx, 5*time.Second, func() bool {
				for i := range readers {
					if readers[i].delivered.Load() != ticks {
						return false
					}
				}
				return true
			})
			disconnected := sustainedWait(ctx, 7*time.Second, func() bool { return h.slowReturns.Load() == 1 && h.slowTCPClosed.Load() == 1 })
			// Only after the server has actually closed does the slow reader drain its
			// kernel-buffered bytes. No harness cancellation may manufacture the proof.
			drainBytes := int64(0)
			drainError := "not attempted; server did not close"
			if disconnected {
				n, err := io.Copy(io.Discard, slowResponse.Body)
				drainBytes = n
				if err == nil {
					drainError = "EOF"
				} else {
					drainError = err.Error()
				}
			}
			slowReturns, slowClosed, slowMetric := h.slowReturns.Load(), h.slowTCPClosed.Load(), f.slowMetric()
			returnAt, closedAt := h.firstSlowReturn.Load(), h.firstSlowTCPClosed.Load()
			h.closing.Store(true)
			closeReaders()
			slowResponse.Body.Close()
			wg.Wait()
			normalTransport.CloseIdleConnections()
			slowTransport.CloseIdleConnections()
			cleaned := sustainedWait(ctx, 3*time.Second, func() bool { return h.activeTCP.Load() == 0 && h.activeHandlers.Load() == 0 })
			runtime.GC()
			resources = append(resources, sseMeasure("all_connections_closed"))
			var durable, minBytes, maxBytes, gaps int
			if err = f.owner.Pool.QueryRow(ctx, `SELECT count(*),coalesce(min(octet_length(payload::text)),0),coalesce(max(octet_length(payload::text)),0) FROM run_events WHERE type='benchmark.sustained'`).Scan(&durable, &minBytes, &maxBytes); err != nil {
				t.Fatal(err)
			}
			if err = f.owner.Pool.QueryRow(ctx, `SELECT count(*) FROM (SELECT run_id FROM run_events GROUP BY run_id HAVING min(seq)<>1 OR count(*)<>max(seq)) g`).Scan(&gaps); err != nil {
				t.Fatal(err)
			}
			allRead := true
			latencies := make([]float64, 0, healthy*ticks)
			for i := range readers {
				r := &readers[i]
				allRead = allRead && r.Error == "" && r.Count == ticks && r.LastSeq == f.run.CoveredSeq+ticks
				for _, sample := range r.Samples {
					latencies = append(latencies, sample.LatencyMS)
				}
			}
			distribution := sseDistribution(latencies)
			passed := publishErr == nil && healthyCaughtUp && allRead && disconnected && slowMetric >= 1 && postDisconnectEvents >= rate && cleaned && durable == ticks && minBytes == payloadBytes && maxBytes == payloadBytes && gaps == 0 && distribution.P95MS < 1000
			out := sustainedOutput(t, started, "sse-backpressure-"+mode.name)
			sustainedProfile(t, out, "final")
			report := map[string]any{"schema_version": 1, "timestamp_utc": started.UTC(), "scope": "Actual TCP slow-reader closure and healthy durable sequence isolation; no models/effects/containers", "mode": mode.name, "manifest": sustainedManifest(t), "database": f.database, "configuration": map[string]any{"healthy_readers": healthy, "nonreading_clients": 1, "runs": 1, "ticks": ticks, "offered_events_per_second": rate, "payload_bytes": payloadBytes, "scheduled_window_seconds": 20, "server_write_buffer_requested": mode.writeBuffer, "slow_receive_buffer_requested": mode.receiveBuffer, "handler_write_deadline_seconds": 5, "default_hub_queue": 128, "default_hub_history": 512, "default_hub_poll_ms": 100}, "publication_seconds": publishSeconds, "publication": publications, "readers": readers, "latency": distribution, "resources": resources, "sockets": h.sockets, "slow": map[string]any{"server_returns_before_harness_close": slowReturns, "tcp_closed_before_harness_close": slowClosed, "server_return_unix_nano": returnAt, "tcp_closed_unix_nano": closedAt, "queue_overflows": f.slowQueues.Load(), "slow_reason_metric": slowMetric, "events_committed_after_slow_tcp_close": postDisconnectEvents, "drained_buffered_bytes_after_close": drainBytes, "drain_result": drainError}, "invariants": map[string]any{"passed": passed, "healthy_caught_up": healthyCaughtUp, "all_healthy_exact_sequence": allRead, "durable_events": durable, "durable_sequence_gaps": gaps, "payload_min": minBytes, "payload_max": maxBytes, "tcp_and_handlers_zero": cleaned}, "limitations": []string{"Single local finite sample; not a production SLO or infinite-stream proof.", "Default socket mode sets no buffer sizes; constrained mode separately requests 4096-byte server send/slow-client receive buffers. Linux reports doubled values and may autotune defaults.", "This intentionally heavier 16-KiB/50-Hz one-run workload is distinct from the existing 200-connection 1-KiB/5-Hz baseline; no queue, deadline or handler delay is reduced.", "API, clients and publisher share a Go process; resources include the harness; latency uses the common local host clock.", "The slow body is not consumed until the server handler and actual TCP close have both been observed. Buffered bytes are then drained; harness cancellation is later."}}
			sustainedWriteReport(t, out, report)
			t.Logf("mode=%s passed=%v slow_return=%d tcp_closed=%d slow_metric=%.0f post_close_events=%d healthy_p95=%.3fms", mode.name, passed, slowReturns, slowClosed, slowMetric, postDisconnectEvents, distribution.P95MS)
			if !passed {
				t.Errorf("actual backpressure isolation acceptance failed; see retained report")
			}
		})
	}
}

// Plateau checks are fixed before observing results. They constrain a finite
// warmed process window; they are not a claim of absence of every possible leak.
type sustainedPlateau struct {
	Samples                     int     `json:"samples"`
	FirstWindowMeanBytes        float64 `json:"first_window_mean_bytes"`
	LastWindowMeanBytes         float64 `json:"last_window_mean_bytes"`
	WindowGrowthBytes           float64 `json:"window_growth_bytes"`
	LastHalfSlopeBytesPerSecond float64 `json:"last_half_slope_bytes_per_second"`
	MaxWindowGrowthBytes        float64 `json:"max_window_growth_bytes"`
	MaxSlopeBytesPerSecond      float64 `json:"max_slope_bytes_per_second"`
	Passed                      bool    `json:"passed"`
}

func assessSSEPlateau(samples []sseResources) sustainedPlateau {
	result := sustainedPlateau{Samples: len(samples), MaxWindowGrowthBytes: 1 << 20, MaxSlopeBytesPerSecond: 8 << 10}
	if len(samples) < 24 {
		return result
	}
	for _, s := range samples[:6] {
		result.FirstWindowMeanBytes += float64(s.HeapAlloc) / 6
	}
	for _, s := range samples[len(samples)-6:] {
		result.LastWindowMeanBytes += float64(s.HeapAlloc) / 6
	}
	result.WindowGrowthBytes = result.LastWindowMeanBytes - result.FirstWindowMeanBytes
	tail := samples[len(samples)/2:]
	start := tail[0].At
	var sumX, sumY, sumXX, sumXY float64
	for _, s := range tail {
		x := s.At.Sub(start).Seconds()
		y := float64(s.HeapAlloc)
		sumX += x
		sumY += y
		sumXX += x * x
		sumXY += x * y
	}
	n := float64(len(tail))
	denominator := n*sumXX - sumX*sumX
	if denominator <= 0 {
		return result
	}
	result.LastHalfSlopeBytesPerSecond = (n*sumXY - sumX*sumY) / denominator
	result.Passed = result.WindowGrowthBytes <= result.MaxWindowGrowthBytes && result.LastHalfSlopeBytesPerSecond <= result.MaxSlopeBytesPerSecond && !math.IsNaN(result.LastHalfSlopeBytesPerSecond)
	return result
}

func TestSSEPlateauOracle(t *testing.T) {
	samples := make([]sseResources, 24)
	at := time.Unix(0, 0)
	for i := range samples {
		samples[i] = sseResources{At: at.Add(time.Duration(i) * 5 * time.Second), HeapAlloc: uint64(8<<20 + (i%2)*(64<<10))}
	}
	if !assessSSEPlateau(samples).Passed {
		t.Fatal("bounded oscillating heap rejected")
	}
	for i := range samples {
		samples[i].HeapAlloc = 8<<20 + uint64(i)*(128<<10)
	}
	if assessSSEPlateau(samples).Passed {
		t.Fatal("persistent 25.6 KiB/s growth accepted")
	}
}
