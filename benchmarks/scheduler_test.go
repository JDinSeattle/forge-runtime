package benchmarks

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/testutil"
)

type sample struct {
	Index      int     `json:"run_index"`
	Worker     int     `json:"worker"`
	DurationMS float64 `json:"duration_ms"`
	Replay     bool    `json:"idempotent_replay,omitempty"`
}
type distribution struct {
	Count int     `json:"count"`
	MinMS float64 `json:"min_ms"`
	P50MS float64 `json:"p50_ms"`
	P95MS float64 `json:"p95_ms"`
	P99MS float64 `json:"p99_ms"`
	MaxMS float64 `json:"max_ms"`
}
type phase struct {
	WallSeconds         float64      `json:"wall_seconds"`
	OperationsPerSecond float64      `json:"operations_per_second"`
	Latency             distribution `json:"latency"`
	Samples             []sample     `json:"samples"`
}
type evidence struct {
	SchemaVersion int               `json:"schema_version"`
	Timestamp     string            `json:"timestamp_utc"`
	Scope         string            `json:"scope"`
	Limitations   []string          `json:"limitations"`
	Machine       map[string]any    `json:"machine"`
	Database      map[string]any    `json:"database"`
	Configuration map[string]any    `json:"configuration"`
	SourceSHA256  map[string]string `json:"source_sha256"`
	Submit        phase             `json:"submit"`
	Claim         phase             `json:"claim"`
	Invariants    map[string]any    `json:"invariants"`
}

// TestSchedulerCompetitionEvidence is opt-in: it creates and drops only its own
// schema in the explicit loopback Forge database. It does not call models,
// runners, shells, or effect executors. It is a submit/claim experiment, not an
// end-to-end repair benchmark or evidence of effect exactly-once execution.
func TestSchedulerCompetitionEvidence(t *testing.T) {
	if os.Getenv("FORGE_RUN_BENCHMARK") != "1" {
		t.Skip("set FORGE_RUN_BENCHMARK=1 and FORGE_TEST_DATABASE_URL for scoped benchmark")
	}
	const count, tenants, submitters, workers = 1000, 10, 8, 2
	const replayEvery = 10
	s := testutil.Database(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	started := time.Now()
	var version, sharedBuffers, synchronousCommit, fsync string
	if err := s.Pool.QueryRow(ctx, "SELECT version(),current_setting('shared_buffers'),current_setting('synchronous_commit'),current_setting('fsync')").Scan(&version, &sharedBuffers, &synchronousCommit, &fsync); err != nil {
		t.Fatal(err)
	}
	runnerID := string(persistence.NewID("benchmark_runner"))
	if err := s.RegisterRunner(ctx, runnerID, "unix:///benchmark-not-started.sock", count); err != nil {
		t.Fatal(err)
	}
	type fixture struct{ tenant, principal, project domain.ID }
	fixtures := make([]fixture, tenants)
	for i := range fixtures {
		f := fixture{tenant: persistence.NewID("benchmark_tenant"), principal: persistence.NewID("benchmark_principal")}
		if err := s.BootstrapTenant(ctx, f.tenant, f.principal, "developer"); err != nil {
			t.Fatal(err)
		}
		p, err := s.CreateProject(ctx, f.tenant, "scheduler fixture", "no-source-dispatched", "no-profile-dispatched")
		if err != nil {
			t.Fatal(err)
		}
		f.project = p.ID
		fixtures[i] = f
		if _, err := s.Pool.Exec(ctx, `UPDATE tenant_runtime SET max_active=$2 WHERE tenant_id=$1`, f.tenant, count/tenants); err != nil {
			t.Fatal(err)
		}
	}
	configuration := persistence.Config{Provider: "fake", Model: "not-dispatched", MaxModelRounds: 1, MaxToolCalls: 1, MaxCost: 1000, MaxRuntimeSeconds: 600}
	ids := make([]domain.ID, count)
	var submitSamples []sample
	var sampleMu sync.Mutex
	var next atomic.Int64
	errs := make(chan error, submitters+workers)
	var wg sync.WaitGroup
	submitStart := time.Now()
	for worker := range submitters {
		wg.Go(func() {
			for {
				i := int(next.Add(1)) - 1
				if i >= count {
					return
				}
				f := fixtures[i%tenants]
				req := persistence.SubmitRequest{TenantID: f.tenant, PrincipalID: f.principal, ProjectID: f.project, Task: "Measure durable admission and claim only", BaseCommit: "benchmark-no-checkout", Config: configuration}
				key := fmt.Sprintf("run-%04d", i)
				at := time.Now()
				r, replay, err := s.Submit(ctx, req, key)
				elapsed := time.Since(at).Seconds() * 1000
				if err != nil {
					errs <- fmt.Errorf("submit %d: %w", i, err)
					return
				}
				if replay {
					errs <- fmt.Errorf("first submission %d unexpectedly replayed", i)
					return
				}
				ids[i] = r.ID
				sampleMu.Lock()
				submitSamples = append(submitSamples, sample{Index: i, Worker: worker, DurationMS: elapsed})
				sampleMu.Unlock()
				if i%replayEvery == 0 {
					at = time.Now()
					again, replayed, err := s.Submit(ctx, req, key)
					elapsed = time.Since(at).Seconds() * 1000
					if err != nil || !replayed || again.ID != r.ID {
						errs <- fmt.Errorf("idempotent replay %d mismatched: replay=%v err=%v", i, replayed, err)
						return
					}
					sampleMu.Lock()
					submitSamples = append(submitSamples, sample{Index: i, Worker: worker, DurationMS: elapsed, Replay: true})
					sampleMu.Unlock()
				}
			}
		})
	}
	wg.Wait()
	submitWall := time.Since(submitStart)
	select {
	case err := <-errs:
		t.Fatal(err)
	default:
	}
	if len(submitSamples) != count+count/replayEvery {
		t.Fatalf("submit samples=%d", len(submitSamples))
	}
	indexByID := make(map[domain.ID]int, count)
	for i, id := range ids {
		if id == "" {
			t.Fatalf("missing submission %d", i)
		}
		if _, duplicate := indexByID[id]; duplicate {
			t.Fatal("duplicate run IDs")
		}
		indexByID[id] = i
	}
	var claimed atomic.Int64
	var emptyPolls atomic.Int64
	var capacityPolls atomic.Int64
	claimSamples := make([]sample, 0, count)
	seen := make(map[domain.ID]bool, count)
	claimsByWorker := make([]int, workers)
	claimStart := time.Now()
	for worker := range workers {
		wg.Go(func() {
			for claimed.Load() < count {
				at := time.Now()
				r, err := s.Claim(ctx, fmt.Sprintf("benchmark-worker-%d", worker), 5*time.Minute)
				elapsed := time.Since(at).Seconds() * 1000
				if errors.Is(err, domain.ErrNotFound) || errors.Is(err, domain.ErrCapacity) {
					if errors.Is(err, domain.ErrCapacity) {
						capacityPolls.Add(1)
					} else {
						emptyPolls.Add(1)
					}
					select {
					case <-ctx.Done():
						errs <- ctx.Err()
						return
					case <-time.After(time.Millisecond):
					}
					continue
				}
				if err != nil {
					errs <- fmt.Errorf("claim worker %d: %w", worker, err)
					return
				}
				if r.State.Status != domain.StatusRunning || r.State.Lease.Epoch != 1 || r.State.Version != 2 || r.CoveredSeq != 2 {
					errs <- fmt.Errorf("claimed run envelope invariant failed: status=%s epoch=%d version=%d covered=%d", r.State.Status, r.State.Lease.Epoch, r.State.Version, r.CoveredSeq)
					return
				}
				sampleMu.Lock()
				index, exists := indexByID[r.ID]
				duplicate := seen[r.ID]
				if exists && !duplicate {
					seen[r.ID] = true
					claimSamples = append(claimSamples, sample{Index: index, Worker: worker, DurationMS: elapsed})
					claimsByWorker[worker]++
				}
				sampleMu.Unlock()
				if !exists || duplicate {
					errs <- fmt.Errorf("unknown or duplicate claim")
					return
				}
				claimed.Add(1)
			}
		})
	}
	wg.Wait()
	claimWall := time.Since(claimStart)
	select {
	case err := <-errs:
		t.Fatal(err)
	default:
	}
	if len(seen) != count || claimsByWorker[0] == 0 || claimsByWorker[1] == 0 {
		t.Fatalf("claims %d by worker %v", len(seen), claimsByWorker)
	}
	var durableRuns, firstEpoch, eventRows, claimEvents, allocations, tenantActive, runnerSlots, snapshots, effects, modelAttempts int
	if err := s.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM runs),(SELECT count(*) FROM runs WHERE state='running' AND lease_epoch=1),(SELECT count(*) FROM run_events),(SELECT count(*) FROM run_events WHERE type='run.claimed'),(SELECT count(*) FROM runner_allocations WHERE state='reserved'),(SELECT sum(active_count) FROM tenant_runtime),(SELECT sum(reserved_slots) FROM runners),(SELECT count(*) FROM run_snapshots),(SELECT count(*) FROM effects),(SELECT count(*) FROM model_attempts)`).Scan(&durableRuns, &firstEpoch, &eventRows, &claimEvents, &allocations, &tenantActive, &runnerSlots, &snapshots, &effects, &modelAttempts); err != nil {
		t.Fatal(err)
	}
	if durableRuns != count || firstEpoch != count || eventRows != count*2 || claimEvents != count || allocations != count || tenantActive != count || runnerSlots != count || snapshots != count*2 || effects != 0 || modelAttempts != 0 {
		t.Fatalf("durable invariants mismatch: runs=%d epoch1=%d events=%d claims=%d allocations=%d active=%d slots=%d snapshots=%d effects=%d model=%d", durableRuns, firstEpoch, eventRows, claimEvents, allocations, tenantActive, runnerSlots, snapshots, effects, modelAttempts)
	}
	var eventGaps int
	if err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM (SELECT run_id FROM run_events GROUP BY run_id HAVING min(seq)<>1 OR max(seq)<>2 OR count(*)<>2) gaps`).Scan(&eventGaps); err != nil || eventGaps != 0 {
		t.Fatalf("event gaps=%d err=%v", eventGaps, err)
	}
	stat := s.Pool.Stat()
	report := evidence{SchemaVersion: 1, Timestamp: started.UTC().Format(time.RFC3339Nano), Scope: "Real PostgreSQL durable submission, idempotent HTTP-equivalent request replay, and two competing in-process scheduler workers; no model or runner execution", Limitations: []string{"One sample on a shared development machine, not a capacity or SLO claim.", "Workers are two goroutines sharing one pgxpool, not separate hosts/processes.", "Runs stop in running after claim and are removed with their isolated test schema; no terminal/effect correctness claim.", "Claims retain 1000 allocation slots to avoid conflating scheduler timing with fake execution or cleanup.", "Submission and claim phases are separate; replay timings are included in the submit distribution.", "No container, provider API, RLS performance, crash recovery, or SSE load is measured."}, Machine: machine(), Database: map[string]any{"version": version, "shared_buffers": sharedBuffers, "synchronous_commit": synchronousCommit, "fsync": fsync, "pool_max_connections": s.Pool.Config().MaxConns, "pool_min_connections": s.Pool.Config().MinConns, "pool_total_connections_at_end": stat.TotalConns(), "pool_acquire_count": stat.AcquireCount(), "pool_acquire_duration_seconds": stat.AcquireDuration().Seconds(), "pool_empty_acquire_count": stat.EmptyAcquireCount()}, Configuration: map[string]any{"runs": count, "tenants": tenants, "submitters": submitters, "scheduler_workers": workers, "idempotent_replays": count / replayEvery, "runner_slots": count, "tenant_max_active": count / tenants, "lease_seconds": 300, "request_context_seconds": 120}, SourceSHA256: sourceHashes(t), Submit: makePhase(submitSamples, submitWall), Claim: makePhase(claimSamples, claimWall), Invariants: map[string]any{"passed": true, "unique_submitted_runs": durableRuns, "unique_claimed_runs": len(seen), "running_at_first_epoch": firstEpoch, "claims_by_worker": claimsByWorker, "empty_skip_locked_polls": emptyPolls.Load(), "capacity_or_runner_lock_polls": capacityPolls.Load(), "durable_events": eventRows, "durable_claim_events": claimEvents, "event_gaps": eventGaps, "snapshots": snapshots, "reserved_allocations": allocations, "tenant_active_total": tenantActive, "runner_reserved_slots": runnerSlots, "effect_rows": effects, "model_attempt_rows": modelAttempts}}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	outDir := filepath.Join("results", "local")
	if err := os.MkdirAll(outDir, 0755); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(outDir, "scheduler-"+started.UTC().Format("20060102T150405.000000000Z")+".json")
	if err := os.WriteFile(out, append(encoded, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
	abs, _ := filepath.Abs(out)
	t.Logf("evidence=%s", abs)
	t.Logf("submit: %d requests, %.1f req/s, p50=%.3fms p95=%.3fms p99=%.3fms", report.Submit.Latency.Count, report.Submit.OperationsPerSecond, report.Submit.Latency.P50MS, report.Submit.Latency.P95MS, report.Submit.Latency.P99MS)
	t.Logf("claim: %d runs, %.1f claims/s, p50=%.3fms p95=%.3fms p99=%.3fms; workers=%v", report.Claim.Latency.Count, report.Claim.OperationsPerSecond, report.Claim.Latency.P50MS, report.Claim.Latency.P95MS, report.Claim.Latency.P99MS, claimsByWorker)
}

func makePhase(samples []sample, elapsed time.Duration) phase {
	values := make([]float64, len(samples))
	for i, s := range samples {
		values[i] = s.DurationMS
	}
	sort.Float64s(values)
	quantile := func(q float64) float64 { return values[max(0, int(math.Ceil(q*float64(len(values))))-1)] }
	return phase{WallSeconds: elapsed.Seconds(), OperationsPerSecond: float64(len(samples)) / elapsed.Seconds(), Latency: distribution{Count: len(values), MinMS: values[0], P50MS: quantile(.5), P95MS: quantile(.95), P99MS: quantile(.99), MaxMS: values[len(values)-1]}, Samples: samples}
}
func machine() map[string]any {
	result := map[string]any{"os": runtime.GOOS, "architecture": runtime.GOARCH, "go_version": runtime.Version(), "logical_cpus": runtime.NumCPU(), "gomaxprocs": runtime.GOMAXPROCS(0)}
	if data, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "model name") {
				_, model, _ := strings.Cut(line, ":")
				result["cpu_model"] = strings.TrimSpace(model)
				break
			}
		}
	}
	if data, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "MemTotal:") {
				result["host_memory"] = strings.TrimSpace(strings.TrimPrefix(line, "MemTotal:"))
				break
			}
		}
	}
	for key, path := range map[string]string{"cgroup_cpu_max": "/sys/fs/cgroup/cpu.max", "cgroup_memory_max": "/sys/fs/cgroup/memory.max"} {
		if data, err := os.ReadFile(path); err == nil {
			result[key] = strings.TrimSpace(string(data))
		}
	}
	return result
}
func sourceHashes(t *testing.T) map[string]string {
	t.Helper()
	result := map[string]string{}
	for _, path := range []string{"benchmarks/scheduler_test.go", "go.mod", "internal/persistence/store.go", "internal/persistence/scheduler.go", "internal/runtime/reducer.go"} {
		data, err := os.ReadFile(filepath.Join("..", path))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		result[path] = hex.EncodeToString(sum[:])
	}
	return result
}
