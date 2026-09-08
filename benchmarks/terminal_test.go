package benchmarks

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/provider"
	"github.com/JDinSeattle/forge-runtime/internal/quota"
	flow "github.com/JDinSeattle/forge-runtime/internal/runtime"
	"github.com/JDinSeattle/forge-runtime/internal/testutil"
)

// simulatedExecutor models a receipt-addressable side effect. Its map is only
// test-fixture idempotency, not production execution evidence. The benchmark
// separately checks that real PostgreSQL effects/confirmations match this map.
type simulatedExecutor struct {
	mu       sync.Mutex
	results  map[domain.ID]flow.EffectReceipt
	starts   map[domain.ID]int
	requests int
}

func (x *simulatedExecutor) execute(e flow.Effect, ref string) (flow.EffectReceipt, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.requests++
	if previous, exists := x.results[e.ID]; exists {
		if previous.ArgsHash != e.ArgsHash || previous.Epoch != e.DispatchEpoch || previous.BeforeRevision != e.ExpectedRevision || previous.Ref != ref {
			return flow.EffectReceipt{}, domain.ErrConflict
		}
		return previous, nil
	}
	x.starts[e.ID]++
	result := flow.EffectReceipt{EffectID: e.ID, ArgsHash: e.ArgsHash, Epoch: e.DispatchEpoch, Status: flow.EffectSucceeded, BeforeRevision: e.ExpectedRevision, AfterRevision: e.ExpectedRevision, Ref: ref, Settled: true}
	x.results[e.ID] = result
	return result, nil
}

type terminalRecord struct {
	RunID                            domain.ID          `json:"run_id"`
	TenantID                         domain.ID          `json:"tenant_id"`
	Worker                           int                `json:"worker"`
	DurationMS                       float64            `json:"duration_ms"`
	Status                           domain.RunStatus   `json:"status"`
	Version                          uint64             `json:"version"`
	CoveredSeq                       uint64             `json:"covered_seq"`
	ModelCalls                       int                `json:"fake_provider_calls"`
	CostMicroUSD                     domain.Money       `json:"synthetic_cost_microusd"`
	Receipt                          flow.EffectReceipt `json:"simulated_receipt"`
	DuplicateConfirmationRejected    bool               `json:"duplicate_confirmation_rejected"`
	RepeatedTerminalControlUnchanged bool               `json:"repeated_terminal_control_unchanged"`
}

type terminalHarness struct {
	store                *persistence.Store
	quota                *quota.Store
	artifacts            *artifact.LocalStore
	executor             *simulatedExecutor
	providerCalls        atomic.Int64
	providerEvents       atomic.Int64
	duplicateSettlements atomic.Int64
}

// TestTerminalSimulationEvidence exercises a finite, explicitly simulated
// controller rather than application.Driver. All claims, transitions, model
// attempts, quota settlement, artifact bytes and ledgers use real adapters.
// FakeProvider performs actual stream assembly; the executor and verifier are
// simulated and do not run commands or establish code-repair success.
func TestTerminalSimulationEvidence(t *testing.T) {
	if os.Getenv("FORGE_RUN_TERMINAL_BENCHMARK") != "1" {
		t.Skip("set FORGE_RUN_TERMINAL_BENCHMARK=1 and the dedicated loopback database")
	}
	count := 1000
	if value := os.Getenv("FORGE_TERMINAL_RUNS"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 10 || n > 1000 {
			t.Fatal("FORGE_TERMINAL_RUNS must be 10..1000")
		}
		count = n
	}
	const tenants, workers, submitters = 10, 2, 8
	s := testutil.Database(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	start := time.Now()
	objects, err := artifact.NewLocalStore(filepath.Join(t.TempDir(), "objects"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = objects.Close() })
	h := &terminalHarness{store: s, quota: quota.New(s.Pool), artifacts: objects, executor: &simulatedExecutor{results: make(map[domain.ID]flow.EffectReceipt), starts: make(map[domain.ID]int)}}
	if err := h.quota.Configure(ctx, quota.Config{CredentialGroup: "terminal-fake", MaxConcurrent: workers, MaxTokens: int64(count) * 4096, MaxCost: domain.Money(count) * 100, WindowDuration: time.Hour, FailureThreshold: 3, BreakerCooldown: time.Second}); err != nil {
		t.Fatal(err)
	}
	if err := s.RegisterRunner(ctx, "terminal-simulated-runner", "simulation-only-no-transport", workers); err != nil {
		t.Fatal(err)
	}
	type fixture struct{ tenant, principal, project domain.ID }
	fixtures := make([]fixture, tenants)
	for i := range fixtures {
		f := fixture{tenant: persistence.NewID("simtenant"), principal: persistence.NewID("simprincipal")}
		if err := s.BootstrapTenant(ctx, f.tenant, f.principal, "developer"); err != nil {
			t.Fatal(err)
		}
		p, err := s.CreateProject(ctx, f.tenant, "terminal simulation", "no-checkout", "simulated-verifier")
		if err != nil {
			t.Fatal(err)
		}
		f.project = p.ID
		fixtures[i] = f
	}
	var version, fsync, commit, sharedBuffers string
	if err := s.Pool.QueryRow(ctx, "SELECT version(),current_setting('fsync'),current_setting('synchronous_commit'),current_setting('shared_buffers')").Scan(&version, &fsync, &commit, &sharedBuffers); err != nil {
		t.Fatal(err)
	}
	ids := make([]domain.ID, count)
	var next atomic.Int64
	var wg sync.WaitGroup
	failures := make(chan error, submitters+workers)
	fail := func(err error) { failures <- err; cancel() }
	submitStart := time.Now()
	for range submitters {
		wg.Go(func() {
			for {
				i := int(next.Add(1)) - 1
				if i >= count {
					return
				}
				f := fixtures[i%tenants]
				r, _, err := s.Submit(ctx, persistence.SubmitRequest{TenantID: f.tenant, PrincipalID: f.principal, ProjectID: f.project, Task: "Synthetic terminal/effect accounting fixture; not a code repair", BaseCommit: "simulation-no-repository", Config: persistence.Config{Provider: "fake", Model: "fake", MaxModelRounds: 3, MaxToolCalls: 2, MaxCost: 100, MaxRuntimeSeconds: 900}}, fmt.Sprintf("terminal-%04d", i))
				if err != nil {
					fail(fmt.Errorf("submit %d: %w", i, err))
					return
				}
				ids[i] = r.ID
			}
		})
	}
	wg.Wait()
	submitWall := time.Since(submitStart)
	select {
	case err := <-failures:
		t.Fatal(err)
	default:
	}
	index := make(map[domain.ID]int, count)
	for i, id := range ids {
		if id == "" {
			t.Fatal("missing submission")
		}
		if _, ok := index[id]; ok {
			t.Fatal("duplicate submitted ID")
		}
		index[id] = i
	}
	var done, emptyPolls, capacityPolls atomic.Int64
	var mu sync.Mutex
	records := make([]terminalRecord, 0, count)
	seen := make(map[domain.ID]bool, count)
	byWorker := make([]int, workers)
	workStart := time.Now()
	for worker := range workers {
		wg.Go(func() {
			for done.Load() < int64(count) {
				at := time.Now()
				r, err := s.ClaimOnRunner(ctx, fmt.Sprintf("terminal-worker-%d", worker), 5*time.Minute, "terminal-simulated-runner")
				if errors.Is(err, domain.ErrNotFound) || errors.Is(err, domain.ErrCapacity) {
					if errors.Is(err, domain.ErrCapacity) {
						capacityPolls.Add(1)
					} else {
						emptyPolls.Add(1)
					}
					select {
					case <-ctx.Done():
						return
					case <-time.After(2 * time.Millisecond):
					}
					continue
				}
				if err != nil {
					fail(fmt.Errorf("claim: %w", err))
					return
				}
				mu.Lock()
				_, known := index[r.ID]
				duplicate := seen[r.ID]
				if known && !duplicate {
					seen[r.ID] = true
				}
				mu.Unlock()
				if !known || duplicate || r.State.Lease.Epoch != 1 {
					fail(fmt.Errorf("unknown/duplicate/reclaimed task %s", r.ID))
					return
				}
				record, err := h.drive(ctx, r)
				if err != nil {
					fail(fmt.Errorf("drive %s: %w", r.ID, err))
					return
				}
				record.Worker = worker
				record.DurationMS = time.Since(at).Seconds() * 1000
				mu.Lock()
				records = append(records, record)
				byWorker[worker]++
				mu.Unlock()
				done.Add(1)
			}
		})
	}
	wg.Wait()
	workWall := time.Since(workStart)
	select {
	case err := <-failures:
		t.Fatal(err)
	default:
	}
	if len(records) != count || byWorker[0] == 0 || byWorker[1] == 0 {
		t.Fatalf("terminal count=%d workers=%v", len(records), byWorker)
	}
	invariants := h.assertLedgers(t, ctx, count)
	invariants["runs_by_worker"] = byWorker
	invariants["empty_polls"] = emptyPolls.Load()
	invariants["capacity_or_runner_lock_polls"] = capacityPolls.Load()
	invariants["simulated_provider_calls"] = h.providerCalls.Load()
	invariants["simulated_provider_events"] = h.providerEvents.Load()
	invariants["duplicate_settlement_calls"] = h.duplicateSettlements.Load()
	latencies := make([]sample, 0, count)
	for _, r := range records {
		latencies = append(latencies, sample{Index: index[r.RunID], Worker: r.Worker, DurationMS: r.DurationMS})
	}
	hashes := sourceHashes(t)
	for _, path := range []string{"benchmarks/terminal_test.go", "internal/quota/store.go", "internal/persistence/execution.go", "internal/persistence/model_pricing.go", "internal/provider/fake.go"} {
		data, err := os.ReadFile(filepath.Join("..", path))
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(data)
		hashes[path] = hex.EncodeToString(digest[:])
	}
	report := map[string]any{"schema_version": 1, "timestamp_utc": start.UTC().Format(time.RFC3339Nano), "scope": "Finite simulated controller through real PostgreSQL Store transitions/attempt/quota APIs, real local artifact bytes, actual FakeProvider stream assembly and an in-memory FakeExecutor; not application.Driver, HTTP or Docker", "machine": machine(), "database": map[string]any{"version": version, "fsync": fsync, "synchronous_commit": commit, "shared_buffers": sharedBuffers, "pool_max_connections": s.Pool.Config().MaxConns, "pool_min_connections": s.Pool.Config().MinConns}, "configuration": map[string]any{"runs": count, "tenants": tenants, "workers": workers, "submitters": submitters, "runner_slots": workers, "tenant_max_active": 2, "provider_concurrency": workers, "fake_model_turns_per_run": 2, "simulated_effects_per_run": 1, "simulated_start_requests_per_effect": 2, "synthetic_cost_per_turn_microusd": 3}, "submit_wall_seconds": submitWall.Seconds(), "execution_phase": makePhase(latencies, workWall), "run_records": records, "invariants": invariants, "source_sha256": hashes, "limitations": []string{"Two goroutines in one process, not separate OS worker processes or a crash-recovery workload.", "Finite event script uses Store APIs directly; it does not test application.Driver command routing or production runner idempotency.", "FakeExecutor map deduplication models an idempotent executor; no shell/file-write side effect occurs.", "Synthetic trusted verification sets regression_only, not verified; no code-correctness claim.", "Two actual FakeProvider assemblies per run; no native network request, paid model, automatic retries or prompt compaction.", "Synthetic cost is fixed test data; PostgreSQL settlement/replay arithmetic is measured, provider billing accuracy is not.", "Artifact bytes are real temporary local files, removed along with fixture database schema after the test.", "One shared-machine sample; no HTTP/SSE throughput or production SLO claim."}}
	body, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join("results", "local")
	if err = os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "terminal-simulation-"+start.UTC().Format("20060102T150405.000000000Z")+".json")
	if err = os.WriteFile(path, append(body, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
	absolute, _ := filepath.Abs(path)
	t.Logf("evidence=%s", absolute)
	t.Logf("terminal=%d effects=%d fake_model_calls=%d rate=%.2f runs/s workers=%v", count, count, h.providerCalls.Load(), float64(count)/workWall.Seconds(), byWorker)
}

func (h *terminalHarness) publish(ctx context.Context, r persistence.Run, kind string, body any, id domain.ID) (string, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	ref, err := h.artifacts.Put(ctx, r.TenantID, r.ID, kind, bytes.NewReader(raw))
	if err != nil {
		return "", err
	}
	verified, err := h.artifacts.Stat(ctx, r.TenantID, r.ID, ref)
	if err != nil || verified.SHA256 != ref.SHA256 || verified.Size != ref.Size {
		return "", fmt.Errorf("artifact validation: %w", err)
	}
	if id == "" {
		id = persistence.NewID("simartifact")
	}
	err = h.store.PublishArtifact(ctx, persistence.Artifact{TenantID: r.TenantID, RunID: r.ID, ID: id, Kind: kind, ObjectKey: ref.ObjectKey, SHA256: ref.SHA256, ByteSize: ref.Size})
	return string(id), err
}

func (h *terminalHarness) drive(ctx context.Context, r persistence.Run) (terminalRecord, error) {
	advance := func(event flow.Event) error {
		event.ExpectedVersion = r.State.Version
		event.Owner = r.State.Lease.Owner
		event.Epoch = r.State.Lease.Epoch
		next, err := h.store.Advance(ctx, r.TenantID, r.ID, event)
		if err == nil {
			r = next
		}
		return err
	}
	baseline, err := h.publish(ctx, r, "simulation_baseline", map[string]any{"simulated": true, "revision": 1}, "")
	if err != nil {
		return terminalRecord{}, err
	}
	if err = advance(flow.Event{Kind: flow.EventWorkspaceReady, WorkspaceRevision: 1, OutputRef: baseline}); err != nil {
		return terminalRecord{}, err
	}
	contextRef, err := h.publish(ctx, r, "simulation_context", map[string]any{"simulated": true, "task": "read synthetic fixture then finish"}, "")
	if err != nil {
		return terminalRecord{}, err
	}
	if err = advance(flow.Event{Kind: flow.EventContextBuilt, OutputRef: contextRef}); err != nil {
		return terminalRecord{}, err
	}
	turn, modelRef, err := h.model(ctx, r, contextRef, false)
	if err != nil {
		return terminalRecord{}, err
	}
	if err = advance(flow.Event{Kind: flow.EventModelCompleted, Complete: true, OutputRef: modelRef, Cost: 3}); err != nil {
		return terminalRecord{}, err
	}
	if len(turn.ToolCalls) != 1 {
		return terminalRecord{}, fmt.Errorf("fake first turn lacks one tool")
	}
	call := turn.ToolCalls[0]
	args, err := domain.CanonicalJSON(call.Arguments)
	if err != nil {
		return terminalRecord{}, err
	}
	hash := sha256.Sum256(args)
	effect := flow.Effect{ID: domain.ID(string(r.ID) + "_read"), Kind: call.Name, Args: args, ArgsHash: hex.EncodeToString(hash[:]), ExpectedRevision: 1, PolicyVersion: "simulation-v1", Status: flow.EffectPlanned}
	if err = advance(flow.Event{Kind: flow.EventToolsValidated, Complete: true, OutputRef: modelRef, Effects: []flow.Effect{effect}}); err != nil {
		return terminalRecord{}, err
	}
	if r.State.PendingEffect == nil {
		return terminalRecord{}, fmt.Errorf("missing pending effect")
	}
	effect = *r.State.PendingEffect
	refID := domain.ID("simreceipt_" + string(r.ID))
	receipt, err := h.executor.execute(effect, string(refID))
	if err != nil {
		return terminalRecord{}, err
	}
	again, err := h.executor.execute(effect, string(refID))
	if err != nil || again != receipt {
		return terminalRecord{}, fmt.Errorf("simulated receipt replay differs")
	}
	if _, err = h.publish(ctx, r, "simulation_effect_receipt", receipt, refID); err != nil {
		return terminalRecord{}, err
	}
	confirmation := flow.Event{Kind: flow.EventEffectCompleted, ExpectedVersion: r.State.Version, Owner: r.State.Lease.Owner, Epoch: r.State.Lease.Epoch, Receipt: &receipt}
	if err = advance(confirmation); err != nil {
		return terminalRecord{}, err
	}
	if _, err = h.store.Advance(ctx, r.TenantID, r.ID, confirmation); !errors.Is(err, domain.ErrConflict) {
		return terminalRecord{}, fmt.Errorf("duplicate confirmation accepted: %v", err)
	}
	if err = advance(flow.Event{Kind: flow.EventResultsIngested, OutputRef: string(refID)}); err != nil {
		return terminalRecord{}, err
	}
	if err = advance(flow.Event{Kind: flow.EventContextBuilt, OutputRef: contextRef}); err != nil {
		return terminalRecord{}, err
	}
	_, modelRef, err = h.model(ctx, r, contextRef, true)
	if err != nil {
		return terminalRecord{}, err
	}
	if err = advance(flow.Event{Kind: flow.EventModelCompleted, Complete: true, Finish: true, OutputRef: modelRef, Cost: 3}); err != nil {
		return terminalRecord{}, err
	}
	verificationRef, err := h.publish(ctx, r, "simulation_verification", map[string]any{"simulated": true, "regression_only": true}, "")
	if err != nil {
		return terminalRecord{}, err
	}
	if err = advance(flow.Event{Kind: flow.EventVerificationCompleted, Verification: &flow.VerificationEvidence{Trusted: true, ReportRef: verificationRef, WorkspaceRevision: 1, RegressionPassed: true}}); err != nil {
		return terminalRecord{}, err
	}
	finalRef, err := h.publish(ctx, r, "simulation_final", map[string]any{"simulated": true, "effect_id": effect.ID, "not_a_patch": true}, "")
	if err != nil {
		return terminalRecord{}, err
	}
	if err = advance(flow.Event{Kind: flow.EventFinalized, OutputRef: finalRef}); err != nil {
		return terminalRecord{}, err
	}
	identity := persistence.Identity{TenantID: r.TenantID, PrincipalID: r.PrincipalID, Role: "developer"}
	repeated, err := h.store.Cancel(ctx, identity, r.ID)
	if err != nil {
		return terminalRecord{}, err
	}
	unchanged := repeated.State.Version == r.State.Version && repeated.State.Status == domain.StatusCompleted
	if !unchanged || r.State.Verification != domain.VerificationRegressionOnly || r.State.Cost != 6 {
		return terminalRecord{}, fmt.Errorf("terminal status/cost/verification mismatch")
	}
	return terminalRecord{RunID: r.ID, TenantID: r.TenantID, Status: r.State.Status, Version: r.State.Version, CoveredSeq: r.CoveredSeq, ModelCalls: 2, CostMicroUSD: r.State.Cost, Receipt: receipt, DuplicateConfirmationRejected: true, RepeatedTerminalControlUnchanged: unchanged}, nil
}

func (h *terminalHarness) model(ctx context.Context, r persistence.Run, requestRef string, finish bool) (provider.ModelTurn, string, error) {
	pricing := json.RawMessage(`{"provider":"fake","model":"fake","price_version":"simulation-price-v1","synthetic_cost_microusd":3}`)
	a, _, err := h.store.BeginPricedAttempt(ctx, r, requestRef, "simulation-price-v1", time.Minute, pricing)
	if err != nil {
		return provider.ModelTurn{}, "", err
	}
	_, err = h.quota.Reserve(ctx, quota.Request{TenantID: string(r.TenantID), RunID: string(r.ID), AttemptID: string(a.ID), CredentialGroup: "terminal-fake", InputTokens: 512, MaxOutputTokens: 64, MaxCost: 10, PriceVersion: "simulation-price-v1", RequestDeadline: a.Deadline})
	if err != nil {
		return provider.ModelTurn{}, "", err
	}
	if err = h.quota.MarkDispatched(ctx, string(r.TenantID), string(a.ID)); err != nil {
		return provider.ModelTurn{}, "", err
	}
	script := provider.Script{Usage: provider.Usage{Input: provider.TokenCount{Value: 32, Known: true}, Output: provider.TokenCount{Value: 16, Known: true}}, FinishReason: "tool_calls", Chunks: []provider.Chunk{{Kind: "tool_start", CallID: "read", Name: "read_file"}, {Kind: "tool_delta", CallID: "read", Delta: `{"path":"synthetic.txt"}`}, {Kind: "tool_end", CallID: "read"}}}
	if finish {
		script.FinishReason = "stop"
		script.Chunks = []provider.Chunk{{Kind: "text", Delta: "Simulation ready for synthetic verification."}}
	}
	request := provider.ModelRequest{RunID: string(r.ID), StepID: strconv.FormatUint(r.State.StepSeq, 10), AttemptID: string(a.ID), ModelID: "fake", MaxOutputTokens: 64, Deadline: a.Deadline, Messages: []provider.Message{{Role: "user", Text: "Exercise simulated accounting"}}, Tools: []provider.Tool{{Name: "read_file", Description: "Simulated bounded read", Schema: json.RawMessage(`{"type":"object","properties":{"path":{"type":"string"}},"required":["path"],"additionalProperties":false}`)}}}
	h.providerCalls.Add(1)
	turn, err := provider.NewFake(script).Stream(ctx, request, func(provider.ModelEvent) error { h.providerEvents.Add(1); return nil })
	if err != nil {
		return provider.ModelTurn{}, "", err
	}
	ref, err := h.publish(ctx, r, "simulation_model_response", turn, "")
	if err != nil {
		return provider.ModelTurn{}, "", err
	}
	usage, _ := json.Marshal(turn.Usage)
	if err = h.store.CompleteAttempt(ctx, a, ref, "simulation-request", usage); err != nil {
		return provider.ModelTurn{}, "", err
	}
	settlement := quota.Settlement{Tokens: 48, Cost: 3}
	if err = h.quota.Settle(ctx, string(r.TenantID), string(a.ID), settlement); err != nil {
		return provider.ModelTurn{}, "", err
	}
	if err = h.quota.Settle(ctx, string(r.TenantID), string(a.ID), settlement); err != nil {
		return provider.ModelTurn{}, "", err
	}
	h.duplicateSettlements.Add(1)
	if err = h.quota.RecordOutcome(ctx, string(r.TenantID), string(a.ID), "success"); err != nil {
		return provider.ModelTurn{}, "", err
	}
	return turn, ref, nil
}

func (h *terminalHarness) assertLedgers(t *testing.T, ctx context.Context, count int) map[string]any {
	t.Helper()
	s := h.store
	var runs, completed, effects, succeeded, attempts, finishedEvents, allocations, released, active, slots, eventRows, snapshotRows, gaps, missingRefs, unfinishedReservations int
	err := s.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM runs),(SELECT count(*) FROM runs WHERE state='completed'),(SELECT count(*) FROM effects),(SELECT count(*) FROM effects WHERE status='succeeded'),(SELECT count(*) FROM model_attempts WHERE status='completed'),(SELECT count(*) FROM run_events WHERE type='run.finished'),(SELECT count(*) FROM runner_allocations),(SELECT count(*) FROM runner_allocations WHERE state='released'),(SELECT sum(active_count) FROM tenant_runtime),(SELECT sum(reserved_slots) FROM runners),(SELECT count(*) FROM run_events),(SELECT count(*) FROM run_snapshots),(SELECT count(*) FROM (SELECT run_id FROM run_events GROUP BY run_id HAVING min(seq)<>1 OR max(seq)<>count(*)) bad),(SELECT count(*) FROM effects e LEFT JOIN artifacts a ON a.tenant_id=e.tenant_id AND a.run_id=e.run_id AND a.id=e.receipt_ref AND a.state='ready' WHERE a.id IS NULL),(SELECT count(*) FROM quota_reservations WHERE status<>'settled' OR NOT request_slot_released)`).Scan(&runs, &completed, &effects, &succeeded, &attempts, &finishedEvents, &allocations, &released, &active, &slots, &eventRows, &snapshotRows, &gaps, &missingRefs, &unfinishedReservations)
	if err != nil {
		t.Fatal(err)
	}
	if runs != count || completed != count || effects != count || succeeded != count || attempts != 2*count || finishedEvents != count || allocations != count || released != count || active != 0 || slots != 0 || gaps != 0 || missingRefs != 0 || unfinishedReservations != 0 {
		t.Fatalf("ledger mismatch runs=%d completed=%d effects=%d/%d attempts=%d finished=%d allocations=%d/%d active=%d slots=%d gaps=%d missing=%d reservations=%d", runs, completed, effects, succeeded, attempts, finishedEvents, allocations, released, active, slots, gaps, missingRefs, unfinishedReservations)
	}
	q, err := h.quota.Snapshot(ctx, "terminal-fake")
	if err != nil {
		t.Fatal(err)
	}
	if q.ActiveRequests != 0 || q.ReservedTokens != 0 || q.ReservedCost != 0 || q.CommittedCost != domain.Money(count)*6 || q.CommittedTokens != int64(count)*96 || h.providerCalls.Load() != int64(count)*2 {
		t.Fatalf("quota/provider mismatch: %+v calls=%d", q, h.providerCalls.Load())
	}
	rows, err := s.Pool.Query(ctx, `SELECT operation_id,args_hash,receipt_ref FROM effects`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	matches := 0
	for rows.Next() {
		var id domain.ID
		var hash, ref string
		if err := rows.Scan(&id, &hash, &ref); err != nil {
			t.Fatal(err)
		}
		actual, ok := h.executor.results[id]
		if !ok || h.executor.starts[id] != 1 || actual.ArgsHash != hash || actual.Ref != ref {
			t.Fatalf("executor/DB mismatch %s", id)
		}
		matches++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if matches != count || len(h.executor.starts) != count || h.executor.requests != 2*count {
		t.Fatal("simulated executor counts mismatch")
	}
	return map[string]any{"passed": true, "runs": runs, "completed_runs": completed, "unique_simulated_effects": effects, "confirmed_effects": succeeded, "simulated_start_requests": h.executor.requests, "simulated_executions": len(h.executor.starts), "duplicate_confirmations_rejected": count, "completed_model_attempts": attempts, "run_finished_events": finishedEvents, "allocations": allocations, "released_allocations": released, "tenant_active_total": active, "runner_reserved_slots": slots, "provider_active_requests": q.ActiveRequests, "provider_reserved_tokens": q.ReservedTokens, "provider_reserved_microusd": q.ReservedCost, "provider_committed_tokens": q.CommittedTokens, "provider_committed_microusd": q.CommittedCost, "event_rows": eventRows, "snapshot_rows": snapshotRows, "event_gaps": gaps, "missing_ready_effect_receipts": missingRefs, "unsettled_reservations": unfinishedReservations}
}
