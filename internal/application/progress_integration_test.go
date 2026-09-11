package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/provider"
	"github.com/JDinSeattle/forge-runtime/internal/quota"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	flow "github.com/JDinSeattle/forge-runtime/internal/runtime"
)

// Optional evidence export contains only this fixture's private schema and
// authenticated artifacts, never connection strings or process environment.
func progressProof(t *testing.T, d *Driver, original persistence.Run, extra map[string]any) {
	t.Helper()
	dir := os.Getenv("FORGE_PROGRESS_EVIDENCE_DIR")
	if dir == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := d.Store.GetRun(ctx, original.TenantID, original.ID)
	if err != nil {
		t.Errorf("evidence state: %v", err)
		return
	}
	proof := map[string]any{"scope": "real private PostgreSQL + production Driver + SQLite runner with TestBackend; no Docker or paid provider", "run": r, "extra": extra}
	for _, table := range []string{"run_snapshots", "effects", "model_attempts", "quota_reservations", "runner_allocations", "run_messages", "run_events"} {
		// Table names are fixed test constants, never external input.
		var body string
		err = d.Store.Pool.QueryRow(ctx, "SELECT coalesce(jsonb_agg(to_jsonb(t)),'[]'::jsonb)::text FROM "+table+" t WHERE tenant_id=$1 AND run_id=$2", r.TenantID, r.ID).Scan(&body)
		if err != nil {
			t.Errorf("evidence %s: %v", table, err)
			continue
		}
		proof[table] = json.RawMessage(body)
	}
	var refs []string
	rows, err := d.Store.Pool.Query(ctx, `SELECT id FROM artifacts WHERE tenant_id=$1 AND run_id=$2 AND kind IN ('operation_receipt','progress_report','verification_report') ORDER BY id`, r.TenantID, r.ID)
	if err != nil {
		t.Errorf("evidence artifacts: %v", err)
		return
	}
	for rows.Next() {
		var ref string
		if err = rows.Scan(&ref); err != nil {
			t.Error(err)
			break
		}
		refs = append(refs, ref)
	}
	rows.Close()
	artifacts := map[string]string{}
	for _, ref := range refs {
		var raw json.RawMessage
		if err = d.load(ctx, r, ref, &raw); err != nil {
			t.Error(err)
			continue
		}
		artifacts[ref] = string(raw)
	}
	proof["artifact_bytes"] = artifacts
	if exe, err := os.Executable(); err == nil {
		if b, err := os.ReadFile(exe); err == nil {
			h := sha256.Sum256(b)
			proof["test_binary_sha256"] = hex.EncodeToString(h[:])
		}
	}
	proof["passed"] = !t.Failed()
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()) + ".json"
	if err = os.MkdirAll(dir, 0700); err != nil {
		t.Error(err)
		return
	}
	b, err := json.MarshalIndent(proof, "", "  ")
	if err != nil {
		t.Error(err)
		return
	}
	if err = os.WriteFile(filepath.Join(dir, name), append(b, '\n'), 0600); err != nil {
		t.Error(err)
	}
}

func progressScripts(n int, kind, args string) []provider.Script {
	var scripts []provider.Script
	for i := 0; i < n; i++ {
		s := toolScript(fmt.Sprintf("call_%d", i), kind, args)
		s.Usage = provider.Usage{}
		scripts = append(scripts, s)
	}
	return scripts
}

func progressTerminal(t *testing.T, d *Driver, r persistence.Run, p *scriptedRepair, wantCalls int64) persistence.Run {
	t.Helper()
	ctx := context.Background()
	got, err := d.Store.GetRun(ctx, r.TenantID, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State.Status != domain.StatusBudgetExhausted || got.State.FailureReason != "repeated_no_progress" || got.State.Progress == nil || got.State.Progress.RepeatedBatches != 3 || p.calls.Load() != wantCalls {
		t.Fatalf("unexpected progress terminal: %+v calls=%d", got.State, p.calls.Load())
	}
	var unsettled, active, slots int
	if err = d.Store.Pool.QueryRow(ctx, `SELECT count(*) FROM effects WHERE tenant_id=$1 AND run_id=$2 AND status IN ('in_flight','unknown')`, r.TenantID, r.ID).Scan(&unsettled); err != nil {
		t.Fatal(err)
	}
	if err = d.Store.Pool.QueryRow(ctx, `SELECT active_count FROM tenant_runtime WHERE tenant_id=$1`, r.TenantID).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if err = d.Store.Pool.QueryRow(ctx, `SELECT reserved_slots FROM runners WHERE id='runner'`).Scan(&slots); err != nil {
		t.Fatal(err)
	}
	if unsettled != 0 || active != 0 || slots != 0 {
		t.Fatalf("unsettled/active/slots %d/%d/%d", unsettled, active, slots)
	}
	events, err := d.Store.Events(ctx, r.TenantID, r.ID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range events {
		if e.Seq != uint64(i+1) {
			t.Fatal("event sequence gap")
		}
	}
	return got
}

func TestProgressDurableClosedBatches(t *testing.T) {
	for _, kind := range []string{"read", "finish", "failed_batch"} {
		t.Run(kind, func(t *testing.T) {
			d, r, p, _ := repairSetup(t)
			extra := map[string]any{}
			defer progressProof(t, d, r, extra)
			spec := d.Models["fake/fake"]
			spec.InputPrice, spec.OutputPrice, spec.PriceVersion = 1_000_000, 1_000_000, "e42-synthetic-positive"
			d.Models["fake/fake"] = spec
			p.scripts = progressScripts(8, "read_file", `{"path":"clamp.py"}`)
			if kind == "finish" {
				p.scripts = progressScripts(8, "finish", `{}`)
			}
			if kind == "failed_batch" {
				p.scripts = progressScripts(8, "read_file", `{"path":"absent.py"}`)
				for i := range p.scripts {
					tail := toolScript("never_started", "read_file", `{"path":"clamp.py"}`)
					p.scripts[i].Chunks = append(p.scripts[i].Chunks, tail.Chunks...)
				}
			}
			ctx := context.Background()
			claimed, err := d.Store.Claim(ctx, "progress-worker", d.leaseDuration())
			if err != nil {
				t.Fatal(err)
			}
			if err = d.Drive(ctx, claimed); err != nil {
				t.Fatal(err)
			}
			got := progressTerminal(t, d, r, p, 4)
			if kind == "failed_batch" {
				var unstarted int
				if err = d.Store.Pool.QueryRow(ctx, `SELECT count(*) FROM effects WHERE run_id=$1 AND ordinal=1 AND status='planned' AND receipt_ref IS NULL`, r.ID).Scan(&unstarted); err != nil || unstarted != 4 {
					t.Fatalf("discarded intentions %d: %v", unstarted, err)
				}
			}
			// Unknown final usage remains in the independent reservation ledger;
			// terminal stop may not refund money merely because its lease expires.
			var unknown int
			if err = d.Store.Pool.QueryRow(ctx, `SELECT count(*) FROM quota_reservations WHERE tenant_id=$1 AND run_id=$2 AND status='unknown'`, r.TenantID, r.ID).Scan(&unknown); err != nil {
				t.Fatal(err)
			}
			if unknown != 4 {
				t.Fatalf("unknown billing was released: %d", unknown)
			}
			var held int64
			if err = d.Store.Pool.QueryRow(ctx, `SELECT coalesce(sum(microusd),0) FROM quota_reservations WHERE tenant_id=$1 AND run_id=$2 AND status='unknown' AND actual_microusd IS NULL`, r.TenantID, r.ID).Scan(&held); err != nil {
				t.Fatal(err)
			}
			if held != 4*(8192+1024) || int64(got.State.Cost) != held {
				t.Fatalf("unknown reservation money lost: held=%d state=%d", held, got.State.Cost)
			}
			extra["synthetic_unknown_microusd_after_stop"] = held
			if kind == "read" {
				var deadline time.Time
				if err = d.Store.Pool.QueryRow(ctx, `SELECT max(request_deadline) FROM quota_reservations WHERE tenant_id=$1 AND run_id=$2`, r.TenantID, r.ID).Scan(&deadline); err != nil {
					t.Fatal(err)
				}
				if wait := time.Until(deadline) + 30*time.Millisecond; wait > 0 {
					time.Sleep(wait)
				}
				if _, err = d.Quota.ExpireRequestSlots(ctx, "fake"); err != nil {
					t.Fatal(err)
				}
				var after int64
				var unreleased int
				if err = d.Store.Pool.QueryRow(ctx, `SELECT coalesce(sum(microusd),0),count(*) FILTER(WHERE NOT request_slot_released) FROM quota_reservations WHERE tenant_id=$1 AND run_id=$2 AND status='unknown' AND actual_microusd IS NULL`, r.TenantID, r.ID).Scan(&after, &unreleased); err != nil {
					t.Fatal(err)
				}
				if after != held || unreleased != 0 {
					t.Fatal("deadline mixed request slots and money", after, unreleased)
				}
				extra["synthetic_unknown_microusd_after_request_deadline"] = after
			}
			extra["model_calls"] = p.calls.Load()
			extra["unknown_reservations_after_stop"] = unknown
			extra["terminal_version"] = got.State.Version
		})
	}
}

func TestProgressMessageRaceRebuildsBeforeStopping(t *testing.T) {
	d, r, p, _ := repairSetup(t)
	extra := map[string]any{}
	defer progressProof(t, d, r, extra)
	p.scripts = progressScripts(8, "read_file", `{"path":"clamp.py"}`)
	ctx := context.Background()
	// Seven deliberately unknown-usage turns require distinct request slots.
	if err := d.Quota.Configure(ctx, quota.Config{CredentialGroup: "fake", MaxConcurrent: 16, MaxTokens: 1_000_000, MaxCost: 100_000_000, WindowDuration: time.Hour, FailureThreshold: 3, BreakerCooldown: time.Second}); err != nil {
		t.Fatal(err)
	}
	identity := persistence.Identity{TenantID: r.TenantID, PrincipalID: r.PrincipalID, Role: "developer"}
	var inserted atomic.Bool
	var rebuilt atomic.Int32
	d.Fault = func(point string) error {
		if point != "after_progress_report_before_transition" {
			return nil
		}
		current, err := d.Store.GetRun(ctx, r.TenantID, r.ID)
		if err != nil {
			return err
		}
		if current.State.StepSeq != 4 {
			return nil
		}
		rebuilt.Add(1)
		m, replay, err := d.Store.AddMessage(ctx, identity, r.ID, "Recheck the same input after this explicit instruction.", "same-message")
		if err != nil {
			return err
		}
		if inserted.CompareAndSwap(false, true) {
			if replay || m.Seq != 1 {
				return domain.ErrInvalid
			}
		} else if !replay || m.Seq != 1 {
			return domain.ErrInvalid
		}
		return nil
	}
	claimed, err := d.Store.Claim(ctx, "message-worker", d.leaseDuration())
	if err != nil {
		t.Fatal(err)
	}
	if err = d.Drive(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	got := progressTerminal(t, d, r, p, 7)
	if rebuilt.Load() != 2 || got.State.Progress.MessageSeq != 1 {
		t.Fatal("message race did not rebuild exactly once", rebuilt.Load(), got.State.Progress)
	}
	extra["step4_context_builds"] = rebuilt.Load()
	extra["model_calls"] = p.calls.Load()
}

func TestProgressCrashBeforeAndAfterCommit(t *testing.T) {
	for _, point := range []string{"after_progress_report_before_transition", "after_progress_transition"} {
		t.Run(point, func(t *testing.T) {
			d, r, p, _ := repairSetup(t)
			extra := map[string]any{}
			defer progressProof(t, d, r, extra)
			p.scripts = progressScripts(8, "read_file", `{"path":"clamp.py"}`)
			ctx := context.Background()
			var injected atomic.Bool
			d.Fault = func(got string) error {
				if got != point {
					return nil
				}
				current, err := d.Store.GetRun(ctx, r.TenantID, r.ID)
				if err != nil {
					return err
				}
				if current.State.StepSeq == 4 && (point != "after_progress_transition" || current.State.Status == domain.StatusCancelRequested) && injected.CompareAndSwap(false, true) {
					return context.Canceled
				}
				return nil
			}
			claimed, err := d.Store.Claim(ctx, "crash-worker", d.leaseDuration())
			if err != nil {
				t.Fatal(err)
			}
			if err = d.Drive(ctx, claimed); !errors.Is(err, context.Canceled) {
				t.Fatalf("fault not reached: %v", err)
			}
			before, err := d.Store.GetRun(ctx, r.TenantID, r.ID)
			if err != nil {
				t.Fatal(err)
			}
			extra["interrupted_state"] = before.State
			want := uint64(2)
			if point == "after_progress_transition" {
				want = 3
			}
			if before.State.Progress.RepeatedBatches != want {
				t.Fatal(before.State.Progress)
			}
			// Natural database lease expiry; no destructive fixture lease override.
			if wait := time.Until(before.State.Lease.Until) + 30*time.Millisecond; wait > 0 {
				time.Sleep(wait)
			}
			next, err := d.Store.Claim(ctx, "recovery-worker", d.leaseDuration())
			if err != nil {
				t.Fatal(err)
			}
			if next.State.Lease.Epoch != 2 {
				t.Fatal(next.State.Lease)
			}
			if err = d.Drive(ctx, next); err != nil {
				t.Fatal(err)
			}
			progressTerminal(t, d, r, p, 4)
			extra["model_calls"] = p.calls.Load()
		})
	}
}

type progressRefuseStop struct {
	runner.Service
	refused atomic.Bool
}

func (r *progressRefuseStop) StopWorkspace(ctx context.Context, request runner.WorkspaceRequest) (runner.StopReceipt, error) {
	if r.refused.CompareAndSwap(false, true) {
		return runner.StopReceipt{}, domain.ErrReconciliation
	}
	return r.Service.StopWorkspace(ctx, request)
}

func progressCapacityRecord(t *testing.T, d *Driver, tenant domain.ID) json.RawMessage {
	t.Helper()
	var raw string
	if err := d.Store.Pool.QueryRow(context.Background(), `SELECT jsonb_build_object('tenant_active',(SELECT active_count FROM tenant_runtime WHERE tenant_id=$1),'runner_slots',(SELECT reserved_slots FROM runners WHERE id='runner'),'allocations',(SELECT jsonb_agg(to_jsonb(a)) FROM runner_allocations a WHERE tenant_id=$1))::text`, tenant).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	return json.RawMessage(raw)
}

func TestProgressStopUnknownKeepsSentinelCapacity(t *testing.T) {
	d, r, p, _ := repairSetup(t)
	extra := map[string]any{}
	defer progressProof(t, d, r, extra)
	d.LeaseDuration = 30 * time.Second
	p.scripts = progressScripts(8, "read_file", `{"path":"clamp.py"}`)
	ctx := context.Background()
	claimed, err := d.Store.Claim(ctx, "stopping-worker", d.leaseDuration())
	if err != nil {
		t.Fatal(err)
	}
	sentinel, _, err := d.Store.Submit(ctx, persistence.SubmitRequest{TenantID: r.TenantID, PrincipalID: r.PrincipalID, ProjectID: r.ProjectID, Task: "sentinel capacity only", BaseCommit: r.BaseCommit, Config: r.Config}, "sentinel")
	if err != nil {
		t.Fatal(err)
	}
	sentinel, err = d.Store.Claim(ctx, "sentinel-worker", d.leaseDuration())
	if err != nil {
		t.Fatal(err)
	}
	capacityCounts(t, ctx, d, 2)
	d.Runner = &progressRefuseStop{Service: d.Runner}
	if err = d.Drive(ctx, claimed); !errors.Is(err, errDeferred) {
		t.Fatalf("unknown stop was not deferred: %v", err)
	}
	current, err := d.Store.GetRun(ctx, r.TenantID, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.State.Status != domain.StatusCancelRequested || current.State.StopTarget != domain.StatusBudgetExhausted || current.State.Progress.RepeatedBatches != 3 {
		t.Fatal(current.State)
	}
	capacityCounts(t, ctx, d, 2)
	extra["unconfirmed_stop_state"] = current.State
	extra["active_while_stop_unknown"] = 2
	extra["capacity_while_stop_unknown"] = progressCapacityRecord(t, d, r.TenantID)
	identity := persistence.Identity{TenantID: r.TenantID, PrincipalID: r.PrincipalID, Role: "developer"}
	if _, _, err = d.Store.AddMessage(ctx, identity, r.ID, "Too late to reopen stopping work.", "late-message"); !errors.Is(err, domain.ErrTransition) {
		t.Fatalf("stopping run accepted reset: %v", err)
	}
	var retry time.Time
	if err = d.Store.Pool.QueryRow(ctx, `SELECT not_before FROM runs WHERE tenant_id=$1 AND id=$2`, r.TenantID, r.ID).Scan(&retry); err != nil {
		t.Fatal(err)
	}
	if wait := time.Until(retry) + 30*time.Millisecond; wait > 0 {
		time.Sleep(wait)
	}
	reclaimed, err := d.Store.Claim(ctx, "stop-recovery", d.leaseDuration())
	if err != nil || reclaimed.ID != r.ID {
		t.Fatalf("reclaim %s: %v", reclaimed.ID, err)
	}
	if err = d.Drive(ctx, reclaimed); err != nil {
		t.Fatal(err)
	}
	final, err := d.Store.GetRun(ctx, r.TenantID, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.State.Status != domain.StatusBudgetExhausted || p.calls.Load() != 4 {
		t.Fatal(final.State, p.calls.Load())
	}
	capacityCounts(t, ctx, d, 1)
	extra["capacity_after_main_stop"] = progressCapacityRecord(t, d, r.TenantID)
	_, err = d.Store.Advance(ctx, r.TenantID, r.ID, flow.Event{Kind: flow.EventCancellationConfirmed, ExpectedVersion: final.State.Version, Owner: final.State.Lease.Owner, Epoch: final.State.Lease.Epoch, Stop: &flow.StopReceipt{Ref: final.State.OutputRef, NoActiveOperations: true, WorkspaceRevision: final.State.WorkspaceRevision}})
	if !errors.Is(err, domain.ErrTerminal) {
		t.Fatalf("duplicate terminal settlement: %v", err)
	}
	capacityCounts(t, ctx, d, 1)
	extra["capacity_after_duplicate_stop"] = progressCapacityRecord(t, d, r.TenantID)
	if _, err = d.Store.Cancel(ctx, identity, sentinel.ID); err != nil {
		t.Fatal(err)
	}
	if err = d.Drive(ctx, sentinel); err != nil {
		t.Fatal(err)
	}
	capacityCounts(t, ctx, d, 0)
	extra["active_after_main_stop"] = 1
	extra["active_after_duplicate_stop"] = 1
	extra["active_after_sentinel_stop"] = 0
	extra["capacity_after_sentinel_stop"] = progressCapacityRecord(t, d, r.TenantID)
	extra["model_calls"] = p.calls.Load()
}

func TestProgressLegacySnapshotsAndDefaultIdempotency(t *testing.T) {
	d, r, p, _ := repairSetup(t)
	extra := map[string]any{}
	defer progressProof(t, d, r, extra)
	ctx := context.Background()
	if r.State.SchemaVersion != 2 || r.Config.MaxNoProgressBatches != 3 || r.State.Limits.MaxNoProgressBatches != 3 {
		t.Fatal("new run default not frozen", r)
	}
	requestConfig := r.Config
	requestConfig.MaxNoProgressBatches = 0
	request := persistence.SubmitRequest{TenantID: r.TenantID, PrincipalID: r.PrincipalID, ProjectID: r.ProjectID, Task: r.Task, BaseCommit: r.BaseCommit, Config: requestConfig}
	replayed, reused, err := d.Store.Submit(ctx, request, "repair")
	if err != nil || !reused || replayed.ID != r.ID {
		t.Fatalf("default changed original idempotency hash: %v", err)
	}
	// Private fixture recreates a pre-feature v1 snapshot/config. No production
	// auto-migration, default injection on reads, or old body conversion occurs.
	legacy := r.State
	legacy.SchemaVersion = 1
	legacy.Limits.MaxNoProgressBatches = 0
	legacy.Progress = nil
	b, _ := json.Marshal(legacy)
	c, _ := json.Marshal(requestConfig)
	if _, err = d.Store.Pool.Exec(ctx, `UPDATE runs SET snapshot=$3::json,config_snapshot=$4::json WHERE tenant_id=$1 AND id=$2`, r.TenantID, r.ID, b, c); err != nil {
		t.Fatal(err)
	}
	if _, err = d.Store.Pool.Exec(ctx, `UPDATE run_snapshots SET schema_version=1,body=$3::json WHERE tenant_id=$1 AND run_id=$2 AND version=1`, r.TenantID, r.ID, b); err != nil {
		t.Fatal(err)
	}
	claimed, err := d.Store.Claim(ctx, "legacy-worker", d.leaseDuration())
	if err != nil {
		t.Fatal(err)
	}
	if err = d.Drive(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	final, err := d.Store.GetRun(ctx, r.TenantID, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.State.Status != domain.StatusCompleted || final.State.SchemaVersion != 1 || final.State.Progress != nil || final.State.Limits.MaxNoProgressBatches != 0 || p.calls.Load() != 3 {
		t.Fatal("legacy behavior changed", final.State)
	}
	extra["legacy_initial_snapshot"] = json.RawMessage(b)
	extra["original_key_reused"] = true
	extra["model_calls"] = p.calls.Load()
}

func TestProgressRejectsUnboundEvidence(t *testing.T) {
	d, r, p, _ := repairSetup(t)
	extra := map[string]any{}
	defer progressProof(t, d, r, extra)
	ctx := context.Background()
	var tested atomic.Bool
	d.Fault = func(point string) error {
		if point != "after_progress_report_before_transition" {
			return nil
		}
		current, err := d.Store.GetRun(ctx, r.TenantID, r.ID)
		if err != nil {
			return err
		}
		if current.State.StepSeq != 1 || !tested.CompareAndSwap(false, true) {
			return nil
		}
		var ref string
		if err = d.Store.Pool.QueryRow(ctx, `SELECT id FROM artifacts WHERE tenant_id=$1 AND run_id=$2 AND kind='progress_report' ORDER BY created_at DESC,id DESC LIMIT 1`, r.TenantID, r.ID).Scan(&ref); err != nil {
			return err
		}
		var original flow.ProgressFrame
		if err = d.load(ctx, r, ref, &original); err != nil {
			return err
		}
		var rejected []string
		for _, mode := range []string{"report_hash", "future_message", "wrong_step", "wrong_context", "foreign_effect", "old_version", "unresolved_ledger"} {
			body, _ := json.Marshal(original)
			var frame flow.ProgressFrame
			_ = json.Unmarshal(body, &frame)
			e := flow.Event{Kind: flow.EventContextBuilt, OutputRef: original.ContextRef, ProgressRef: ref, Progress: &frame, ExpectedVersion: current.State.Version, Owner: current.State.Lease.Owner, Epoch: current.State.Lease.Epoch}
			want := domain.ErrUntrusted
			switch mode {
			case "report_hash":
				frame.Observations[0].Fingerprint = strings.Repeat("f", 64)
			case "future_message":
				frame.MessageSeq = 1
			case "wrong_step":
				frame.StepSeq++
			case "wrong_context":
				frame.ContextRef = "other_context"
			case "foreign_effect":
				frame.Observations[0].EffectID = "foreign_effect"
				e.ProgressRef, err = d.put(ctx, r, "progress_report", &frame)
				if err != nil {
					return err
				}
			case "old_version":
				e.ExpectedVersion--
				want = domain.ErrConflict
			case "unresolved_ledger":
				want = domain.ErrReconciliation
				_, err = d.Store.Pool.Exec(ctx, `INSERT INTO effects(tenant_id,run_id,step_seq,ordinal,operation_id,kind,args,canonical_args,args_hash,expected_revision,epoch,policy_version,status) SELECT tenant_id,run_id,step_seq,100,operation_id||'_unresolved',kind,args,canonical_args,args_hash,expected_revision,epoch,policy_version,'unknown' FROM effects WHERE tenant_id=$1 AND run_id=$2 AND operation_id=$3`, r.TenantID, r.ID, original.Observations[0].EffectID)
				if err != nil {
					return err
				}
			}
			_, err = d.Store.Advance(ctx, r.TenantID, r.ID, e)
			if !errors.Is(err, want) {
				return fmt.Errorf("%s: got %v want %v", mode, err, want)
			}
			rejected = append(rejected, mode+":"+err.Error())
			if mode == "unresolved_ledger" {
				if _, err = d.Store.Pool.Exec(ctx, `DELETE FROM effects WHERE tenant_id=$1 AND run_id=$2 AND operation_id=$3`, r.TenantID, r.ID, string(original.Observations[0].EffectID)+"_unresolved"); err != nil {
					return err
				}
			}
			after, err := d.Store.GetRun(ctx, r.TenantID, r.ID)
			if err != nil {
				return err
			}
			if after.State.Version != current.State.Version || after.State.Stage != current.State.Stage || after.State.Progress.LastCheckedStep != 0 {
				return domain.ErrConflict
			}
		}
		extra["rejections"] = rejected
		return nil
	}
	claimed, err := d.Store.Claim(ctx, "evidence-worker", d.leaseDuration())
	if err != nil {
		t.Fatal(err)
	}
	if err = d.Drive(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	final, err := d.Store.GetRun(ctx, r.TenantID, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !tested.Load() || final.State.Status != domain.StatusCompleted || p.calls.Load() != 3 {
		t.Fatal("binding regression did not finish", final.State)
	}
}
