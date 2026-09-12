//go:build linux

package applicationfaults

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	flow "github.com/JDinSeattle/forge-runtime/internal/runtime"
)

// Explicit synthetic control observations exercise the refusal oracle only.
// They are not PG, Docker, abort, cancellation completion, or recovery evidence.
func lifecycleOfflineObservation(cancelled bool) (flow.State, lifecycleAbortObservation) {
	initial := flow.NewState("tenant-fixture", "run_fixture", flow.Limits{MaxModelRounds: 4, MaxToolCalls: 4, MaxCost: 1000000, Deadline: time.Date(2026, 9, 11, 21, 4, 17, 356217000, time.FixedZone("PDT", -7*3600))})
	o := lifecycleAbortObservation{State: initial, Counts: map[string]int64{"runs": 1, "steps": 0}, Immutable: json.RawMessage(`{"task":"retained","input_snapshot":"retained","config_snapshot":{"max_runtime_seconds":170}}`)}
	for _, k := range append(append([]string{}, lifecycleZeroTables...), "tenant_active", "runner_reserved", "quota_active", "quota_tokens", "quota_cost") {
		o.Counts[k] = 0
	}
	enc := func(v any) json.RawMessage {
		b, e := json.Marshal(v)
		if e != nil {
			panic(e)
		}
		return b
	}
	o.Events = []json.RawMessage{enc(map[string]any{"tenant_id": initial.TenantID, "run_id": initial.RunID, "seq": 1, "type": "run.created", "payload": map[string]any{"state": "queued"}, "created_at": "original"})}
	o.Snapshots = []json.RawMessage{enc(map[string]any{"version": 1, "body": initial, "created_at": "original", "tenant_id": initial.TenantID, "run_id": initial.RunID, "step_seq": 0, "schema_version": initial.SchemaVersion})}
	if cancelled {
		o.State.Status = domain.StatusCancelRequested
		o.State.Version = 2
		o.State.StopTarget = domain.StatusCancelled
		o.State.FailureReason = "cancellation requested"
		o.Commands = []flow.Command{{Kind: flow.CommandStopExecution}}
		o.Counts["steps"] = 1
		o.Steps = []json.RawMessage{enc(map[string]any{"tenant_id": initial.TenantID, "run_id": initial.RunID, "seq": 0, "kind": "initialize", "status": "committed", "input_hash": "", "output_ref": ""})}
		o.Events = append(o.Events, enc(map[string]any{"tenant_id": initial.TenantID, "run_id": initial.RunID, "seq": 2, "type": "run.cancel_requested"}))
		o.Snapshots = append(o.Snapshots, enc(map[string]any{"version": 2, "body": o.State, "tenant_id": initial.TenantID, "run_id": initial.RunID, "step_seq": 0, "schema_version": initial.SchemaVersion}))
	}
	o.StateColumn, o.VersionColumn = o.State.Status, o.State.Version
	return initial, o
}
func TestLifecycleAbortOracleControlOnly(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		initial, o := lifecycleOfflineObservation(cancelled)
		o.State.Limits.Deadline = o.State.Limits.Deadline.UTC()
		if e := lifecycleUnstartedOracle(o, initial, cancelled); e != nil {
			t.Fatal(e)
		}
	}
	_, before := lifecycleOfflineObservation(false)
	_, after := lifecycleOfflineObservation(true)
	if e := lifecyclePreserved(before, after); e != nil {
		t.Fatal(e)
	}
	// A control cancellation step and pending stop intention are mandatory. Neither
	// claims that an execution happened or that the no-active acknowledgment exists.
	if after.State.Status.Terminal() || after.Counts["effects"] != 0 || after.Counts["steps"] != 1 {
		t.Fatal("invalid synthetic control fixture")
	}
}
func TestLifecycleAbortOracleRejectsUnexpectedState(t *testing.T) {
	cases := map[string]func(*lifecycleAbortObservation){
		"effects":            func(o *lifecycleAbortObservation) { o.Counts["effects"] = 1 },
		"model":              func(o *lifecycleAbortObservation) { o.Counts["model_attempts"] = 1 },
		"allocation":         func(o *lifecycleAbortObservation) { o.Counts["runner_allocations"] = 1 },
		"quota":              func(o *lifecycleAbortObservation) { o.Counts["quota_reservations"] = 1 },
		"artifact":           func(o *lifecycleAbortObservation) { o.Counts["artifacts"] = 1 },
		"cleanup":            func(o *lifecycleAbortObservation) { o.Counts["workspace_cleanup"] = 1 },
		"second_run":         func(o *lifecycleAbortObservation) { o.Counts["runs"] = 2 },
		"missing_count":      func(o *lifecycleAbortObservation) { delete(o.Counts, "quota_cost") },
		"capacity":           func(o *lifecycleAbortObservation) { o.Counts["tenant_active"] = 1 },
		"reserved_runner":    func(o *lifecycleAbortObservation) { o.Counts["runner_reserved"] = 1 },
		"deadline_extension": func(o *lifecycleAbortObservation) { o.State.Limits.Deadline = o.State.Limits.Deadline.Add(time.Second) },
		"lease_epoch":        func(o *lifecycleAbortObservation) { o.LeaseEpoch = 1 },
		"lease_owner":        func(o *lifecycleAbortObservation) { o.LeaseOwner = "worker" },
		"lease_until":        func(o *lifecycleAbortObservation) { v := time.Now(); o.LeaseUntil = &v },
		"workspace":          func(o *lifecycleAbortObservation) { o.Workspace = "workspace" },
		"runner":             func(o *lifecycleAbortObservation) { o.RunnerID = "runner" },
		"snapshot_lease":     func(o *lifecycleAbortObservation) { o.State.Lease.Epoch = 1 },
		"snapshot_effect":    func(o *lifecycleAbortObservation) { o.State.PendingEffect = &flow.Effect{ID: "op"} },
		"premature_terminal": func(o *lifecycleAbortObservation) {
			o.State.Status = domain.StatusCancelled
			o.StateColumn = domain.StatusCancelled
		},
		"wrong_stop_target": func(o *lifecycleAbortObservation) { o.State.StopTarget = domain.StatusFailed },
		"dropped_stop":      func(o *lifecycleAbortObservation) { o.Commands = nil },
		"execute_command":   func(o *lifecycleAbortObservation) { o.Commands[0].Kind = flow.CommandExecuteEffect },
		"false_zero_steps":  func(o *lifecycleAbortObservation) { o.Counts["steps"] = 0; o.Steps = nil },
		"execution_step": func(o *lifecycleAbortObservation) {
			o.Steps[0] = json.RawMessage(`{"tenant_id":"tenant-fixture","run_id":"run_fixture","seq":1,"kind":"execute","status":"committed","input_hash":"","output_ref":""}`)
		},
		"event_missing": func(o *lifecycleAbortObservation) { o.Events = o.Events[:1] },
		"event_replaced": func(o *lifecycleAbortObservation) {
			o.Events[1] = json.RawMessage(`{"tenant_id":"tenant-fixture","run_id":"run_other","seq":2,"type":"run.cancel_requested"}`)
		},
		"snapshot_rewritten": func(o *lifecycleAbortObservation) { o.Snapshots[0] = o.Snapshots[1] },
		"state_column":       func(o *lifecycleAbortObservation) { o.StateColumn = domain.StatusQueued },
		"version_column":     func(o *lifecycleAbortObservation) { o.VersionColumn = 3 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			initial, o := lifecycleOfflineObservation(true)
			mutate(&o)
			if lifecycleUnstartedOracle(o, initial, true) == nil {
				t.Fatal("unsafe original accepted")
			}
		})
	}
}
func TestLifecycleAbortHistoryIsAppendOnly(t *testing.T) {
	for _, name := range []string{"immutable", "event", "snapshot"} {
		t.Run(name, func(t *testing.T) {
			_, before := lifecycleOfflineObservation(false)
			_, after := lifecycleOfflineObservation(true)
			switch name {
			case "immutable":
				after.Immutable = json.RawMessage(`{"task":"changed"}`)
			case "event":
				after.Events[0] = json.RawMessage(`{}`)
			case "snapshot":
				after.Snapshots[0] = json.RawMessage(`{}`)
			}
			if lifecyclePreserved(before, after) == nil {
				t.Fatal("rewritten original accepted")
			}
		})
	}
}
func TestLifecycleExplicitAttemptAndBinaryRouting(t *testing.T) {
	scope := "/project/var/lifecycle-rehearsals/lr20260912_a"
	for _, attempt := range []string{"01", "02"} {
		p, e := lifecyclePrivate(scope, filepath.Join(scope, "evidence", "sigterm-"+attempt))
		if e != nil {
			t.Fatal(e)
		}
		want := "sigterm-private"
		if attempt == "02" {
			want += "-02"
		}
		if p != filepath.Join(scope, "runtime", want) {
			t.Fatalf("private routing: %s", p)
		}
	}
	for _, path := range []string{scope + "/evidence/sigterm-03", scope + "/evidence/logs-01", scope + "/evidence/sigterm-02/../sigterm-01", "/other/evidence/sigterm-02"} {
		if _, e := lifecyclePrivate(scope, path); e == nil {
			t.Fatal("implicit/private route accepted", path)
		}
	}
	rev := strings.Repeat("a", 40)
	a := lifecycleAcceptance{ScopeRoot: scope, RunnerBinary: scope + "/bin/" + rev + "/forge-runner", WorkerBinary: scope + "/bin/" + rev + "/forge-worker", TestBinary: scope + "/bin/" + rev + "/application-faults.test"}
	if !lifecycleBinarySet(a) {
		t.Fatal("full frozen revision rejected")
	}
	for _, bad := range []string{scope + "/bin/forge-worker", scope + "/bin/" + strings.Repeat("b", 40) + "/forge-worker", scope + "/bin/abcdef0/forge-worker", scope + "/bin/" + strings.Repeat("A", 40) + "/forge-worker"} {
		b := a
		b.WorkerBinary = bad
		if lifecycleBinarySet(b) {
			t.Fatal("mixed/short/uppercase revision accepted")
		}
	}
}

func TestLifecycleOriginalDatabaseRoute(t *testing.T) {
	good := "postgres://role:private@127.0.0.1:32773/forge?sslmode=disable&search_path=appfault_lifecycle_fixture"
	if e := lifecycleOriginalDSN(good, "role", "appfault_lifecycle_fixture"); e != nil {
		t.Fatal(e)
	}
	for _, raw := range []string{strings.Replace(good, "127.0.0.1", "remote", 1), strings.Replace(good, "32773", "5432", 1), strings.Replace(good, "/forge?", "/other?", 1), strings.Replace(good, "role:", "admin:", 1), good + "&host=remote", good + "&options=-csearch_path%3Dpublic", good + "&search_path=public", good + "&service=other", good + "#override"} {
		if lifecycleOriginalDSN(raw, "role", "appfault_lifecycle_fixture") == nil {
			t.Fatal("database authority override accepted")
		}
	}
}
