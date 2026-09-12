//go:build linux

package applicationfaults

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/configuration"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	flow "github.com/JDinSeattle/forge-runtime/internal/runtime"
	"github.com/jackc/pgx/v5"
)

// This is NOT business cancellation completion. It persists cancellation intent
// for a proven never-started fixture. No worker, runner, synthetic stop receipt,
// lease extension, migration, SQL update, or cleanup is permitted here.
func TestAbortUnstartedLifecycle(t *testing.T) {
	if os.Getenv("FORGE_ABORT_UNSTARTED_LIFECYCLE") != "1" {
		t.Skip("explicit operator opt-in for the retained before_runner fixture only")
	}
	path := os.Getenv("FORGE_WORKER_RUNNER_SIGTERM_ACCEPTANCE")
	var a lifecycleAcceptance
	var c lifecycleRunnerConfig
	if err := slReadJSON(path, &a); err != nil {
		t.Fatal(err)
	}
	if path != filepath.Join(a.ScopeRoot, "acceptance-abort-01.json") || a.EvidenceDir != filepath.Join(a.ScopeRoot, "evidence", "sigterm-01") {
		t.Fatal("explicit abort-01 manifest required")
	}
	if err := slReadJSON(a.RunnerConfig, &c); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := lifecycleShape(a, c); err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(a.TestBinary) == filepath.Join(a.ScopeRoot, "bin") {
		t.Fatal("abort requires preserved binaries in a new full revision directory")
	}
	unlock, err := lifecycleControlLock(a.ScopeRoot)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	if _, err = lifecyclePreflight(ctx, a, c); err != nil {
		t.Fatal("no action; fresh original pool/journal required:", err)
	}
	in, err := lifecycleLoadFirst(ctx, a, c)
	if err != nil {
		t.Fatal("no action; original authority:", err)
	}
	store, err := lifecycleOriginalStore(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	dir := filepath.Join(a.ScopeRoot, "evidence", "sigterm-01", "abort-before-runner")
	// A durable intent makes the sole transaction safe to reobserve after a crash.
	// Never overwrite an existing report, intent, or original failure evidence.
	intentPath, reportPath := filepath.Join(dir, "intent.json"), filepath.Join(dir, "report.json")
	before, err := lifecycleCaptureUnstarted(ctx, store, in)
	if err != nil {
		t.Fatal(err)
	}
	var intent lifecycleAbortIntent
	if err = os.Mkdir(dir, 0700); err == nil {
		parent, e := os.Open(filepath.Dir(dir))
		if e != nil {
			t.Fatal(e)
		}
		e = parent.Sync()
		_ = parent.Close()
		if e != nil {
			t.Fatal(e)
		}
		if err = lifecycleUnstartedOracle(before, in.Initial, false); err != nil {
			t.Fatal("no cancellation:", err)
		}
		intent = lifecycleAbortIntent{Purpose: lifecyclePurpose, Inputs: in.Hashes, Before: before}
		if err = slSave(intentPath, intent); err != nil {
			t.Fatal(err)
		}
	} else if os.IsExist(err) {
		if err = slReadJSON(intentPath, &intent); err != nil {
			t.Fatal("existing abort directory requires durable valid intent:", err)
		}
		if intent.Purpose != lifecyclePurpose || !reflect.DeepEqual(intent.Inputs, in.Hashes) {
			t.Fatal("abort intent authority changed")
		}
		if err = lifecycleUnstartedOracle(intent.Before, in.Initial, false); err != nil {
			t.Fatal(err)
		}
		if err = lifecycleUnstartedOracle(before, in.Initial, before.State.Status == domain.StatusCancelRequested); err != nil {
			t.Fatal("retained intent no longer applies:", err)
		}
		if err = lifecyclePreserved(intent.Before, before); err != nil {
			t.Fatal(err)
		}
	} else {
		t.Fatal(err)
	}
	if _, e := os.Lstat(reportPath); e == nil {
		if _, e = lifecycleVerifyAbortedWithStore(ctx, in, store, reportPath); e != nil {
			t.Fatal(e)
		}
		t.Log("original cancellation intent and report revalidated; no transaction repeated, terminal=false")
		return
	} else if !os.IsNotExist(e) {
		t.Fatal(e)
	}
	// Recheck readonly live process/container gates immediately before Cancel.
	if _, err = lifecycleNoOriginalProcesses(ctx, a, c, in); err != nil {
		t.Fatal(err)
	}
	run, err := store.Cancel(ctx, persistence.Identity{TenantID: in.Tenant, PrincipalID: "fixture-operator", Role: "admin"}, in.Run)
	if err != nil {
		t.Fatal("Store.Cancel outcome may need same-intent inspection; retained:", err)
	}
	if run.State.Status != domain.StatusCancelRequested || run.State.Status.Terminal() {
		t.Fatal("Cancel must remain a nonterminal request")
	}
	after, err := lifecycleCaptureUnstarted(ctx, store, in)
	if err != nil {
		t.Fatal(err)
	}
	if err = lifecycleUnstartedOracle(after, in.Initial, true); err != nil {
		t.Fatal(err)
	}
	if err = lifecyclePreserved(intent.Before, after); err != nil {
		t.Fatal(err)
	}
	if _, err = lifecyclePreflight(ctx, a, c); err != nil {
		t.Fatal("unexpected physical state after cancellation intent:", err)
	}
	physical, err := lifecycleNoOriginalProcesses(ctx, a, c, in)
	if err != nil {
		t.Fatal(err)
	}
	report := lifecycleAbortReport{Execution: a, TestPID: os.Getpid(), Passed: true, Status: domain.StatusCancelRequested, Terminal: false, StopTarget: domain.StatusCancelled, RunID: in.Run, Schema: in.Schema, Version: after.State.Version, LeaseEpoch: 0, OriginalDeadline: in.Initial.Limits.Deadline, Inputs: in.Hashes, Before: intent.Before, After: after, ObservedAt: time.Now().UTC(), Physical: physical, Scope: "cancellation intent only; no stop acknowledgment or lifecycle acceptance; original schema must never be routed to a worker"}
	if err = slSave(reportPath, report); err != nil {
		t.Fatal(err)
	}
	t.Log("abort-before-runner passed: cancel_requested, terminal=false; original deadline and history retained")
}

type lifecycleFirstInput struct {
	Schema                  string
	Tenant, Run             domain.ID
	Role, DSN, WorkerConfig string
	Original                lifecycleAcceptance
	Initial                 flow.State
	Hashes                  map[string]string
	RunnerPID               int
	RunnerTicks             string
}
type lifecycleAbortObservation struct {
	State         flow.State        `json:"state"`
	Commands      []flow.Command    `json:"commands"`
	Workspace     string            `json:"workspace_id"`
	LeaseOwner    string            `json:"lease_owner"`
	LeaseEpoch    uint64            `json:"lease_epoch"`
	LeaseUntil    *time.Time        `json:"lease_until"`
	RunnerID      string            `json:"runner_id"`
	StateColumn   domain.RunStatus  `json:"state_column"`
	VersionColumn uint64            `json:"version_column"`
	Counts        map[string]int64  `json:"counts"`
	Immutable     json.RawMessage   `json:"immutable_run_columns"`
	Snapshots     []json.RawMessage `json:"snapshots"`
	Steps         []json.RawMessage `json:"steps"`
	Events        []json.RawMessage `json:"events"`
	ObservedAt    time.Time         `json:"observed_at"`
}
type lifecycleAbortIntent struct {
	Purpose string                    `json:"purpose"`
	Inputs  map[string]string         `json:"inputs_sha256"`
	Before  lifecycleAbortObservation `json:"before"`
}
type lifecycleAbortReport struct {
	Execution        lifecycleAcceptance       `json:"execution"`
	TestPID          int                       `json:"test_pid"`
	Passed           bool                      `json:"passed"`
	Status           domain.RunStatus          `json:"status"`
	Terminal         bool                      `json:"terminal"`
	StopTarget       domain.RunStatus          `json:"stop_target"`
	RunID            domain.ID                 `json:"run_id"`
	Schema           string                    `json:"schema"`
	Version          uint64                    `json:"version"`
	LeaseEpoch       uint64                    `json:"lease_epoch"`
	OriginalDeadline time.Time                 `json:"original_deadline"`
	Inputs           map[string]string         `json:"inputs_sha256"`
	Before           lifecycleAbortObservation `json:"before"`
	After            lifecycleAbortObservation `json:"after"`
	ObservedAt       time.Time                 `json:"observed_at"`
	Physical         map[string]any            `json:"physical"`
	Scope            string                    `json:"scope"`
}

func lifecycleControlLock(scope string) (func(), error) {
	p := filepath.Join(scope, "runtime", ".sigterm-control.lock")
	if err := lifecyclePath(p); err != nil {
		return nil, err
	}
	fd, err := syscall.Open(p, syscall.O_CREAT|syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), p)
	info, e := f.Stat()
	if e != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Sys().(*syscall.Stat_t).Nlink != 1 {
		f.Close()
		return nil, fmt.Errorf("private regular fixture control lock required")
	}
	if err = syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("another lifecycle control action holds fixture lock")
	}
	return func() { _ = syscall.Flock(fd, syscall.LOCK_UN); _ = f.Close() }, nil
}

func lifecycleLoadFirst(ctx context.Context, a lifecycleAcceptance, c lifecycleRunnerConfig) (*lifecycleFirstInput, error) {
	in := &lifecycleFirstInput{Hashes: map[string]string{}}
	scope := a.ScopeRoot
	first := filepath.Join(scope, "evidence", "sigterm-01", "worker-runner-sigterm")
	private := filepath.Join(scope, "runtime", "sigterm-private")
	read := func(p string, v any) error {
		b, e := slRead(p, 4<<20)
		if e != nil {
			return e
		}
		if e = json.Unmarshal(b, v); e != nil {
			return e
		}
		h, e := sigtermDigest(p)
		if e == nil {
			in.Hashes[p] = h
		}
		return e
	}
	if err := read(filepath.Join(scope, "acceptance.json"), &in.Original); err != nil {
		return nil, err
	}
	var archived lifecycleAcceptance
	if err := read(filepath.Join(first, "acceptance-input.json"), &archived); err != nil {
		return nil, err
	}
	compare := a
	compare.RunnerBinary, compare.RunnerSHA256 = in.Original.RunnerBinary, in.Original.RunnerSHA256
	compare.WorkerBinary, compare.WorkerSHA256 = in.Original.WorkerBinary, in.Original.WorkerSHA256
	compare.TestBinary, compare.TestSHA256 = in.Original.TestBinary, in.Original.TestSHA256
	compare.EvidenceDir = in.Original.EvidenceDir
	if archived != in.Original || compare != in.Original || in.Original.EvidenceDir != filepath.Join(scope, "evidence", "sigterm-01") {
		return nil, fmt.Errorf("original acceptance authority mismatch")
	}
	for _, b := range []struct{ p, h string }{{in.Original.RunnerBinary, in.Original.RunnerSHA256}, {in.Original.WorkerBinary, in.Original.WorkerSHA256}, {in.Original.TestBinary, in.Original.TestSHA256}} {
		if e := lifecyclePath(b.p); e != nil {
			return nil, e
		}
		h, e := sigtermDigest(b.p)
		if e != nil || h != b.h {
			return nil, fmt.Errorf("original binary changed")
		}
		in.Hashes[b.p] = h
	}
	var rec map[string]json.RawMessage
	if err := read(filepath.Join(first, "recovery.json"), &rec); err != nil {
		return nil, err
	}
	str := func(k string) string { var s string; _ = json.Unmarshal(rec[k], &s); return s }
	var failed bool
	_ = json.Unmarshal(rec["test_failed"], &failed)
	if str("phase") != "before_runner" || !failed || str("purpose") != lifecyclePurpose || str("journal_uuid") != "" || str("container_id") != "" {
		return nil, fmt.Errorf("only the retained before_runner failure is eligible")
	}
	in.Schema, in.Role, in.Run, in.Tenant = str("schema"), str("worker_role"), domain.ID(str("run_id")), domain.ID(str("tenant_id"))
	if !regexp.MustCompile(`^appfault_lifecycle_[a-z0-9]{26}$`).MatchString(in.Schema) || !regexp.MustCompile(`^appfault_worker_[a-z0-9]{26}$`).MatchString(in.Role) || in.Run.Validate() != nil || in.Tenant.Validate() != nil || str("operation_id") != string(in.Run)+"_step_1_op_0" {
		return nil, fmt.Errorf("original schema/run/role binding invalid")
	}
	in.WorkerConfig = filepath.Join(private, "worker.json")
	for k, want := range map[string]string{"runner_config": a.RunnerConfig, "journal": c.JournalPath, "artifact_root": c.ArtifactRoot, "private_settings": private, "worker_config": in.WorkerConfig} {
		if str(k) != want {
			return nil, fmt.Errorf("recovery %s changed", k)
		}
	}
	var worker configuration.Config
	if err := read(in.WorkerConfig, &worker); err != nil {
		return nil, err
	}
	var current lifecycleRunnerConfig
	if err := read(a.RunnerConfig, &current); err != nil {
		return nil, err
	}
	if in.Hashes[in.WorkerConfig] != str("worker_config_sha256") || in.Hashes[a.RunnerConfig] != str("runner_config_sha256") {
		return nil, fmt.Errorf("retained config hash mismatch")
	}
	if worker.WorkerID != "lifecycle-worker" || worker.WorkerSlots != 1 || worker.RunnerID != "application-fault-runner" || worker.Runner.UnixSocket != c.Server.UnixSocket || worker.ArtifactRoot != c.ArtifactRoot || worker.SigningKeyFile != c.SigningKeyFile || len(worker.Sources) != 1 || len(worker.Models) != 1 || worker.Models["fake/fake"].PriceVersion != modelSpec().PriceVersion {
		return nil, fmt.Errorf("retained private worker authority mismatch")
	}
	var database struct {
		DSN    string `json:"worker_dsn"`
		Schema string `json:"schema"`
	}
	stat, e := os.Stat(filepath.Join(private, "database.json"))
	if e != nil || stat.Mode().Perm() != 0600 {
		return nil, fmt.Errorf("private original database binding required")
	}
	if err := read(filepath.Join(private, "database.json"), &database); err != nil {
		return nil, err
	}
	if lifecycleOriginalDSN(database.DSN, in.Role, in.Schema) != nil || database.Schema != in.Schema {
		return nil, fmt.Errorf("original private database route invalid")
	}
	in.DSN = database.DSN
	var fixture struct {
		Schema     string    `json:"schema"`
		Tenant     domain.ID `json:"tenant"`
		Run        domain.ID `json:"run_id"`
		SourceHash string    `json:"source_hash"`
		SourceID   string    `json:"source_id"`
		ProfileID  string    `json:"profile_id"`
	}
	if e = read(filepath.Join(first, "fixture.json"), &fixture); e != nil {
		return nil, e
	}
	s, ok := worker.Sources[fixture.SourceID]
	if fixture.Schema != in.Schema || fixture.Tenant != in.Tenant || fixture.Run != in.Run || !ok || s.Hash != fixture.SourceHash || s.Path != c.Sources[fixture.SourceID] || s.ProfileID != fixture.ProfileID {
		return nil, fmt.Errorf("fixture source/run binding mismatch")
	}
	var audit struct {
		Schema      string `json:"schema"`
		Observation struct {
			Run struct {
				ID        domain.ID  `json:"id"`
				State     string     `json:"state"`
				Version   uint64     `json:"version"`
				Epoch     uint64     `json:"lease_epoch"`
				Owner     string     `json:"lease_owner"`
				Workspace string     `json:"workspace_id"`
				Snapshot  flow.State `json:"snapshot"`
			} `json:"run"`
			Model, Effects, Steps, Allocations, Quota, Artifacts, Cleanup int64
		} `json:"observation"`
	}
	// Decode the count map separately so missing counts cannot silently mean zero.
	auditPath := filepath.Join(scope, "evidence", "sigterm-01", "before-runner-readonly-audit.json")
	if e = read(auditPath, &audit); e != nil {
		return nil, e
	}
	var rawAudit struct {
		Observation map[string]json.RawMessage `json:"observation"`
	}
	if e = slReadJSON(auditPath, &rawAudit); e != nil {
		return nil, e
	}
	for _, k := range []string{"model_attempts", "effects", "steps", "runner_allocations", "quota_reservations", "artifacts", "workspace_cleanup"} {
		var n int64
		b, ok := rawAudit.Observation[k]
		if !ok || json.Unmarshal(b, &n) != nil || n != 0 {
			return nil, fmt.Errorf("original audit missing or nonzero %s", k)
		}
	}
	ar := audit.Observation.Run
	if audit.Schema != in.Schema || ar.ID != in.Run || ar.State != "queued" || ar.Version != 1 || ar.Epoch != 0 || ar.Owner != "" || ar.Workspace != "" || ar.Snapshot.RunID != in.Run || ar.Snapshot.TenantID != in.Tenant || ar.Snapshot.Limits.Deadline.IsZero() {
		return nil, fmt.Errorf("original readonly audit binding mismatch")
	}
	in.Initial = ar.Snapshot
	var identity struct {
		PID   int      `json:"pid"`
		Ticks string   `json:"proc_start_ticks"`
		Exe   string   `json:"exe_path"`
		Hash  string   `json:"exe_sha256"`
		Args  []string `json:"args"`
	}
	if e = read(filepath.Join(first, "runner-original.log.identity.json"), &identity); e != nil {
		return nil, e
	}
	if identity.PID <= 1 || identity.Ticks == "" || identity.Exe != in.Original.RunnerBinary || identity.Hash != in.Original.RunnerSHA256 || !equalStrings(identity.Args, []string{in.Original.RunnerBinary, "-config", a.RunnerConfig}) {
		return nil, fmt.Errorf("original runner PID/exe/argv binding mismatch")
	}
	in.RunnerPID, in.RunnerTicks = identity.PID, identity.Ticks
	allowed := map[string]bool{"runner-original.log": true, "runner-original.log.identity.json": true, "worker-role-preflight.json": true, "recovery.json": true, "preflight.json": true, "acceptance-input.json": true, "fixture.json": true}
	entries, e := os.ReadDir(first)
	if e != nil {
		return nil, e
	}
	for _, entry := range entries {
		if entry.IsDir() || !allowed[entry.Name()] {
			return nil, fmt.Errorf("unexpected original case file %s", entry.Name())
		}
	}
	if _, e = lifecycleNoOriginalProcesses(ctx, a, c, in); e != nil {
		return nil, e
	}
	return in, nil
}

func lifecycleNoOriginalProcesses(ctx context.Context, a lifecycleAcceptance, c lifecycleRunnerConfig, in *lifecycleFirstInput) (map[string]any, error) {
	entries, e := os.ReadDir("/proc")
	if e != nil {
		return nil, e
	}
	observed := 0
	for _, entry := range entries {
		pid, e := strconv.Atoi(entry.Name())
		if e != nil || pid == os.Getpid() {
			continue
		}
		p := filepath.Join("/proc", entry.Name())
		st, e := os.Stat(p)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return nil, e
		}
		uid := st.Sys().(*syscall.Stat_t).Uid
		if uid != uint32(os.Geteuid()) && pid != in.RunnerPID {
			continue
		}
		raw, e := os.ReadFile(filepath.Join(p, "cmdline"))
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return nil, fmt.Errorf("cannot observe same-owner process %d", pid)
		}
		observed++
		for _, arg := range bytes.Split(raw, []byte{0}) {
			s := string(arg)
			if s == a.RunnerConfig || s == in.WorkerConfig || s == in.Original.WorkerBinary || s == in.Original.RunnerBinary || s == a.WorkerBinary || s == a.RunnerBinary {
				return nil, fmt.Errorf("fixture worker/runner process remains: pid %d", pid)
			}
		}
		if pid == in.RunnerPID {
			raw, e = os.ReadFile(filepath.Join(p, "stat"))
			if os.IsNotExist(e) {
				continue
			}
			if e != nil {
				return nil, e
			}
			f := strings.Fields(string(raw)[strings.LastIndexByte(string(raw), ')')+1:])
			if len(f) < 20 || f[19] == in.RunnerTicks {
				return nil, fmt.Errorf("original runner identity still exists")
			}
		}
	}
	raw, e := lifecycleDocker(ctx, c, "ps", "--all", "--no-trunc", "--filter", "label=forge.runtime=1", "--format", `{{.ID}} {{.Label "forge.operation_id"}}`)
	if e != nil {
		return nil, e
	}
	for _, line := range strings.Split(string(raw), "\n") {
		f := strings.Fields(line)
		if len(f) > 1 && strings.HasPrefix(f[1], string(in.Run)+"_") {
			return nil, fmt.Errorf("original run has a managed container: %s", f[0])
		}
	}
	return map[string]any{"observed_at": time.Now().UTC(), "same_owner_processes_read": observed, "original_pid": in.RunnerPID, "original_start_ticks": in.RunnerTicks, "original_process_absent": true, "original_run_containers": 0, "docker_read_only_query": "ps --all --filter label=forge.runtime=1"}, nil
}

func lifecycleOriginalDSN(raw, role, schema string) error {
	u, e := url.Parse(raw)
	if e != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Hostname() != "127.0.0.1" || u.Port() != "32773" || u.Path != "/forge" || u.User == nil || u.User.Username() != role || u.Fragment != "" {
		return fmt.Errorf("private loopback database route invalid")
	}
	q, e := url.ParseQuery(u.RawQuery)
	if e != nil || q.Get("search_path") != schema {
		return fmt.Errorf("private schema route invalid")
	}
	for k, values := range q {
		if len(values) != 1 || (k != "search_path" && k != "sslmode" && k != "connect_timeout") {
			return fmt.Errorf("private database overrides forbidden")
		}
	}
	return nil
}

func lifecycleOriginalStore(ctx context.Context, in *lifecycleFirstInput) (*persistence.Store, error) {
	if _, e := preflightWorkerRole(ctx, in.DSN, in.Role, in.Schema); e != nil {
		return nil, fmt.Errorf("original restricted role preflight failed (credentials withheld)")
	}
	s, e := persistence.Open(ctx, in.DSN)
	if e != nil {
		return nil, fmt.Errorf("original private database unavailable (credentials withheld)")
	}
	if e = s.CheckWorkerRole(ctx); e != nil {
		s.Close()
		return nil, e
	}
	return s, nil
}

var lifecycleZeroTables = []string{"model_attempts", "effects", "runner_allocations", "quota_reservations", "artifacts", "workspace_cleanup", "approvals", "run_messages"}

func lifecycleCaptureUnstarted(ctx context.Context, s *persistence.Store, in *lifecycleFirstInput) (lifecycleAbortObservation, error) {
	o := lifecycleAbortObservation{Counts: map[string]int64{}}
	tx, e := s.Tx(ctx, in.Tenant, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if e != nil {
		return o, e
	}
	defer tx.Rollback(ctx)
	var raw, commands []byte
	e = tx.QueryRow(ctx, `SELECT snapshot,pending_commands,workspace_id,lease_owner,lease_epoch,lease_until,coalesce(runner_id,''),state,version,clock_timestamp(),to_jsonb(r)-ARRAY['snapshot','pending_commands','state','version','next_event_seq','updated_at']::text[] FROM runs r WHERE tenant_id=$1 AND id=$2`, in.Tenant, in.Run).Scan(&raw, &commands, &o.Workspace, &o.LeaseOwner, &o.LeaseEpoch, &o.LeaseUntil, &o.RunnerID, &o.StateColumn, &o.VersionColumn, &o.ObservedAt, &o.Immutable)
	if e != nil {
		return o, e
	}
	if e = json.Unmarshal(raw, &o.State); e != nil {
		return o, e
	}
	if e = json.Unmarshal(commands, &o.Commands); e != nil {
		return o, e
	}
	for _, table := range append([]string{"runs", "steps"}, lifecycleZeroTables...) {
		var n int64
		if e = tx.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{table}.Sanitize()).Scan(&n); e != nil {
			return o, e
		}
		o.Counts[table] = n
	}
	for name, q := range map[string]string{"tenant_active": "SELECT coalesce(sum(active_count),0) FROM tenant_runtime", "runner_reserved": "SELECT coalesce(sum(reserved_slots),0) FROM runners", "quota_active": "SELECT coalesce(sum(active_requests),0) FROM provider_quotas", "quota_tokens": "SELECT coalesce(sum(reserved_tokens),0) FROM provider_quotas", "quota_cost": "SELECT coalesce(sum(reserved_microusd+committed_microusd),0) FROM provider_quotas"} {
		var n int64
		if e = tx.QueryRow(ctx, q).Scan(&n); e != nil {
			return o, e
		}
		o.Counts[name] = n
	}
	for _, spec := range []struct {
		table, order string
		dest         *[]json.RawMessage
	}{{"run_snapshots", "version", &o.Snapshots}, {"run_events", "seq", &o.Events}, {"steps", "seq", &o.Steps}} {
		rows, e := tx.Query(ctx, "SELECT to_jsonb(t) FROM "+spec.table+" t ORDER BY "+spec.order)
		if e != nil {
			return o, e
		}
		for rows.Next() {
			var b []byte
			if e = rows.Scan(&b); e != nil {
				rows.Close()
				return o, e
			}
			*spec.dest = append(*spec.dest, b)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return o, e
		}
	}
	return o, tx.Commit(ctx)
}

func lifecycleJSONEqual(a, b any) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	var p, q any
	return json.Unmarshal(x, &p) == nil && json.Unmarshal(y, &q) == nil && reflect.DeepEqual(p, q)
}
func lifecycleUnstartedOracle(o lifecycleAbortObservation, initial flow.State, cancelled bool) error {
	if initial.Status != domain.StatusQueued || initial.Version != 1 || initial.Stage != domain.StageInitialize || initial.Lease.Owner != "" || initial.Lease.Epoch != 0 || !initial.Lease.Until.IsZero() || initial.StepSeq != 0 || initial.PendingEffect != nil || initial.WorkspaceRevision != 0 || initial.ModelRounds != 0 || initial.ToolCalls != 0 || initial.Cost != 0 || initial.Progress != nil || initial.Approval != nil || len(initial.RemainingEffects) != 0 || initial.ResumeStage != "" || initial.OutputRef != "" || initial.VerificationReportRef != "" || initial.StopTarget != "" || initial.FailureReason != "" || initial.Verification != domain.VerificationUnverified || initial.VerificationRevision != 0 || initial.ReconciliationRevision != 0 {
		return fmt.Errorf("original audit is not an unstarted initial snapshot")
	}
	initial.Limits.Deadline = initial.Limits.Deadline.UTC()
	state := o.State
	state.Limits.Deadline = state.Limits.Deadline.UTC()
	if cancelled {
		if state.Status != domain.StatusCancelRequested || state.Version != 2 || state.StopTarget != domain.StatusCancelled || state.FailureReason != "cancellation requested" || len(o.Commands) != 1 || o.Commands[0].Kind != flow.CommandStopExecution || o.Commands[0].Effect != nil || o.Commands[0].Binding != nil {
			return fmt.Errorf("expected the original nonterminal cancel request and pending stop command")
		}
		state.Status = domain.StatusQueued
		state.Version = 1
		state.StopTarget = ""
		state.FailureReason = ""
	} else if len(o.Commands) != 0 {
		return fmt.Errorf("unstarted original has commands")
	}
	if !lifecycleJSONEqual(initial, state) || o.StateColumn != o.State.Status || o.VersionColumn != o.State.Version || o.Workspace != "" || o.LeaseOwner != "" || o.LeaseEpoch != 0 || o.LeaseUntil != nil || o.RunnerID != "" {
		return fmt.Errorf("original snapshot, deadline, lease, or workspace changed")
	}
	if o.Counts["runs"] != 1 {
		return fmt.Errorf("private schema no longer has one original run")
	}
	for _, k := range append(append([]string{}, lifecycleZeroTables...), "tenant_active", "runner_reserved", "quota_active", "quota_tokens", "quota_cost") {
		n, ok := o.Counts[k]
		if !ok || n != 0 {
			return fmt.Errorf("unexpected work/capacity %s", k)
		}
	}
	stepCount := int64(0)
	if cancelled {
		stepCount = 1
	}
	if count, ok := o.Counts["steps"]; !ok || count != stepCount || int64(len(o.Steps)) != stepCount {
		return fmt.Errorf("unexpected control step count")
	}
	if cancelled {
		var v struct {
			Tenant       domain.ID `json:"tenant_id"`
			Run          domain.ID `json:"run_id"`
			Seq          int       `json:"seq"`
			Kind, Status string
			Input        string  `json:"input_hash"`
			Output       *string `json:"output_ref"`
		}
		if json.Unmarshal(o.Steps[0], &v) != nil || v.Tenant != initial.TenantID || v.Run != initial.RunID || v.Seq != 0 || v.Kind != "initialize" || v.Status != "committed" || v.Input != "" || v.Output == nil || *v.Output != "" {
			return fmt.Errorf("unexpected cancellation control step")
		}
	}
	n := 1
	if cancelled {
		n = 2
	}
	if len(o.Events) != n || len(o.Snapshots) != n {
		return fmt.Errorf("unexpected event/snapshot history length")
	}
	for i, b := range o.Events {
		var v struct {
			Tenant domain.ID `json:"tenant_id"`
			Run    domain.ID `json:"run_id"`
			Seq    int       `json:"seq"`
			Type   string    `json:"type"`
		}
		if json.Unmarshal(b, &v) != nil || v.Tenant != initial.TenantID || v.Run != initial.RunID || v.Seq != i+1 || (i == 0 && v.Type != "run.created") || (i == 1 && v.Type != "run.cancel_requested") {
			return fmt.Errorf("unexpected event history")
		}
	}
	for i, b := range o.Snapshots {
		var v struct {
			Version uint64     `json:"version"`
			Tenant  domain.ID  `json:"tenant_id"`
			Run     domain.ID  `json:"run_id"`
			Step    uint64     `json:"step_seq"`
			Schema  uint32     `json:"schema_version"`
			Body    flow.State `json:"body"`
		}
		if json.Unmarshal(b, &v) != nil || v.Version != uint64(i+1) || v.Tenant != initial.TenantID || v.Run != initial.RunID || v.Step != 0 || v.Schema != initial.SchemaVersion {
			return fmt.Errorf("unexpected snapshot history")
		}
		v.Body.Limits.Deadline = v.Body.Limits.Deadline.UTC()
		want := initial
		if i == 1 {
			want = o.State
			want.Limits.Deadline = want.Limits.Deadline.UTC()
		}
		if !lifecycleJSONEqual(v.Body, want) {
			return fmt.Errorf("snapshot body changed")
		}
	}
	return nil
}
func lifecyclePreserved(before, after lifecycleAbortObservation) error {
	if !lifecycleJSONEqual(before.Immutable, after.Immutable) || len(before.Events) != 1 || len(before.Snapshots) != 1 || len(after.Events) < 1 || len(after.Snapshots) < 1 || !lifecycleJSONEqual(before.Events[0], after.Events[0]) || !lifecycleJSONEqual(before.Snapshots[0], after.Snapshots[0]) {
		return fmt.Errorf("original run columns or historical rows changed")
	}
	return nil
}
func lifecycleVerifyAbortedWithStore(ctx context.Context, in *lifecycleFirstInput, s *persistence.Store, path string) (map[string]any, error) {
	var report lifecycleAbortReport
	if e := slReadJSON(path, &report); e != nil {
		return nil, e
	}
	if !report.Passed || report.Terminal || report.Status != domain.StatusCancelRequested || report.StopTarget != domain.StatusCancelled || report.RunID != in.Run || report.Schema != in.Schema || report.Version != 2 || report.LeaseEpoch != 0 || !report.OriginalDeadline.Equal(in.Initial.Limits.Deadline) || !reflect.DeepEqual(report.Inputs, in.Hashes) {
		return nil, fmt.Errorf("abort report binding mismatch")
	}
	if e := lifecycleUnstartedOracle(report.Before, in.Initial, false); e != nil {
		return nil, e
	}
	if e := lifecycleUnstartedOracle(report.After, in.Initial, true); e != nil {
		return nil, e
	}
	if e := lifecyclePreserved(report.Before, report.After); e != nil {
		return nil, e
	}
	live, e := lifecycleCaptureUnstarted(ctx, s, in)
	if e != nil {
		return nil, e
	}
	if e = lifecycleUnstartedOracle(live, in.Initial, true); e != nil {
		return nil, e
	}
	if e = lifecyclePreserved(report.Before, live); e != nil {
		return nil, e
	}
	if !lifecycleJSONEqual(report.After.Events, live.Events) || !lifecycleJSONEqual(report.After.Snapshots, live.Snapshots) || !lifecycleJSONEqual(report.After.Steps, live.Steps) {
		return nil, fmt.Errorf("original cancellation history changed since abort")
	}
	hash, e := sigtermDigest(path)
	if e != nil {
		return nil, e
	}
	return map[string]any{"original_run_id": in.Run, "original_schema": in.Schema, "original_worker_role": in.Role, "abort_report": path, "abort_report_sha256": hash, "original_live": live, "original_terminal": false, "scope": "second independent acceptance; never resume, extend, or route original worker credentials"}, nil
}
func lifecycleVerifyAbortedFirst(ctx context.Context, a lifecycleAcceptance, c lifecycleRunnerConfig) (map[string]any, error) {
	if a.EvidenceDir != filepath.Join(a.ScopeRoot, "evidence", "sigterm-02") {
		return nil, fmt.Errorf("explicit second attempt required")
	}
	in, e := lifecycleLoadFirst(ctx, a, c)
	if e != nil {
		return nil, e
	}
	s, e := lifecycleOriginalStore(ctx, in)
	if e != nil {
		return nil, e
	}
	defer s.Close()
	return lifecycleVerifyAbortedWithStore(ctx, in, s, filepath.Join(a.ScopeRoot, "evidence", "sigterm-01", "abort-before-runner", "report.json"))
}
