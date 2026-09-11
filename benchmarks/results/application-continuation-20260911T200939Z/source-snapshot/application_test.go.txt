// Package applicationfaults contains opt-in operator acceptance executables.
// It never substitutes a simulated runner for real application.Driver effects.
package applicationfaults

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	migrations "github.com/JDinSeattle/forge-runtime/db"
	"github.com/JDinSeattle/forge-runtime/internal/application"
	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/configuration"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/provider"
	"github.com/JDinSeattle/forge-runtime/internal/quota"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	"github.com/JDinSeattle/forge-runtime/internal/runnerclient"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
	"github.com/jackc/pgx/v5"
)

type settings struct {
	RunnerConfig string            `json:"runner_config"`
	Socket       string            `json:"socket"`
	Evidence     string            `json:"evidence"`
	SourceHash   string            `json:"source_hash"`
	Mode         string            `json:"mode"`
	RunID        domain.ID         `json:"run_id"`
	Tenant       domain.ID         `json:"tenant"`
	TargetOp     domain.ID         `json:"target_op"`
	Scripts      []provider.Script `json:"scripts"`
}
type runnerSettings struct {
	RootDir          string                     `json:"root_dir"`
	JournalPath      string                     `json:"journal_path"`
	ArtifactRoot     string                     `json:"artifact_root"`
	SigningKeyFile   string                     `json:"signing_key_file"`
	DockerHost       string                     `json:"docker_host"`
	AllowTestBackend bool                       `json:"allow_test_backend"`
	Sources          map[string]string          `json:"sources"`
	Profiles         map[string]sandbox.Profile `json:"profiles"`
	VolumeSlots      []sandbox.VolumeSpec       `json:"volume_slots"`
}

func readJSON(path string, v any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, v)
}
func save(path string, v any) error {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0600)
}
func modelSpec() application.ModelSpec {
	return application.ModelSpec{CredentialGroup: "application-faults-fake", PriceVersion: "synthetic-1microusd-per-token-v1", InputPrice: 1_000_000, OutputPrice: 1_000_000, ExactPricing: true, MaxOutputTokens: 1024, ContextTokens: 32768, RequestTimeout: 3 * time.Second}
}
func driver(ctx context.Context, s settings, c runnerSettings, dsn string, client runner.Service) (*application.Driver, func(), error) {
	store, err := persistence.Open(ctx, dsn)
	if err != nil {
		return nil, nil, err
	}
	if err = store.CheckWorkerRole(ctx); err != nil {
		store.Close()
		return nil, nil, err
	}
	objects, err := artifact.NewLocalStore(c.ArtifactRoot, 64<<20)
	if err != nil {
		store.Close()
		return nil, nil, err
	}
	key, err := os.ReadFile(c.SigningKeyFile)
	if err != nil {
		objects.Close()
		store.Close()
		return nil, nil, err
	}
	signer, err := runner.NewSigner(key)
	if err != nil {
		objects.Close()
		store.Close()
		return nil, nil, err
	}
	d := &application.Driver{Store: store, Quota: quota.New(store.Pool), Runner: client, RunnerID: "application-fault-runner", Signer: signer, Artifacts: objects, Providers: map[string]provider.Provider{"fake": configuration.ScriptedProvider{Scripts: s.Scripts}}, Models: map[string]application.ModelSpec{"fake/fake": modelSpec()}, Sources: map[string]application.SourceSpec{"clamp": {Hash: s.SourceHash, HasTarget: true}}, TrustedVerification: true, LeaseDuration: 5 * time.Second, Logger: slog.New(slog.NewTextHandler(os.Stdout, nil))}
	return d, func() { objects.Close(); store.Close() }, nil
}

// Only this opt-in test child can pause at RPC boundaries; production workers
// have no new fault API. Every underlying call is the typed real runner client.
type observedRunner struct {
	runner.Service
	fast         *runnerclient.Client
	s            settings
	seen, failed bool
	owner        string
}

func (o *observedRunner) pause(phase string, op runner.Operation, err error) {
	event := map[string]any{"pid": os.Getpid(), "worker": o.owner, "phase": phase, "operation_id": o.s.TargetOp, "at": time.Now().UTC(), "status": op.Status, "model": "deterministic fake; synthetic fee rates"}
	if err != nil {
		event["error"] = err.Error()
	}
	if save(filepath.Join(o.s.Evidence, "worker1-paused.json"), event) != nil {
		os.Exit(87)
	}
	// Parent sends real SIGKILL after observing this private test marker. The
	// driver's independent heartbeat continues until the process actually dies.
	select {}
}
func (o *observedRunner) StartOperation(ctx context.Context, r runner.OperationRequest) (runner.Operation, error) {
	service := o.Service
	if r.OperationID == o.s.TargetOp && o.fast != nil {
		service = o.fast
	}
	op, err := service.StartOperation(ctx, r)
	if r.OperationID == o.s.TargetOp {
		o.seen = true
		if o.owner == "worker1" && o.s.Mode != "F07" {
			o.pause("accepted_before_driver_receipt", op, err)
		}
	}
	return op, err
}
func (o *observedRunner) InspectOperation(ctx context.Context, r runner.InspectRequest) (runner.Operation, error) {
	if o.owner == "worker1" && o.s.Mode == "F07" && r.OperationID == o.s.TargetOp && o.failed {
		o.pause("unknown_committed_before_reinspection", runner.Operation{}, nil)
	}
	op, err := o.Service.InspectOperation(ctx, r)
	if o.owner == "worker1" && o.s.Mode == "F07" && o.seen && r.OperationID == o.s.TargetOp && err != nil && !errors.Is(err, domain.ErrNotFound) {
		o.failed = true
	}
	return op, err
}
func TestApplicationFaultWorkerProcess(t *testing.T) {
	path := os.Getenv("FORGE_APP_FAULT_WORKER_SETTINGS")
	if path == "" {
		t.Skip("subprocess entry point only")
	}
	var s settings
	var c runnerSettings
	if err := readJSON(path, &s); err != nil {
		t.Fatal(err)
	}
	if err := readJSON(s.RunnerConfig, &c); err != nil {
		t.Fatal(err)
	}
	dsn := os.Getenv("FORGE_APP_FIXTURE_DSN")
	owner := os.Getenv("FORGE_APP_WORKER_ID")
	u, err := url.Parse(dsn)
	if err != nil || !strings.HasPrefix(u.Query().Get("search_path"), "appfault_") || u.Hostname() != "127.0.0.1" || u.Path != "/forge" || (owner != "worker1" && owner != "worker2") {
		t.Fatal("exact isolated worker fixture required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	client, err := runnerclient.Dial(ctx, runnerclient.ClientConfig{UnixSocket: s.Socket, RPCTimeout: 2 * time.Second, ReconcileTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	observed := &observedRunner{Service: client, s: s, owner: owner}
	if s.Mode == "F05" && owner == "worker1" {
		observed.fast, err = runnerclient.Dial(ctx, runnerclient.ClientConfig{UnixSocket: s.Socket, RPCTimeout: 500 * time.Millisecond, ReconcileTimeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		defer observed.fast.Close()
	}
	d, closeDriver, err := driver(ctx, s, c, dsn, observed)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDriver()
	if err = save(filepath.Join(s.Evidence, owner+"-ready.json"), map[string]any{"pid": os.Getpid(), "owner": owner, "at": time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	err = d.RunWorker(ctx, owner, 1)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

type process struct {
	cmd    *exec.Cmd
	done   chan error
	exited bool
	log    *os.File
}
type harness struct {
	t                                                                                     *testing.T
	ctx                                                                                   context.Context
	s                                                                                     settings
	c                                                                                     runnerSettings
	dir, private, binary, dsn, workerDSN, workerRole, schema, workerSettings, localRunner string
	db                                                                                    *persistence.Store
	journal                                                                               *sql.DB
	runner                                                                                *process
	workers                                                                               []*process
	client                                                                                *runnerclient.Client
	sequence                                                                              int
}

func (h *harness) fatal(err error) {
	h.t.Helper()
	if err != nil {
		h.t.Fatal(err)
	}
}
func (h *harness) launch(binary string, args, extraEnv []string, name string) *process {
	log, err := os.OpenFile(filepath.Join(h.dir, name+".log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	h.fatal(err)
	cmd := exec.Command(binary, args...)
	// Worker children receive only their restricted credential. In particular,
	// the migration/audit superuser credential is not inherited by either worker.
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if key == "FORGE_TEST_DATABASE_URL" || key == "FORGE_DATABASE_URL" || key == "FORGE_APP_FIXTURE_DSN" {
			continue
		}
		cmd.Env = append(cmd.Env, entry)
	}
	cmd.Env = append(cmd.Env, extraEnv...)
	cmd.Stdout = log
	cmd.Stderr = log
	p := &process{cmd: cmd, done: make(chan error, 1), log: log}
	h.fatal(cmd.Start())
	go func() { p.done <- cmd.Wait() }()
	return p
}
func (h *harness) stop(p *process) {
	if p == nil {
		return
	}
	if !p.exited {
		p.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			p.cmd.Process.Kill()
			<-p.done
		}
		p.exited = true
	}
	p.log.Close()
}
func (h *harness) stopAll() {
	for _, p := range h.workers {
		h.stop(p)
	}
	if h.client != nil {
		h.client.Close()
	}
	h.stop(h.runner)
}
func (h *harness) runnerStart(plan string) {
	if h.client != nil {
		h.client.Close()
		h.client = nil
	}
	h.stop(h.runner)
	h.runner = nil
	if err := os.Remove(h.s.Socket); err != nil && !os.IsNotExist(err) {
		h.t.Fatal(err)
	}
	h.sequence++
	args := []string{"-config", h.localRunner}
	if plan != "" {
		args = append(args, "-allow-fault-injection", "-fault-file", plan)
	}
	h.runner = h.launch(h.binary, args, nil, fmt.Sprintf("runner-%02d", h.sequence))
	var err error
	h.client, err = runnerclient.Dial(h.ctx, runnerclient.ClientConfig{UnixSocket: h.s.Socket, RPCTimeout: 10 * time.Second})
	h.fatal(err)
}
func (h *harness) worker(owner string) *process {
	p := h.launch(os.Args[0], []string{"-test.run=^TestApplicationFaultWorkerProcess$", "-test.v"}, []string{"FORGE_APP_FAULT_WORKER_SETTINGS=" + h.workerSettings, "FORGE_APP_FIXTURE_DSN=" + h.workerDSN, "FORGE_APP_WORKER_ID=" + owner}, owner)
	h.workers = append(h.workers, p)
	h.wait(func() bool { _, err := os.Stat(filepath.Join(h.dir, owner+"-ready.json")); return err == nil }, 10*time.Second, "worker ready")
	return p
}
func (h *harness) wait(predicate func() bool, d time.Duration, description string) {
	h.t.Helper()
	until := time.Now().Add(d)
	for !predicate() {
		for _, p := range h.workers {
			if p.exited {
				continue
			}
			select {
			case err := <-p.done:
				p.exited = true
				raw, _ := os.ReadFile(p.log.Name())
				if len(raw) > 8192 {
					raw = raw[len(raw)-8192:]
				}
				h.t.Fatalf("worker exited while waiting for %s: %v; log %s:\n%s", description, err, p.log.Name(), raw)
			default:
			}
		}
		if h.ctx.Err() != nil || time.Now().After(until) {
			h.t.Fatalf("timeout waiting for %s; own schema/workspace retained", description)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
func (h *harness) getRun() persistence.Run {
	r, err := h.db.GetRun(h.ctx, h.s.Tenant, h.s.RunID)
	h.fatal(err)
	return r
}
func (h *harness) approve(r persistence.Run) {
	if r.State.Approval == nil {
		h.t.Fatal("missing exact approval binding")
	}
	// Fixture operator authorization covers only these two literal generated
	// commands. Arbitrary model output or another run can never be auto-approved.
	expected := map[domain.ID]bool{h.s.TargetOp: true, domain.ID(string(r.ID) + "_step_2_op_0"): true}
	if !expected[r.State.Approval.EffectID] {
		h.t.Fatal("unexpected approval operation")
	}
	ordinal := 1
	if r.State.Approval.EffectID != h.s.TargetOp {
		ordinal = 2
	}
	var args string
	for _, chunk := range h.s.Scripts[ordinal-1].Chunks {
		if chunk.Kind == "tool_delta" {
			args += chunk.Delta
		}
	}
	canonical, err := domain.CanonicalJSON([]byte(args))
	h.fatal(err)
	digest := sha256.Sum256(canonical)
	if r.State.Approval.ArgsHash != hex.EncodeToString(digest[:]) {
		h.t.Fatal("approval args differ from fixed fixture")
	}
	_, err = h.db.Decide(h.ctx, persistence.Identity{TenantID: h.s.Tenant, PrincipalID: "fixture-operator", Role: "admin"}, domain.ID(string(r.State.Approval.EffectID)+"_approval"), *r.State.Approval, true)
	h.fatal(err)
}
func (h *harness) sqlRows(query string, args ...any) json.RawMessage {
	rows, err := h.db.Pool.Query(h.ctx, query, args...)
	h.fatal(err)
	defer rows.Close()
	result := []json.RawMessage{}
	for rows.Next() {
		var raw []byte
		h.fatal(rows.Scan(&raw))
		result = append(result, raw)
	}
	h.fatal(rows.Err())
	raw, err := json.Marshal(result)
	h.fatal(err)
	return raw
}
func (h *harness) capture(name string) map[string]json.RawMessage {
	rows := map[string]json.RawMessage{}
	for _, table := range []string{"runs", "effects", "model_attempts", "quota_reservations", "runner_allocations", "run_events", "run_snapshots", "artifacts", "workspace_cleanup", "approvals"} {
		column := "run_id"
		if table == "runs" {
			column = "id"
		}
		rows[table] = h.sqlRows("SELECT to_jsonb(t) FROM "+table+" t WHERE tenant_id=$1 AND "+column+"=$2", h.s.Tenant, h.s.RunID)
	}
	rows["tenant_runtime"] = h.sqlRows(`SELECT to_jsonb(t) FROM tenant_runtime t WHERE tenant_id=$1`, h.s.Tenant)
	rows["runners"] = h.sqlRows(`SELECT to_jsonb(t) FROM runners t WHERE id='application-fault-runner'`)
	rows["provider_quotas"] = h.sqlRows(`SELECT to_jsonb(t) FROM provider_quotas t WHERE credential_group='application-faults-fake'`)
	h.fatal(save(filepath.Join(h.dir, name+"-postgres.json"), rows))
	return rows
}
func (h *harness) createdPlan(epoch uint64) string {
	point, action := "after_start_acceptance", "delay"
	if h.s.Mode == "F07" {
		point, action = "after_job_exit", "exit"
	}
	path := filepath.Join(h.c.RootDir, "operator-faults", string(h.s.RunID)+".json")
	h.fatal(os.MkdirAll(filepath.Dir(path), 0700))
	p := map[string]any{"point": point, "action": action, "tenant_id": h.s.Tenant, "run_id": h.s.RunID, "workspace_id": h.s.RunID, "operation_id": h.s.TargetOp, "epoch": epoch, "expires_at": time.Now().Add(5 * time.Minute)}
	if action == "delay" {
		p["delay_millis"] = 2000
	}
	h.fatal(save(path, p))
	return path
}
func tool(id, name string, args any) provider.Script {
	raw, _ := json.Marshal(args)
	return provider.Script{Chunks: []provider.Chunk{{Kind: "tool_start", CallID: id, Name: name}, {Kind: "tool_delta", CallID: id, Delta: string(raw)}, {Kind: "tool_end", CallID: id}}, FinishReason: "tool_calls", Usage: provider.Usage{Input: provider.TokenCount{Known: true, Value: 100}, Output: provider.TokenCount{Known: true, Value: 50}}}
}
func scripts(mode string, source string) ([]provider.Script, error) {
	before, err := os.ReadFile(filepath.Join(source, "app.py"))
	if err != nil {
		return nil, err
	}
	expected, err := os.ReadFile(filepath.Join(filepath.Dir(source), "expected", "app.py"))
	if err != nil {
		return nil, err
	}
	if string(expected) == string(before) {
		return nil, fmt.Errorf("fixture source must differ from trusted expected oracle")
	}
	after := string(expected)
	sum := sha256.Sum256(before)
	code := `import os,json
with open('app-counter.txt','a') as f:
 f.write('1\n');f.flush();os.fsync(f.fileno())
print(json.dumps({'counter':1}),flush=True)`
	if mode == "F08" {
		code = `import os,time,json
with open('old-writer.txt','a') as f:
 for i in range(1000):
  n=time.time_ns();f.write(str(n)+'\n');f.flush();os.fsync(f.fileno());print(json.dumps({'old_write_ns':n}),flush=True);time.sleep(.05)`
	}
	next := `import os,time,json
n=time.time_ns()
with open('new-writer.txt','w') as f:
 f.write(str(n)+'\n');f.flush();os.fsync(f.fileno())
print(json.dumps({'new_write_ns':n}),flush=True)`
	return []provider.Script{tool("target", "run_command", runner.CommandArgs{Command: []string{"python", "-I", "-B", "-c", code}}), tool("next", "run_command", runner.CommandArgs{Command: []string{"python", "-I", "-B", "-c", next}}), tool("repair", "apply_patch", runner.PatchArgs{Files: []runner.FileEdit{{Path: "app.py", ExpectedSHA256: hex.EncodeToString(sum[:]), Content: &after}}}), {Chunks: []provider.Chunk{{Kind: "text", Delta: "Fixture repair ready for trusted verification."}}, FinishReason: "stop", Usage: provider.Usage{Input: provider.TokenCount{Known: true, Value: 100}, Output: provider.TokenCount{Known: true, Value: 20}}}}, nil
}

func TestRealApplicationFaultMatrix(t *testing.T) {
	runnerConfig := os.Getenv("FORGE_APP_FAULT_RUNNER_CONFIG")
	if runnerConfig == "" {
		t.Skip("operator opt-in requires quiesced real runner/pool and isolated loopback PostgreSQL")
	}
	binary := os.Getenv("FORGE_APP_FAULT_RUNNER_BINARY")
	output := os.Getenv("FORGE_APP_FAULT_EVIDENCE")
	dsn := os.Getenv("FORGE_TEST_DATABASE_URL")
	if !filepath.IsAbs(runnerConfig) || !filepath.IsAbs(binary) || !filepath.IsAbs(output) {
		t.Fatal("absolute runner config/binary/evidence required")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || u.Path != "/forge" {
		t.Fatal("FORGE_TEST_DATABASE_URL must select isolated loopback /forge")
	}
	var c runnerSettings
	if err = readJSON(runnerConfig, &c); err != nil {
		t.Fatal(err)
	}
	if c.AllowTestBackend || c.DockerHost == "" || len(c.VolumeSlots) == 0 || !strings.Contains(c.Profiles["python-clamp"].Image, "@sha256:") {
		t.Fatal("real fixed-volume Docker with pinned python-clamp profile required")
	}
	// Resolve every source/oracle script before creating any schema, process or
	// workspace so fixture setup errors cannot consume external resources.
	preparedScripts := map[string][]provider.Script{}
	for _, mode := range []string{"F05", "F07", "F08"} {
		preparedScripts[mode], err = scripts(mode, c.Sources["clamp"])
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = os.MkdirAll(output, 0700); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(output); err != nil || info.Mode().Perm()&0077 != 0 {
		t.Fatal("evidence root must be owner-only")
	}
	for _, mode := range []string{"F05", "F07", "F08"} {
		if !t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
			defer cancel()
			dir := filepath.Join(output, mode)
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			private, err := os.MkdirTemp("/tmp", "app-fault-")
			if err != nil {
				t.Fatal(err)
			}
			h := &harness{t: t, ctx: ctx, c: c, dir: dir, private: private, binary: binary}
			defer h.stopAll()
			h.schema = "appfault_" + strings.ToLower(rand.Text())
			admin, err := pgx.Connect(ctx, dsn)
			h.fatal(err)
			_, err = admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{h.schema}.Sanitize())
			h.fatal(err)
			admin.Close(ctx)
			scoped := *u
			q := scoped.Query()
			q.Set("search_path", h.schema)
			scoped.RawQuery = q.Encode()
			h.dsn = scoped.String()
			h.fatal(migrations.Migrate(ctx, h.dsn))
			h.db, err = persistence.Open(ctx, h.dsn)
			h.fatal(err)
			defer h.db.Close()
			h.workerDSN, h.workerRole, err = restrictedWorker(ctx, h.db, scoped, h.schema)
			h.fatal(err)
			roleEvidence, err := preflightWorkerRole(ctx, h.workerDSN, h.workerRole, h.schema)
			h.fatal(err)
			h.fatal(save(filepath.Join(dir, "worker-role-preflight.json"), roleEvidence))
			jq := url.Values{"mode": {"ro"}, "_pragma": {"busy_timeout(5000)", "query_only(1)"}}
			ju := url.URL{Scheme: "file", Path: c.JournalPath, RawQuery: jq.Encode()}
			h.journal, err = sql.Open("sqlite", ju.String())
			h.fatal(err)
			defer h.journal.Close()
			h.journal.SetMaxOpenConns(1)
			tenant := domain.ID("app-fault-" + strings.ToLower(rand.Text()))
			h.fatal(h.db.BootstrapTenant(ctx, tenant, "fixture-operator", "admin"))
			project, err := h.db.CreateProject(ctx, tenant, "real application fault fixture", "clamp", "python-clamp")
			h.fatal(err)
			h.fatal(h.db.RegisterRunner(ctx, "application-fault-runner", "private-uds", 1))
			h.fatal(quota.New(h.db.Pool).Configure(ctx, quota.Config{CredentialGroup: "application-faults-fake", MaxConcurrent: 2, MaxTokens: 1_000_000, MaxCost: 100_000_000, WindowDuration: time.Hour, FailureThreshold: 3, BreakerCooldown: time.Second}))
			hash, err := runner.ComputeSourceHash(ctx, c.Sources["clamp"], 0, 0)
			h.fatal(err)
			scripts := preparedScripts[mode]
			run, _, err := h.db.Submit(ctx, persistence.SubmitRequest{TenantID: tenant, PrincipalID: "fixture-operator", ProjectID: project.ID, Task: "Run the explicitly approved fixture commands and repair clamp; deterministic fake model only.", BaseCommit: hash, Config: persistence.Config{Provider: "fake", Model: "fake", MaxModelRounds: 8, MaxToolCalls: 20, MaxCost: 1_000_000, MaxRuntimeSeconds: 95}}, "application-fault-"+mode)
			h.fatal(err)
			h.s = settings{RunnerConfig: runnerConfig, Socket: filepath.Join(private, "runner.sock"), Evidence: dir, SourceHash: hash, Mode: mode, RunID: run.ID, Tenant: tenant, TargetOp: domain.ID(string(run.ID) + "_step_1_op_0"), Scripts: scripts}
			h.workerSettings = filepath.Join(private, "worker.json")
			h.fatal(save(h.workerSettings, h.s))
			raw, err := os.ReadFile(runnerConfig)
			h.fatal(err)
			var copied map[string]json.RawMessage
			h.fatal(json.Unmarshal(raw, &copied))
			copied["server"], err = json.Marshal(runnerclient.ServerConfig{UnixSocket: h.s.Socket})
			h.fatal(err)
			h.localRunner = filepath.Join(private, "runner.json")
			h.fatal(save(h.localRunner, copied))
			h.fatal(save(filepath.Join(dir, "fixture.json"), map[string]any{"schema": h.schema, "tenant_id": tenant, "run_id": run.ID, "target_operation": h.s.TargetOp, "model": "deterministic fake; no API request or actual provider fee", "synthetic_expected_total_microusd": 570, "source_hash": hash, "private_settings": h.workerSettings}))
			h.runCase()
		}) {
			return
		}
	}
}
func (h *harness) runCase() {
	h.runnerStart("")
	one := h.worker("worker1")
	h.wait(func() bool { return h.getRun().State.Status == domain.StatusWaitingApproval }, 20*time.Second, "first explicit command approval")
	approved := h.getRun()
	targetEpoch := approved.State.Lease.Epoch + 1
	plan := ""
	if h.s.Mode != "F08" {
		plan = h.createdPlan(targetEpoch)
		h.runnerStart(plan)
	}
	h.approve(approved)
	h.wait(func() bool { _, err := os.Stat(filepath.Join(h.dir, "worker1-paused.json")); return err == nil }, 20*time.Second, "worker1 actual RPC boundary pause")
	if h.s.Mode == "F07" {
		select {
		case err := <-h.runner.done:
			h.runner.exited = true
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 86 {
				h.t.Fatalf("runner did not exit86: %v", err)
			}
		case <-time.After(5 * time.Second):
			h.t.Fatal("runner receipt fault not reached")
		}
		if h.getRun().State.Status != domain.StatusNeedsReconciliation {
			h.t.Fatal("ambiguous real runner failure did not preserve needs_reconciliation")
		}
	}
	before := h.getRun()
	if before.State.Lease.Epoch != targetEpoch || !strings.HasPrefix(before.State.Lease.Owner, "worker1-") {
		h.t.Fatal("wrong original owner/epoch")
	}
	h.capture("before-worker-death")
	var active, reserved int
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT active_count FROM tenant_runtime WHERE tenant_id=$1`, h.s.Tenant).Scan(&active))
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT reserved_slots FROM runners WHERE id='application-fault-runner'`).Scan(&reserved))
	if active != 1 || reserved != 1 {
		h.t.Fatalf("unsettled effect released capacity %d/%d", active, reserved)
	}
	if h.s.Mode == "F07" {
		h.runnerStart("")
	}
	two := h.worker("worker2")
	time.Sleep(250 * time.Millisecond)
	check := h.getRun()
	if check.State.Lease.Owner != before.State.Lease.Owner || check.State.Lease.Epoch != targetEpoch {
		h.t.Fatal("second process claimed unexpired lease")
	}
	// Actual OS death stops heartbeat. Neither timestamp nor lease row is edited.
	killed := time.Now().UTC()
	h.fatal(one.cmd.Process.Kill())
	err := <-one.done
	one.exited = true
	var killedExit *exec.ExitError
	if !errors.As(err, &killedExit) || killedExit.ProcessState.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
		h.t.Fatalf("worker1 did not die by SIGKILL: %v", err)
	}
	atDeath := h.getRun()
	if atDeath.State.Lease.Epoch != targetEpoch {
		h.t.Fatal("lease changed before observed worker death")
	}
	h.capture("after-worker1-sigkill")
	h.wait(func() bool { return h.getRun().State.Lease.Epoch > targetEpoch }, 12*time.Second, "natural database lease expiry and worker2 adoption")
	adopted := h.getRun()
	h.capture("after-worker2-claim")
	if !strings.HasPrefix(adopted.State.Lease.Owner, "worker2-") {
		h.t.Fatal("replacement was not independent worker2")
	}
	if !time.Now().After(atDeath.State.Lease.Until) {
		h.t.Fatal("replacement preceded original lease expiry")
	}
	var claimTimeRaw string
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT input_event->>'at' FROM run_snapshots WHERE tenant_id=$1 AND run_id=$2 AND input_event->>'kind'='claimed' AND (input_event->>'epoch')::bigint>$3 ORDER BY version LIMIT 1`, h.s.Tenant, h.s.RunID, targetEpoch).Scan(&claimTimeRaw))
	claimTime, parseErr := time.Parse(time.RFC3339Nano, claimTimeRaw)
	h.fatal(parseErr)
	if claimTime.Before(atDeath.State.Lease.Until) {
		h.t.Fatal("database claim event predates durable expired lease")
	}
	h.wait(func() bool {
		r := h.getRun()
		if r.State.Status == domain.StatusWaitingApproval {
			h.approve(r)
		}
		return r.State.Status.Terminal()
	}, 40*time.Second, "terminal verified application outcome")
	final := h.getRun()
	if final.State.Status != domain.StatusCompleted || final.State.Verification != domain.VerificationVerified {
		h.t.Fatalf("repair did not finish verified: status=%s verification=%s reason=%s", final.State.Status, final.State.Verification, final.State.FailureReason)
	}
	if _, err = h.db.Heartbeat(h.ctx, h.s.Tenant, h.s.RunID, before.State.Lease.Owner, targetEpoch, 5*time.Second); !errors.Is(err, domain.ErrFenced) {
		h.t.Fatalf("dead worker's old epoch heartbeat accepted: %v", err)
	}
	h.stop(two)
	if plan != "" {
		var marker json.RawMessage
		raw, err := os.ReadFile(plan + ".used")
		h.fatal(err)
		marker = raw
		h.fatal(save(filepath.Join(h.dir, "runner-fault-marker.json"), marker))
	}
	h.capture("completed")
	proof := h.verifyLedgers(final, targetEpoch)
	proof["worker1_pid"] = one.cmd.Process.Pid
	proof["worker2_pid"] = two.cmd.Process.Pid
	proof["worker1_sigkill_at"] = killed
	proof["original_epoch"] = targetEpoch
	proof["original_lease_until"] = atDeath.State.Lease.Until
	proof["replacement_claim_db_time"] = claimTime
	proof["first_replacement_epoch"] = adopted.State.Lease.Epoch
	proof["old_epoch_heartbeat_fenced"] = true
	proof["schema"] = h.schema
	d, closeDriver, err := driver(h.ctx, h.s, h.c, h.workerDSN, h.client)
	h.fatal(err)
	defer closeDriver()
	cleanup, err := d.CleanupWorkspace(h.ctx, h.s.Tenant, h.s.RunID, "application-fault-cleanup", 0)
	h.fatal(err)
	if cleanup.Phase != "released" || cleanup.SnapshotRef == "" {
		h.t.Fatal("real application GC did not publish snapshot before release")
	}
	proof["cleanup"] = cleanup
	h.capture("cleanup-released")
	h.archiveArtifacts()
	proof["passed"] = true
	h.fatal(save(filepath.Join(h.dir, "acceptance.json"), proof))
	h.t.Logf("%s run=%s original_op=%s epochs=%d->%d worker_pids=%d/%d ledger_fee=570 synthetic microusd", h.s.Mode, h.s.RunID, h.s.TargetOp, targetEpoch, final.State.Lease.Epoch, one.cmd.Process.Pid, two.cmd.Process.Pid)
}
func (h *harness) verifyLedgers(final persistence.Run, targetEpoch uint64) map[string]any {
	var attempts, reservations, settled, activeRequests, runnerSlots, tenantActive int
	var actual, reservedCost, reservedTokens, committedCost, committedTokens int64
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT count(*) FROM model_attempts WHERE tenant_id=$1 AND run_id=$2`, h.s.Tenant, h.s.RunID).Scan(&attempts))
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT count(*),count(*)FILTER(WHERE status='settled' AND request_slot_released),coalesce(sum(actual_microusd),0)::bigint FROM quota_reservations WHERE tenant_id=$1 AND run_id=$2`, h.s.Tenant, h.s.RunID).Scan(&reservations, &settled, &actual))
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT active_requests,reserved_microusd,reserved_tokens,committed_microusd,committed_tokens FROM provider_quotas WHERE credential_group='application-faults-fake'`).Scan(&activeRequests, &reservedCost, &reservedTokens, &committedCost, &committedTokens))
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT reserved_slots FROM runners WHERE id='application-fault-runner'`).Scan(&runnerSlots))
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT active_count FROM tenant_runtime WHERE tenant_id=$1`, h.s.Tenant).Scan(&tenantActive))
	if attempts != 4 || reservations != 4 || settled != 4 || actual != 570 || committedCost != 570 || committedTokens != 570 || int64(final.State.Cost) != 570 || activeRequests != 0 || reservedCost != 0 || reservedTokens != 0 || runnerSlots != 0 || tenantActive != 0 {
		h.t.Fatalf("ledger mismatch attempts=%d reservations=%d settled=%d actual=%d committed=%d tokens=%d run=%d active=%d reserved=%d/%d capacity=%d/%d", attempts, reservations, settled, actual, committedCost, committedTokens, final.State.Cost, activeRequests, reservedCost, reservedTokens, runnerSlots, tenantActive)
	}
	var rowCount int
	var effectEpoch uint64
	var status, receipt, argsHash string
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT count(*) FROM effects WHERE tenant_id=$1 AND run_id=$2 AND operation_id=$3`, h.s.Tenant, h.s.RunID, h.s.TargetOp).Scan(&rowCount))
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT epoch,status,coalesce(receipt_ref,''),args_hash FROM effects WHERE tenant_id=$1 AND operation_id=$2`, h.s.Tenant, h.s.TargetOp).Scan(&effectEpoch, &status, &receipt, &argsHash))
	expected := "succeeded"
	if h.s.Mode == "F08" {
		expected = "cancelled"
	}
	if rowCount != 1 || effectEpoch != targetEpoch || status != expected || receipt == "" {
		h.t.Fatalf("original effect identity/outcome changed count=%d epoch=%d status=%s receipt=%s", rowCount, effectEpoch, status, receipt)
	}
	key, err := os.ReadFile(h.c.SigningKeyFile)
	h.fatal(err)
	signer, err := runner.NewSigner(key)
	h.fatal(err)
	now := time.Now()
	claims := runner.Claims{TenantID: h.s.Tenant, RunID: h.s.RunID, WorkspaceID: h.s.RunID, Epoch: final.State.Lease.Epoch, IssuedAt: now, ExpiresAt: now.Add(time.Minute), Permissions: []string{"inspect"}}
	grant, err := signer.Sign(claims, now.Add(2*time.Minute))
	h.fatal(err)
	op, err := h.client.InspectOperation(h.ctx, runner.InspectRequest{WorkspaceRequest: runner.WorkspaceRequest{TenantID: h.s.Tenant, RunID: h.s.RunID, WorkspaceID: h.s.RunID, Epoch: claims.Epoch, Grant: grant}, OperationID: h.s.TargetOp})
	h.fatal(err)
	if op.Request.Epoch != targetEpoch || op.Request.ArgsHash != argsHash || string(op.Status) != status {
		h.t.Fatal("PG and runner immutable bindings differ")
	}
	h.fatal(save(filepath.Join(h.dir, "original-operation.json"), op))
	var job sandbox.Job
	h.fatal(json.Unmarshal(op.Result, &job))
	h.fatal(os.WriteFile(filepath.Join(h.dir, "original-command.stdout"), job.Output, 0600))
	var count int
	h.fatal(h.journal.QueryRow(`SELECT count(*) FROM operations WHERE id=?`, h.s.TargetOp).Scan(&count))
	if count != 1 {
		h.t.Fatal("runner operation replayed")
	}
	var slot string
	h.fatal(h.journal.QueryRow(`SELECT slot_id FROM volume_leases WHERE workspace_id=? AND released=0`, h.s.RunID).Scan(&slot))
	checkout := ""
	for _, spec := range h.c.VolumeSlots {
		if spec.ID == slot {
			checkout = filepath.Join(spec.MountPath, "workspace-"+string(h.s.RunID), "checkout")
		}
	}
	if checkout == "" {
		h.t.Fatal("missing active test slot")
	}
	proof := map[string]any{"target_operation": h.s.TargetOp, "target_outcome": status, "target_dispatch_epoch": targetEpoch, "model_attempts": attempts, "quota_reservations": reservations, "settled_reservations": settled, "synthetic_microusd": actual, "committed_tokens": committedTokens, "runner_reserved_slots": runnerSlots, "tenant_active": tenantActive, "provider_active_requests": activeRequests, "reserved_microusd": reservedCost, "reserved_tokens": reservedTokens, "model": "deterministic fake; no real provider fee", "terminal_status": final.State.Status, "verification": final.State.Verification}
	if h.s.Mode == "F08" {
		old, err := os.ReadFile(filepath.Join(checkout, "old-writer.txt"))
		h.fatal(err)
		next, err := os.ReadFile(filepath.Join(checkout, "new-writer.txt"))
		h.fatal(err)
		times := []int64{}
		for _, s := range strings.Fields(string(old)) {
			n, err := strconv.ParseInt(s, 10, 64)
			h.fatal(err)
			times = append(times, n)
		}
		first, err := strconv.ParseInt(strings.TrimSpace(string(next)), 10, 64)
		h.fatal(err)
		if len(times) == 0 || times[len(times)-1] >= first {
			h.t.Fatal("old/new writer intervals overlap")
		}
		for i := 1; i < len(times); i++ {
			if times[i] <= times[i-1] {
				h.t.Fatal("writer timestamps not increasing")
			}
		}
		proof["old_writer_ns"] = times
		proof["new_writer_first_ns"] = first
	} else {
		counter, err := os.ReadFile(filepath.Join(checkout, "app-counter.txt"))
		h.fatal(err)
		if string(counter) != "1\n" {
			h.t.Fatalf("real command repeated: %q", counter)
		}
		proof["counter"] = string(counter)
	}
	events, err := h.db.Events(h.ctx, h.s.Tenant, h.s.RunID, 0, 1000)
	h.fatal(err)
	if len(events) == 0 {
		h.t.Fatal("missing durable events")
	}
	for i, e := range events {
		if e.Seq != uint64(i+1) {
			h.t.Fatal("noncontiguous event sequence")
		}
	}
	proof["events"] = len(events)
	observed, cancel := context.WithTimeout(h.ctx, 5*time.Second)
	defer cancel()
	raw, err := exec.CommandContext(observed, "docker", "--host", h.c.DockerHost, "events", "--since", final.CreatedAt.Add(-time.Second).Format(time.RFC3339Nano), "--until", time.Now().Format(time.RFC3339Nano), "--filter", "container="+job.ID, "--format", "{{json .}}").CombinedOutput()
	h.fatal(err)
	h.fatal(os.WriteFile(filepath.Join(h.dir, "original-docker-events.jsonl"), raw, 0600))
	creates, starts := 0, 0
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if line == "" {
			continue
		}
		var event struct {
			Action string
			Actor  struct{ Attributes map[string]string }
		}
		h.fatal(json.Unmarshal([]byte(line), &event))
		if event.Actor.Attributes["name"] != job.ID {
			h.t.Fatal("foreign Docker event")
		}
		if event.Action == "create" {
			creates++
		}
		if event.Action == "start" {
			starts++
		}
	}
	if creates != 1 || starts != 1 {
		h.t.Fatalf("original real Docker job replayed: create=%d start=%d", creates, starts)
	}
	proof["docker_creates"] = creates
	proof["docker_starts"] = starts
	return proof
}
func (h *harness) archiveArtifacts() {
	dir := filepath.Join(h.dir, "artifacts")
	h.fatal(os.Mkdir(dir, 0700))
	rows, err := h.db.Pool.Query(h.ctx, `SELECT id,object_key FROM artifacts WHERE tenant_id=$1 AND run_id=$2 AND state='ready'`, h.s.Tenant, h.s.RunID)
	h.fatal(err)
	defer rows.Close()
	for rows.Next() {
		var id, key string
		h.fatal(rows.Scan(&id, &key))
		if domain.ID(id).Validate() != nil || !strings.HasPrefix(key, string(h.s.Tenant)+"/"+string(h.s.RunID)+"/") {
			h.t.Fatal("foreign artifact in fixture")
		}
		raw, err := os.ReadFile(filepath.Join(h.c.ArtifactRoot, key))
		h.fatal(err)
		h.fatal(os.WriteFile(filepath.Join(dir, id+".json"), raw, 0600))
	}
	h.fatal(rows.Err())
}

// This always runs without opt-in environment, Docker or PostgreSQL. It checks
// the actual repository fixture, not a second hand-written approximation.
func TestApplicationFixturePreflight(t *testing.T) {
	source, err := filepath.Abs("../../../testdata/repairs/clamp/source")
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(source, "app.py"))
	if err != nil {
		t.Fatal(err)
	}
	expected, err := os.ReadFile(filepath.Join(filepath.Dir(source), "expected", "app.py"))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(before)
	for _, mode := range []string{"F05", "F07", "F08"} {
		t.Run(mode, func(t *testing.T) {
			generated, err := scripts(mode, source)
			if err != nil {
				t.Fatal(err)
			}
			if len(generated) != 4 {
				t.Fatalf("unexpected model rounds: %d", len(generated))
			}
			// The subprocess settings serialize these scripts before the real Driver
			// consumes them. Exercise that boundary rather than inspecting memory alone.
			raw, err := json.Marshal(settings{Scripts: generated})
			if err != nil {
				t.Fatal(err)
			}
			var settings settings
			if err = json.Unmarshal(raw, &settings); err != nil {
				t.Fatal(err)
			}
			script := settings.Scripts[2]
			if len(script.Chunks) != 3 || script.Chunks[0].Name != "apply_patch" || script.FinishReason != "tool_calls" {
				t.Fatal("invalid fixture patch tool envelope")
			}
			var patch runner.PatchArgs
			if err = json.Unmarshal([]byte(script.Chunks[1].Delta), &patch); err != nil {
				t.Fatal(err)
			}
			if len(patch.Files) != 1 || patch.Files[0].Path != "app.py" || patch.Files[0].ExpectedSHA256 != hex.EncodeToString(digest[:]) || patch.Files[0].Content == nil || *patch.Files[0].Content != string(expected) {
				t.Fatal("script does not apply complete trusted oracle to exact source")
			}
			for i := 0; i < 2; i++ {
				var command runner.CommandArgs
				if err = json.Unmarshal([]byte(settings.Scripts[i].Chunks[1].Delta), &command); err != nil {
					t.Fatal(err)
				}
				if len(command.Command) != 5 || command.Command[0] != "python" || command.Command[1] != "-I" || command.Command[2] != "-B" || command.Command[3] != "-c" || command.Command[4] == "" {
					t.Fatal("unexpected approved command fixture")
				}
			}
		})
	}
}

// Only the fixture administrator creates this unique login and grants runtime
// DML in the newly migrated schema. It owns nothing and cannot create roles,
// databases or schema objects. Credentials remain in parent memory/child env.
func restrictedWorker(ctx context.Context, admin *persistence.Store, scoped url.URL, schema string) (string, string, error) {
	role := "appfault_worker_" + strings.ToLower(rand.Text())
	password := rand.Text() + rand.Text()
	identifier, schemaID := pgx.Identifier{role}.Sanitize(), pgx.Identifier{schema}.Sanitize()
	tx, err := admin.Pool.Begin(ctx)
	if err != nil {
		return "", "", err
	}
	defer tx.Rollback(ctx)
	// password is generated exclusively from crypto/rand.Text's base32 alphabet.
	statements := []string{
		"CREATE ROLE " + identifier + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT BYPASSRLS PASSWORD '" + password + "'",
		"GRANT USAGE ON SCHEMA " + schemaID + " TO " + identifier,
		"GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA " + schemaID + " TO " + identifier,
		"REVOKE ALL ON " + schemaID + ".goose_db_version," + schemaID + ".api_tokens," + schemaID + ".memberships," + schemaID + ".tenants FROM " + identifier,
		"GRANT SELECT ON " + schemaID + ".memberships," + schemaID + ".tenants TO " + identifier,
	}
	for _, statement := range statements {
		if _, err = tx.Exec(ctx, statement); err != nil {
			return "", "", err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return "", "", err
	}
	scoped.User = url.UserPassword(role, password)
	return scoped.String(), role, nil
}
func preflightWorkerRole(ctx context.Context, dsn, expectedRole, expectedSchema string) (map[string]any, error) {
	worker, err := persistence.Open(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer worker.Close()
	if err = worker.CheckWorkerRole(ctx); err != nil {
		return nil, err
	}
	var role, session, schema string
	var super, bypass, createRole, createDB, createSchema, member, read, insert, update, remove bool
	err = worker.Pool.QueryRow(ctx, `SELECT current_user,session_user,current_schema(),rolsuper,rolbypassrls,rolcreaterole,rolcreatedb,has_schema_privilege(current_schema(),'CREATE'),pg_has_role(current_user,(SELECT relowner FROM pg_class WHERE oid=to_regclass('runs')),'MEMBER'),has_table_privilege('runs','SELECT'),has_table_privilege('runs','INSERT'),has_table_privilege('runs','UPDATE'),has_table_privilege('runs','DELETE') FROM pg_roles WHERE rolname=current_user`).Scan(&role, &session, &schema, &super, &bypass, &createRole, &createDB, &createSchema, &member, &read, &insert, &update, &remove)
	if err != nil {
		return nil, err
	}
	if role != expectedRole || session != role || schema != expectedSchema || super || !bypass || createRole || createDB || createSchema || member || !read || !insert || !update || !remove {
		return nil, fmt.Errorf("restricted fixture worker privilege preflight failed")
	}
	return map[string]any{"current_user": role, "session_user": session, "schema": schema, "superuser": super, "explicit_bypassrls": bypass, "create_role": createRole, "create_database": createDB, "create_schema": createSchema, "member_of_runs_owner": member, "runtime_dml": read && insert && update && remove, "production_CheckWorkerRole": "passed"}, nil
}
