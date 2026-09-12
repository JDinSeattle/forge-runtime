package applicationfaults

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
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

const sigtermCommand = `import os,time
with open('sigterm-counter.txt','a') as f:
 f.write('1\n');f.flush();os.fsync(f.fileno())
print('SIGTERM_OPERATION_RUNNING',flush=True)
time.sleep(12)
print('SIGTERM_OPERATION_FINISHED',flush=True)`

// No context override, paused Driver, artificial lease expiry, or production
// worker fault flag is used. The actual binary handles the operating-system
// signal. The runner backend below is explicitly a no-process recording fixture.
type sigtermRPC struct {
	runner.Service
	mu     sync.Mutex
	events []map[string]any
}

func (o *sigtermRPC) record(method string, r runner.WorkspaceRequest, id domain.ID, status runner.Status, err error) {
	e := map[string]any{"method": method, "at": time.Now().UTC(), "tenant_id": r.TenantID, "run_id": r.RunID, "workspace_id": r.WorkspaceID, "epoch": r.Epoch, "operation_id": id, "status": status}
	if err != nil {
		e["error"] = err.Error()
	}
	o.mu.Lock()
	o.events = append(o.events, e)
	o.mu.Unlock()
}
func (o *sigtermRPC) snapshot() []map[string]any {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]map[string]any{}, o.events...)
}
func (o *sigtermRPC) StartOperation(ctx context.Context, r runner.OperationRequest) (runner.Operation, error) {
	o.record("Start.request", r.WorkspaceRequest, r.OperationID, "", nil)
	op, err := o.Service.StartOperation(ctx, r)
	o.record("Start.return", r.WorkspaceRequest, r.OperationID, op.Status, err)
	return op, err
}
func (o *sigtermRPC) InspectOperation(ctx context.Context, r runner.InspectRequest) (runner.Operation, error) {
	op, err := o.Service.InspectOperation(ctx, r)
	o.record("Inspect.return", r.WorkspaceRequest, r.OperationID, op.Status, err)
	return op, err
}
func (o *sigtermRPC) AdoptWorkspace(ctx context.Context, r runner.WorkspaceRequest) (runner.StopReceipt, error) {
	o.record("Adopt.request", r, "", "", nil)
	result, err := o.Service.AdoptWorkspace(ctx, r)
	o.record("Adopt.return", r, "", "", err)
	return result, err
}
func (o *sigtermRPC) StopWorkspace(ctx context.Context, r runner.WorkspaceRequest) (runner.StopReceipt, error) {
	o.record("Stop.request", r, "", "", nil)
	result, err := o.Service.StopWorkspace(ctx, r)
	o.record("Stop.return", r, "", "", err)
	return result, err
}
func (o *sigtermRPC) CancelOperation(ctx context.Context, r runner.InspectRequest) (runner.Operation, error) {
	o.record("Cancel.request", r.WorkspaceRequest, r.OperationID, "", nil)
	return o.Service.CancelOperation(ctx, r)
}

type sigtermJob struct {
	job    sandbox.Job
	finish time.Time
}
type sigtermBackend struct {
	mu     sync.Mutex
	jobs   map[string]sigtermJob
	starts map[string]int
	before string
	after  string
}

func (b *sigtermBackend) backend() *sandbox.TestBackend {
	return &sandbox.TestBackend{StartFunc: b.start, InspectFunc: b.inspect, CancelFunc: b.cancel}
}
func (b *sigtermBackend) start(ctx context.Context, s sandbox.JobSpec) (sandbox.Job, error) {
	if s.BeforeStart != nil {
		if err := s.BeforeStart(ctx); err != nil {
			return sandbox.Job{}, err
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.starts[s.ID]++
	job := sandbox.Job{ID: s.ID, Started: true, Output: []byte("recording backend; no Python/container executed")}
	if len(s.Command) == 5 && s.Command[4] == sigtermCommand {
		job.Running = true
		b.jobs[s.ID] = sigtermJob{job: job, finish: time.Now().Add(12 * time.Second)}
		return job, nil
	}
	if !s.TrustedVerification || len(s.Command) != 1 || (s.Command[0] != "target" && s.Command[0] != "regression") {
		return sandbox.Job{}, domain.ErrInvalid
	}
	content, err := os.ReadFile(filepath.Join(s.Workspace, "app.py"))
	if err != nil {
		return sandbox.Job{}, err
	}
	if string(content) != b.before && string(content) != b.after {
		return sandbox.Job{}, domain.ErrUntrusted
	}
	if s.Command[0] == "target" && string(content) != b.after {
		job.ExitCode = 1
	}
	b.jobs[s.ID] = sigtermJob{job: job}
	return job, nil
}
func (b *sigtermBackend) inspect(_ context.Context, id string) (sandbox.Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	x, exists := b.jobs[id]
	if !exists {
		return sandbox.Job{}, sandbox.ErrJobNotFound
	}
	if x.job.Running && !time.Now().Before(x.finish) {
		x.job.Running = false
		b.jobs[id] = x
	}
	return x.job, nil
}
func (b *sigtermBackend) cancel(_ context.Context, id string) (sandbox.Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	x, exists := b.jobs[id]
	if !exists {
		return sandbox.Job{}, sandbox.ErrJobNotFound
	}
	if x.job.Running {
		x.job.Running, x.job.Interrupted, x.job.ExitCode = false, true, 137
		b.jobs[id] = x
	}
	return x.job, nil
}

// An allowlist avoids leaking any parent admin DSN or provider key to a worker.
func sigtermEnvironment(parent []string, dsn string) []string {
	result := []string{}
	for _, item := range parent {
		key, _, _ := strings.Cut(item, "=")
		switch key {
		case "PATH", "HOME", "LANG", "LC_ALL", "TZ", "TMPDIR":
			result = append(result, item)
		}
	}
	return append(result, "FORGE_DATABASE_URL="+dsn, "FORGE_METRICS_LISTEN=127.0.0.1:0")
}
func sigtermDigest(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:]), nil
}
func sigtermProcessProof(p *process, expectedHash string) (map[string]any, error) {
	pid := p.cmd.Process.Pid
	proc := fmt.Sprintf("/proc/%d", pid)
	actual, err := sigtermDigest(filepath.Join(proc, "exe"))
	if err != nil || actual != expectedHash {
		return nil, fmt.Errorf("worker PID/executable identity mismatch: %w", err)
	}
	exe, err := os.Readlink(filepath.Join(proc, "exe"))
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(filepath.Join(proc, "stat"))
	if err != nil {
		return nil, err
	}
	fields := strings.Fields(string(raw)[strings.LastIndexByte(string(raw), ')')+1:])
	if len(fields) < 20 {
		return nil, domain.ErrInvalid
	}
	return map[string]any{"pid": pid, "exe_path": exe, "exe_sha256": actual, "proc_start_ticks": fields[19], "observed_at": time.Now().UTC(), "args": p.cmd.Args}, nil
}
func sigtermLaunch(h *harness, binary, config, owner string) *process {
	log, err := os.OpenFile(filepath.Join(h.dir, owner+".log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	h.fatal(err)
	cmd := exec.Command(binary, "-config", config, "-id", owner)
	cmd.Env, cmd.Stdout, cmd.Stderr = sigtermEnvironment(os.Environ(), h.workerDSN), log, log
	p := &process{cmd: cmd, done: make(chan error, 1), log: log}
	h.fatal(cmd.Start())
	go func() { p.done <- cmd.Wait() }()
	h.workers = append(h.workers, p)
	hash, err := sigtermDigest(binary)
	h.fatal(err)
	identity, err := sigtermProcessProof(p, hash)
	h.fatal(err)
	h.fatal(save(log.Name()+".identity.json", identity))
	return p
}

type sigtermBoundary struct {
	SentAt, DBBefore, DBAfter, ExitedAt time.Time
}

func sigtermExit(h *harness, p *process, label, binaryHash string) sigtermBoundary {
	proof, err := sigtermProcessProof(p, binaryHash)
	h.fatal(err)
	var started map[string]any
	h.fatal(readJSON(p.log.Name()+".identity.json", &started))
	if started["proc_start_ticks"] != proof["proc_start_ticks"] || started["exe_sha256"] != binaryHash {
		h.t.Fatal("PID identity changed since this child was launched")
	}
	var boundary sigtermBoundary
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT clock_timestamp()`).Scan(&boundary.DBBefore))
	boundary.SentAt = time.Now().UTC()
	proof["signal_sent_at"], proof["db_before_signal"] = boundary.SentAt, boundary.DBBefore
	h.fatal(p.cmd.Process.Signal(syscall.SIGTERM))
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT clock_timestamp()`).Scan(&boundary.DBAfter))
	proof["db_after_signal_return"] = boundary.DBAfter
	select {
	case err := <-p.done:
		p.exited = true
		boundary.ExitedAt = time.Now().UTC()
		proof["exited_at"], proof["exit_code"] = boundary.ExitedAt, p.cmd.ProcessState.ExitCode()
		h.fatal(save(filepath.Join(h.dir, label+"-signal.json"), proof))
		h.fatal(err)
		if p.cmd.ProcessState.ExitCode() != 0 {
			h.t.Fatal("SIGTERM did not exit cleanly")
		}
	case <-time.After(8 * time.Second):
		proof["bounded_exit_failed"] = true
		_ = save(filepath.Join(h.dir, label+"-signal.json"), proof)
		h.t.Fatal("worker did not exit within fixture's 8-second bound; own state retained")
	}
	return boundary
}

func sigtermScripts(before, after string) []provider.Script {
	h := sha256.Sum256([]byte(before))
	return []provider.Script{
		tool("running", "run_command", runner.CommandArgs{Command: []string{"python", "-I", "-B", "-c", sigtermCommand}}),
		tool("repair", "apply_patch", runner.PatchArgs{Files: []runner.FileEdit{{Path: "app.py", ExpectedSHA256: hex.EncodeToString(h[:]), Content: &after}}}),
		{Chunks: []provider.Chunk{{Kind: "text", Delta: "Deterministic fixture repair ready for verification."}}, FinishReason: "stop", Usage: provider.Usage{Input: provider.TokenCount{Known: true, Value: 100}, Output: provider.TokenCount{Known: true, Value: 20}}},
	}
}

func TestWorkerSIGTERMFixturePreflight(t *testing.T) {
	env := sigtermEnvironment([]string{"PATH=/bin", "HOME=/tmp/home", "FORGE_TEST_DATABASE_URL=admin-secret", "FORGE_REVIEW_DATABASE_URL=other-secret", "OPENAI_API_KEY=paid-secret", "FORGE_DATABASE_URL=old-secret"}, "fixture-only")
	if strings.Contains(strings.Join(env, "\n"), "secret") || !strings.Contains(strings.Join(env, "\n"), "FORGE_DATABASE_URL=fixture-only") {
		t.Fatal("child environment did not isolate credentials")
	}
	s := sigtermScripts("before", "after")
	var args runner.CommandArgs
	if len(s) != 3 || json.Unmarshal([]byte(s[0].Chunks[1].Delta), &args) != nil || len(args.Command) != 5 || args.Command[4] != sigtermCommand {
		t.Fatal("fixed command/script binding differs")
	}
	b := &sigtermBackend{jobs: map[string]sigtermJob{}, starts: map[string]int{}, before: "before", after: "after"}
	job, err := b.start(context.Background(), sandbox.JobSpec{ID: "job", Command: args.Command})
	if err != nil || !job.Running || b.starts["job"] != 1 {
		t.Fatal("recording backend did not retain running job")
	}
	job, err = b.inspect(context.Background(), "job")
	if err != nil || !job.Running {
		t.Fatal("inspection changed running state early")
	}
	job, err = b.cancel(context.Background(), "job")
	if err != nil || job.Running || !job.Interrupted {
		t.Fatal("recording cancellation did not settle")
	}
}

func TestRealWorkerSIGTERMRecording(t *testing.T) {
	if os.Getenv("FORGE_RUN_WORKER_SIGTERM") != "1" {
		t.Skip("operator opt-in: actual forge-worker binary, private PostgreSQL and real gRPC; recording backend only")
	}
	binary, dir := os.Getenv("FORGE_WORKER_SIGTERM_BINARY"), os.Getenv("FORGE_WORKER_SIGTERM_EVIDENCE")
	u, err := url.Parse(os.Getenv("FORGE_TEST_DATABASE_URL"))
	if err != nil || u.Hostname() != "127.0.0.1" || u.Path != "/forge" || !filepath.IsAbs(binary) || !filepath.IsAbs(dir) {
		t.Fatal("absolute worker binary/evidence and dedicated loopback /forge required")
	}
	info, err := buildinfo.ReadFile(binary)
	if err != nil || info.Path != "github.com/JDinSeattle/forge-runtime/cmd/forge-worker" {
		t.Fatal("actual production forge-worker executable required")
	}
	if err = os.Mkdir(dir, 0700); err != nil {
		t.Fatal("fresh evidence directory required; previous evidence retained:", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	private, err := os.MkdirTemp("/tmp", "forge-worker-signal-")
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, ctx: ctx, dir: dir, private: private, schema: "appfault_sigterm_" + strings.ToLower(rand.Text())}
	binaryHash, err := sigtermDigest(binary)
	h.fatal(err)
	h.fatal(save(filepath.Join(dir, "binary.json"), map[string]any{"path": binary, "sha256": binaryHash, "go_version": info.GoVersion, "package": info.Path, "build_settings": info.Settings, "scope": "provided worker binary identity; no independent build attestation"}))
	self, err := os.Executable()
	h.fatal(err)
	selfHash, err := sigtermDigest(self)
	h.fatal(err)
	sourceRoot := os.Getenv("FORGE_WORKER_SIGTERM_SOURCE_ROOT")
	if !filepath.IsAbs(sourceRoot) {
		t.Fatal("absolute FORGE_WORKER_SIGTERM_SOURCE_ROOT required for source capture")
	}
	selectedSources := map[string]string{}
	for _, path := range []string{"go.mod", "go.sum", "cmd/forge-worker/main.go", "internal/application/driver.go", "internal/configuration/config.go", "internal/configuration/fake.go", "internal/persistence/scheduler.go", "internal/runner/engine.go", "internal/runnerclient/client.go", "internal/runnerclient/transport.go", "scripts/faults/application/application_test.go", "scripts/faults/application/worker_sigterm_test.go"} {
		digest, err := sigtermDigest(filepath.Join(sourceRoot, path))
		h.fatal(err)
		selectedSources[path] = digest
	}
	h.fatal(save(filepath.Join(dir, "harness-identity.json"), map[string]any{"test_executable": self, "test_sha256": selfHash, "selected_observed_source_sha256": selectedSources, "source_root": sourceRoot, "scope": "test binary identity and selected source files read before execution; not an independent rebuild or complete transitive input attestation"}))
	admin, err := pgx.Connect(ctx, u.String())
	h.fatal(err)
	_, err = admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{h.schema}.Sanitize())
	h.fatal(err)
	admin.Close(ctx)
	q := u.Query()
	q.Set("search_path", h.schema)
	u.RawQuery = q.Encode()
	h.dsn = u.String()
	h.fatal(migrations.Migrate(ctx, h.dsn))
	h.db, err = persistence.Open(ctx, h.dsn)
	h.fatal(err)
	defer h.db.Close()
	h.workerDSN, h.workerRole, err = restrictedWorker(ctx, h.db, *u, h.schema)
	h.fatal(err)
	role, err := preflightWorkerRole(ctx, h.workerDSN, h.workerRole, h.schema)
	h.fatal(err)
	h.fatal(save(filepath.Join(dir, "worker-role-preflight.json"), role))
	source := filepath.Join(dir, "source")
	h.fatal(os.Mkdir(source, 0700))
	before, after := "def clamp(v, lo, hi):\n    return v\n", "def clamp(v, lo, hi):\n    return max(lo, min(v, hi))\n"
	h.fatal(os.WriteFile(filepath.Join(source, "app.py"), []byte(before), 0600))
	hash, err := runner.ComputeSourceHash(ctx, source, 0, 0)
	h.fatal(err)
	key := make([]byte, 32)
	_, err = rand.Read(key)
	h.fatal(err)
	keyFile := filepath.Join(private, "signing-key")
	h.fatal(os.WriteFile(keyFile, key, 0600))
	signer, err := runner.NewSigner(key)
	h.fatal(err)
	objects, err := artifact.NewLocalStore(filepath.Join(dir, "objects"), 64<<20)
	h.fatal(err)
	defer objects.Close()
	backend := &sigtermBackend{jobs: map[string]sigtermJob{}, starts: map[string]int{}, before: before, after: after}
	h.c = runnerSettings{RootDir: filepath.Join(dir, "runner"), JournalPath: filepath.Join(dir, "journal.sqlite"), ArtifactRoot: filepath.Join(dir, "objects"), SigningKeyFile: keyFile, AllowTestBackend: true, Sources: map[string]string{"clamp": source}}
	engine, err := runner.Open(runner.Config{RootDir: h.c.RootDir, JournalPath: h.c.JournalPath, Artifacts: objects, Backend: backend.backend(), Signer: signer, Sources: h.c.Sources, Profiles: map[string]sandbox.Profile{"python-clamp": {ID: "python-clamp", TargetCommand: []string{"target"}, VerifyCommand: []string{"regression"}}}})
	h.fatal(err)
	defer engine.Close()
	observer := &sigtermRPC{Service: engine}
	defer func() { _ = save(filepath.Join(dir, "rpc-observer.json"), observer.snapshot()) }()
	socket := filepath.Join(private, "runner.sock")
	server, err := runnerclient.Serve(runnerclient.ServerConfig{UnixSocket: socket}, observer)
	h.fatal(err)
	defer func() {
		closeCtx, end := context.WithTimeout(context.Background(), 3*time.Second)
		defer end()
		_ = server.Close(closeCtx)
	}()
	h.client, err = runnerclient.Dial(ctx, runnerclient.ClientConfig{UnixSocket: socket})
	h.fatal(err)
	tenant := domain.ID("sigterm-" + strings.ToLower(rand.Text()))
	h.fatal(h.db.BootstrapTenant(ctx, tenant, "fixture-operator", "admin"))
	project, err := h.db.CreateProject(ctx, tenant, "actual worker signal fixture", "clamp", "python-clamp")
	h.fatal(err)
	h.fatal(h.db.RegisterRunner(ctx, "application-fault-runner", "private-uds", 1))
	h.fatal(quota.New(h.db.Pool).Configure(ctx, quota.Config{CredentialGroup: "application-faults-fake", MaxConcurrent: 2, MaxTokens: 1_000_000, MaxCost: 100_000_000, WindowDuration: time.Hour, FailureThreshold: 3, BreakerCooldown: time.Second}))
	cfg := persistence.Config{Provider: "fake", Model: "fake", MaxModelRounds: 8, MaxToolCalls: 20, MaxCost: 1_000_000, MaxRuntimeSeconds: 175}
	request := persistence.SubmitRequest{TenantID: tenant, PrincipalID: "fixture-operator", ProjectID: project.ID, Task: "Deterministic worker lifecycle fixture; not model-quality evidence", BaseCommit: hash, Config: cfg}
	run, _, err := h.db.Submit(ctx, request, "sigterm-main")
	h.fatal(err)
	h.s = settings{Socket: socket, Evidence: dir, SourceHash: hash, Mode: "SIGTERM-recording", Tenant: tenant, RunID: run.ID, TargetOp: domain.ID(string(run.ID) + "_step_1_op_0"), Scripts: sigtermScripts(before, after)}
	workerConfig := configuration.Config{ArtifactRoot: h.c.ArtifactRoot, SigningKeyFile: keyFile, WorkerID: "signal-worker", WorkerSlots: 1, RunnerID: "application-fault-runner", Runner: runnerclient.ClientConfig{UnixSocket: socket}, Sources: map[string]configuration.Source{"clamp": {Path: source, Hash: hash, ProfileID: "python-clamp", HasTarget: true}}, Configs: map[string]persistence.Config{"fixture": cfg}, Models: map[string]application.ModelSpec{"fake/fake": modelSpec()}, Providers: map[string]provider.Registry{"fake": {"fake": {ToolCalling: true, ContextWindow: 32768, MaxOutputTokens: 1024}}}, FakeScripts: map[string][]provider.Script{"clamp": h.s.Scripts}}
	configFile := filepath.Join(private, "worker.json")
	h.fatal(save(configFile, workerConfig))
	h.fatal(save(filepath.Join(dir, "fixture.json"), map[string]any{"schema": h.schema, "worker_role": h.workerRole, "tenant": tenant, "main_run": run.ID, "target_operation": h.s.TargetOp, "source_hash": hash, "worker_config": configFile, "worker_default_lease_seconds": 30, "backend": "recording TestBackend, no process or container", "provider": "deterministic ScriptedProvider indexed by durable step", "scripts": h.s.Scripts}))
	defer h.stopAll()
	one := sigtermLaunch(h, binary, configFile, "signal-original")
	h.wait(func() bool { return h.getRun().State.Status == domain.StatusWaitingApproval }, 15*time.Second, "literal command approval")
	h.approve(h.getRun())
	var running runner.Operation
	h.wait(func() bool {
		current := h.getRun()
		if current.State.PendingEffect == nil || current.State.PendingEffect.Status != "in_flight" {
			return false
		}
		r, err := sigtermInspect(h, engine, signer, current, h.s.TargetOp)
		if err == nil && r.Status == runner.Running {
			running = r
			return true
		}
		return false
	}, 15*time.Second, "actual runner operation running")
	h.capture("before-signal")
	h.fatal(save(filepath.Join(dir, "operation-before-signal.json"), running))
	signaled := sigtermExit(h, one, "original", binaryHash)
	atExit := h.getRun()
	if atExit.State.Status != domain.StatusRunning || atExit.State.Lease.Owner != "signal-original-0" || atExit.State.PendingEffect == nil || atExit.State.PendingEffect.ID != h.s.TargetOp {
		t.Fatal("process signal changed business state or pending operation")
	}
	runningAfter, err := sigtermInspect(h, engine, signer, atExit, h.s.TargetOp)
	h.fatal(err)
	if runningAfter.Status != runner.Running || runningAfter.JobID != running.JobID {
		t.Fatal("worker exit stopped/replaced the running job")
	}
	h.fatal(save(filepath.Join(dir, "operation-after-worker-exit.json"), runningAfter))
	h.capture("after-worker-exit")
	var activeAtExit, slotsAtExit, reservedAtExit int
	h.fatal(h.db.Pool.QueryRow(ctx, `SELECT active_count FROM tenant_runtime WHERE tenant_id=$1`, tenant).Scan(&activeAtExit))
	h.fatal(h.db.Pool.QueryRow(ctx, `SELECT reserved_slots FROM runners WHERE id='application-fault-runner'`).Scan(&slotsAtExit))
	h.fatal(h.db.Pool.QueryRow(ctx, `SELECT count(*) FROM runner_allocations WHERE tenant_id=$1 AND run_id=$2 AND state='reserved' AND lease_epoch=$3`, tenant, run.ID, atExit.State.Lease.Epoch).Scan(&reservedAtExit))
	if activeAtExit != 1 || slotsAtExit != 1 || reservedAtExit != 1 {
		t.Fatal("worker shutdown released the unsettled run's allocation", activeAtExit, slotsAtExit, reservedAtExit)
	}
	for _, rpc := range observer.snapshot() {
		if rpc["run_id"] == run.ID && (rpc["method"] == "Stop.request" || rpc["method"] == "Cancel.request") {
			t.Fatal("worker SIGTERM attempted a business stop/cancel")
		}
	}
	probe, _, err := h.db.Submit(ctx, request, "sigterm-queued-probe")
	h.fatal(err)
	if probe.State.Version != 1 || probe.State.Lease.Epoch != 0 || probe.State.Lease.Owner != "" || probe.State.Status != domain.StatusQueued {
		t.Fatal("post-exit queued probe unexpectedly claimed")
	}
	// Fixture-only delayed admission lets the same single slot be reused after
	// the main run is sealed/released; it never changes the target run's lease.
	_, err = h.db.Pool.Exec(ctx, `UPDATE runs SET not_before=clock_timestamp()+interval '3 minutes' WHERE tenant_id=$1 AND id=$2 AND lease_epoch=0`, tenant, probe.ID)
	h.fatal(err)
	probeView := *h
	probeView.s.RunID, probeView.s.TargetOp = probe.ID, domain.ID(string(probe.ID)+"_step_1_op_0")
	probeView.capture("probe-queued-after-exit")
	two := sigtermLaunch(h, binary, configFile, "signal-successor")
	probeView.workers = h.workers
	proof, err := sigtermProcessProof(two, binaryHash)
	h.fatal(err)
	h.fatal(save(filepath.Join(dir, "successor-process.json"), proof))
	var claimedAt time.Time
	h.wait(func() bool {
		var epoch uint64
		var now time.Time
		h.fatal(h.db.Pool.QueryRow(ctx, `SELECT clock_timestamp(),lease_epoch FROM runs WHERE tenant_id=$1 AND id=$2`, tenant, run.ID).Scan(&now, &epoch))
		if epoch == atExit.State.Lease.Epoch {
			return false
		}
		if epoch != atExit.State.Lease.Epoch+1 || now.Before(atExit.State.Lease.Until) {
			t.Fatal("successor bypassed natural lease expiry")
		}
		claimedAt = now
		return true
	}, 45*time.Second, "successor natural lease claim")
	h.capture("successor-claimed")
	h.wait(func() bool { return h.getRun().State.Status.Terminal() }, 20*time.Second, "same operation settles and main run completes")
	final := h.getRun()
	if final.State.Status != domain.StatusCompleted {
		t.Fatal("unexpected main terminal", final.State.Status)
	}
	h.capture("main-completed")
	sigtermCleanup(h)
	stillQueued := probeView.getRun()
	if stillQueued.State.Version != 1 || stillQueued.State.Lease.Epoch != 0 || stillQueued.State.Status != domain.StatusQueued {
		t.Fatal("probe admitted before the main workspace was released")
	}
	_, err = h.db.Pool.Exec(ctx, `UPDATE runs SET not_before=clock_timestamp() WHERE tenant_id=$1 AND id=$2 AND lease_epoch=0`, tenant, probe.ID)
	h.fatal(err)
	probeView.wait(func() bool { return probeView.getRun().State.Status == domain.StatusWaitingApproval }, 15*time.Second, "probe approval after main release")
	probeView.approve(probeView.getRun())
	probeView.wait(func() bool { return probeView.getRun().State.Status.Terminal() }, 25*time.Second, "probe completion")
	if probeView.getRun().State.Status != domain.StatusCompleted {
		t.Fatal("probe failed")
	}
	sigtermExit(h, two, "successor", binaryHash)
	sigtermCleanup(&probeView)
	acceptance := sigtermAssertions(h, observer, backend, atExit, running, final, signaled, claimedAt)
	acceptance["at_worker_exit_capacity"] = map[string]int{"tenant_active": activeAtExit, "runner_slots": slotsAtExit, "reserved_allocation": reservedAtExit}
	acceptance["probe_version_before_admission"], acceptance["probe_epoch_before_admission"] = stillQueued.State.Version, stillQueued.State.Lease.Epoch
	acceptance["probe_run_id"] = probe.ID
	acceptance["passed"] = true
	h.fatal(save(filepath.Join(dir, "acceptance.json"), acceptance))
	t.Logf("actual forge-worker SIGTERM / successor recording acceptance: %s", dir)
}

func sigtermInspect(h *harness, service runner.Service, signer *runner.Signer, r persistence.Run, id domain.ID) (runner.Operation, error) {
	lease, now, err := h.db.LeaseProof(h.ctx, r.TenantID, r.ID, r.State.Lease.Owner, r.State.Lease.Epoch)
	if err != nil {
		return runner.Operation{}, err
	}
	request := runner.WorkspaceRequest{TenantID: r.TenantID, RunID: r.ID, WorkspaceID: r.ID, Epoch: lease.Epoch}
	request.Grant, err = signer.Sign(runner.Claims{TenantID: r.TenantID, RunID: r.ID, WorkspaceID: r.ID, Epoch: lease.Epoch, Permissions: []string{"inspect"}, IssuedAt: now, ExpiresAt: lease.Until.Add(-signer.Skew)}, lease.Until)
	if err != nil {
		return runner.Operation{}, err
	}
	op, err := service.InspectOperation(h.ctx, runner.InspectRequest{WorkspaceRequest: request, OperationID: id})
	op.Request.Grant = ""
	return op, err
}

func sigtermCleanup(h *harness) {
	d, closeDriver, err := driver(h.ctx, h.s, h.c, h.workerDSN, h.client)
	h.fatal(err)
	defer closeDriver()
	cleanup, err := d.CleanupWorkspace(h.ctx, h.s.Tenant, h.s.RunID, "signal-fixture-cleanup", 0)
	h.fatal(err)
	if cleanup.Phase != "released" || cleanup.SnapshotRef == "" {
		h.t.Fatal("own workspace was not sealed/published/released")
	}
	view := *h
	view.dir = filepath.Join(h.dir, string(h.s.RunID))
	h.fatal(os.Mkdir(view.dir, 0700))
	view.capture("cleanup")
	view.archiveArtifacts()
}

func sigtermAssertions(h *harness, observer *sigtermRPC, backend *sigtermBackend, atExit persistence.Run, original runner.Operation, final persistence.Run, signal sigtermBoundary, claimedAt time.Time) map[string]any {
	var cancelEvents, unknown, active, slots, requestSlots, targetRows, targetConfirmations int
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT count(*) FROM run_snapshots WHERE tenant_id=$1 AND input_event->>'kind'='cancel_requested'`, h.s.Tenant).Scan(&cancelEvents))
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT count(*) FROM effects WHERE tenant_id=$1 AND status IN ('in_flight','unknown')`, h.s.Tenant).Scan(&unknown))
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT active_count FROM tenant_runtime WHERE tenant_id=$1`, h.s.Tenant).Scan(&active))
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT reserved_slots FROM runners WHERE id='application-fault-runner'`).Scan(&slots))
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT active_requests FROM provider_quotas WHERE credential_group='application-faults-fake'`).Scan(&requestSlots))
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT count(*) FROM effects WHERE tenant_id=$1 AND run_id=$2 AND operation_id=$3 AND epoch=$4 AND status='succeeded' AND receipt_ref IS NOT NULL`, h.s.Tenant, h.s.RunID, h.s.TargetOp, original.Request.Epoch).Scan(&targetRows))
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT count(*) FROM run_snapshots WHERE tenant_id=$1 AND run_id=$2 AND input_event->>'kind'='effect_completed' AND input_event->'receipt'->>'effect_id'=$3`, h.s.Tenant, h.s.RunID, h.s.TargetOp).Scan(&targetConfirmations))
	if cancelEvents != 0 || unknown != 0 || active != 0 || slots != 0 || requestSlots != 0 || targetRows != 1 || targetConfirmations != 1 {
		h.t.Fatalf("signal/terminal ledger mismatch: cancel=%d unknown=%d active=%d slots=%d requests=%d target=%d confirmations=%d", cancelEvents, unknown, active, slots, requestSlots, targetRows, targetConfirmations)
	}
	var afterSignalClaims, wrongLease, mainAttempts int
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT count(*) FROM run_snapshots WHERE tenant_id=$1 AND input_event->>'kind'='claimed' AND input_event->'lease'->>'owner'='signal-original-0' AND (input_event->>'at')::timestamptz>$2`, h.s.Tenant, signal.DBAfter).Scan(&afterSignalClaims))
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT count(*) FROM run_snapshots WHERE tenant_id=$1 AND input_event->>'kind'='claimed' AND extract(epoch FROM ((input_event->'lease'->>'until')::timestamptz-(input_event->>'at')::timestamptz))<>30`, h.s.Tenant).Scan(&wrongLease))
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT count(*) FROM model_attempts WHERE tenant_id=$1 AND run_id=$2 AND status='completed'`, h.s.Tenant, h.s.RunID).Scan(&mainAttempts))
	if afterSignalClaims != 0 || wrongLease != 0 || mainAttempts != 3 {
		h.t.Fatal("worker claim/default lease/model replay mismatch", afterSignalClaims, wrongLease, mainAttempts)
	}
	starts, successorStarts, successorInspects, successorAdopts := 0, 0, 0, 0
	for _, event := range observer.snapshot() {
		if event["run_id"] != h.s.RunID {
			continue
		}
		method, epoch := event["method"].(string), event["epoch"].(uint64)
		if event["operation_id"] == h.s.TargetOp && method == "Start.request" {
			starts++
			if epoch > atExit.State.Lease.Epoch {
				successorStarts++
			}
		}
		if epoch > atExit.State.Lease.Epoch && method == "Inspect.return" && event["operation_id"] == h.s.TargetOp {
			successorInspects++
		}
		if epoch > atExit.State.Lease.Epoch && method == "Adopt.request" {
			successorAdopts++
		}
		if method == "Cancel.request" {
			h.t.Fatal("worker issued business Cancel RPC")
		}
	}
	backend.mu.Lock()
	backendStarts := backend.starts[original.JobID]
	backend.mu.Unlock()
	if starts != 1 || backendStarts != 1 || successorStarts != 0 || successorInspects == 0 || successorAdopts == 0 {
		h.t.Fatal("original operation not reused", starts, backendStarts, successorStarts, successorInspects, successorAdopts)
	}
	events, err := h.db.Events(h.ctx, h.s.Tenant, h.s.RunID, 0, 1000)
	h.fatal(err)
	for index, event := range events {
		if event.Seq != uint64(index+1) {
			h.t.Fatal("event sequence gap")
		}
	}
	return map[string]any{"scope": "actual forge-worker OS processes and SIGTERM; real private PostgreSQL restricted LOGIN and gRPC; production Engine with recording no-process backend; deterministic provider", "signal_boundary": signal, "old_lease_until": atExit.State.Lease.Until, "successor_claim_observed_db_time": claimedAt, "original_epoch": atExit.State.Lease.Epoch, "final_epoch": final.State.Lease.Epoch, "run_id": final.ID, "original_operation_id": h.s.TargetOp, "original_job_id": original.JobID, "start_rpc_count": starts, "recording_backend_start_count": backendStarts, "successor_start_count": successorStarts, "successor_inspect_count": successorInspects, "successor_adopt_count": successorAdopts, "business_cancel_events": cancelEvents, "target_effect_confirmations": targetConfirmations, "claims_after_signal_db_window": afterSignalClaims, "all_claim_leases_seconds": 30, "completed_main_model_attempts": mainAttempts, "tenant_active": active, "runner_slots": slots, "provider_requests": requestSlots, "main_event_count": len(events), "final_state": final.State}
}
