//go:build linux

package applicationfaults

// Opt-in host acceptance only. This file never runs a daemon, mounts a volume,
// edits runner policy, borrows a released demonstration slot, or calls a paid provider.
import (
	"bytes"
	"context"
	"crypto/rand"
	"debug/buildinfo"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
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
	"github.com/JDinSeattle/forge-runtime/internal/httpapi"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/provider"
	"github.com/JDinSeattle/forge-runtime/internal/quota"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	"github.com/JDinSeattle/forge-runtime/internal/runnerclient"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
	"github.com/jackc/pgx/v5"
	"golang.org/x/sys/unix"
)

const slCompleteProgram = `import os
os.write(1,b'SL_STDOUT\x00\xff'+bytes(range(256))*128)
os.write(2,b'SL_STDERR\x00\xfe'+bytes(range(255,-1,-1))*128)`

// The finite program emits both markers before flooding. Docker multiplexing
// may fill the retained prefix from either stream; seen counters prove drainage.
// Demand is 1 MiB; a real policy kill can prevent some of it being emitted.
const slOverflowProgram = `import os,time
os.write(1,b'SL_STDOUT\x00\xff');os.write(2,b'SL_STDERR\x00\xfe')
for i in range(128):
 os.write(1,b'A'*4096);os.write(2,b'B'*4096)
time.sleep(10)`
const slCancelProgram = `import os,time,signal
signal.signal(signal.SIGTERM,signal.SIG_IGN)
pid=os.fork()
if pid==0:
 os.write(2,('SL_CHILD_READY %d\n'%os.getpid()).encode());time.sleep(60);os._exit(0)
os.write(1,('SL_PARENT_READY %d %d\n'%(os.getpid(),pid)).encode());time.sleep(60)`
const slENOSPCProgram = `import os,time
os.write(1,b'SL_STDOUT\x00\xff');os.write(2,b'SL_STDERR\x00\xfe')
time.sleep(8)
for i in range(300):
 os.write(1,b'X'*4096);os.write(2,b'Y'*4096);time.sleep(.05)`

type slFixture struct {
	t         *testing.T
	ctx       context.Context
	a         slAcceptance
	c         slRunnerConfig
	dir       string
	private   string
	journalID string
	seq       int
	ownRunner *process
	client    *runnerclient.Client
	signer    *runner.Signer
	objects   *artifact.LocalStore
	h         *harness
	configs   map[string]string
	inputs    map[string]string
	cases     map[string]any
	started   time.Time
	creates   int
	completed bool
}

func (f *slFixture) check(e error) {
	f.t.Helper()
	if e != nil {
		f.t.Fatal(e)
	}
}
func (f *slFixture) save(name string, v any) { f.check(slSave(filepath.Join(f.dir, name), v)) }
func (f *slFixture) snapshot(name string) slJournal {
	j, e := slJournalSnapshot(f.ctx, f.c.JournalPath)
	f.check(e)
	if j.Identity != f.journalID {
		f.t.Fatal("same-path journal identity changed")
	}
	f.save(name, j)
	return j
}
func (f *slFixture) wait(d time.Duration, label string, fn func() bool) {
	f.t.Helper()
	until := time.NewTimer(d)
	defer until.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if fn() {
			return
		}
		select {
		case <-f.ctx.Done():
			f.t.Fatal(label, f.ctx.Err())
		case <-until.C:
			f.t.Fatal("bounded wait failed:", label)
		case <-ticker.C:
		}
	}
}
func (f *slFixture) binding(tenant, run domain.ID, epoch uint64) runner.WorkspaceRequest {
	now := time.Now()
	r := runner.WorkspaceRequest{TenantID: tenant, RunID: run, WorkspaceID: run, Epoch: epoch}
	var e error
	r.Grant, e = f.signer.Sign(runner.Claims{TenantID: tenant, RunID: run, WorkspaceID: run, Epoch: epoch, IssuedAt: now, ExpiresAt: now.Add(80 * time.Second), Permissions: []string{"prepare", "execute", "verify", "inspect", "cancel", "snapshot", "release", "adopt"}}, now.Add(90*time.Second))
	f.check(e)
	return r
}
func (f *slFixture) daemon(args ...string) []byte {
	if len(args) == 0 || (args[0] != "inspect" && args[0] != "events" && args[0] != "top" && args[0] != "info" && args[0] != "ps") {
		f.t.Fatal("fixture daemon helper is read-only")
	}
	ctx, end := context.WithTimeout(f.ctx, 5*time.Second)
	defer end()
	binary := f.c.DockerBinary
	if binary == "" {
		binary = "docker"
	}
	cmd := exec.CommandContext(ctx, binary, append([]string{"--host", f.c.DockerHost}, args...)...)
	raw, e := cmd.CombinedOutput()
	if e != nil {
		f.t.Fatalf("read-only docker %v: %v %s", args, e, raw)
	}
	return raw
}
func (f *slFixture) stopRunner() {
	if f.client != nil {
		f.check(f.client.Close())
		f.client = nil
	}
	if f.ownRunner == nil {
		return
	}
	p := f.ownRunner
	// Signal only the actual runner child created here. The parent launcher has
	// already established the full subordinate mapping; never nest rootlesskit.
	proof, err := sigtermProcessProof(p, f.a.RunnerSHA)
	f.check(err)
	proof["signal_sent_at"] = time.Now().UTC()
	f.check(p.cmd.Process.Signal(syscall.SIGTERM))
	select {
	case e := <-p.done:
		proof["exited_at"], proof["exit_code"] = time.Now().UTC(), p.cmd.ProcessState.ExitCode()
		f.save(fmt.Sprintf("runner-%02d-stop.json", f.seq), proof)
		f.check(e)
	case <-time.After(15 * time.Second):
		f.t.Fatal("dedicated runner shutdown unresolved; retain state")
	}
	p.log.Close()
	f.ownRunner = nil
}
func (f *slFixture) startRunner(phase, fault string) {
	if f.ownRunner != nil {
		f.check(slIdle(f.snapshot(fmt.Sprintf("restart-%02d-pre-stop.json", f.seq))))
	}
	f.stopRunner()
	path, ok := f.configs[phase]
	if !ok {
		f.t.Fatal("missing operator-prepared policy config", phase)
	}
	var c slRunnerConfig
	f.check(slReadJSON(path, &c))
	p := c.Logs
	c.Logs = f.c.Logs
	if !reflect.DeepEqual(c, f.c) {
		f.t.Fatal("policy variant changes non-log authority")
	}
	want := f.c.Logs
	if phase == "bytes" {
		want.RunBytes = 2 << 20
	}
	if phase == "count" {
		want.MaxOperations = 2
	}
	if p != want {
		f.t.Fatal("policy variant differs from frozen limits")
	}
	if f.seq > 0 {
		j := f.snapshot(fmt.Sprintf("restart-%02d-before.json", f.seq))
		f.check(slIdle(j))
	}
	// A stale socket may be removed only after this fixture's own process reaped.
	if _, e := os.Lstat(c.Server.UnixSocket); e == nil {
		conn, e := net.DialTimeout("unix", c.Server.UnixSocket, 100*time.Millisecond)
		if e == nil {
			conn.Close()
			f.t.Fatal("runner endpoint already live; no takeover")
		}
		f.check(os.Remove(c.Server.UnixSocket))
	} else if !os.IsNotExist(e) {
		f.check(e)
	}
	f.seq++
	log, e := os.OpenFile(filepath.Join(f.dir, fmt.Sprintf("runner-%02d-%s.log", f.seq, phase)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	f.check(e)
	args := []string{"-config", path}
	if fault != "" {
		args = append(args, "-allow-fault-injection", "-fault-file", fault)
	}
	cmd := exec.Command(f.a.RunnerBinary, args...)
	cmd.Env = sigtermEnvironment(os.Environ(), "")
	cmd.Stdout, cmd.Stderr = log, log
	proc := &process{cmd: cmd, done: make(chan error, 1), log: log}
	f.check(cmd.Start())
	go func() { proc.done <- cmd.Wait() }()
	f.ownRunner = proc
	f.wait(10*time.Second, "dedicated UDS ready", func() bool {
		conn, e := net.DialTimeout("unix", c.Server.UnixSocket, 100*time.Millisecond)
		if e != nil {
			return false
		}
		defer conn.Close()
		uc, ok := conn.(*net.UnixConn)
		if !ok {
			return false
		}
		rc, e := uc.SyscallConn()
		f.check(e)
		var cred *unix.Ucred
		f.check(rc.Control(func(fd uintptr) { cred, e = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED) }))
		f.check(e)
		hash, e := sigtermDigest(fmt.Sprintf("/proc/%d/exe", cred.Pid))
		if e != nil {
			return false
		}
		if hash != f.a.RunnerSHA || int(cred.Pid) != proc.cmd.Process.Pid {
			f.t.Fatal("UDS peer binary differs")
		}
		f.save(fmt.Sprintf("runner-%02d-peer.json", f.seq), map[string]any{"observed_at": time.Now().UTC(), "peer_pid": cred.Pid, "uid": cred.Uid, "gid": cred.Gid, "sha256": hash, "spawned_runner_pid": proc.cmd.Process.Pid, "config": path, "phase": phase, "scope": "UDS peer sampling; not cryptographic session attestation"})
		return true
	})
	f.client, e = runnerclient.Dial(f.ctx, runnerclient.ClientConfig{UnixSocket: c.Server.UnixSocket})
	f.check(e)
}
func (f *slFixture) request(w runner.Workspace, id domain.ID, program string) runner.OperationRequest {
	args, e := domain.CanonicalJSON(slJSON(runner.CommandArgs{Command: []string{"python", "-I", "-B", "-c", program}}))
	f.check(e)
	return runner.OperationRequest{WorkspaceRequest: f.binding(w.TenantID, w.RunID, w.Epoch), OperationID: id, ExpectedRevision: w.Revision, Kind: "run_command", Args: args, ArgsHash: slSHA(args), PolicyVersion: "strict-logs-combined-v1", Deadline: time.Now().Add(75 * time.Second)}
}
func (f *slFixture) inspect(r runner.OperationRequest) (runner.Operation, error) {
	return f.client.InspectOperation(f.ctx, runner.InspectRequest{WorkspaceRequest: f.binding(r.TenantID, r.RunID, r.Epoch), OperationID: r.OperationID})
}
func (f *slFixture) settle(r runner.OperationRequest) runner.Operation {
	var op runner.Operation
	f.wait(70*time.Second, "operation terminal", func() bool { var e error; op, e = f.inspect(r); f.check(e); return op.Status.Terminal() })
	return op
}
func (f *slFixture) prepare(label string) runner.Workspace {
	run := domain.ID("sl-" + label + "-" + strings.ToLower(rand.Text()))
	w, e := f.client.PrepareWorkspace(f.ctx, runner.PrepareRequest{WorkspaceRequest: f.binding("strict-log-fixture", run, 1), SourceID: "lifecycle", ProfileID: "lifecycle-python"})
	f.check(e)
	f.save(label+"-workspace.json", w)
	return w
}
func (f *slFixture) release(w runner.Workspace, label string) {
	stop, e := f.client.StopWorkspace(f.ctx, f.binding(w.TenantID, w.RunID, w.Epoch))
	f.check(e)
	f.save(label+"-stop.json", stop)
	if !stop.NoActiveOperations {
		f.t.Fatal("own workspace still active; no release")
	}
	snap, e := f.client.SealSnapshot(f.ctx, f.binding(w.TenantID, w.RunID, w.Epoch))
	f.check(e)
	f.save(label+"-snapshot.json", snap)
	if snap.Artifact.ObjectKey == "" {
		f.t.Fatal("snapshot not durable")
	}
	rel, e := f.client.ReleaseWorkspace(f.ctx, f.binding(w.TenantID, w.RunID, w.Epoch))
	f.check(e)
	f.save(label+"-release.json", rel)
	if !rel.Released {
		f.t.Fatal("release unconfirmed")
	}
	f.check(slIdle(f.snapshot(label + "-released-journal.json")))
}
func (f *slFixture) spoolPath(j slJournal, r runner.OperationRequest) string {
	lease, e := slRow(j, "volume_leases", "workspace_id", string(r.WorkspaceID))
	f.check(e)
	var slot sandbox.VolumeSpec
	for _, v := range f.c.VolumeSlots {
		if v.ID == lease["slot_id"] {
			slot = v
		}
	}
	if slot.ID == "" || lease["run_id"] != string(r.RunID) || lease["tenant_id"] != string(r.TenantID) {
		f.t.Fatal("spool authority differs")
	}
	return filepath.Join(slot.MountPath, "workspace-"+string(r.WorkspaceID), "logs", string(r.OperationID)+".spool")
}
func (f *slFixture) object(ref artifact.Ref) []byte {
	r, e := f.objects.Open(f.ctx, ref.TenantID, ref.RunID, ref)
	f.check(e)
	defer r.Close()
	b, e := io.ReadAll(io.LimitReader(r, 64<<20))
	f.check(e)
	if slSHA(b) != ref.SHA256 || int64(len(b)) != ref.Size {
		f.t.Fatal("immutable object bytes differ")
	}
	return b
}
func (f *slFixture) captureOperation(label string, request runner.OperationRequest, op runner.Operation) slFrames {
	request.Grant = ""
	op.Request.Grant = ""
	f.save(label+"-request.json", request)
	f.save(label+"-operation.json", op)
	j := f.snapshot(label + "-journal.json")
	row, e := slRow(j, "operations", "id", string(request.OperationID))
	f.check(e)
	var persisted runner.OperationRequest
	f.check(json.Unmarshal([]byte(fmt.Sprint(row["request_json"])), &persisted))
	if !slSameRequest(persisted, request) || row["status"] != string(op.Status) || row["job_id"] != op.JobID {
		f.t.Fatal("SQLite/RPC intent/status/job differs")
	}
	f.check(slStoredOperation(row, op))
	var job sandbox.Job
	f.check(json.Unmarshal(op.Result, &job))
	if job.Log == nil {
		f.t.Fatal("strict log absent")
	}
	ref := job.Log.Artifact
	raw := f.object(ref)
	receipt := f.object(op.Receipt)
	f.check(slWrite(filepath.Join(f.dir, label+"-log.flg"), raw))
	f.check(slWrite(filepath.Join(f.dir, label+"-receipt.json"), receipt))
	var receiptOp runner.Operation
	f.check(json.Unmarshal(receipt, &receiptOp))
	if !slSameRequest(receiptOp.Request, request) || receiptOp.Status != op.Status || !bytes.Equal(receiptOp.Result, op.Result) {
		f.t.Fatal("immutable receipt intent/status/result differs")
	}
	path := f.spoolPath(j, request)
	disk, e := slRead(path, int64(f.c.Logs.OperationBytes))
	f.check(e)
	f.check(slWrite(filepath.Join(f.dir, label+"-disk.spool"), disk))
	meta, e := slRead(path+".meta", 128<<10)
	if e != nil && !os.IsNotExist(e) {
		f.check(e)
	}
	if meta != nil {
		f.check(slWrite(filepath.Join(f.dir, label+"-disk.meta"), meta))
	}
	prefix, e := slParseFrames(disk, job.Log.Policy, !job.Log.Complete)
	f.check(e)
	if !bytes.Equal(raw, disk[:prefix.Bytes]) {
		f.t.Fatal("published log differs from disk prefix")
	}
	parsed, e := slValidateLog(op, request, raw, meta)
	f.check(e)
	f.check(slVerifyPins(j, ref, op.Receipt))
	logRow, e := slRow(j, "operation_logs", "operation_id", string(request.OperationID))
	f.check(e)
	cid := fmt.Sprint(logRow["container_id"])
	if len(cid) != 64 {
		f.t.Fatal("actual container ID absent")
	}
	expectedJob := "forge-" + slSHA([]byte(strings.Join([]string{f.c.RootDir, string(request.TenantID), string(request.RunID), string(request.WorkspaceID), string(request.OperationID)}, "\n")))[:40]
	if op.JobID != expectedJob || job.ID != expectedJob {
		f.t.Fatal("container name is not bound to exact engine/run/operation")
	}
	inspected := f.daemon("inspect", cid)
	f.check(slWrite(filepath.Join(f.dir, label+"-docker.json"), inspected))
	f.check(slDockerOracle(inspected, cid, request, filepath.Dir(filepath.Dir(path)), false))
	var names []struct{ Name string }
	f.check(json.Unmarshal(inspected, &names))
	if len(names) != 1 || names[0].Name != "/"+op.JobID {
		f.t.Fatal("daemon name not bound to original operation")
	}
	var statLog, statCheckout unix.Stat_t
	f.check(unix.Stat(path, &statLog))
	f.check(unix.Stat(filepath.Join(filepath.Dir(filepath.Dir(path)), "checkout"), &statCheckout))
	if statLog.Dev != statCheckout.Dev {
		f.t.Fatal("real spool not on enforced workspace filesystem")
	}
	f.save(label+"-disk-identity.json", map[string]any{"spool_device": statLog.Dev, "spool_inode": statLog.Ino, "spool_allocated_bytes": statLog.Blocks * 512, "checkout_device": statCheckout.Dev, "spool_bytes": len(disk)})
	f.save(label+"-oracle.json", parsed)
	return parsed
}
func slDockerOracle(raw []byte, cid string, r runner.OperationRequest, base string, running bool) error {
	var rows []struct {
		ID      string `json:"Id"`
		Name    string
		LogPath string
		State   struct {
			Running   bool
			StartedAt time.Time
		}
		Config struct {
			Image  string
			User   string
			Labels map[string]string
		}
		HostConfig struct {
			LogConfig      struct{ Type string }
			NetworkMode    string
			ReadonlyRootfs bool
			Memory         int64
			MemorySwap     int64
			PidsLimit      int64
		}
		Mounts []struct{ Source, Destination string }
	}
	if json.Unmarshal(raw, &rows) != nil || len(rows) != 1 {
		return fmt.Errorf("docker inspection shape")
	}
	x := rows[0]
	if x.ID != cid || x.State.Running != running || x.State.StartedAt.IsZero() || x.LogPath != "" || x.HostConfig.LogConfig.Type != "none" || !x.HostConfig.ReadonlyRootfs || x.HostConfig.NetworkMode != "none" || x.HostConfig.Memory != 256<<20 || x.HostConfig.MemorySwap != 256<<20 || x.HostConfig.PidsLimit != 64 || x.Config.User != "1000:1000" {
		return fmt.Errorf("actual Docker identity/limits/state")
	}
	// The exact production label spelling is asserted in the offline preflight
	// against the existing source; no fuzzy suffix matching is allowed here.
	for k, want := range map[string]string{"forge.runtime": "1", "forge.operation_id": string(r.OperationID)} {
		if x.Config.Labels[k] != want {
			return fmt.Errorf("container label %s mismatch", k)
		}
	}
	mounted := false
	for _, m := range x.Mounts {
		if m.Destination == "/workspace" {
			mounted = m.Source == filepath.Join(base, "checkout")
		}
	}
	if !mounted {
		return fmt.Errorf("workspace mount differs")
	}
	return nil
}

func (f *slFixture) setupPG() {
	u, e := url.Parse(os.Getenv("FORGE_TEST_DATABASE_URL"))
	f.check(e)
	if u.Hostname() != "127.0.0.1" || u.Port() != "32773" || u.Path != "/forge" || u.Query().Get("search_path") != "" {
		f.t.Fatal("dedicated loopback32773 /forge without search_path required")
	}
	private := f.private
	f.check(os.Mkdir(private, 0700))
	h := &harness{t: f.t, ctx: f.ctx, dir: f.dir, private: private, schema: "appfault_strictlogs_" + strings.ToLower(rand.Text())}
	f.h = h
	admin, e := pgx.Connect(f.ctx, u.String())
	f.check(e)
	_, e = admin.Exec(f.ctx, "CREATE SCHEMA "+pgx.Identifier{h.schema}.Sanitize())
	f.check(e)
	admin.Close(f.ctx)
	q := u.Query()
	q.Set("search_path", h.schema)
	u.RawQuery = q.Encode()
	h.dsn = u.String()
	f.check(migrations.Migrate(f.ctx, h.dsn))
	h.db, e = persistence.Open(f.ctx, h.dsn)
	f.check(e)
	h.workerDSN, h.workerRole, e = restrictedWorker(f.ctx, h.db, *u, h.schema)
	f.check(e)
	role, e := preflightWorkerRole(f.ctx, h.workerDSN, h.workerRole, h.schema)
	f.check(e)
	f.save("worker-role-preflight.json", role)
	f.check(slSave(filepath.Join(private, "database.json"), map[string]string{"worker_dsn": h.workerDSN, "schema": h.schema}))
	f.save("private-schema.json", map[string]string{"schema": h.schema, "worker_role": h.workerRole, "cleanup": "retained for audit; no public schema or credentials in raw"})
	f.check(h.db.RegisterRunner(f.ctx, "application-fault-runner", "dedicated-fixed-pool", 1))
	f.check(quota.New(h.db.Pool).Configure(f.ctx, quota.Config{CredentialGroup: "application-faults-fake", MaxConcurrent: 2, MaxTokens: 1_000_000, MaxCost: 100_000_000, WindowDuration: time.Hour, FailureThreshold: 3, BreakerCooldown: time.Second}))
}
func (f *slFixture) submit(label, program string) (*harness, string) {
	root := f.h
	h := &harness{t: f.t, ctx: f.ctx, db: root.db, private: root.private, dir: filepath.Join(f.dir, label), workerDSN: root.workerDSN, workerRole: root.workerRole, schema: root.schema}
	f.check(os.Mkdir(h.dir, 0700))
	tenant := domain.ID("sl-" + label + "-" + strings.ToLower(rand.Text()))
	f.check(h.db.BootstrapTenant(f.ctx, tenant, "fixture-operator", "admin"))
	project, e := h.db.CreateProject(f.ctx, tenant, "strict log fixture", "lifecycle", "lifecycle-python")
	f.check(e)
	hash, e := runner.ComputeSourceHash(f.ctx, f.c.Sources["lifecycle"], 0, 0)
	f.check(e)
	cfg := persistence.Config{Provider: "fake", Model: "fake", MaxModelRounds: 8, MaxToolCalls: 20, MaxCost: 1_000_000, MaxRuntimeSeconds: 240}
	r, _, e := h.db.Submit(f.ctx, persistence.SubmitRequest{TenantID: tenant, PrincipalID: "fixture-operator", ProjectID: project.ID, Task: "Fixed strict-log command and repair fixture; synthetic provider", BaseCommit: hash, Config: cfg}, "strict-logs-"+label)
	f.check(e)
	ss := sigtermScripts("def clamp(v, lo, hi):\n    return v\n", "def clamp(v, lo, hi):\n    return max(lo, min(v, hi))\n")
	ss[0] = tool("strict-log", "run_command", runner.CommandArgs{Command: []string{"python", "-I", "-B", "-c", program}})
	h.s = settings{Tenant: tenant, RunID: r.ID, TargetOp: domain.ID(string(r.ID) + "_step_1_op_0"), Scripts: ss, SourceHash: hash, Socket: f.c.Server.UnixSocket}
	h.c = runnerSettings{RootDir: f.c.RootDir, JournalPath: f.c.JournalPath, ArtifactRoot: f.c.ArtifactRoot, SigningKeyFile: f.c.SigningKeyFile, Sources: f.c.Sources, Profiles: f.c.Profiles, VolumeSlots: f.c.VolumeSlots, DockerHost: f.c.DockerHost}
	worker := configuration.Config{ArtifactRoot: f.c.ArtifactRoot, SigningKeyFile: f.c.SigningKeyFile, WorkerID: "strict-logs", WorkerSlots: 1, RunnerID: "application-fault-runner", Runner: runnerclient.ClientConfig{UnixSocket: f.c.Server.UnixSocket}, Sources: map[string]configuration.Source{"lifecycle": {Path: f.c.Sources["lifecycle"], Hash: hash, ProfileID: "lifecycle-python", HasTarget: true}}, Configs: map[string]persistence.Config{"fixture": cfg}, Models: map[string]application.ModelSpec{"fake/fake": modelSpec()}, Providers: map[string]provider.Registry{"fake": {"fake": {ToolCalling: true, ContextWindow: 32768, MaxOutputTokens: 1024}}}, FakeScripts: map[string][]provider.Script{"lifecycle": ss}}
	file := filepath.Join(root.private, label+"-worker.json")
	f.check(slSave(file, worker))
	f.check(slSave(filepath.Join(h.dir, "fixture.json"), map[string]any{"tenant": tenant, "run_id": r.ID, "target_operation": h.s.TargetOp, "source_hash": hash, "scripts": ss, "worker_config_sha256": slSHA(slJSON(worker)), "worker_default_lease_seconds": 30, "provider": "deterministic native fixture; no paid model", "driver": "actual forge-worker binary production application.Driver"}))
	return h, file
}
func (f *slFixture) worker(h *harness, file, label string) *process {
	h.client = f.client
	p := sigtermLaunch(h, f.a.WorkerBinary, file, "sl-"+label)
	f.h.workers = append(f.h.workers, p)
	return p
}
func (f *slFixture) approve(h *harness) {
	h.wait(func() bool { return h.getRun().State.Status == domain.StatusWaitingApproval }, 30*time.Second, "exact fixture approval")
	h.approve(h.getRun())
}
func (f *slFixture) finishDriver(h *harness, p *process, label string) {
	h.wait(func() bool {
		r := h.getRun()
		if r.State.Status == domain.StatusWaitingApproval {
			h.approve(r)
		}
		return r.State.Status.Terminal()
	}, 60*time.Second, "production Driver terminal")
	r := h.getRun()
	if r.State.Status != domain.StatusCompleted {
		f.t.Fatal("repair fixture did not complete", r.State.Status)
	}
	h.capture("terminal")
	sigtermExit(h, p, "worker-finished", f.a.WorkerSHA)
	// This reuse calls production CleanupWorkspace only. The old helper's clamp
	// source map is irrelevant to cleanup; no source import/model call occurs.
	d, closeDriver, e := driver(f.ctx, h.s, h.c, h.workerDSN, f.client)
	f.check(e)
	cleanup, e := d.CleanupWorkspace(f.ctx, h.s.Tenant, h.s.RunID, "sl-explicit-cleanup", 0)
	closeDriver()
	f.check(e)
	if cleanup.Phase != "released" || cleanup.SnapshotRef == "" {
		f.t.Fatal("PG snapshot/release incomplete")
	}
	f.check(slSave(filepath.Join(h.dir, "cleanup.json"), cleanup))
	h.capture("cleanup")
	var effectRows, confirmations, active, slots, allocations, requests int
	f.check(h.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM effects e JOIN artifacts a ON a.tenant_id=e.tenant_id AND a.id=e.receipt_ref WHERE e.tenant_id=$1 AND e.run_id=$2 AND e.operation_id=$3 AND e.status IN ('succeeded','failed','cancelled') AND a.kind='operation_receipt' AND a.state='ready'`, h.s.Tenant, h.s.RunID, h.s.TargetOp).Scan(&effectRows))
	f.check(h.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM run_snapshots WHERE tenant_id=$1 AND run_id=$2 AND input_event->>'kind' IN ('effect_completed','reconciled') AND input_event->'receipt'->>'effect_id'=$3`, h.s.Tenant, h.s.RunID, h.s.TargetOp).Scan(&confirmations))
	f.check(h.db.Pool.QueryRow(f.ctx, `SELECT active_count FROM tenant_runtime WHERE tenant_id=$1`, h.s.Tenant).Scan(&active))
	f.check(h.db.Pool.QueryRow(f.ctx, `SELECT reserved_slots FROM runners WHERE id='application-fault-runner'`).Scan(&slots))
	f.check(h.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM runner_allocations WHERE tenant_id=$1 AND run_id=$2 AND state<>'released'`, h.s.Tenant, h.s.RunID).Scan(&allocations))
	f.check(h.db.Pool.QueryRow(f.ctx, `SELECT active_requests FROM provider_quotas WHERE credential_group='application-faults-fake'`).Scan(&requests))
	if effectRows != 1 || confirmations != 1 || active != 0 || slots != 0 || allocations != 0 || requests != 0 {
		f.t.Fatal("Driver receipt/effect/capacity closure differs", effectRows, confirmations, active, slots, allocations, requests)
	}
	f.check(slSave(filepath.Join(h.dir, "closure-oracle.json"), map[string]int{"ready_receipt_joined_effects": effectRows, "durable_effect_confirmations": confirmations, "tenant_active": active, "runner_reserved": slots, "unreleased_allocations": allocations, "provider_active_requests": requests}))
	h.archiveArtifacts()
	f.check(slIdle(f.snapshot(label + "-idle.json")))
}
func (f *slFixture) download(db *persistence.Store, tenant, run domain.ID, ref artifact.Ref, label string) {
	artifacts, e := db.ListArtifacts(f.ctx, tenant, run, "", 1000)
	f.check(e)
	var found *persistence.Artifact
	for _, a := range artifacts {
		if a.ObjectKey == ref.ObjectKey {
			copyA := a
			if found != nil {
				f.t.Fatal("duplicate PG artifact identity")
			}
			found = &copyA
		}
	}
	if found == nil || found.State != "ready" || found.TenantID != ref.TenantID || found.RunID != ref.RunID || found.Kind != ref.Kind || found.SHA256 != ref.SHA256 || found.ByteSize != ref.Size {
		f.t.Fatal("PG READY differs from receipt")
	}
	token, e := db.IssueToken(f.ctx, "fixture-operator", time.Hour)
	f.check(e)
	api := httptest.NewServer((&httpapi.Server{Store: db, Artifacts: f.objects}).Handler())
	defer api.Close()
	client := &http.Client{Timeout: 10 * time.Second}
	get := func(path string, tenant domain.ID) (int, http.Header, []byte) {
		req, e := http.NewRequestWithContext(f.ctx, http.MethodGet, api.URL+path, nil)
		f.check(e)
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("X-Forge-Tenant", string(tenant))
		resp, e := client.Do(req)
		f.check(e)
		defer resp.Body.Close()
		raw, e := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
		f.check(e)
		return resp.StatusCode, resp.Header, raw
	}
	status, headers, body := get("/v1/artifacts/"+string(found.ID), tenant)
	if status != 200 || slSHA(body) != ref.SHA256 || int64(len(body)) != ref.Size || headers.Get("ETag") != "\""+ref.SHA256+"\"" {
		f.t.Fatal("authorized download bytes/headers differ")
	}
	f.check(slWrite(filepath.Join(f.dir, label+"-http.flg"), body))
	listStatus, _, list := get("/v1/runs/"+string(run)+"/artifacts?limit=1000", tenant)
	var listed []persistence.Artifact
	f.check(json.Unmarshal(list, &listed))
	matches := 0
	for _, a := range listed {
		if a.TenantID == found.TenantID && a.ID == found.ID && a.RunID == found.RunID && a.Kind == found.Kind && a.ObjectKey == found.ObjectKey && a.SHA256 == found.SHA256 && a.ByteSize == found.ByteSize && a.State == found.State && a.CreatedAt.Equal(found.CreatedAt) {
			matches++
		}
	}
	if listStatus != 200 || matches != 1 {
		f.t.Fatal("authorized artifact listing mismatch")
	}
	other := domain.ID("sl-other-" + strings.ToLower(rand.Text()))
	f.check(db.BootstrapTenant(f.ctx, other, "fixture-operator", "admin"))
	denied, _, denial := get("/v1/artifacts/"+string(found.ID), other)
	otherStatus, _, otherList := get("/v1/runs/"+string(run)+"/artifacts", other)
	if denied != http.StatusNotFound || bytes.Contains(denial, body) && len(body) > 0 {
		f.t.Fatal("cross-tenant download disclosed bytes")
	}
	if otherStatus != http.StatusNotFound || bytes.Contains(otherList, []byte(found.ID)) {
		f.t.Fatal("cross-tenant listing disclosed log")
	}
	f.save(label+"-http-proof.json", map[string]any{"observed_at": time.Now().UTC(), "artifact": found, "status": status, "headers": headers, "body_sha256": slSHA(body), "body_bytes": len(body), "list_status": listStatus, "listed_matches": matches, "cross_tenant_download": denied, "cross_tenant_list": otherStatus, "scope": "real handler/authenticated TCP; service DB identity, not separate deployment API-role verification"})
}
func (f *slFixture) l1() {
	h, file := f.submit("L1", slCompleteProgram)
	dir := filepath.Join(f.c.RootDir, "operator-faults")
	f.check(os.MkdirAll(dir, 0700))
	plan := filepath.Join(dir, "strict-log-publication-"+string(h.s.RunID)+".json")
	f.check(slSave(plan, map[string]any{"point": "after_receipt_before_commit", "action": "delay", "tenant_id": h.s.Tenant, "run_id": h.s.RunID, "workspace_id": h.s.RunID, "operation_id": h.s.TargetOp, "epoch": 2, "expires_at": time.Now().Add(5 * time.Minute), "delay_millis": 10000}))
	f.startRunner("default", plan)
	p := f.worker(h, file, "L1")
	f.approve(h)
	f.wait(40*time.Second, "durable receipt hook marker", func() bool { _, e := os.Stat(plan + ".used"); return e == nil })
	marker, e := slRead(plan+".used", 8192)
	f.check(e)
	f.check(slWrite(filepath.Join(f.dir, "L1-publication.used.json"), marker))
	j := f.snapshot("L1-publication-window-journal.json")
	row, e := slRow(j, "operations", "id", string(h.s.TargetOp))
	f.check(e)
	if row["status"] != "running" {
		f.t.Fatal("missed pre-terminal publication window")
	}
	var req runner.OperationRequest
	f.check(json.Unmarshal([]byte(fmt.Sprint(row["request_json"])), &req))
	if req.Epoch != 2 || req.RunID != h.s.RunID {
		f.t.Fatal("fault window does not bind actual intent")
	}
	var logRef, receiptRef artifact.Ref
	for _, pin := range j.Tables["runner_artifacts"] {
		var ref artifact.Ref
		f.check(json.Unmarshal([]byte(fmt.Sprint(pin["ref_json"])), &ref))
		if ref.TenantID != h.s.Tenant || ref.RunID != h.s.RunID {
			continue
		}
		if ref.Kind == "operation_receipt" {
			var op runner.Operation
			if json.Unmarshal(f.object(ref), &op) == nil && op.Request.OperationID == h.s.TargetOp {
				receiptRef = ref
				var job sandbox.Job
				f.check(json.Unmarshal(op.Result, &job))
				if job.Log != nil {
					logRef = job.Log.Artifact
				}
			}
		}
	}
	if receiptRef.ObjectKey == "" || logRef.ObjectKey == "" {
		f.t.Fatal("receipt/log pins not yet durable")
	}
	f.check(slVerifyPins(j, logRef, receiptRef))
	var ready int
	f.check(h.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM artifacts WHERE tenant_id=$1 AND run_id=$2 AND object_key IN($3,$4)`, h.s.Tenant, h.s.RunID, logRef.ObjectKey, receiptRef.ObjectKey).Scan(&ready))
	if ready != 0 {
		f.t.Fatal("PG published before hook returns")
	}
	h.capture("publication-window")
	op := f.settle(req)
	parsed := f.captureOperation("L1-complete", req, op)
	stdout := append([]byte("SL_STDOUT\x00\xff"), bytes.Repeat(slByteRange(false), 128)...)
	stderr := append([]byte("SL_STDERR\x00\xfe"), bytes.Repeat(slByteRange(true), 128)...)
	if op.Status != runner.Succeeded || !bytes.Equal(parsed.streams[0], stdout) || !bytes.Equal(parsed.streams[1], stderr) || parsed.MaxEntry <= 8192 {
		f.t.Fatal("binary two-stream complete output differs")
	}
	h.wait(func() bool {
		var n int
		f.check(h.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM artifacts WHERE tenant_id=$1 AND run_id=$2 AND object_key=$3 AND state='ready'`, h.s.Tenant, h.s.RunID, logRef.ObjectKey).Scan(&n))
		return n == 1
	}, 10*time.Second, "Driver publishes actual log")
	f.download(h.db, h.s.Tenant, h.s.RunID, logRef, "L1")
	f.finishDriver(h, p, "L1")
	f.cases["L1"] = map[string]any{"passed": true, "publication_window_pins": 2, "pre_terminal_pg_rows": ready, "binary_streams_exact": true}
}
func slByteRange(reverse bool) []byte {
	b := make([]byte, 256)
	for i := range b {
		if reverse {
			b[i] = byte(255 - i)
		} else {
			b[i] = byte(i)
		}
	}
	return b
}

func (f *slFixture) quotaCase(label, phase string, count int, includeCancel bool) {
	f.startRunner(phase, "")
	w := f.prepare(label)
	policy := f.c.Logs
	if phase == "bytes" {
		policy.RunBytes = 2 << 20
	}
	if phase == "count" {
		policy.MaxOperations = 2
	}
	total := int64(0)
	var last runner.Operation
	var lastRequest runner.OperationRequest
	for i := 0; i < count; i++ {
		program := slOverflowProgram
		if includeCancel && i == 1 {
			program = slCancelProgram
		}
		id := domain.ID(fmt.Sprintf("%s-op-%02d", w.ID, i))
		r := f.request(w, id, program)
		started := time.Now().UTC()
		ack, e := f.client.StartOperation(f.ctx, r)
		f.check(e)
		ackAt := time.Now().UTC()
		if includeCancel && i == 1 {
			var path string
			var running []byte
			f.wait(15*time.Second, "both cancellation markers", func() bool {
				j, e := slJournalSnapshot(f.ctx, f.c.JournalPath)
				f.check(e)
				path = f.spoolPath(j, r)
				data, e := slRead(path, 512<<10)
				if e != nil {
					return false
				}
				frames, e := slParseFrames(data, policy, true)
				return e == nil && bytes.Contains(frames.streams[0], []byte("SL_PARENT_READY")) && bytes.Contains(frames.streams[1], []byte("SL_CHILD_READY"))
			})
			j, e := slJournalSnapshot(f.ctx, f.c.JournalPath)
			f.check(e)
			row, e := slRow(j, "operation_logs", "operation_id", string(id))
			f.check(e)
			running = f.daemon("inspect", fmt.Sprint(row["container_id"]))
			f.check(slWrite(filepath.Join(f.dir, label+"-cancel-running-docker.json"), running))
			f.check(slDockerOracle(running, fmt.Sprint(row["container_id"]), r, filepath.Dir(filepath.Dir(path)), true))
			top := f.daemon("top", fmt.Sprint(row["container_id"]), "-eo", "pid,ppid,comm")
			f.check(slWrite(filepath.Join(f.dir, label+"-cancel-running-top.txt"), top))
			if len(bytes.Split(bytes.TrimSpace(top), []byte("\n"))) < 4 {
				f.t.Fatal("parent/child/init process observations absent")
			}
			_, e = f.client.CancelOperation(f.ctx, runner.InspectRequest{WorkspaceRequest: f.binding(r.TenantID, r.RunID, r.Epoch), OperationID: id})
			f.check(e)
		}
		op := f.settle(r)
		labelOp := fmt.Sprintf("%s-op-%02d", label, i)
		parsed := f.captureOperation(labelOp, r, op)
		var job sandbox.Job
		f.check(json.Unmarshal(op.Result, &job))
		if includeCancel && i == 1 {
			if op.Status != runner.Cancelled || !job.Interrupted || job.Log.TerminationRequested || !job.Log.Complete || job.Log.Truncated {
				f.t.Fatal("business cancel confused with policy/gap")
			}
		} else {
			f.check(slOverflowDrain(op.Status, job, parsed))
		}
		w.Revision = op.AfterRevision
		total += parsed.Bytes
		f.creates++
		if f.creates > 64 {
			f.t.Fatal("frozen container cap exceeded")
		}
		f.save(labelOp+"-timing.json", map[string]any{"start_requested_at": started, "ack_at": ackAt, "observed_terminal_at": time.Now().UTC(), "ack_status": ack.Status, "operation_id": id, "actual_job_id": op.JobID})
		last, lastRequest = op, r
	}
	before := f.snapshot(label + "-before-rejection.json")
	f.check(slReservation(before, w.TenantID, w.RunID, policy, count))
	if total > int64(policy.RunBytes) {
		f.t.Fatal("actual framed disk sum exceeded run cap")
	}
	reject := f.request(w, domain.ID(string(w.ID)+"-rejected"), slCompleteProgram)
	_, e := f.client.StartOperation(f.ctx, reject)
	if !errors.Is(e, domain.ErrCapacity) {
		f.t.Fatal("new operation not refused at exact bound", e)
	}
	after := f.snapshot(label + "-after-rejection.json")
	if !reflect.DeepEqual(before, after) {
		f.t.Fatal("capacity rejection changed durable authority")
	}
	ids := f.daemon("ps", "--all", "--no-trunc", "--filter", "label=forge.operation_id="+string(reject.OperationID), "--format", "{{.ID}}")
	if len(bytes.TrimSpace(ids)) != 0 {
		f.t.Fatal("rejected operation created a container")
	}
	lastRequest.Grant = f.binding(w.TenantID, w.RunID, w.Epoch).Grant
	retry, e := f.client.StartOperation(f.ctx, lastRequest)
	f.check(e)
	if retry.Receipt != last.Receipt || retry.JobID != last.JobID || retry.Status != last.Status || !slSameRequest(retry.Request, lastRequest) {
		f.t.Fatal("exact retry changed immutable effect")
	}
	retryJ := f.snapshot(label + "-after-duplicate.json")
	if !reflect.DeepEqual(after, retryJ) {
		f.t.Fatal("retry consumed another reservation")
	}
	// Sample actual stopped spools again after the rejected/duplicate requests.
	var sampledTotal int64
	for _, oprow := range retryJ.Tables["operations"] {
		if oprow["run_id"] != string(w.RunID) || oprow["tenant_id"] != string(w.TenantID) {
			continue
		}
		var request runner.OperationRequest
		f.check(json.Unmarshal([]byte(fmt.Sprint(oprow["request_json"])), &request))
		disk, e := slRead(f.spoolPath(retryJ, request), int64(policy.OperationBytes))
		f.check(e)
		sampledTotal += int64(len(disk))
		events := f.daemon("events", "--since", f.started.Format(time.RFC3339Nano), "--until", time.Now().Format(time.RFC3339Nano), "--filter", "container="+fmt.Sprint(oprow["job_id"]), "--format", "{{json .}}")
		f.check(slWrite(filepath.Join(f.dir, string(request.OperationID)+"-events.jsonl"), events))
		create, start := 0, 0
		for _, line := range bytes.Split(bytes.TrimSpace(events), []byte("\n")) {
			if len(line) == 0 {
				continue
			}
			var event struct {
				Action string
				Actor  struct {
					ID         string
					Attributes map[string]string
				}
			}
			f.check(json.Unmarshal(line, &event))
			lr, e := slRow(retryJ, "operation_logs", "operation_id", oprow["id"])
			f.check(e)
			if event.Actor.ID != lr["container_id"] || event.Actor.Attributes["name"] != oprow["job_id"] {
				f.t.Fatal("foreign daemon operation event")
			}
			if event.Action == "create" {
				create++
			}
			if event.Action == "start" {
				start++
			}
		}
		if create != 1 || start != 1 {
			f.t.Fatal("actual daemon did not confirm one create/start", create, start)
		}
	}
	if sampledTotal != total {
		f.t.Fatal("settled spool bytes changed during idle/retry window")
	}
	f.release(w, label)
	for _, lr := range f.snapshot(label + "-daemon-cleaned.json").Tables["operation_logs"] {
		oprow, e := slRow(retryJ, "operations", "id", lr["operation_id"])
		if e != nil || oprow["run_id"] != string(w.RunID) {
			continue
		}
		if lr["cleanup_state"] != "removed" {
			f.t.Fatal("strict container cleanup not durable")
		}
		ids := f.daemon("ps", "--all", "--no-trunc", "--filter", "label=forge.operation_id="+fmt.Sprint(lr["operation_id"]), "--format", "{{.ID}}")
		if len(bytes.TrimSpace(ids)) != 0 {
			f.t.Fatal("own stopped container remains after release")
		}
	}
	released := f.snapshot(label + "-lifetime-budget.json")
	f.check(slReservation(released, w.TenantID, w.RunID, policy, count))
	f.startRunner(phase, "")
	reopened := f.snapshot(label + "-reopened.json")
	f.check(slReservation(reopened, w.TenantID, w.RunID, policy, count))
	f.check(slIdle(reopened))
	f.cases[label] = map[string]any{"passed": true, "scope": "direct production typed RPC and actual Engine/Docker/spool; Driver/PG publication supplied separately by L1/L4/L5", "run_id": w.RunID, "policy": policy, "actual_operations": count, "physical_spool_sum": total, "reserved_bytes": count * policy.OperationBytes, "new_id_capacity_rejected": true, "duplicate_unchanged": true, "same_journal_reopen_preserved": true, "release_refunded": false}
}

// Physical pressure is confined to one trusted sibling on the exact assigned
// fixed filesystem. An unresolved writer retains this file and its slot.
func (f *slFixture) pressure(path string) func() {
	dir := filepath.Dir(path)
	fd, e := unix.Open(dir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	f.check(e)
	defer unix.Close(fd)
	name := "strict-logs-enospc-pressure"
	n, e := unix.Openat(fd, name, unix.O_CREAT|unix.O_EXCL|unix.O_WRONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0600)
	f.check(e)
	file := os.NewFile(uintptr(n), filepath.Join(dir, name))
	var st unix.Stat_t
	f.check(unix.Fstat(n, &st))
	var logsDevice, checkoutDevice unix.Stat_t
	f.check(unix.Stat(path, &logsDevice))
	f.check(unix.Stat(filepath.Join(dir, "checkout"), &checkoutDevice))
	if st.Dev != logsDevice.Dev || st.Dev != checkoutDevice.Dev {
		file.Close()
		f.t.Fatal("pressure FD/spool/checkout devices differ")
	}
	var before unix.Statfs_t
	f.check(unix.Fstatfs(n, &before))
	pressure, fillErr := slFillPressure(file, file.Sync, 256<<20)
	f.check(unix.Fstat(n, &st))
	var after unix.Statfs_t
	f.check(unix.Fstatfs(n, &after))
	f.check(file.Close())
	f.save("L4-pressure.json", map[string]any{"path": filepath.Join(dir, name), "device": st.Dev, "inode": st.Ino, "effective_uid": os.Geteuid(), "owner_uid": st.Uid, "written_bytes": pressure.Written, "allocated_bytes": st.Blocks * 512, "stages": pressure.Stages, "fill_error": fmt.Sprint(fillErr), "before": before, "after": after, "observed_at": time.Now().UTC()})
	f.check(fillErr)
	if after.Bavail != 0 {
		f.t.Fatal("pressure left available filesystem blocks; retain evidence and pressure")
	}
	if st.Uid != uint32(os.Geteuid()) || st.Blocks*512 <= 0 || st.Blocks*512 > 256<<20 {
		f.t.Fatal("pressure allocation/identity differs")
	}
	return func() {
		var current unix.Stat_t
		f.check(unix.Lstat(filepath.Join(dir, name), &current))
		if current.Ino != st.Ino || current.Dev != st.Dev || current.Nlink != 1 || current.Uid != st.Uid {
			f.t.Fatal("pressure identity changed; no removal")
		}
		f.check(os.Remove(filepath.Join(dir, name)))
		f.save("L4-pressure-removed.json", map[string]any{"at": time.Now().UTC(), "device": st.Dev, "inode": st.Ino, "only_after_confirmed_job_stop": true})
	}
}

// The watchdog outlives fixture contexts and Fatal: it only resumes the exact
// still-identical child. No signal is sent to a PID copied from an old report.
type slWorkerPause struct {
	mu       sync.Mutex
	gate     slPauseGate
	once     sync.Once
	done     chan struct{}
	timer    *time.Timer
	deadline time.Time
	record   map[string]any
	resume   func(bool)
	f        *slFixture
}

func (p *slWorkerPause) finish() {
	p.timer.Stop()
	p.resume(false)
	<-p.done
	clock := func(key string) time.Time { v, _ := p.record[key].(time.Time); return v }
	if e := slPauseWindow(clock("db_before_pause"), clock("lease_until"), clock("stop_sent_at"), clock("stopped_confirmed_at"), clock("continued_at"), p.record["watchdog"] == true); e != nil {
		p.record["window_error"] = e.Error()
		p.f.t.Error(e)
	}
	if e := slSave(filepath.Join(p.f.dir, "L4-worker-pause.json"), p.record); e != nil {
		p.f.t.Error(e)
	}
	if p.record["watchdog"] == true || p.record["resume_error"] != nil {
		p.f.t.Error("worker pause exceeded its bound or resume identity failed")
	}
}
func (p *slWorkerPause) check() {
	select {
	case <-p.done:
		p.f.t.Fatal("pressure observation exceeded the paused-worker window")
	default:
	}
	if !time.Now().Before(p.deadline.Add(-time.Second)) {
		p.f.t.Fatal("insufficient paused-worker time remains")
	}
}
func (f *slFixture) pauseWorker(h *harness, worker *process) *slWorkerPause {
	var launched slProcessIdentity
	f.check(slReadJSON(worker.log.Name()+".identity.json", &launched))
	current, e := sigtermProcessProof(worker, f.a.WorkerSHA)
	f.check(e)
	var observed slProcessIdentity
	f.check(json.Unmarshal(slJSON(current), &observed))
	f.check(slBindProcess(launched, observed))
	var dbNow, until time.Time
	var owner string
	var epoch uint64
	f.check(h.db.Pool.QueryRow(f.ctx, `SELECT clock_timestamp(),lease_until,lease_owner,lease_epoch FROM runs WHERE tenant_id=$1 AND id=$2`, h.s.Tenant, h.s.RunID).Scan(&dbNow, &until, &owner, &epoch))
	if owner != "sl-L4-0" || epoch == 0 || until.Sub(dbNow) < 24*time.Second {
		f.t.Fatal("worker pause needs actual same-owner DB lease with 24 seconds remaining")
	}
	p := &slWorkerPause{done: make(chan struct{}), f: f, record: map[string]any{"identity": observed, "db_before_pause": dbNow, "lease_until": until, "lease_owner": owner, "lease_epoch": epoch, "watchdog_seconds": 18}}
	p.resume = func(watchdog bool) {
		p.once.Do(func() {
			defer close(p.done)
			p.mu.Lock()
			defer p.mu.Unlock()
			p.gate.resume()
			p.record["watchdog"] = watchdog
			proof, e := sigtermProcessProof(worker, f.a.WorkerSHA)
			if e == nil {
				var latest slProcessIdentity
				e = json.Unmarshal(slJSON(proof), &latest)
				if e == nil {
					e = slBindProcess(launched, latest)
				}
			}
			if e == nil {
				e = worker.cmd.Process.Signal(syscall.SIGCONT)
			}
			if e != nil {
				p.record["resume_error"] = e.Error()
			}
			p.record["continued_at"] = time.Now().UTC()
		})
	}
	// Timer is armed before SIGSTOP so even a setup Fatal or cancellation cannot
	// strand this child. Normal bounds are measured from the recorded signal.
	stoppedAt := time.Now().UTC()
	p.record["stop_sent_at"] = stoppedAt
	p.deadline = stoppedAt.Add(18 * time.Second)
	p.timer = time.AfterFunc(18*time.Second, func() { p.resume(true) })
	setupComplete := false
	defer func() {
		if !setupComplete {
			p.finish()
		}
	}()
	// Serialize STOP with the watchdog. If CONT already won, no later STOP
	// may strand the process after the one-shot recovery has been consumed.
	p.mu.Lock()
	stopErr := p.gate.stop(time.Now(), p.deadline)
	if stopErr == nil {
		stopErr = worker.cmd.Process.Signal(syscall.SIGSTOP)
	}
	p.mu.Unlock()
	f.check(stopErr)
	f.wait(time.Second, "actual worker stopped", func() bool {
		raw, e := os.ReadFile(fmt.Sprintf("/proc/%d/status", worker.cmd.Process.Pid))
		f.check(e)
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.HasPrefix(line, "State:") {
				return strings.Contains(line, "T (stopped)")
			}
		}
		return false
	})
	p.mu.Lock()
	p.record["stopped_confirmed_at"] = time.Now().UTC()
	pausedRaw := slJSON(p.record)
	p.mu.Unlock()
	f.check(slWrite(filepath.Join(f.dir, "L4-worker-paused.json"), pausedRaw))
	setupComplete = true
	return p
}

func (f *slFixture) l4() {
	f.startRunner("default", "")
	h, file := f.submit("L4", slENOSPCProgram)
	p := f.worker(h, file, "L4")
	f.approve(h)
	var req runner.OperationRequest
	var path, cid string
	f.wait(20*time.Second, "ENOSPC valid two-stream prefix", func() bool {
		j, e := slJournalSnapshot(f.ctx, f.c.JournalPath)
		f.check(e)
		row, e := slRow(j, "operations", "id", string(h.s.TargetOp))
		if e != nil {
			return false
		}
		f.check(json.Unmarshal([]byte(fmt.Sprint(row["request_json"])), &req))
		path = f.spoolPath(j, req)
		raw, e := slRead(path, 512<<10)
		if e != nil {
			return false
		}
		prefix, e := slParseFrames(raw, f.c.Logs, true)
		if e != nil || prefix.StreamBytes[0] == 0 || prefix.StreamBytes[1] == 0 {
			return false
		}
		lr, e := slRow(j, "operation_logs", "operation_id", string(h.s.TargetOp))
		f.check(e)
		cid = fmt.Sprint(lr["container_id"])
		f.check(slWrite(filepath.Join(f.dir, "L4-before-pressure.spool"), raw))
		return len(cid) == 64
	})
	pause := f.pauseWorker(h, p)
	defer pause.finish()
	pause.mu.Lock()
	pausedEpoch := pause.record["lease_epoch"]
	pause.mu.Unlock()
	if pausedEpoch != req.Epoch {
		f.t.Fatal("paused worker SQL epoch differs from original operation")
	}
	before := f.snapshot("L4-before-pressure-journal.json")
	pause.check()
	lease, e := slRow(before, "volume_leases", "workspace_id", string(req.WorkspaceID))
	f.check(e)
	for _, v := range f.c.VolumeSlots {
		if v.ID == lease["slot_id"] {
			f.check(sandbox.VerifyVolume(f.ctx, v))
			var spool, slot unix.Stat_t
			f.check(unix.Stat(path, &spool))
			f.check(unix.Stat(v.MountPath, &slot))
			if spool.Dev != slot.Dev {
				f.t.Fatal("pressure target no longer on selected fixed volume")
			}
		}
	}
	releasePressure := f.pressure(filepath.Dir(path))
	// Read SQLite, not Inspect, while full: the observation must precede any
	// reconciliation that could replace the original spool I/O error with a gap.
	var failed slJournal
	f.wait(12*time.Second, "actual spool write failure", func() bool {
		pause.check()
		j, e := slJournalSnapshot(f.ctx, f.c.JournalPath)
		f.check(e)
		row, e := slRow(j, "operations", "id", string(h.s.TargetOp))
		f.check(e)
		if row["status"] == "succeeded" {
			f.t.Fatal("ENOSPC unexpectedly succeeded")
		}
		if strings.Contains(fmt.Sprint(row["error"]), "no space left") || strings.Contains(fmt.Sprint(row["result_json"]), "spool_io_error") {
			failed = j
			return true
		}
		return false
	})
	f.save("L4-spool-failure-journal.json", failed)
	h.capture("physical-full")
	inspect := f.daemon("inspect", cid)
	f.check(slWrite(filepath.Join(f.dir, "L4-stopped-before-pressure-removal.json"), inspect))
	f.check(slDockerOracle(inspect, cid, req, filepath.Dir(filepath.Dir(path)), false))
	// Finish can itself fail to write metadata while full. An ENOSPC string
	// alone therefore does not identify the failing sink. The same command is
	// finite but needs at least 23 seconds to exit naturally; a non-OOM early
	// stop, before its immutable deadline and while the only worker is paused,
	// binds this observation to the production sink-failure kill branch.
	f.check(slENOSPCStop(inspect, req, time.Now().UTC()))
	fullSpool, e := slRead(path, int64(f.c.Logs.OperationBytes))
	f.check(e)
	initialSpool, e := slRead(filepath.Join(f.dir, "L4-before-pressure.spool"), int64(f.c.Logs.OperationBytes))
	f.check(e)
	fullFrames, e := slParseFrames(fullSpool, f.c.Logs, true)
	f.check(e)
	if !bytes.HasPrefix(fullSpool, initialSpool) || fullFrames.StreamBytes[0] == 0 || fullFrames.StreamBytes[1] == 0 || len(fullSpool) >= f.c.Logs.OperationBytes-f.c.Logs.EntryBytes {
		f.t.Fatal("physical failure lost the original prefix or could be an operation-byte-limit stop")
	}
	f.check(slWrite(filepath.Join(f.dir, "L4-physical-full.spool"), fullSpool))
	f.save("L4-physical-full-spool-oracle.json", fullFrames)
	for _, suffix := range []string{".meta", ".meta.tmp"} {
		if raw, e := slRead(path+suffix, 1<<20); e == nil {
			f.check(slWrite(filepath.Join(f.dir, "L4-physical-full"+suffix), raw))
		} else if !os.IsNotExist(e) {
			f.check(e)
		}
	}
	if f.ctx.Err() != nil || h.getRun().State.Status != domain.StatusRunning {
		f.t.Fatal("external cancellation contaminated the physical spool failure")
	}
	f.save("L4-full-spool-stop-proof.json", map[string]any{"container_id": cid, "operation_id": req.OperationID, "natural_runtime_min_seconds": 23, "observed_before_request_deadline": true, "worker_paused": true, "same_run_still_running": true, "source_of_error": "SQLite ENOSPC plus early non-OOM Docker stop while no worker/fixture cancellation is possible; metadata alone is not the oracle"})
	var count int
	for _, r := range before.Tables["log_runs"] {
		if r["tenant_id"] == string(req.TenantID) && r["run_id"] == string(req.RunID) {
			count = int(slNum(r["operations"]))
		}
	}
	f.check(slReservation(failed, req.TenantID, req.RunID, f.c.Logs, count))
	lease, e = slRow(failed, "volume_leases", "workspace_id", string(req.WorkspaceID))
	f.check(e)
	if slNum(lease["released"]) != 0 {
		f.t.Fatal("full/unknown workspace prematurely released")
	}
	pause.check()
	releasePressure()
	// Reconcile before allowing the production worker to consume the receipt;
	// capability expiry comes from the unchanged real DB lease, not a fake lease.
	run := h.getRun()
	if run.State.Lease.Epoch != req.Epoch {
		f.t.Fatal("pressure fixture epoch changed while worker paused")
	}
	leaseProof, dbClock, e := h.db.LeaseProof(f.ctx, h.s.Tenant, h.s.RunID, run.State.Lease.Owner, req.Epoch)
	f.check(e)
	grant, e := f.signer.Sign(runner.Claims{TenantID: req.TenantID, RunID: req.RunID, WorkspaceID: req.WorkspaceID, Epoch: req.Epoch, IssuedAt: dbClock, ExpiresAt: leaseProof.Until.Add(-f.signer.Skew), Permissions: []string{"inspect"}}, leaseProof.Until)
	f.check(e)
	reconcileCtx, endReconcile := context.WithDeadline(f.ctx, pause.deadline.Add(-time.Second))
	op, e := f.client.InspectOperation(reconcileCtx, runner.InspectRequest{WorkspaceRequest: runner.WorkspaceRequest{TenantID: req.TenantID, RunID: req.RunID, WorkspaceID: req.WorkspaceID, Epoch: req.Epoch, Grant: grant}, OperationID: req.OperationID})
	endReconcile()
	f.check(e)
	if !op.Status.Terminal() {
		f.t.Fatal("ENOSPC same-epoch recovery unresolved")
	}
	pause.check()
	pause.timer.Stop()
	pause.resume(false)
	<-pause.done
	if pause.record["watchdog"] == true || pause.record["resume_error"] != nil {
		f.t.Fatal("pressure fixture worker continuation failed")
	}

	parsed := f.captureOperation("L4-recovered", req, op)
	var job sandbox.Job
	f.check(json.Unmarshal(op.Result, &job))
	if op.Status == runner.Succeeded || job.Log == nil || sandbox.VerificationLogValid(job) || parsed.Bytes == 0 {
		f.t.Fatal("ENOSPC recovered as trustworthy success or lost prefix")
	}
	if !job.Log.Complete && (job.Log.DroppedKnown || job.Log.DroppedBytes != 0) {
		f.t.Fatal("incomplete ENOSPC invents exact drop count")
	}
	h.wait(func() bool {
		var n int
		f.check(h.db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM artifacts WHERE tenant_id=$1 AND run_id=$2 AND object_key=$3 AND state='ready'`, h.s.Tenant, h.s.RunID, job.Log.Artifact.ObjectKey).Scan(&n))
		return n == 1
	}, 15*time.Second, "Driver publishes real recovered prefix")
	f.download(h.db, h.s.Tenant, h.s.RunID, job.Log.Artifact, "L4")
	f.finishDriver(h, p, "L4")
	for _, v := range f.c.VolumeSlots {
		f.check(sandbox.VerifyVolume(f.ctx, v))
	}
	health := f.prepare("L4-health")
	r := f.request(health, domain.ID(string(health.ID)+"-health"), slCompleteProgram)
	_, e = f.client.StartOperation(f.ctx, r)
	f.check(e)
	healthy := f.settle(r)
	if healthy.Status != runner.Succeeded {
		f.t.Fatal("fixed slot not reusable after settled ENOSPC")
	}
	f.captureOperation("L4-health", r, healthy)
	health.Revision = healthy.AfterRevision
	f.release(health, "L4-health")
	f.cases["L4"] = map[string]any{"passed": true, "physical_errno": "ENOSPC", "actual_spool_failure_observed": true, "pressure_removed_after_actual_stop": true, "retained_log_bytes": parsed.Bytes, "log_summary": job.Log, "same_id_inspection_only": true, "all_four_volume_identities_reverified": true, "fresh_real_health_operation": true}
}

func (f *slFixture) l5() {
	dir := filepath.Join(f.a.EvidenceDir, "worker-runner-sigterm")
	privateDir, err := lifecyclePrivate(f.a.ScopeRoot, f.a.EvidenceDir)
	f.check(err)
	var historical slAcceptance
	f.check(slReadJSON(filepath.Join(dir, "acceptance-input.json"), &historical))
	historicalInputs, err := slHistoricalAuthority(f.a, historical, f.c, filepath.Base(f.dir) != "logs-01")
	f.check(err)
	for path, hash := range historicalInputs {
		f.inputs[path] = hash
	}
	var historicalPreflight map[string]json.RawMessage
	f.check(slReadJSON(filepath.Join(dir, "preflight.json"), &historicalPreflight))
	var historicalBinary struct {
		SHA256 string `json:"sha256"`
	}
	f.check(json.Unmarshal(historicalPreflight[historical.TestBinary], &historicalBinary))
	if historicalBinary.SHA256 != historical.TestSHA {
		f.t.Fatal("E50 executed binary proof differs")
	}
	var accepted map[string]json.RawMessage
	f.check(slReadJSON(filepath.Join(dir, "acceptance.json"), &accepted))
	var passed bool
	f.check(json.Unmarshal(accepted["passed"], &passed))
	var uuid string
	f.check(json.Unmarshal(accepted["journal_uuid"], &uuid))
	if !passed || uuid != f.journalID {
		f.t.Fatal("prior E50 identity/result not valid")
	}
	var op runner.Operation
	f.check(slReadJSON(filepath.Join(dir, "original-operation-settled.json"), &op))
	if !op.Status.Terminal() || op.Status == runner.Succeeded {
		f.t.Fatal("E50 original gap lacks truthful terminal")
	}
	raw, e := slRead(filepath.Join(dir, "runner-shutdown.spool"), 512<<10)
	f.check(e)
	after, e := slRead(filepath.Join(dir, "after-adoption.spool"), 512<<10)
	f.check(e)
	if !bytes.Equal(raw, after) {
		f.t.Fatal("E50 spool appended/replaced after shutdown")
	}
	meta, e := slRead(filepath.Join(dir, "runner-shutdown.meta.json"), 128<<10)
	f.check(e)
	parsed, e := slValidateLog(op, op.Request, raw, meta)
	f.check(e)
	var job sandbox.Job
	f.check(json.Unmarshal(op.Result, &job))
	if job.Log.Complete || job.Log.DroppedKnown || job.Log.Reason != "runner_shutdown" || sandbox.VerificationLogValid(job) || parsed.StreamBytes[0] == 0 || parsed.StreamBytes[1] == 0 {
		f.t.Fatal("E50 real two-stream capture gap not established")
	}
	var cid string
	f.check(json.Unmarshal(accepted["original_container_id"], &cid))
	if len(cid) != 64 {
		f.t.Fatal("E50 original full container identity absent")
	}
	// Independently recount raw daemon events; report booleans are not the oracle.
	events, e := slRead(filepath.Join(dir, "original-container-events.jsonl"), 1<<20)
	f.check(e)
	counts := map[string]int{}
	for _, line := range bytes.Split(bytes.TrimSpace(events), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var event struct {
			Action string
			Actor  struct {
				ID         string
				Attributes map[string]string
			}
		}
		f.check(json.Unmarshal(line, &event))
		if event.Actor.ID != cid || event.Actor.Attributes["name"] != op.JobID {
			f.t.Fatal("E50 foreign container event")
		}
		counts[event.Action]++
	}
	if counts["create"] != 1 || counts["start"] != 1 || counts["die"] != 1 {
		f.t.Fatal("E50 unique actual lifecycle not proven", counts)
	}
	signals := map[string]slSignalRecord{}
	for _, spec := range []struct {
		label, launchName, path, hash string
		args                          []string
	}{
		{"worker-original", "lifecycle-original", historical.WorkerBinary, historical.WorkerSHA, []string{historical.WorkerBinary, "-config", filepath.Join(privateDir, "worker.json"), "-id", "lifecycle-original"}},
		{"runner-original", "runner-original", historical.RunnerBinary, historical.RunnerSHA, []string{historical.RunnerBinary, "-config", historical.RunnerConfig}},
		{"worker-successor", "lifecycle-successor", historical.WorkerBinary, historical.WorkerSHA, []string{historical.WorkerBinary, "-config", filepath.Join(privateDir, "worker.json"), "-id", "lifecycle-successor"}},
		{"runner-successor", "runner-successor", historical.RunnerBinary, historical.RunnerSHA, []string{historical.RunnerBinary, "-config", historical.RunnerConfig}},
	} {
		launchRaw, e := slRead(filepath.Join(dir, spec.launchName+".log.identity.json"), 64<<10)
		f.check(e)
		signalRaw, e := slRead(filepath.Join(dir, spec.label+"-signal.json"), 64<<10)
		f.check(e)
		signal, e := slAuditSignal(launchRaw, signalRaw, spec.path, spec.hash, spec.args)
		f.check(e)
		signals[spec.label] = signal
	}
	for _, pair := range []struct{ label, clock, summary string }{{"worker-original", "after-worker-sigterm", "worker_signal"}, {"runner-original", "after-runner-sigterm", "runner_signal"}} {
		clockRaw, e := slRead(filepath.Join(dir, pair.clock+"-clock.json"), 1024)
		f.check(e)
		dockerRaw, e := slRead(filepath.Join(dir, pair.clock+"-docker.json"), 1<<20)
		f.check(e)
		f.check(slAuditAfterSignal(signals[pair.label], clockRaw, dockerRaw, cid, op.JobID))
		f.check(slAuditSignalSummary(signals[pair.label], accepted[pair.summary]))
	}
	var cleanupCapture map[string]json.RawMessage
	f.check(slReadJSON(filepath.Join(dir, "cleanup-released-postgres.json"), &cleanupCapture))
	var cleanupRows []struct {
		Tenant     domain.ID `json:"tenant_id"`
		Run        domain.ID `json:"run_id"`
		Phase      string    `json:"phase"`
		ReleasedAt time.Time `json:"released_at"`
	}
	f.check(json.Unmarshal(cleanupCapture["workspace_cleanup"], &cleanupRows))
	if len(cleanupRows) != 1 || cleanupRows[0].Tenant != op.Request.TenantID || cleanupRows[0].Run != op.Request.RunID || cleanupRows[0].Phase != "released" || cleanupRows[0].ReleasedAt.IsZero() || cleanupRows[0].ReleasedAt.Before(signals["worker-successor"].ExitedAt) || signals["runner-successor"].DBBefore.Before(cleanupRows[0].ReleasedAt) {
		f.t.Fatal("successor exits not bound around actual released cleanup")
	}
	f.save("L5-signal-audit.json", signals)
	var oldUntil, claimed time.Time
	f.check(json.Unmarshal(accepted["old_lease_until"], &oldUntil))
	f.check(json.Unmarshal(accepted["successor_claim_db_time"], &claimed))
	if claimed.Before(oldUntil) {
		f.t.Fatal("E50 successor bypassed natural lease expiry")
	}
	j := f.snapshot("L5-live-authority.json")
	f.check(slIdle(j))
	f.check(slVerifyPins(j, job.Log.Artifact, op.Receipt))
	f.check(slReservation(j, op.Request.TenantID, op.Request.RunID, f.c.Logs, 2))
	stored, e := slRow(j, "operations", "id", string(op.Request.OperationID))
	f.check(e)
	f.check(slStoredOperation(stored, op))
	var private struct {
		WorkerDSN string `json:"worker_dsn"`
		Schema    string `json:"schema"`
	}
	f.check(slReadJSON(filepath.Join(privateDir, "database.json"), &private))
	u, e := url.Parse(private.WorkerDSN)
	f.check(e)
	if u.Hostname() != "127.0.0.1" || u.Port() != "32773" || u.Path != "/forge" || !strings.HasPrefix(private.Schema, "appfault_lifecycle_") || u.Query().Get("search_path") != private.Schema {
		f.t.Fatal("E50 private authority mismatch")
	}
	db, e := persistence.Open(f.ctx, private.WorkerDSN)
	f.check(e)
	defer db.Close()
	f.check(db.CheckWorkerRole(f.ctx))
	r, e := db.GetRun(f.ctx, op.Request.TenantID, op.Request.RunID)
	f.check(e)
	if r.State.Status != domain.StatusCancelled {
		f.t.Fatal("E50 current run not terminal cancelled")
	}
	var confirmations, allocations int
	f.check(db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM run_snapshots WHERE tenant_id=$1 AND run_id=$2 AND input_event->>'kind' IN ('effect_completed','reconciled') AND input_event->'receipt'->>'effect_id'=$3`, r.TenantID, r.ID, op.Request.OperationID).Scan(&confirmations))
	f.check(db.Pool.QueryRow(f.ctx, `SELECT count(*) FROM runner_allocations WHERE tenant_id=$1 AND run_id=$2 AND state<>'released'`, r.TenantID, r.ID).Scan(&allocations))
	if confirmations != 1 || allocations != 0 {
		f.t.Fatal("E50 current effect/capacity authority differs")
	}
	rows, e := db.Pool.Query(f.ctx, `SELECT to_jsonb(t) FROM run_snapshots t WHERE tenant_id=$1 AND run_id=$2 AND input_event->>'kind'='claimed' ORDER BY version`, op.Request.TenantID, op.Request.RunID)
	f.check(e)
	var liveClaimRows []json.RawMessage
	for rows.Next() {
		var raw json.RawMessage
		f.check(rows.Scan(&raw))
		liveClaimRows = append(liveClaimRows, raw)
	}
	f.check(rows.Err())
	rows.Close()
	afterBoth, e := slRead(filepath.Join(dir, "after-both-signals-postgres.json"), 16<<20)
	f.check(e)
	afterAdoption, e := slRead(filepath.Join(dir, "after-adoption-postgres.json"), 16<<20)
	f.check(e)
	claimAudit, e := slAuditClaim(afterBoth, afterAdoption, liveClaimRows, op.Request, oldUntil, claimed)
	f.check(e)
	f.save("L5-live-claimed-snapshots.json", liveClaimRows)
	f.save("L5-claim-audit.json", claimAudit)
	// Worker role remains unchanged/restricted. Only the explicitly authorized
	// fixture admin connection can issue test tokens in E50's proven schema.
	apiURL, e := url.Parse(os.Getenv("FORGE_TEST_DATABASE_URL"))
	f.check(e)
	if apiURL.Hostname() != "127.0.0.1" || apiURL.Port() != "32773" || apiURL.Path != "/forge" || apiURL.Query().Get("search_path") != "" {
		f.t.Fatal("dedicated fixture admin connection required")
	}
	apiQ := apiURL.Query()
	apiQ.Set("search_path", private.Schema)
	apiURL.RawQuery = apiQ.Encode()
	apiDB, e := persistence.Open(f.ctx, apiURL.String())
	f.check(e)
	defer apiDB.Close()
	f.download(apiDB, r.TenantID, r.ID, job.Log.Artifact, "L5")
	hashes := map[string]string{}
	f.check(filepath.WalkDir(dir, func(path string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			return nil
		}
		raw, e := slRead(path, 64<<20)
		if e != nil {
			return e
		}
		rel, e := filepath.Rel(dir, path)
		if e != nil {
			return e
		}
		hashes[rel] = slSHA(raw)
		return nil
	}))
	f.save("L5-referenced-evidence.json", map[string]any{"source_directory": dir, "sha256": hashes, "source_test_binary_sha256": historicalBinary.SHA256, "historical_acceptance": historical, "current_logs_execution": f.a, "historical_input_sha256": historicalInputs, "journal_uuid": uuid, "scope": "recomputed prior actual E50 evidence plus live PG/SQLite and new authenticated HTTP download; no repeated lifecycle workload"})
	f.cases["L5"] = map[string]any{"passed": true, "original_operation": op.Request.OperationID, "original_container_id": cid, "raw_daemon_counts": counts, "unchanged_spool_bytes": len(raw), "log_gap": job.Log, "current_effect_confirmations": confirmations, "current_allocations": allocations, "same_journal_uuid": uuid}
}
func slMapping(raw []byte) bool {
	zero, task := false, false
	for _, line := range strings.Split(string(raw), "\n") {
		var a, b, n uint64
		if _, e := fmt.Sscan(line, &a, &b, &n); e != nil {
			continue
		}
		if a == 0 && b == 1000 && n == 1 {
			zero = true
		}
		if a == 1 && b >= 100000 && n >= 65536 {
			task = true
		}
	}
	return zero && task
}
func (f *slFixture) finalize() {
	defer func() {
		proof := map[string]any{"passed": f.completed && !f.t.Failed(), "started_at": f.started, "finished_at": time.Now().UTC(), "cases": f.cases, "scope": "real Engine/Docker/fixed-volume disk spool and private PG; deterministic provider, no paid calls; sequential cross-system observations", "failure_policy": "unknown operation/pressure/slot/schema preserved; no automatic cancellation or erasure", "input_sha256_before": f.inputs}
		after := map[string]string{}
		for path, want := range f.inputs {
			got, e := sigtermDigest(path)
			if e != nil || got != want {
				proof["passed"] = false
				proof["changed_input"] = path
				f.t.Errorf("frozen input changed: %s", path)
			}
			after[path] = got
		}
		proof["input_sha256_after"] = after
		if e := slSave(filepath.Join(f.dir, "acceptance.json"), proof); e != nil {
			f.t.Error(e)
		}
		manifest := map[string]string{}
		e := filepath.WalkDir(f.dir, func(path string, d os.DirEntry, e error) error {
			if e != nil {
				return e
			}
			if d.IsDir() {
				return nil
			}
			raw, e := slRead(path, 64<<20)
			if e != nil {
				return e
			}
			rel, e := filepath.Rel(f.dir, path)
			if e != nil {
				return e
			}
			manifest[rel] = slSHA(raw)
			return nil
		})
		if e != nil {
			f.t.Error(e)
		} else if e = slSave(filepath.Join(f.dir, "manifest.json"), manifest); e != nil {
			f.t.Error(e)
		}
		if f.objects != nil {
			f.objects.Close()
		}
		if f.h != nil && f.h.db != nil {
			f.h.db.Close()
		}

	}()
	// Failure cleanup stops only our processes; unknown operations/pressure/files
	// and schemas remain for operator recovery. No implicit business cancellation.
	if f.h != nil {
		for _, p := range f.h.workers {
			f.h.stop(p)
		}
	}
	if f.ownRunner != nil {
		f.stopRunner()
	}
}

func TestStrictLogsCombinedAcceptance(t *testing.T) {
	if os.Getenv("FORGE_RUN_STRICT_LOGS_COMBINED") != "1" {
		t.Skip("operator opt-in: completed E50 dedicated fixed pool; actual runner/worker binaries, private PG, finite physical ENOSPC")
	}
	slRunLogsAcceptance(t, false)
}

func TestStrictLogsTargetedL4Acceptance(t *testing.T) {
	if os.Getenv("FORGE_RUN_STRICT_LOGS_L4") != "1" {
		t.Skip("operator opt-in: archived logs03 failure and completed explicit L4 recovery; only new L4 and health operation")
	}
	slRunLogsAcceptance(t, true)
}

func slRunLogsAcceptance(t *testing.T, targeted bool) {
	path := os.Getenv("FORGE_STRICT_LOGS_ACCEPTANCE")
	var a slAcceptance
	var c slRunnerConfig
	if e := slReadJSON(path, &a); e != nil {
		t.Fatal(e)
	}
	if e := slReadJSON(a.RunnerConfig, &c); e != nil {
		t.Fatal(e)
	}
	if e := slValidateContract(a, c); e != nil {
		t.Fatal(e)
	}
	execution := os.Getenv("FORGE_STRICT_LOGS_EXECUTION")
	dir, private, err := slExecutionPaths(a, path, execution)
	if targeted {
		if path != filepath.Join(a.ScopeRoot, "acceptance-logs-l4-01.json") || execution != "" {
			t.Fatal("targeted L4 requires its own explicit acceptance and no combined execution selector")
		}
		dir, private = filepath.Join(a.ScopeRoot, "evidence/logs-l4-01"), filepath.Join(a.ScopeRoot, "runtime/strict-logs-l4-private-01")
	} else if err != nil {
		t.Fatal(err)
	}
	unlock, err := lifecycleControlLock(a.ScopeRoot)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	uid, e := os.ReadFile("/proc/self/uid_map")
	if e != nil {
		t.Fatal(e)
	}
	gid, e := os.ReadFile("/proc/self/gid_map")
	if e != nil {
		t.Fatal(e)
	}
	if os.Geteuid() != 0 || !slMapping(uid) || !slMapping(gid) {
		t.Fatal("same full mapped lifecycle launcher required; host root/ordinary owner not sufficient")
	}
	self, e := os.Executable()
	if e != nil || self != a.TestBinary {
		t.Fatal("actual declared test binary required")
	}
	ctx, end := context.WithTimeout(context.Background(), 10*time.Minute)
	defer end()
	f := &slFixture{t: t, ctx: ctx, a: a, c: c, dir: dir, private: private, configs: map[string]string{"default": a.RunnerConfig, "bytes": filepath.Join(a.ScopeRoot, "runtime", "runner-byteguard.json"), "count": filepath.Join(a.ScopeRoot, "runtime", "runner-countguard.json")}, inputs: map[string]string{}, cases: map[string]any{}, started: time.Now().UTC()}
	for _, x := range []struct{ path, hash, pkg string }{{a.RunnerBinary, a.RunnerSHA, "github.com/JDinSeattle/forge-runtime/cmd/forge-runner"}, {a.WorkerBinary, a.WorkerSHA, "github.com/JDinSeattle/forge-runtime/cmd/forge-worker"}, {a.TestBinary, a.TestSHA, ""}} {
		raw, e := slRead(x.path, 128<<20)
		f.check(e)
		if slSHA(raw) != x.hash {
			t.Fatal("executable hash differs")
		}
		bi, e := buildinfo.ReadFile(x.path)
		f.check(e)
		if x.pkg != "" && bi.Path != x.pkg {
			t.Fatal("production binary package differs")
		}
		f.inputs[x.path] = x.hash
	}
	for _, p := range []string{path, a.RunnerConfig, f.configs["bytes"], f.configs["count"], filepath.Join(a.ScopeRoot, "runtime", "source", "app.py")} {
		raw, e := slRead(p, 1<<20)
		f.check(e)
		f.inputs[p] = slSHA(raw)
	}
	source, e := slRead(filepath.Join(c.Sources["lifecycle"], "app.py"), 1024)
	f.check(e)
	if string(source) != "def clamp(v, lo, hi):\n    return v\n" {
		t.Fatal("trusted fixture source changed")
	}
	volumeObservations := []map[string]any{}
	for _, v := range c.VolumeSlots {
		f.check(sandbox.VerifyVolume(ctx, v))
		owner, e := slRead(filepath.Join(v.MountPath, ".forge-pool.owner"), 4096)
		f.check(e)
		if string(owner) != c.JournalPath+"\n" {
			t.Fatal("existing pool owner differs; never rebind")
		}
		var fs unix.Statfs_t
		f.check(unix.Statfs(v.MountPath, &fs))
		var st unix.Stat_t
		f.check(unix.Stat(v.MountPath, &st))
		volumeObservations = append(volumeObservations, map[string]any{"slot": v, "VerifyVolume": "passed", "statfs": fs, "st_dev": st.Dev, "owner_marker_sha256": slSHA(owner), "observed_at": time.Now().UTC()})
	}
	j, e := slJournalSnapshot(ctx, c.JournalPath)
	f.check(e)
	f.check(slIdle(j))
	f.journalID = j.Identity
	if !targeted && (execution == "02" || execution == "03") {
		cleanupInputs, err := slCleanupPrerequisite(a, c, j)
		f.check(err)
		for path, hash := range cleanupInputs {
			f.inputs[path] = hash
		}
	}
	if !targeted && execution == "03" {
		priorInputs, err := slLogs02Prerequisite(a, c, j)
		f.check(err)
		for path, hash := range priorInputs {
			f.inputs[path] = hash
		}
	}
	if targeted {
		priorInputs, err := slL4Prerequisite(a, c, j)
		f.check(err)
		for path, hash := range priorInputs {
			f.inputs[path] = hash
		}
	}
	var old struct {
		Passed bool   `json:"passed"`
		UUID   string `json:"journal_uuid"`
	}
	f.check(slReadJSON(filepath.Join(a.EvidenceDir, "worker-runner-sigterm", "acceptance.json"), &old))
	if !old.Passed || old.UUID != j.Identity {
		t.Fatal("completed actual E50 on same journal required")
	}
	if conn, e := net.DialTimeout("unix", c.Server.UnixSocket, 200*time.Millisecond); e == nil {
		conn.Close()
		t.Fatal("previous fixture runner still active; refuse takeover")
	}
	f.check(os.Mkdir(f.dir, 0700))
	defer f.finalize()
	for phase, configPath := range f.configs {
		var variant slRunnerConfig
		f.check(slReadJSON(configPath, &variant))
		logPolicy := variant.Logs
		variant.Logs = c.Logs
		want := c.Logs
		if phase == "bytes" {
			want.RunBytes = 2 << 20
		}
		if phase == "count" {
			want.MaxOperations = 2
		}
		if !reflect.DeepEqual(variant, c) || logPolicy != want {
			t.Fatal("operator variant changes authority/defaults", phase)
		}
		raw, e := slRead(configPath, 1<<20)
		f.check(e)
		f.check(slWrite(filepath.Join(f.dir, "config-"+phase+".json"), raw))
	}
	f.save("preflight.json", map[string]any{"acceptance": a, "journal": j, "uid_map": string(uid), "gid_map": string(gid), "checked_all_four_volumes": volumeObservations, "observed_at": time.Now().UTC(), "input_sha256": f.inputs})
	key, e := slRead(c.SigningKeyFile, 4096)
	f.check(e)
	f.signer, e = runner.NewSigner(key)
	f.check(e)
	clear(key)
	f.objects, e = artifact.NewLocalStore(c.ArtifactRoot, 64<<20)
	f.check(e)
	if !targeted {
		f.l5()
	}
	f.setupPG()
	if !targeted {
		f.l1()
		f.quotaCase("L2-L3-default", "default", 32, true)
		f.quotaCase("L3-bytes", "bytes", 4, false)
		f.quotaCase("L3-count", "count", 2, false)
	}
	f.l4()
	f.stopRunner()
	final := f.snapshot("final-journal.json")
	f.check(slIdle(final))
	for _, v := range c.VolumeSlots {
		f.check(sandbox.VerifyVolume(ctx, v))
	}
	wantCases := 6
	if targeted {
		wantCases = 1
	}
	if len(f.cases) != wantCases {
		t.Fatal("all five groups including independent byte/count subcases required")
	}
	f.completed = true
	t.Logf("strict combined local acceptance: %s", f.dir)
}
