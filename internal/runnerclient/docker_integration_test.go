package runnerclient

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
)

// This test requires the externally provisioned fixed volume pool and the real
// mapped-UID runner service. It creates only its uniquely named fixture run,
// preserves unknown effects, never mounts/unmounts volumes, and never substitutes
// a test backend for real Docker evidence.
func TestRealDockerRunnerIsolationQuotaAndCancellation(t *testing.T) {
	configPath := os.Getenv("FORGE_REAL_RUNNER_CONFIG")
	if configPath == "" {
		t.Skip("set FORGE_REAL_RUNNER_CONFIG to an operator-owned real runner configuration")
	}
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	var c struct {
		SigningKeyFile   string       `json:"signing_key_file"`
		DockerHost       string       `json:"docker_host"`
		AllowTestBackend bool         `json:"allow_test_backend"`
		Server           ServerConfig `json:"server"`
	}
	if err = json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	if c.AllowTestBackend || c.DockerHost == "" {
		t.Fatal("real Docker test requires an explicit daemon and production backend")
	}
	key, err := os.ReadFile(c.SigningKeyFile)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := runner.NewSigner(key)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	client, err := Dial(ctx, ClientConfig{UnixSocket: c.Server.UnixSocket})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	nonce := make([]byte, 8)
	if _, err = rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	runID := domain.ID("docker-probe-" + hex.EncodeToString(nonce))
	binding := func() runner.WorkspaceRequest {
		now := time.Now()
		claims := runner.Claims{TenantID: "integration-test", RunID: runID, WorkspaceID: runID, Epoch: 1, IssuedAt: now, ExpiresAt: now.Add(time.Minute), Permissions: []string{"prepare", "execute", "inspect", "cancel", "snapshot", "release", "adopt"}}
		grant, err := signer.Sign(claims, now.Add(2*time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		return runner.WorkspaceRequest{TenantID: claims.TenantID, RunID: runID, WorkspaceID: runID, Epoch: 1, Grant: grant}
	}
	w, err := client.PrepareWorkspace(ctx, runner.PrepareRequest{WorkspaceRequest: binding(), SourceID: "clamp", ProfileID: "python-clamp"})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("real fixture workspace %s baseline %s", runID, w.BaselineHash)
	// Cleanup is restricted to this test-created workspace, after no-active proof
	// and durable snapshot. Failure to prove settlement retains the entire slot.
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		stop, err := client.StopWorkspace(cleanup, binding())
		if err != nil || !stop.NoActiveOperations {
			t.Logf("retained test workspace %s: stop=%v", runID, err)
			return
		}
		snapshot, err := client.SealSnapshot(cleanup, binding())
		if err != nil || snapshot.Artifact.ObjectKey == "" {
			t.Logf("retained test workspace %s: snapshot=%v", runID, err)
			return
		}
		if _, err = client.ReleaseWorkspace(cleanup, binding()); err != nil {
			t.Logf("retained test workspace %s: release=%v", runID, err)
		}
	}()
	// All daemon calls in this test are read-only observations of its own job.
	dockerRead := func(args ...string) ([]byte, error) {
		observe, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return exec.CommandContext(observe, "docker", append([]string{"--host", c.DockerHost}, args...)...).CombinedOutput()
	}
	revision := uint64(1)
	var lastRequest runner.OperationRequest
	sequence := 0
	start := func(kind string, args any) runner.Operation {
		sequence++
		encoded, err := json.Marshal(args)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err = domain.CanonicalJSON(encoded)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(encoded)
		lastRequest = runner.OperationRequest{WorkspaceRequest: binding(), OperationID: domain.ID(fmt.Sprintf("%s-op-%d", runID, sequence)), ExpectedRevision: revision, Kind: kind, Args: encoded, ArgsHash: hex.EncodeToString(digest[:]), PolicyVersion: "real-docker-probe-v1", Deadline: time.Now().Add(45 * time.Second)}
		op, err := client.StartOperation(ctx, lastRequest)
		if err != nil {
			t.Fatal(err)
		}
		return op
	}
	wait := func(op runner.Operation) runner.Operation {
		for !op.Status.Terminal() {
			if op.Status == runner.Unknown {
				t.Fatalf("real effect unknown (retained for inspection): %s", op.Error)
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(50 * time.Millisecond):
			}
			op, err = client.InspectOperation(ctx, runner.InspectRequest{WorkspaceRequest: binding(), OperationID: op.Request.OperationID})
			if err != nil {
				t.Fatal(err)
			}
		}
		revision = op.AfterRevision
		return op
	}
	run := func(code string) sandbox.Job {
		op := wait(start("run_command", runner.CommandArgs{Command: []string{"python", "-I", "-B", "-c", code}}))
		var job sandbox.Job
		if err = json.Unmarshal(op.Result, &job); err != nil {
			t.Fatal(err)
		}
		if op.Status != runner.Succeeded || job.ExitCode != 0 {
			t.Fatalf("container failed: status=%s exit=%d output=%s error=%s", op.Status, job.ExitCode, job.Output, op.Error)
		}
		t.Logf("operation %s receipt=%s output=%s", op.Request.OperationID, op.Receipt.SHA256, job.Output)
		return job
	}
	run(`import os,json
assert os.getuid()==1000 and os.getgid()==1000
status=dict(line.split(':',1) for line in open('/proc/self/status') if ':' in line)
assert int(status['CapEff'].strip(),16)==0
assert status['NoNewPrivs'].strip()=='1'
assert os.statvfs('/').f_flag & os.ST_RDONLY
assert os.statvfs('/tmp').f_blocks*os.statvfs('/tmp').f_frsize<=67108864
assert set(os.listdir('/sys/class/net'))=={'lo'}
assert not os.path.exists('/var/run/docker.sock')
limits={name:open('/sys/fs/cgroup/'+name).read().strip() for name in ['memory.max','pids.max','cpu.max']}
assert limits['memory.max']=='268435456',limits
assert limits['pids.max']=='64',limits
quota,period=map(int,limits['cpu.max'].split())
assert quota>0 and quota==period,limits
os.mkdir('private',0o700)
fd=os.open('private/secret.txt',os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
os.write(fd,b'private-content');os.fsync(fd);os.close(fd)
print(json.dumps({'uid':os.getuid(),'gid':os.getgid(),'cap_eff':status['CapEff'].strip(),'no_new_privs':1,'private_file_mode':oct(os.stat('private/secret.txt').st_mode & 0o777),'cgroup_limits':limits}))`)
	read := wait(start("read_file", runner.ReadArgs{Path: "private/secret.txt"}))
	if read.Status != runner.Succeeded || !strings.Contains(string(read.Result), "private-content") {
		t.Fatalf("mapped runner cannot read task-owned 0600/0700 files: %s", read.Result)
	}
	content := "runner-patched"
	sum := sha256.Sum256([]byte("private-content"))
	patched := wait(start("apply_patch", runner.PatchArgs{Files: []runner.FileEdit{{Path: "private/secret.txt", ExpectedSHA256: hex.EncodeToString(sum[:]), Content: &content}}}))
	if patched.Status != runner.Succeeded {
		t.Fatalf("mapped runner cannot patch private file: %s", patched.Error)
	}
	// Resubmit byte-for-byte the same operation, including its original revision,
	// arguments and deadline. An accidental replay increments the counter twice.
	write := start("run_command", runner.CommandArgs{Command: []string{"python", "-I", "-B", "-c", `import os
with open('operation-count.txt','a') as f:
 f.write('1\n');f.flush();os.fsync(f.fileno())`}})
	originalRequest := lastRequest
	written := wait(write)
	if written.Status != runner.Succeeded {
		t.Fatalf("idempotency fixture write failed: %s", written.Error)
	}
	retry, err := client.StartOperation(ctx, originalRequest)
	if err != nil || retry.Status != written.Status || retry.Receipt.SHA256 != written.Receipt.SHA256 || retry.Receipt.ObjectKey != written.Receipt.ObjectKey || retry.AfterRevision != written.AfterRevision || retry.AfterHash != written.AfterHash {
		t.Fatalf("same-ID write did not return original receipt: %+v %v", retry, err)
	}
	counted := wait(start("read_file", runner.ReadArgs{Path: "operation-count.txt"}))
	var counter struct {
		Content string `json:"content"`
	}
	if err = json.Unmarshal(counted.Result, &counter); err != nil || counted.Status != runner.Succeeded || counter.Content != "1\n" {
		t.Fatalf("write effect replayed: result=%s err=%v", counted.Result, err)
	}
	t.Logf("same-ID write operation=%s original_receipt=%s retry_receipt=%s count=1", written.Request.OperationID, written.Receipt.SHA256, retry.Receipt.SHA256)

	run(`import os,errno,signal,json
children=[];limited=False
before=dict(line.split() for line in open('/sys/fs/cgroup/pids.events'))
try:
 for _ in range(80):
  try:
   pid=os.fork()
  except OSError as e:
   assert e.errno==errno.EAGAIN,e
   limited=True;break
  if pid==0:
   while True: signal.pause()
  children.append(pid)
 after=dict(line.split() for line in open('/sys/fs/cgroup/pids.events'))
 assert limited,'PID ceiling did not reject fork within 80 children'
 assert int(after['max'])>int(before['max']),'EAGAIN lacked a cgroup PID-limit event'
 print(json.dumps({'pid_limit_eagain':limited,'forked_children':len(children),'pids_max_events_before':int(before['max']),'pids_max_events_after':int(after['max'])}),flush=True)
finally:
 for pid in children:
  try: os.kill(pid,signal.SIGKILL)
  except ProcessLookupError: pass
 for pid in children:
  try: os.waitpid(pid,0)
  except ChildProcessError: pass`)

	// Exit 137 by itself is not OOM evidence. Require Docker's observed kernel
	// OOMKilled flag as well, for this specific bounded allocation job.
	oom := wait(start("run_command", runner.CommandArgs{Command: []string{"python", "-I", "-B", "-c", "x=bytearray(320*1024*1024); print(len(x),flush=True)"}}))
	var oomJob sandbox.Job
	if err = json.Unmarshal(oom.Result, &oomJob); err != nil {
		t.Fatal(err)
	}
	stateRaw, err := dockerRead("container", "inspect", "--format", "{{json .State}}", oom.JobID)
	if err != nil {
		t.Fatalf("OOM state observation: %v %s", err, stateRaw)
	}
	var oomState struct {
		OOMKilled bool
		Running   bool
		ExitCode  int
	}
	if err = json.Unmarshal(stateRaw, &oomState); err != nil {
		t.Fatal(err)
	}
	if oom.Status != runner.Failed || oomJob.ExitCode != 137 || oomState.Running || oomState.ExitCode != 137 || !oomState.OOMKilled {
		t.Fatalf("memory limit lacked kernel OOM evidence: status=%s job_exit=%d state=%s", oom.Status, oomJob.ExitCode, stateRaw)
	}
	t.Logf("memory ceiling operation=%s exit=%d OOMKilled=%t receipt=%s", oom.Request.OperationID, oomState.ExitCode, oomState.OOMKilled, oom.Receipt.SHA256)

	run(`import os,errno,json
os.mkdir('.venv',0o700)
f=open('.venv/quota-probe','wb',buffering=0)
chunk=b'x'*1048576;written=0;full=False
try:
 while written<300*1048576:
  written+=f.write(chunk)
except OSError as e:
 assert e.errno==errno.ENOSPC,e
 full=True
finally:
 f.close();os.unlink('.venv/quota-probe');os.rmdir('.venv')
assert full,'physical filesystem limit was not enforced'
assert written<268435456
print(json.dumps({'enospc':full,'allocated_bytes_before_enospc':written,'filesystem_bytes':os.statvfs('.').f_blocks*os.statvfs('.').f_frsize}))`)
	active := start("run_command", runner.CommandArgs{Command: []string{"python", "-I", "-B", "-c", `import subprocess,sys,time
child=subprocess.Popen([sys.executable,'-I','-B','-c',"import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); print('FORGE_CHILD_READY',flush=True); time.sleep(40)"])
print('FORGE_PARENT_READY',flush=True)
time.sleep(40)`}})
	docker := &sandbox.Docker{Host: c.DockerHost}
	deadline := time.Now().Add(10 * time.Second)
	for {
		job, inspectErr := docker.Inspect(ctx, active.JobID)
		if inspectErr == nil && job.Running && job.Started {
			logs, logErr := dockerRead("logs", active.JobID)
			if logErr == nil && strings.Contains(string(logs), "FORGE_CHILD_READY") && strings.Contains(string(logs), "FORGE_PARENT_READY") {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("real command never became running: %v", inspectErr)
		}
		time.Sleep(50 * time.Millisecond)
	}
	// Resolve the host-side cgroup from one of this container's actual host
	// PIDs before stopping. Afterwards its cgroup must be gone or have no tasks.
	top, err := dockerRead("top", active.JobID, "-eo", "pid,ppid,comm")
	if err != nil {
		t.Fatalf("process-tree observation: %v %s", err, top)
	}
	lines := strings.Split(strings.TrimSpace(string(top)), "\n")
	if len(lines) < 4 {
		t.Fatalf("expected init, parent and child before Stop: %s", top)
	}
	fields := strings.Fields(lines[1])
	if len(fields) < 3 {
		t.Fatalf("invalid owned-container process table: %s", top)
	}
	hostPID, err := strconv.Atoi(fields[0])
	if err != nil || hostPID <= 0 {
		t.Fatalf("invalid owned-container PID: %s", top)
	}
	groupRaw, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", hostPID))
	if err != nil {
		t.Fatalf("cannot inspect owned-container cgroup: %v", err)
	}
	groupPath := ""
	for _, line := range strings.Split(string(groupRaw), "\n") {
		if strings.HasPrefix(line, "0::/") {
			groupPath = strings.TrimPrefix(line, "0::")
			break
		}
	}
	if groupPath == "" || groupPath == "/" {
		t.Fatalf("missing dedicated container cgroup: %s", groupRaw)
	}
	cgroupProcs := filepath.Join("/sys/fs/cgroup", groupPath, "cgroup.procs")
	beforeProcs, err := os.ReadFile(cgroupProcs)
	if err != nil || len(strings.Fields(string(beforeProcs))) < 3 {
		t.Fatalf("process-tree cgroup was not populated: %q %v", beforeProcs, err)
	}
	stopped, err := client.StopWorkspace(ctx, binding())
	if err != nil || !stopped.NoActiveOperations || !stopped.Workspace.Stopped {
		t.Fatalf("stop lacks durable no-active proof: %+v %v", stopped, err)
	}
	job, err := docker.Inspect(ctx, active.JobID)
	if err != nil || job.Running {
		t.Fatalf("stop receipt preceded actual process exit: %+v %v", job, err)
	}
	afterProcs, procErr := os.ReadFile(cgroupProcs)
	if !errors.Is(procErr, os.ErrNotExist) && (procErr != nil || len(strings.Fields(string(afterProcs))) != 0) {
		t.Fatalf("stop receipt left container descendants: %q %v", afterProcs, procErr)
	}
	t.Logf("process-tree Stop pre_stop_tasks=%d remaining_tasks=0 cgroup_removed=%t", len(strings.Fields(string(beforeProcs))), errors.Is(procErr, os.ErrNotExist))
	if _, err = client.AdoptWorkspace(ctx, binding()); !errors.Is(err, domain.ErrFenced) {
		t.Fatalf("delayed same-epoch adoption reopened stopped workspace: %v", err)
	}
	t.Logf("real cancellation job=%s exit=%d no_active=%t stop_receipt=%s", active.JobID, job.ExitCode, stopped.NoActiveOperations, stopped.Ref.SHA256)
}
