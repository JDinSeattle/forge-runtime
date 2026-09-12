package runner

// This opt-in test borrows one operator-quiesced fixed volume ONLY for backend
// and capture testing. It never opens an Engine or any SQLite journal. Ownership
// files are read-only; the existing lock must already exist and be acquirable.
import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
	"golang.org/x/sys/unix"
)

type e44BackendConfig struct {
	Volume           sandbox.VolumeSpec `json:"volume"`
	DockerHost       string             `json:"docker_host"`
	Image            string             `json:"image"`
	OutputDir        string             `json:"output_dir"`
	ExclusiveHandoff bool               `json:"exclusive_handoff"`
}
type e44DockerFacts struct {
	ID         string
	Name       string
	LogPath    string
	Config     struct{ Labels map[string]string }
	HostConfig struct {
		LogConfig   struct{ Type string }
		NetworkMode string
	}
	State struct {
		Running    bool
		Pid        int
		ExitCode   int
		StartedAt  string
		FinishedAt string
	}
}
type e44BackendCase struct {
	Profile              sandbox.Profile   `json:"profile"`
	Command              []string          `json:"command"`
	ExpectedLabels       map[string]string `json:"expected_labels"`
	Name                 string            `json:"name"`
	Operation            OperationRequest  `json:"operation"`
	ContainerID          string            `json:"container_id"`
	StartIntents         int32             `json:"start_intents"`
	Creates              int32             `json:"creates"`
	Job                  sandbox.Job       `json:"job"`
	Error                string            `json:"error,omitempty"`
	Facts                e44DockerFacts    `json:"facts"`
	RetryFacts           *e44DockerFacts   `json:"retry_facts,omitempty"`
	RetainedHash         string            `json:"retained_sha256,omitempty"`
	RetainedBytes        int               `json:"retained_bytes"`
	PrefixVerified       bool              `json:"prefix_verified"`
	DetachedStillRunning bool              `json:"detached_still_running"`
	Removed              bool              `json:"removed"`
	Passed               bool              `json:"passed"`
}
type e44BackendReport struct {
	SchemaVersion   int                `json:"schema_version"`
	Scope           string             `json:"scope"`
	StartedAt       time.Time          `json:"started_at"`
	FinishedAt      time.Time          `json:"finished_at"`
	PID             int                `json:"pid"`
	Binary          string             `json:"binary"`
	BinaryHash      string             `json:"binary_sha256"`
	FixtureRoot     string             `json:"fixture_root"`
	OwnerHashBefore string             `json:"owner_hash_before"`
	OwnerHashAfter  string             `json:"owner_hash_after"`
	Volume          sandbox.VolumeSpec `json:"volume"`
	DiskBefore      syscall.Statfs_t   `json:"disk_before"`
	DiskAfter       syscall.Statfs_t   `json:"disk_after"`
	FileLimit       int                `json:"file_limit"`
	ByteLimit       int64              `json:"byte_limit"`
	FixtureFiles    int                `json:"fixture_files"`
	FixtureBytes    int64              `json:"fixture_bytes"`
	FixtureRemoved  bool               `json:"fixture_removed"`
	Cases           []*e44BackendCase  `json:"cases"`
	Passed          bool               `json:"passed"`
}

func e44Write(path string, v any) error {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(raw)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	parent, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer parent.Close()
	return parent.Sync()
}
func e44Inspect(ctx context.Context, c e44BackendConfig, id string) (e44DockerFacts, error) {
	var facts e44DockerFacts
	command := exec.CommandContext(ctx, "docker", "container", "inspect", id)
	command.Env = append(os.Environ(), "DOCKER_HOST="+c.DockerHost)
	out, err := command.Output()
	if err != nil {
		return facts, err
	}
	var rows []e44DockerFacts
	if err = json.Unmarshal(out, &rows); err != nil || len(rows) != 1 {
		return facts, errors.New("invalid container inspect")
	}
	return rows[0], nil
}
func TestStrictLogDockerBackendAcceptance(t *testing.T) {
	path := os.Getenv("FORGE_E44_BACKEND_CONFIG")
	if path == "" {
		t.Skip("operator-only: requires explicit exclusive fixed-volume handoff config")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var c e44BackendConfig
	if json.Unmarshal(raw, &c) != nil || !c.ExclusiveHandoff || !filepath.IsAbs(c.OutputDir) || !strings.HasPrefix(c.DockerHost, "unix:///") {
		t.Fatal("invalid explicit config")
	}
	const pinned = "python@sha256:229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36"
	if c.Image != pinned {
		t.Fatal("fixture requires its reviewed pinned Python image")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	if err = sandbox.VerifyVolume(ctx, c.Volume); err != nil {
		t.Fatal(err)
	}
	// Open read-only, never create/truncate an ownership marker or pool lock.
	lock, err := os.Open(filepath.Join(c.Volume.MountPath, ".forge-pool.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal("original pool is not exclusively handed off", err)
	}
	defer unix.Flock(int(lock.Fd()), unix.LOCK_UN)
	ownerPath := filepath.Join(c.Volume.MountPath, ".forge-pool.owner")
	owner, err := os.ReadFile(ownerPath)
	if err != nil || len(owner) == 0 {
		t.Fatal("missing existing pool owner", err)
	}
	info, err := os.Lstat(c.OutputDir)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		t.Fatal("output root must be existing private directory", err)
	}
	c.OutputDir = filepath.Join(c.OutputDir, "backend")
	if err = os.Mkdir(c.OutputDir, 0700); err != nil {
		t.Fatal("backend output child must be new", err)
	}
	var nonce [8]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	prefix := "forge-e44-" + hex.EncodeToString(nonce[:])
	base := filepath.Join(c.Volume.MountPath, prefix)
	if err = os.Mkdir(base, 0700); err != nil {
		t.Fatal(err)
	}
	binary, _ := os.Executable()
	binBytes, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	report := e44BackendReport{SchemaVersion: 1, Scope: "actual Docker backend + fixed-volume capture; no Engine/SQLite/PG", StartedAt: time.Now().UTC(), PID: os.Getpid(), Binary: binary, BinaryHash: hashBytes(binBytes), FixtureRoot: base, OwnerHashBefore: hashBytes(owner), Volume: c.Volume, FileLimit: 20, ByteLimit: 4 << 20}
	if err = syscall.Statfs(base, &report.DiskBefore); err != nil {
		t.Fatal(err)
	}
	reportPath := filepath.Join(c.OutputDir, "report.json")
	defer func() {
		after, e := os.ReadFile(ownerPath)
		if e == nil {
			report.OwnerHashAfter = hashBytes(after)
		}
		if e != nil || report.OwnerHashAfter != report.OwnerHashBefore {
			t.Error("pool owner changed", e)
		}
		syscall.Statfs(c.Volume.MountPath, &report.DiskAfter)
		// The fixture may retain a bounded unknown spool after failure, but never
		// remove it or an unresolved container merely to make cleanup look successful.
		filepath.WalkDir(base, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			report.FixtureFiles++
			if entry.Type().IsRegular() {
				info, e := entry.Info()
				if e != nil {
					return e
				}
				report.FixtureBytes += info.Size()
			}
			return nil
		})
		if report.FixtureFiles > report.FileLimit || report.FixtureBytes > report.ByteLimit {
			t.Error("declared fixture bounds exceeded")
		}
		all := len(report.Cases) == 4 && !t.Failed()
		for _, item := range report.Cases {
			all = all && item.Passed && item.Removed
		}
		if all {
			if e = os.RemoveAll(base); e != nil {
				t.Error(e)
			} else {
				report.FixtureRemoved = true
				dir, e := os.Open(c.Volume.MountPath)
				if e == nil {
					e = dir.Sync()
					dir.Close()
				}
				if e != nil {
					t.Error(e)
				}
			}
		}
		report.Passed = all && !t.Failed()
		report.FinishedAt = time.Now().UTC()
		if e = e44Write(reportPath, report); e != nil {
			t.Error(e)
		}
	}()
	policy, _ := (sandbox.LogPolicy{}).Normalize()
	backend := &sandbox.Docker{Host: c.DockerHost, WorkspaceQuota: sandbox.FixedVolumeQuota{Slots: []sandbox.VolumeSpec{c.Volume}}}
	profile := sandbox.Profile{ID: "e44-fixture", Image: c.Image, MemoryBytes: 256 << 20, WorkspaceQuotaBytes: c.Volume.ImageBytes, CPUs: 1, PIDs: 64, User: "1000:1000"}
	for _, kind := range []string{"binary", "flood", "cancel", "shutdown"} {
		if !t.Run(kind, func(t *testing.T) {
			name := prefix + "-" + kind
			workspace := filepath.Join(base, kind)
			if err = os.Mkdir(workspace, 0777); err != nil {
				t.Fatal(err)
			}
			if err = os.Chmod(workspace, 0777); err != nil {
				t.Fatal(err)
			}
			program := e44Program(kind)
			command := []string{"python", "-B", "-u", "-c", program}
			args, _ := json.Marshal(CommandArgs{Command: command})
			r := OperationRequest{WorkspaceRequest: WorkspaceRequest{TenantID: domain.ID(prefix), RunID: domain.ID(prefix), WorkspaceID: domain.ID(name), Epoch: 1}, OperationID: domain.ID(name), Kind: "run_command", Args: args, ArgsHash: hashBytes(args), PolicyVersion: "e44-strict-v1", Deadline: time.Now().Add(35 * time.Second).UTC()}
			item := &e44BackendCase{Name: kind, Operation: r, Profile: profile, Command: command, ExpectedLabels: map[string]string{"forge.runtime": "1", "forge.operation_id": name}}
			report.Cases = append(report.Cases, item)
			planPath := filepath.Join(c.OutputDir, name+"-intent.json")
			// Save stable name + full operation binding before any Docker create can run.
			if err = e44Write(planPath, item); err != nil {
				t.Fatal(err)
			}
			captureDir := filepath.Join(base, kind+"-logs")
			capture, err := newLogCapture(captureDir, Operation{Request: r}, policy)
			if err != nil {
				t.Fatal(err)
			}
			var creates, intents atomic.Int32
			var callbackMu sync.Mutex
			jobCtx, jobCancel := context.WithDeadline(ctx, r.Deadline)
			defer jobCancel()
			spec := sandbox.JobSpec{ID: name, OperationID: r.OperationID, TenantID: r.TenantID, RunID: r.RunID, WorkspaceID: r.WorkspaceID, Epoch: 1, Workspace: workspace, Profile: profile, Command: command, Deadline: r.Deadline, Capture: capture, DetachOnCancel: func() bool { return kind == "shutdown" }, OnCreated: func(_ context.Context, id string) error {
				callbackMu.Lock()
				defer callbackMu.Unlock()
				item.ContainerID = id
				item.Creates = creates.Add(1)
				return e44Write(planPath, item)
			}, BeforeStart: func(context.Context) error {
				callbackMu.Lock()
				defer callbackMu.Unlock()
				item.StartIntents = intents.Add(1)
				return e44Write(planPath, item)
			}}
			defer func() {
				_ = capture.Finish(false, "capture_gap", false, false)
				callbackMu.Lock()
				id := item.ContainerID
				callbackMu.Unlock()
				if id != "" {
					cleanupCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
					defer stop()
					facts, e := e44Inspect(cleanupCtx, c, id)
					// Identity-complete cleanup may stop ONLY this fixture's running container.
					if e == nil && facts.ID == id && facts.Name == "/"+name && facts.Config.Labels["forge.runtime"] == "1" && facts.Config.Labels["forge.operation_id"] == name && facts.HostConfig.LogConfig.Type == "none" {
						if facts.State.Running {
							_, e = backend.Cancel(cleanupCtx, id)
						}
						if e == nil {
							e = backend.RemoveOwned(cleanupCtx, name, id)
						}
						if e == nil {
							item.Removed = true
						}
					}
					if !item.Removed {
						t.Error("owned fixture not confirmed removed; retain plan", e)
					}
				}
				if e := e44Write(planPath, item); e != nil {
					t.Error(e)
				}
			}()
			type result struct {
				job sandbox.Job
				err error
			}
			done := make(chan result, 1)
			go func() { job, e := backend.Start(jobCtx, spec); done <- result{job, e} }()
			doneRead := false
			defer func() {
				jobCancel()
				if !doneRead {
					select {
					case <-done:
					case <-time.After(20 * time.Second):
						t.Error("backend goroutine did not close; retain fixture")
					}
				}
			}()
			if kind == "cancel" || kind == "shutdown" {
				ready := false
				deadline := time.NewTimer(8 * time.Second)
				ticker := time.NewTicker(10 * time.Millisecond)
				for !ready {
					select {
					case early := <-done:
						doneRead = true
						t.Fatalf("job returned before control: %v", early.err)
					case <-deadline.C:
						t.Fatal("both-stream readiness timed out")
					case <-ticker.C:
						s, _ := capture.Snapshot()
						ready = s.StdoutSeen >= 8 && s.StderrSeen >= 8
					}
				}
				deadline.Stop()
				ticker.Stop()
				jobCancel()
			}
			var resultValue result
			select {
			case resultValue = <-done:
				doneRead = true
			case <-ctx.Done():
				t.Fatal("backend did not return within fixture deadline")
			}
			item.Job = resultValue.job
			if resultValue.err != nil {
				item.Error = resultValue.err.Error()
			}
			facts, e := e44Inspect(ctx, c, item.ContainerID)
			if e != nil {
				t.Fatal(e)
			}
			item.Facts = facts
			if facts.ID != item.ContainerID || facts.Name != "/"+name || facts.HostConfig.LogConfig.Type != "none" || facts.LogPath != "" || facts.Config.Labels["forge.runtime"] != "1" || facts.Config.Labels["forge.operation_id"] != name || facts.HostConfig.NetworkMode != "none" {
				t.Fatal("strict owned container facts mismatch")
			}
			summary, data, _, e := readLogCapture(captureDir, Operation{Request: r}, policy)
			if e != nil {
				t.Fatal(e)
			}
			item.RetainedBytes = len(data)
			item.RetainedHash = hashBytes(data)
			item.PrefixVerified = true
			if len(data) > policy.OperationBytes || summary.StdoutSeen == 0 || summary.StderrSeen == 0 || item.StartIntents != 1 || item.Creates != 1 {
				t.Fatal("capture or launch bounds")
			}
			if e = os.WriteFile(filepath.Join(c.OutputDir, name+".spool"), data, 0600); e != nil {
				t.Fatal(e)
			}
			switch kind {
			case "binary":
				if resultValue.err != nil || !summary.Complete || summary.Truncated || facts.State.Running || facts.State.ExitCode != 0 || !e44BinaryStreams(data) {
					t.Fatal("early binary stream integrity", resultValue.err)
				}
			case "flood":
				if resultValue.err != nil || !summary.Complete || summary.Reason != "output_limit" || !summary.TerminationRequested || !summary.TerminationObserved || !summary.Truncated || facts.State.Running || sandbox.VerificationLogValid(item.Job) {
					t.Fatal("flood policy did not terminate/drain conservatively", summary, resultValue.err)
				}
			case "cancel":
				if resultValue.err != nil || !summary.Complete || summary.Truncated || summary.TerminationRequested || !item.Job.Interrupted || facts.State.Running {
					t.Fatal("business cancellation confused with policy", summary, resultValue.err)
				}
			case "shutdown":
				if !errors.Is(resultValue.err, sandbox.ErrCaptureGap) || summary.Complete || summary.Reason != "runner_shutdown" || summary.TerminationRequested || !facts.State.Running {
					t.Fatal("shutdown killed job or claimed complete", summary, resultValue.err)
				}
				item.DetachedStillRunning = true
			}
			// A repeated Start using exactly the original identity must be inspect-only.
			// This is backend launch evidence; receipt/budget idempotence is tested with SQLite separately.
			if _, e = backend.Start(ctx, spec); e != nil {
				t.Fatal("same-id inspection retry", e)
			}
			retry, e := e44Inspect(ctx, c, item.ContainerID)
			if e != nil {
				t.Fatal(e)
			}
			item.RetryFacts = &retry
			if creates.Load() != 1 || intents.Load() != 1 || retry.ID != facts.ID || retry.State.StartedAt != facts.State.StartedAt {
				t.Fatal("same operation launched again")
			}
			item.Passed = true
		}) {
			break
		}
	}
}
func e44Program(kind string) string {
	switch kind {
	case "binary":
		return "import os,time\nos.write(1,b'out\\0')\ntime.sleep(.03)\nos.write(2,b'err\\xff')\n"
	case "flood":
		return "import os,threading,time\ndef emit(fd):\n while True: os.write(fd,b'x'*65536)\nthreads=[threading.Thread(target=emit,args=(fd,),daemon=True) for fd in (1,2)]\nfor thread in threads: thread.start()\ntime.sleep(30)\n"
	default:
		return "import os,sys,subprocess,time\nchild=subprocess.Popen([sys.executable,'-B','-u','-c',\"import os,time\\nwhile True: os.write(2,b'child-err\\\\n'); time.sleep(.02)\"])\nwhile True: os.write(1,b'parent-out\\n'); time.sleep(.02)\n"
	}
}

func e44BinaryStreams(data []byte) bool {
	var streams [3][]byte
	for len(data) >= logHeaderBytes {
		n := int(binary.BigEndian.Uint32(data[16:20]))
		if n < 1 || n > len(data)-logHeaderBytes {
			return false
		}
		streams[data[4]] = append(streams[data[4]], data[logHeaderBytes:logHeaderBytes+n]...)
		data = data[logHeaderBytes+n:]
	}
	return len(data) == 0 && bytes.Equal(streams[1], []byte{'o', 'u', 't', 0}) && bytes.Equal(streams[2], []byte{'e', 'r', 'r', 255})
}

func TestStrictLogBackendFixtureProgramsParse(t *testing.T) {
	for _, kind := range []string{"binary", "flood", "cancel", "shutdown"} {
		t.Run(kind, func(t *testing.T) {
			cmd := exec.Command("python3", "-c", "import ast,sys; ast.parse(sys.stdin.read())")
			cmd.Stdin = strings.NewReader(e44Program(kind))
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("fixture syntax: %v %s", err, out)
			}
		})
	}
}
