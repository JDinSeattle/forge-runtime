package sandbox

// This fixture exercises ONLY startCaptured after fixture-owned Docker create.
// It never calls Docker.Start or installs a quota verifier, and creates no host
// mounts, Engine, journal, pool lock, or service. Its recording sink is explicitly
// in-memory and does not stand in for runner framing/durability/quota tests.
import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
)

const noMountImage = "python@sha256:229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36"
const noMountPayloadLimit = 512 << 10
const noMountPreviewLimit = 64 << 10

type noMountCapture struct {
	mu       sync.Mutex
	summary  LogSummary
	streams  [3][]byte
	preview  []byte
	finished bool
}

func newNoMountCapture(id string) *noMountCapture {
	policy, _ := (LogPolicy{}).Normalize()
	return &noMountCapture{summary: LogSummary{SchemaVersion: 1, Policy: policy, OperationID: domain.ID(id)}}
}
func (c *noMountCapture) Write(stream byte, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finished {
		return io.ErrClosedPipe
	}
	if stream != 1 && stream != 2 {
		return domain.ErrInvalid
	}
	if stream == 1 {
		c.summary.StdoutSeen += uint64(len(data))
	} else {
		c.summary.StderrSeen += uint64(len(data))
	}
	left := noMountPayloadLimit - int(c.summary.RetainedBytes)
	keep := min(len(data), left)
	c.streams[stream] = append(c.streams[stream], data[:keep]...)
	preview := min(keep, noMountPreviewLimit-len(c.preview))
	c.preview = append(c.preview, data[:preview]...)
	c.summary.RetainedBytes += int64(keep)
	c.summary.RetainedPayload += int64(keep)
	if keep > 0 {
		c.summary.Records++
	}
	if keep < len(data) {
		c.summary.Truncated = true
		c.summary.Reason = "output_limit"
		return ErrLogLimit
	}
	return nil
}
func (c *noMountCapture) Finish(complete bool, reason string, requested, observed bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finished {
		return nil
	}
	c.finished = true
	c.summary.Complete = complete
	c.summary.DroppedKnown = complete
	if complete {
		c.summary.DroppedBytes = c.summary.StdoutSeen + c.summary.StderrSeen - uint64(c.summary.RetainedPayload)
	}
	if c.summary.Reason == "" {
		c.summary.Reason = reason
	}
	if !complete {
		c.summary.Truncated = true
	}
	c.summary.TerminationRequested = requested
	c.summary.TerminationObserved = observed
	return nil
}
func (c *noMountCapture) Snapshot() (LogSummary, []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.summary, append([]byte(nil), c.preview...)
}
func (c *noMountCapture) stream(stream byte) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.streams[stream]...)
}
func (c *noMountCapture) markers() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return bytes.Contains(c.streams[1], []byte("parent-out\n")) && bytes.Contains(c.streams[2], []byte("child-err\n"))
}

type noMountFacts struct {
	ID      string
	Name    string
	Image   string
	LogPath string
	Mounts  []json.RawMessage
	Config  struct {
		Image   string
		User    string
		Labels  map[string]string
		Volumes map[string]json.RawMessage
		Tty     bool
	}
	HostConfig struct {
		ReadonlyRootfs bool
		NetworkMode    string
		Memory         int64
		MemorySwap     int64
		NanoCpus       int64
		PidsLimit      int64
		CapDrop        []string
		SecurityOpt    []string
		Binds          []string
		Tmpfs          map[string]string
		Privileged     bool
		LogConfig      struct{ Type string }
	}
	State struct {
		Running    bool
		Pid        int
		ExitCode   int
		StartedAt  string
		FinishedAt string
		OOMKilled  bool
	}
}
type noMountCase struct {
	Name                  string            `json:"name"`
	JobName               string            `json:"job_name"`
	ContainerID           string            `json:"container_id"`
	Labels                map[string]string `json:"expected_labels"`
	Image                 string            `json:"image"`
	Command               []string          `json:"command"`
	CreateArgs            []string          `json:"create_args"`
	CreateBegin           time.Time         `json:"create_begin"`
	CreateAcknowledged    time.Time         `json:"create_acknowledged"`
	BeforeStart           time.Time         `json:"attach_acknowledged_before_start"`
	StartCount            int               `json:"start_count"`
	ControlRequested      time.Time         `json:"control_requested,omitempty"`
	StageReturned         time.Time         `json:"stage_returned"`
	CreatedFacts          noMountFacts      `json:"created_facts"`
	ReturnFacts           noMountFacts      `json:"return_facts"`
	StoppedFacts          noMountFacts      `json:"stopped_facts"`
	Job                   Job               `json:"job"`
	Error                 string            `json:"error,omitempty"`
	StreamHashes          map[string]string `json:"stream_sha256"`
	RetainedStreamBytes   map[string]int    `json:"retained_stream_bytes"`
	MarkersObserved       bool              `json:"markers_observed"`
	DetachObservedRunning bool              `json:"detach_observed_running"`
	NaturalExitObserved   bool              `json:"natural_exit_observed"`
	CleanupConfirmed      bool              `json:"cleanup_confirmed"`
	CleanupError          string            `json:"cleanup_error,omitempty"`
	Passed                bool              `json:"passed"`
}

func noMountHash(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}
func noMountWrite(path string, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path+".tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = file.Write(raw)
	if err == nil {
		err = file.Sync()
	}
	closed := file.Close()
	if err == nil {
		err = closed
	}
	if err != nil {
		return err
	}
	if err = os.Rename(path+".tmp", path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
func noMountInspect(ctx context.Context, d *Docker, id string) (noMountFacts, error) {
	var result noMountFacts
	raw, err := d.run(ctx, "container", "inspect", id)
	if err != nil {
		return result, err
	}
	var rows []noMountFacts
	if json.Unmarshal(raw, &rows) != nil || len(rows) != 1 {
		return result, domain.ErrReconciliation
	}
	return rows[0], nil
}
func noMountOwned(f noMountFacts, item *noMountCase) bool {
	decoded, e := hex.DecodeString(f.ID)
	return e == nil && len(decoded) == 32 && hex.EncodeToString(decoded) == f.ID && (item.ContainerID == "" || item.ContainerID == f.ID) && f.Name == "/"+item.JobName && f.Config.Image == item.Image && f.Config.Labels["forge.runtime"] == "1" && f.Config.Labels["forge.operation_id"] == item.JobName && f.Config.Labels["forge.e44_fixture"] == item.Labels["forge.e44_fixture"]
}
func noMountConstrained(f noMountFacts) bool {
	h := f.HostConfig
	return h.ReadonlyRootfs && !h.Privileged && h.NetworkMode == "none" && h.Memory == 256<<20 && h.MemorySwap == 256<<20 && h.NanoCpus == 1_000_000_000 && h.PidsLimit == 64 && f.Config.User == "1000:1000" && !f.Config.Tty && len(f.Mounts) == 0 && len(f.Config.Volumes) == 0 && len(h.Binds) == 0 && len(h.Tmpfs) == 0 && h.LogConfig.Type == "none" && f.LogPath == "" && len(h.CapDrop) == 1 && h.CapDrop[0] == "ALL" && len(h.SecurityOpt) == 1 && h.SecurityOpt[0] == "no-new-privileges"
}
func TestNoMountCapturedDockerAcceptance(t *testing.T) {
	host := os.Getenv("FORGE_E44_NOMOUNT_DOCKER_HOST")
	output := os.Getenv("FORGE_E44_NOMOUNT_OUTPUT")
	if host == "" && output == "" {
		t.Skip("operator opt-in: standalone no-mount containers, no services or pool")
	}
	if !filepath.IsAbs(output) {
		t.Fatal("explicit absolute output directory required")
	}
	d := &Docker{Host: host}
	if _, err := d.logSocket(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 65*time.Second)
	defer cancel()
	if err := d.Check(ctx); err != nil {
		t.Fatal(err)
	}
	// No profiles/config/key/DB/environment files are read. Image is exactly pinned.
	if err := os.Mkdir(output, 0700); err != nil {
		t.Fatal("output must be new", err)
	}
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	fixture := "forge-e44-nomount-" + hex.EncodeToString(nonce[:])
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.ReadFile(executable)
	if err != nil {
		t.Fatal(err)
	}
	report := struct {
		Scope      string         `json:"scope"`
		Fixture    string         `json:"fixture"`
		Binary     string         `json:"binary"`
		BinarySHA  string         `json:"binary_sha256"`
		PID        int            `json:"pid"`
		Started    time.Time      `json:"started_at"`
		Finished   time.Time      `json:"finished_at"`
		PayloadCap int            `json:"in_memory_payload_cap"`
		PreviewCap int            `json:"preview_cap"`
		Cases      []*noMountCase `json:"cases"`
		Passed     bool           `json:"passed"`
	}{Scope: "production startCaptured only; fixture creates containers; recording sink is bounded in-memory raw payload, NOT runner framed spool, Engine, SQLite, PG or physical disk quota", Fixture: fixture, Binary: executable, BinarySHA: noMountHash(binary), PID: os.Getpid(), Started: time.Now().UTC(), PayloadCap: noMountPayloadLimit, PreviewCap: noMountPreviewLimit}
	defer func() {
		report.Finished = time.Now().UTC()
		report.Passed = !t.Failed() && len(report.Cases) == 4
		for _, item := range report.Cases {
			report.Passed = report.Passed && item.Passed && item.CleanupConfirmed
		}
		if err := noMountWrite(filepath.Join(output, "report.json"), report); err != nil {
			t.Error(err)
		}
	}()
	for _, kind := range []string{"binary", "flood", "cancel", "detach"} {
		if !t.Run(kind, func(t *testing.T) {
			name := fixture + "-" + kind
			item := &noMountCase{Name: kind, JobName: name, Image: noMountImage, Labels: map[string]string{"forge.runtime": "1", "forge.operation_id": name, "forge.e44_fixture": fixture}, Command: []string{"python", "-B", "-u", "-c", noMountProgram(kind)}}
			report.Cases = append(report.Cases, item)
			args := []string{"create", "--pull=never", "--name", name, "--label", "forge.runtime=1", "--label", "forge.operation_id=" + name, "--label", "forge.e44_fixture=" + fixture, "--read-only", "--network", "none", "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--user", "1000:1000", "--pids-limit", "64", "--memory", "268435456", "--memory-swap", "268435456", "--cpus", "1", "--log-driver", "none", "--init", noMountImage}
			args = append(args, item.Command...)
			item.CreateArgs = args
			plan := filepath.Join(output, name+".json")
			if err := noMountWrite(plan, item); err != nil {
				t.Fatal(err)
			}
			// Cleanup exists before create. A lost create reply is resolved read-only by
			// exact name+unique labels+image, then the full immutable ID is used thereafter.
			defer func() {
				cleanupCtx, end := context.WithTimeout(context.Background(), 15*time.Second)
				defer end()
				query := item.ContainerID
				if query == "" {
					query = name
				}
				facts, err := noMountInspect(cleanupCtx, d, query)
				if err != nil {
					if strings.Contains(err.Error(), "No such container") || strings.Contains(err.Error(), "No such object") {
						item.CleanupConfirmed = true
					} else {
						item.CleanupError = err.Error()
					}
				} else if !noMountOwned(facts, item) {
					item.CleanupError = "foreign identity: retained"
				} else {
					item.ContainerID = facts.ID
					if facts.State.Running {
						_, err = d.Cancel(cleanupCtx, facts.ID)
					}
					if err == nil {
						facts, err = noMountInspect(cleanupCtx, d, facts.ID)
					}
					if err == nil && noMountOwned(facts, item) && !facts.State.Running && facts.State.Pid == 0 {
						item.StoppedFacts = facts
						err = d.RemoveOwned(cleanupCtx, name, facts.ID)
					} else if err == nil {
						err = domain.ErrReconciliation
					}
					if err == nil {
						_, err = d.Inspect(cleanupCtx, facts.ID)
						if errors.Is(err, ErrJobNotFound) {
							item.CleanupConfirmed = true
							err = nil
						}
					}
					if err != nil {
						item.CleanupError = err.Error()
					}
				}
				if !item.CleanupConfirmed {
					t.Error("fixture cleanup unconfirmed; retain exact plan", item.CleanupError)
				}
				if err := noMountWrite(plan, item); err != nil {
					t.Error(err)
				}
			}()
			item.CreateBegin = time.Now().UTC()
			created, err := d.run(ctx, args...)
			if err != nil {
				t.Fatal(err)
			}
			item.CreateAcknowledged = time.Now().UTC()
			candidateID := strings.TrimSpace(string(created))
			decoded, decodeErr := hex.DecodeString(candidateID)
			if decodeErr != nil || len(decoded) != 32 || hex.EncodeToString(decoded) != candidateID {
				t.Fatal("create response lacks full immutable ID; cleanup resolves exact labels/name")
			}
			item.ContainerID = candidateID
			if err = noMountWrite(plan, item); err != nil {
				t.Fatal(err)
			}
			facts, err := noMountInspect(ctx, d, item.ContainerID)
			if err != nil {
				t.Fatal(err)
			}
			item.CreatedFacts = facts
			if !noMountOwned(facts, item) || !noMountConstrained(facts) || facts.State.Running || facts.State.Pid != 0 || !strings.HasPrefix(facts.State.StartedAt, "0001-") {
				t.Fatal("unstarted isolated fixture facts mismatch")
			}
			capture := newNoMountCapture(name)
			intentReady := make(chan struct{})
			jobCtx, jobCancel := context.WithTimeout(ctx, 12*time.Second)
			spec := JobSpec{ID: name, OperationID: domain.ID(name), Capture: capture, DetachOnCancel: func() bool { return kind == "detach" }, BeforeStart: func(context.Context) error {
				item.StartCount++
				item.BeforeStart = time.Now().UTC()
				err := noMountWrite(plan, item)
				if err == nil {
					close(intentReady)
				}
				return err
			}}
			type result struct {
				job Job
				err error
			}
			done := make(chan result, 1)
			consumed := false
			go func() { job, err := d.startCaptured(jobCtx, spec); done <- result{job, err} }()
			defer func() {
				jobCancel()
				if !consumed {
					select {
					case <-done:
					case <-time.After(20 * time.Second):
						t.Error("capture did not join before cleanup")
					}
				}
			}()
			if kind == "cancel" || kind == "detach" {
				tick := time.NewTicker(5 * time.Millisecond)
				timer := time.NewTimer(2 * time.Second)
				for !capture.markers() {
					select {
					case early := <-done:
						consumed = true
						t.Fatalf("stage ended before parent+child readiness: %v", early.err)
					case <-timer.C:
						t.Fatal("exact parent+child markers missing")
					case <-tick.C:
					}
				}
				tick.Stop()
				timer.Stop()
				select {
				case <-intentReady:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				item.MarkersObserved = true
				item.ControlRequested = time.Now().UTC()
				jobCancel()
			}
			var resultValue result
			select {
			case resultValue = <-done:
				consumed = true
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			item.StageReturned = time.Now().UTC()
			item.Job = resultValue.job
			if resultValue.err != nil {
				item.Error = resultValue.err.Error()
			}
			facts, err = noMountInspect(ctx, d, item.ContainerID)
			if err != nil {
				t.Fatal(err)
			}
			item.ReturnFacts = facts
			if !noMountOwned(facts, item) || !noMountConstrained(facts) || item.StartCount != 1 || item.Job.Log == nil {
				t.Fatal("stage result identity/capture mismatch")
			}
			started, err := time.Parse(time.RFC3339Nano, facts.State.StartedAt)
			if err != nil || started.Before(item.BeforeStart) {
				t.Fatal("Docker start preceded acknowledged attach hook", err)
			}
			item.StreamHashes = map[string]string{}
			item.RetainedStreamBytes = map[string]int{}
			for n, label := range map[byte]string{1: "stdout", 2: "stderr"} {
				data := capture.stream(n)
				item.StreamHashes[label] = noMountHash(data)
				item.RetainedStreamBytes[label] = len(data)
				if err = os.WriteFile(filepath.Join(output, name+"-"+label+".bin"), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			summary := item.Job.Log
			if summary.RetainedBytes > noMountPayloadLimit || summary.StdoutSeen == 0 || summary.StderrSeen == 0 || len(item.Job.Output) > noMountPreviewLimit {
				t.Fatal("recording sink limit or stream capture failed")
			}
			switch kind {
			case "binary":
				if resultValue.err != nil || !summary.Complete || summary.Truncated || facts.State.Running || facts.State.ExitCode != 0 || !bytes.Equal(capture.stream(1), []byte{'o', 'u', 't', 0}) || !bytes.Equal(capture.stream(2), []byte{'e', 'r', 'r', 255}) {
					t.Fatal("binary output lost", resultValue.err)
				}
			case "flood":
				if resultValue.err != nil || summary.StdoutSeen < 65536 || summary.StderrSeen < 65536 || summary.Reason != "output_limit" || !summary.Complete || !summary.TerminationRequested || !summary.TerminationObserved || !summary.Truncated || facts.State.Running || VerificationLogValid(item.Job) {
					t.Fatal("output limit not stopped/drained", summary, resultValue.err)
				}
			case "cancel":
				if resultValue.err != nil || !summary.Complete || summary.Truncated || summary.TerminationRequested || !item.Job.Interrupted || facts.State.Running {
					t.Fatal("business cancel confused with policy", summary, resultValue.err)
				}
			case "detach":
				if !errors.Is(resultValue.err, ErrCaptureGap) || summary.Complete || summary.DroppedKnown || summary.Reason != "runner_shutdown" || summary.TerminationRequested || !facts.State.Running {
					t.Fatal("detach killed the process or invented complete capture", summary, resultValue.err)
				}
				item.DetachObservedRunning = true
				// Observe natural completion of the same finite process. Do not issue a
				// replacement Start or use a second attach to fabricate missing output.
				stopAt := time.Now().Add(6 * time.Second)
				for facts.State.Running && time.Now().Before(stopAt) {
					time.Sleep(25 * time.Millisecond)
					facts, err = noMountInspect(ctx, d, item.ContainerID)
					if err != nil {
						t.Fatal(err)
					}
				}
				if facts.State.Running || facts.State.ExitCode != 0 || facts.State.StartedAt != item.ReturnFacts.State.StartedAt {
					t.Fatal("detached finite original process did not naturally finish")
				}
				item.NaturalExitObserved = true
				item.StoppedFacts = facts
			}
			if !facts.State.Running && facts.State.Pid != 0 {
				t.Fatal("stopped container retained init PID")
			}
			item.Passed = true
		}) {
			break
		}
	}
}
func noMountProgram(kind string) string {
	switch kind {
	case "binary":
		return "import os,time\nos.write(1,b'out\\0')\ntime.sleep(.03)\nos.write(2,b'err\\xff')\n"
	case "flood":
		return "import os,threading,time\nbarrier=threading.Barrier(2)\ndef emit(fd):\n os.write(fd,b'x'*65536)\n barrier.wait()\n end=time.monotonic()+3.0\n while time.monotonic()<end: os.write(fd,b'x'*65536)\nfor fd in (1,2): threading.Thread(target=emit,args=(fd,),daemon=True).start()\ntime.sleep(3.5)\nos._exit(0)\n"
	default:
		return "import os,sys,subprocess,time\nchild_code=\"import os,time\\nfor _ in range(100): os.write(2,b'child-err\\\\n'); time.sleep(.02)\\n\"\nchild=subprocess.Popen([sys.executable,'-B','-u','-c',child_code])\nend=time.monotonic()+3.5\nwhile time.monotonic()<end: os.write(1,b'parent-out\\n'); time.sleep(.02)\nchild.wait(timeout=.2)\n"
	}
}
func TestNoMountCaptureSinkBoundAndFinitePrograms(t *testing.T) {
	capture := newNoMountCapture("operation")
	var wait sync.WaitGroup
	for _, stream := range []byte{1, 2} {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for n := 0; n < 128; n++ {
				_ = capture.Write(stream, bytes.Repeat([]byte{stream}, 16384))
			}
		}()
	}
	wait.Wait()
	if err := capture.Finish(true, "output_limit", true, true); err != nil {
		t.Fatal(err)
	}
	summary, preview := capture.Snapshot()
	if summary.StdoutSeen != 2<<20 || summary.StderrSeen != 2<<20 || summary.RetainedBytes != 512<<10 || summary.DroppedBytes != (4<<20)-(512<<10) || len(preview) != 64<<10 || len(capture.stream(1))+len(capture.stream(2)) != 512<<10 {
		t.Fatal(summary)
	}
	for _, kind := range []string{"binary", "flood", "cancel", "detach"} {
		t.Run(kind, func(t *testing.T) {
			// Parse the outer program and literal child_code without executing either.
			script := "import ast,sys\ntree=ast.parse(sys.stdin.read())\nfor node in ast.walk(tree):\n if isinstance(node,ast.Assign) and any(isinstance(n,ast.Name) and n.id=='child_code' for n in node.targets): ast.parse(ast.literal_eval(node.value))\n"
			command := exec.Command("python3", "-c", script)
			command.Stdin = strings.NewReader(noMountProgram(kind))
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("fixture AST: %v %s", err, output)
			}
			if strings.Contains(noMountProgram(kind), "while True") {
				t.Fatal("unbounded fixture")
			}
		})
	}
}
