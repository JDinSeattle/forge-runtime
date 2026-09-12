package sandbox

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
)

type Docker struct {
	// Fault is local executable wiring, never request/profile configuration.
	Fault          func(string, JobSpec) error
	Host           string
	Binary         string
	MaxLogBytes    int
	CommandTimeout time.Duration
	WorkspaceQuota WorkspaceQuotaVerifier
}

func (d *Docker) Check(ctx context.Context) error {
	out, err := d.run(ctx, "info", "--format", "{{json .}}")
	if err != nil {
		return fmt.Errorf("%w: docker info: %v", ErrUnavailable, err)
	}
	return checkDaemonInfo(out)
}

func checkDaemonInfo(out []byte) error {
	var info struct {
		SecurityOptions []string
		CgroupVersion   string
		MemoryLimit     bool
		PidsLimit       bool
		CPUCfsQuota     bool
		CPUCfsPeriod    bool
	}
	if json.Unmarshal(out, &info) != nil {
		return fmt.Errorf("%w: invalid daemon capability response", ErrUnavailable)
	}
	rootless := false
	for _, option := range info.SecurityOptions {
		if option == "name=rootless" {
			rootless = true
		}
	}
	if !rootless || info.CgroupVersion != "2" || !info.MemoryLimit || !info.PidsLimit || !info.CPUCfsQuota || !info.CPUCfsPeriod {
		return fmt.Errorf("%w: rootless daemon with delegated cgroup v2 memory/PID/CPU limits required", ErrUnavailable)
	}
	return nil
}

func (d *Docker) Start(ctx context.Context, s JobSpec) (Job, error) {
	if s.Capture != nil {
		if _, err := d.logSocket(); err != nil {
			return Job{}, err
		}
	}
	if err := d.Check(ctx); err != nil {
		return Job{}, err
	}
	if err := domain.ID(s.ID).Validate(); err != nil {
		return Job{}, err
	}
	p := s.Profile
	if p.WorkspaceQuotaBytes <= 0 || d.WorkspaceQuota == nil {
		return Job{}, fmt.Errorf("%w: an enforceable writable-workspace quota is required", ErrUnavailable)
	}
	if err := d.WorkspaceQuota.VerifyWorkspaceQuota(ctx, s.Workspace, p.WorkspaceQuotaBytes); err != nil {
		return Job{}, fmt.Errorf("%w: workspace quota: %v", ErrUnavailable, err)
	}
	imageParts := strings.Split(p.Image, "@sha256:")
	var imageDigest []byte
	if len(imageParts) == 2 {
		imageDigest, _ = hex.DecodeString(imageParts[1])
	}
	if p.ID == "" || len(imageDigest) != 32 || p.MemoryBytes <= 0 || p.PIDs <= 0 || p.CPUs <= 0 || p.User == "" || strings.Split(p.User, ":")[0] == "0" || strings.Split(p.User, ":")[0] == "root" || len(s.Command) == 0 || !filepath.IsAbs(s.Workspace) || strings.Contains(s.Workspace, ",") {
		return Job{}, fmt.Errorf("%w: untrusted or unconstrained sandbox profile", domain.ErrInvalid)
	}
	if !s.Deadline.After(time.Now()) {
		return Job{}, context.DeadlineExceeded
	}
	// An existing name is evidence to inspect, never permission to restart.
	if job, err := d.Inspect(ctx, s.ID); err == nil {
		return job, nil
	} else if err != ErrJobNotFound {
		return Job{}, err
	}
	workspaceMount := "type=bind,src=" + s.Workspace + ",dst=/workspace"
	if s.TrustedVerification {
		workspaceMount += ",readonly"
	}
	tmpfsMount := "/tmp:rw,nosuid,nodev,noexec,size=67108864"
	if p.TmpfsExecutable {
		tmpfsMount = "/tmp:rw,nosuid,nodev,exec,size=67108864"
	}
	args := []string{"create", "--name", s.ID, "--label", "forge.runtime=1", "--read-only", "--network", "none", "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--user", p.User, "--pids-limit", strconv.FormatInt(p.PIDs, 10), "--memory", strconv.FormatInt(p.MemoryBytes, 10), "--memory-swap", strconv.FormatInt(p.MemoryBytes, 10), "--cpus", strconv.FormatFloat(p.CPUs, 'f', 3, 64), "--log-driver", "local", "--log-opt", "max-size=1m", "--log-opt", "max-file=2", "--tmpfs", tmpfsMount, "--workdir", "/workspace", "--mount", workspaceMount, "--init"}
	if s.Capture != nil {
		if s.OperationID.Validate() != nil {
			return Job{}, domain.ErrInvalid
		}
		args = append(args, "--label", "forge.operation_id="+string(s.OperationID))
		for i, arg := range args {
			if arg == "--log-driver" {
				args = append(append(args[:i:i], "--log-driver", "none"), args[i+6:]...)
				break
			}
		}
	}
	if s.TrustedVerification && p.TrustedTestsDir != "" {
		if !filepath.IsAbs(p.TrustedTestsDir) || strings.Contains(p.TrustedTestsDir, ",") {
			return Job{}, domain.ErrInvalid
		}
		args = append(args, "--mount", "type=bind,src="+p.TrustedTestsDir+",dst=/forge-tests,readonly")
	}
	args = append(args, p.Image)
	args = append(args, s.Command...)
	created, err := d.run(ctx, args...)
	if err != nil {
		return Job{}, err
	}
	if s.OnCreated != nil {
		id := strings.TrimSpace(string(created))
		decoded, err := hex.DecodeString(id)
		if err != nil || len(decoded) != 32 || hex.EncodeToString(decoded) != id {
			return Job{}, domain.ErrReconciliation
		}
		if err = s.OnCreated(ctx, id); err != nil {
			return Job{}, err
		}
	}
	if d.Fault != nil {
		if err := d.Fault("after_docker_create", s); err != nil {
			return Job{}, err
		}
	}
	if s.Capture != nil {
		return d.startCaptured(ctx, s)
	}
	if s.BeforeStart != nil {
		if err := s.BeforeStart(ctx); err != nil {
			return Job{}, err
		}
	}
	if _, err := d.run(ctx, "start", s.ID); err != nil {
		return Job{}, err
	}
	if d.Fault != nil {
		if err := d.Fault("after_docker_start", s); err != nil {
			return Job{}, err
		}
	}
	return d.Inspect(ctx, s.ID)
}

func (d *Docker) Inspect(ctx context.Context, id string) (Job, error) {
	if err := domain.ID(id).Validate(); err != nil {
		return Job{}, err
	}
	out, err := d.run(ctx, "container", "inspect", id)
	if err != nil {
		if strings.Contains(err.Error(), "No such container") || strings.Contains(err.Error(), "No such object") {
			return Job{}, ErrJobNotFound
		}
		return Job{}, err
	}
	var rows []struct {
		ID         string
		Name       string
		HostConfig struct{ LogConfig struct{ Type string } }
		Config     struct{ Labels map[string]string }
		State      struct {
			Running   bool
			ExitCode  int
			StartedAt string
			Error     string
		}
	}
	if json.Unmarshal(out, &rows) != nil || len(rows) != 1 || rows[0].Config.Labels["forge.runtime"] != "1" {
		return Job{}, fmt.Errorf("%w: foreign or malformed job", domain.ErrConflict)
	}
	r := rows[0]
	job := Job{ID: id, ContainerID: r.ID, ContainerName: r.Name, StrictLogs: r.HostConfig.LogConfig.Type == "none", Running: r.State.Running, ExitCode: r.State.ExitCode, Started: r.State.StartedAt != "" && !strings.HasPrefix(r.State.StartedAt, "0001-"), Error: r.State.Error}
	if !job.Running && !job.StrictLogs {
		logs, err := d.runOutput(ctx, true, "logs", id)
		if err != nil {
			return Job{}, err
		}
		job.Output = logs
		max := d.MaxLogBytes
		if max <= 0 {
			max = 1 << 20
		}
		if len(job.Output) > max {
			job.Output = job.Output[:max]
			job.Truncated = true
		}
	}
	return job, nil
}

func (d *Docker) Cancel(ctx context.Context, id string) (Job, error) {
	job, err := d.Inspect(ctx, id)
	if err != nil {
		return Job{}, err
	}
	if !job.Running {
		if !job.Started {
			// An observed created state alone cannot exclude an in-flight Start.
			return Job{}, domain.ErrReconciliation
		}
		return job, nil
	}
	if _, err = d.run(ctx, "kill", id); err != nil {
		return Job{}, err
	}
	job, err = d.Inspect(ctx, id)
	if err == nil && !job.Running {
		job.Interrupted = true
	}
	return job, err
}

// CancelNeverDispatched is for the runner's durable start-intent=0 proof only.
// Caller must own the workspace writer lock, with no previous writer alive.
func (d *Docker) CancelNeverDispatched(ctx context.Context, id string) (Job, error) {
	job, err := d.Inspect(ctx, id)
	if err != nil {
		return Job{}, err
	}
	if job.Running || job.Started {
		return Job{}, domain.ErrReconciliation
	}
	if _, err = d.run(ctx, "container", "rm", id); err != nil {
		return Job{}, err
	}
	job.NeverStarted = true
	return job, nil
}

// run returns only protocol stdout. CLI diagnostics are not JSON/container IDs.
func (d *Docker) run(ctx context.Context, args ...string) ([]byte, error) {
	return d.runOutput(ctx, false, args...)
}

// combineLogs is used only for legacy `docker logs`: that command routes the
// actual container stderr through CLI stderr. It cannot distinguish those bytes
// from CLI warnings, so preserve its existing bounded combined-output semantics.
// Strict logging uses the separate daemon attach protocol and never this path.
func (d *Docker) runOutput(ctx context.Context, combineLogs bool, args ...string) ([]byte, error) {
	if len(args) == 0 {
		return nil, domain.ErrInvalid
	}
	timeout := d.CommandTimeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	bin := d.Binary
	if bin == "" {
		bin = "docker"
	}
	cmd := exec.CommandContext(ctx, bin, args...)
	if d.Host != "" {
		cmd.Env = append(os.Environ(), "DOCKER_HOST="+d.Host)
	}
	limit := d.MaxLogBytes
	if limit <= 0 {
		limit = 1 << 20
	}
	// One extra byte preserves legacy truncation detection, without integer wrap
	// for an extreme operator-supplied limit. Diagnostic memory is independently
	// bounded: total retained bytes <= captureLimit + min(captureLimit, 64 KiB).
	captureLimit := limit
	if captureLimit < int(^uint(0)>>1) {
		captureLimit++
	}
	stdout := &boundedBuffer{max: captureLimit}
	stderr := &boundedBuffer{max: min(captureLimit, 64<<10)}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	if combineLogs {
		cmd.Stderr = stdout
	}
	err := cmd.Run()
	if err != nil {
		if combineLogs {
			return nil, fmt.Errorf("docker %s: %w: output: %s", args[0], err, stdout.Bytes())
		}
		return nil, fmt.Errorf("docker %s: %w: stdout: %s; stderr: %s", args[0], err, stdout.Bytes(), stderr.Bytes())
	}
	return stdout.Bytes(), nil
}

// Write always consumes all bytes, even after truncation, so command output
// cannot block a subprocess on a full pipe or grow host memory without bound.
type boundedBuffer struct {
	// Composition prevents bytes.Buffer.ReadFrom from bypassing our bounded
	// Write when os/exec drains a pipe with io.Copy.
	buffer bytes.Buffer
	max    int
}

func (b *boundedBuffer) Len() int      { return b.buffer.Len() }
func (b *boundedBuffer) Bytes() []byte { return b.buffer.Bytes() }

func (b *boundedBuffer) Write(p []byte) (int, error) {
	n := len(p)
	left := b.max - b.Len()
	if left > 0 {
		if left > n {
			left = n
		}
		_, _ = b.buffer.Write(p[:left])
	}
	return n, nil
}

var _ io.Writer = (*boundedBuffer)(nil)
