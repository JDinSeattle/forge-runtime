package sandbox

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A local executable is the only CLI invoked by these tests. There is no daemon,
// socket, container, user mapping, volume, provider or credential involved.
func dockerOutputStub(t *testing.T, script string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "docker-output-stub")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nset -eu\n"+script), 0700); err != nil {
		t.Fatal(err)
	}
	return path
}

const dockerInfoFixture = `{"SecurityOptions":["name=rootless"],"CgroupVersion":"2","MemoryLimit":true,"PidsLimit":true,"CpuCfsQuota":true,"CpuCfsPeriod":true}`
const dockerCLIWarning = "WARNING: Error loading config file: open /root/.docker/config.json: permission denied"

func TestDockerCLIWarningDoesNotCorruptProtocolOutput(t *testing.T) {
	binary := dockerOutputStub(t, "printf '%s\\n' '"+dockerCLIWarning+"' >&2\nprintf '%s' '"+dockerInfoFixture+"'\n")
	d := Docker{Binary: binary}
	if err := d.Check(context.Background()); err != nil {
		t.Fatalf("valid daemon stdout was contaminated by stderr: %v", err)
	}
}

func TestDockerCLIWarningDoesNotCorruptCreateAndInspect(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("DOCKER_OUTPUT_STUB_DIR", dir)
	id := strings.Repeat("a", 64)
	binary := dockerOutputStub(t, `
printf '%s\n' "$1" >>"$DOCKER_OUTPUT_STUB_DIR/calls"
printf '%s\n' '`+dockerCLIWarning+`' >&2
case "$1" in
 info) printf '%s' '`+dockerInfoFixture+`' ;;
 container)
  if test -f "$DOCKER_OUTPUT_STUB_DIR/created"; then
   printf '%s' '[{"Id":"`+id+`","Config":{"Labels":{"forge.runtime":"1"}},"State":{"Running":true,"StartedAt":"2026-09-12T00:00:00Z"}}]'
  else printf 'No such container' >&2;exit 1;fi ;;
 create) : >"$DOCKER_OUTPUT_STUB_DIR/created";printf '%s\n' '`+id+`' ;;
 start) printf 'fixture\n' ;;
 *) exit 2 ;;
esac
`)
	var observed string
	d := Docker{Binary: binary, WorkspaceQuota: recordingQuota{}}
	job, err := d.Start(context.Background(), JobSpec{ID: "fixture", Workspace: dir, Profile: Profile{ID: "fixture", Image: "fixture@sha256:" + strings.Repeat("b", 64), MemoryBytes: 256 << 20, WorkspaceQuotaBytes: 256 << 20, CPUs: 1, PIDs: 64, User: "1000:1000"}, Command: []string{"fixture"}, Deadline: time.Now().Add(time.Minute), OnCreated: func(_ context.Context, id string) error { observed = id; return nil }})
	if err != nil || observed != id || job.ContainerID != id || !job.Running {
		t.Fatalf("wrong create/inspect result: observed=%q job=%+v err=%v", observed, job, err)
	}
	calls, err := os.ReadFile(filepath.Join(dir, "calls"))
	if err != nil {
		t.Fatal(err)
	}
	if string(calls) != "info\ncontainer\ncreate\nstart\ncontainer\n" {
		t.Fatalf("retry or changed command order: %q", calls)
	}
}

func TestDockerCLIFailureRetainsBothBoundedDiagnostics(t *testing.T) {
	binary := dockerOutputStub(t, "printf 'stdout-detail';printf 'stderr-detail' >&2;exit 7\n")
	d := Docker{Binary: binary, MaxLogBytes: 32}
	out, err := d.run(context.Background(), "info")
	var exit *exec.ExitError
	if out != nil || !errors.As(err, &exit) || exit.ExitCode() != 7 || !strings.Contains(err.Error(), "stdout-detail") || !strings.Contains(err.Error(), "stderr-detail") {
		t.Fatalf("lost CLI failure diagnostics: %q %v", out, err)
	}
}

func TestDockerLegacyLogsKeepContainerStderrAndTruncation(t *testing.T) {
	binary := dockerOutputStub(t, `
case "$1" in
 container) printf '%s' '[{"Config":{"Labels":{"forge.runtime":"1"}},"HostConfig":{"LogConfig":{"Type":"local"}},"State":{"Running":false,"ExitCode":1,"StartedAt":"2026-09-12T00:00:00Z"}}]' ;;
 logs) printf 'container-stdout';printf 'container-stderr' >&2 ;;
 *) exit 2 ;;
esac
`)
	d := Docker{Binary: binary}
	job, err := d.Inspect(context.Background(), "fixture")
	if err != nil || job.Truncated || !bytes.Contains(job.Output, []byte("container-stdout")) || !bytes.Contains(job.Output, []byte("container-stderr")) {
		t.Fatalf("legacy stderr lost: %+v %v", job, err)
	}
	d.MaxLogBytes = 256 // enough for the inspect JSON, smaller than long log output
	d.Binary = dockerOutputStub(t, `
case "$1" in
 container) printf '%s' '[{"Config":{"Labels":{"forge.runtime":"1"}},"State":{"Running":false,"StartedAt":"2026-09-12T00:00:00Z"}}]' ;;
 logs) i=0;while test "$i" -lt 1000;do printf 'stdout';printf 'stderr' >&2;i=$((i+1));done ;;
 *) exit 2 ;;
esac
`)
	job, err = d.Inspect(context.Background(), "fixture")
	if err != nil || !job.Truncated || len(job.Output) != 256 {
		t.Fatalf("legacy truncation changed: bytes=%d truncated=%v err=%v", len(job.Output), job.Truncated, err)
	}
}

func TestDockerCLISeparateBuffersDrainAndRemainBounded(t *testing.T) {
	binary := dockerOutputStub(t, "i=0;while test \"$i\" -lt 10000;do printf 'stdout';printf 'stderr' >&2;i=$((i+1));done\n")
	d := Docker{Binary: binary, MaxLogBytes: 1024, CommandTimeout: 5 * time.Second}
	out, err := d.run(context.Background(), "info")
	if err != nil || len(out) != 1025 || bytes.Contains(out, []byte("stderr")) {
		t.Fatalf("stdout bound or isolation: %d %v", len(out), err)
	}
	d.Binary = dockerOutputStub(t, "i=0;while test \"$i\" -lt 10000;do printf 'stdout';printf 'stderr' >&2;i=$((i+1));done;exit 9\n")
	_, err = d.run(context.Background(), "info")
	if err == nil || len(err.Error()) > 2*1025+256 || !strings.Contains(err.Error(), "stdout") || !strings.Contains(err.Error(), "stderr") {
		t.Fatal("failure diagnostics exceed two bounded prefixes or lost a stream")
	}
}

func TestDockerCLICommandTimeoutIsUnchanged(t *testing.T) {
	d := Docker{Binary: dockerOutputStub(t, "exec /bin/sleep 5\n"), CommandTimeout: 20 * time.Millisecond}
	started := time.Now()
	_, err := d.run(context.Background(), "info")
	if err == nil || time.Since(started) > time.Second {
		t.Fatalf("CLI timeout ignored: %v", err)
	}
}

// Hide the source's WriterTo so io.Copy must use its destination path, just as
// os/exec's pipe drain can. An embedded bytes.Buffer.ReadFrom bypassed Write.
func TestDockerCLIBufferCopyCannotBypassLimit(t *testing.T) {
	b := &boundedBuffer{max: 128}
	src := struct{ io.Reader }{strings.NewReader(strings.Repeat("x", 10000))}
	n, err := io.Copy(b, src)
	if err != nil || n != 10000 || b.Len() != 128 {
		t.Fatalf("copy bypassed bound: copied=%d retained=%d err=%v", n, b.Len(), err)
	}
}
