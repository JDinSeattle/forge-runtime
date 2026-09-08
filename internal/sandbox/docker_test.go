package sandbox

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDockerFailsClosedWithoutActualRootlessCgroupCapabilities(t *testing.T) {
	for _, raw := range []string{
		`{"SecurityOptions":["name=apparmor","name=seccomp"],"CgroupVersion":"2","MemoryLimit":true,"PidsLimit":true,"CpuCfsQuota":true,"CpuCfsPeriod":true}`,
		`{"SecurityOptions":["name=rootless"],"CgroupVersion":"1","MemoryLimit":true,"PidsLimit":true,"CpuCfsQuota":true,"CpuCfsPeriod":true}`,
		`{"SecurityOptions":["name=rootless"],"CgroupVersion":"2","MemoryLimit":false,"PidsLimit":true,"CpuCfsQuota":true,"CpuCfsPeriod":true}`,
		`{broken`,
	} {
		if err := checkDaemonInfo([]byte(raw)); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("unsafe daemon accepted: %s: %v", raw, err)
		}
	}
	if err := checkDaemonInfo([]byte(`{"SecurityOptions":["name=rootless"],"CgroupVersion":"2","MemoryLimit":true,"PidsLimit":true,"CpuCfsQuota":true,"CpuCfsPeriod":true}`)); err != nil {
		t.Fatal(err)
	}
}
func TestBoundedLogWriterDrainsWithoutGrowing(t *testing.T) {
	b := &boundedBuffer{max: 128}
	input := bytes.Repeat([]byte("x"), 1<<20)
	for range 10 {
		n, err := b.Write(input)
		if err != nil || n != len(input) {
			t.Fatal("writer failed to drain")
		}
	}
	if b.Len() != 128 {
		t.Fatal("log buffer grew beyond limit")
	}
}

// This records the real Docker adapter's CLI admission arguments; it creates
// no container and makes no kernel-isolation claim.
func TestDockerTmpfsExecutionIsExplicitAndKeepsFixedBounds(t *testing.T) {
	for _, executable := range []bool{false, true} {
		t.Run(fmt.Sprint(executable), func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("FORGE_DOCKER_STUB_DIR", dir)
			binary := filepath.Join(dir, "docker-stub")
			script := `#!/bin/sh
set -eu
case "$1" in
 info) printf '%s' '{"SecurityOptions":["name=rootless"],"CgroupVersion":"2","MemoryLimit":true,"PidsLimit":true,"CpuCfsQuota":true,"CpuCfsPeriod":true}' ;;
 container)
  if test -f "$FORGE_DOCKER_STUB_DIR/created"; then
   printf '%s' '[{"Config":{"Labels":{"forge.runtime":"1"}},"State":{"Running":true,"ExitCode":0,"StartedAt":"2026-09-08T00:00:00Z","Error":""}}]'
  else echo 'No such container' >&2;exit 1;fi ;;
 create) printf '%s\n' "$@" >"$FORGE_DOCKER_STUB_DIR/argv"; : >"$FORGE_DOCKER_STUB_DIR/created" ;;
 start) : ;;
 *) exit 2 ;;
esac
`
			if err := os.WriteFile(binary, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			d := Docker{Binary: binary, WorkspaceQuota: recordingQuota{}}
			_, err := d.Start(context.Background(), JobSpec{ID: "fixture", Workspace: dir, Profile: Profile{ID: "fixture", Image: "fixture@sha256:" + strings.Repeat("a", 64), MemoryBytes: 256 << 20, WorkspaceQuotaBytes: 256 << 20, CPUs: 1, PIDs: 64, User: "1000:1000", TmpfsExecutable: executable}, Command: []string{"fixture"}, Deadline: time.Now().Add(time.Minute), TrustedVerification: true})
			if err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(filepath.Join(dir, "argv"))
			if err != nil {
				t.Fatal(err)
			}
			args := strings.Split(strings.TrimSpace(string(raw)), "\n")
			tmpfsCount := 0
			mountCount := 0
			for i, arg := range args {
				if arg == "--tmpfs" {
					tmpfsCount++
					want := "/tmp:rw,nosuid,nodev,noexec,size=67108864"
					if executable {
						want = "/tmp:rw,nosuid,nodev,exec,size=67108864"
					}
					if i+1 >= len(args) || args[i+1] != want {
						t.Fatalf("unbounded or unexpected scratch mount: %s", raw)
					}
				}
				if arg == "--mount" {
					mountCount++
					if i+1 >= len(args) || args[i+1] != "type=bind,src="+dir+",dst=/workspace,readonly" {
						t.Fatalf("verification checkout became writable: %s", raw)
					}
				}
			}
			if tmpfsCount != 1 || mountCount != 1 {
				t.Fatalf("profile introduced additional mounts: %s", raw)
			}
		})
	}
}

type recordingQuota struct{}

func (recordingQuota) VerifyWorkspaceQuota(context.Context, string, int64) error { return nil }
