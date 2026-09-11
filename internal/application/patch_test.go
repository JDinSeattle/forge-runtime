package application

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/JDinSeattle/forge-runtime/internal/runner"
)

func TestUnifiedPatchActuallyAppliesWithGit(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git unavailable")
	}
	dir := t.TempDir()
	// TempDir may be inside the checkout (for example with GOTMPDIR). Without
	// its own repository, git apply can discover the parent and skip every
	// patch path as outside the current prefix while still returning success.
	cmd := exec.Command("git", "--git-dir", filepath.Join(dir, ".git"), "--work-tree", dir, "init", "--quiet")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("initialize isolated patch repository: %v %s", err, out)
	}
	hash := func(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }
	files := []runner.DiffEntry{
		{Path: "space name.txt", Before: "old without newline", After: "new\nsecond", BeforeSHA256: hash("old without newline"), AfterSHA256: hash("new\nsecond")},
		{Path: "new.txt", After: "added\n", AfterSHA256: hash("added\n")},
		{Path: "removed.txt", Before: "gone\n", BeforeSHA256: hash("gone\n"), Deleted: true},
		{Path: "script.sh", Before: "exit 0\n", After: "exit 0\n", BeforeSHA256: hash("exit 0\n"), AfterSHA256: hash("exit 0\n"), AfterExecutable: true},
		{Path: "empty.txt", AfterSHA256: hash("")},
	}
	for _, f := range files {
		if f.BeforeSHA256 != "" {
			if err := os.WriteFile(filepath.Join(dir, f.Path), []byte(f.Before), 0644); err != nil {
				t.Fatal(err)
			}
		}
	}
	body, _ := json.Marshal(map[string]any{"files": files})
	patch, err := unifiedPatch(body)
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(dir, "repair.patch")
	if err := os.WriteFile(name, patch, 0600); err != nil {
		t.Fatal(err)
	}
	cmd = exec.Command("git", "apply", "--check", name)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("check: %v %s\n%s", err, out, patch)
	}
	cmd = exec.Command("git", "apply", name)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("apply: %v %s", err, out)
	}
	for _, f := range files {
		got, err := os.ReadFile(filepath.Join(dir, f.Path))
		if f.Deleted {
			if !os.IsNotExist(err) {
				t.Fatalf("delete %s: %v", f.Path, err)
			}
			continue
		}
		if err != nil || string(got) != f.After {
			t.Fatalf("content %s: %q %v", f.Path, got, err)
		}
		info, err := os.Stat(filepath.Join(dir, f.Path))
		if err != nil {
			t.Fatal(err)
		}
		if (info.Mode()&0111 != 0) != f.AfterExecutable {
			t.Fatalf("mode %s", f.Path)
		}
	}
}
