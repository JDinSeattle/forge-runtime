package localsetup

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestEnvironmentURLSurvivesShellSource(t *testing.T) {
	for _, value := range []string{
		"postgres://fixture:fake@127.0.0.1:5432/forge?sslmode=disable&connect_timeout=5",
		"literal 'quote' & $(exit 70) `exit 71` ; value",
	} {
		file := filepath.Join(t.TempDir(), "worker.env")
		if err := os.WriteFile(file, []byte("FORGE_DATABASE_URL="+shellQuote(value)+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command("/bin/sh", "-c", `set -a; . "$1"; printf '%s' "$FORGE_DATABASE_URL"`, "env-fixture", file)
		cmd.Env = []string{"PATH=/usr/bin:/bin", "FORGE_DATABASE_URL=inherited-admin-fixture"}
		got, err := cmd.Output()
		if err != nil || string(got) != value {
			t.Fatalf("sourced URL did not preserve the fixture value: %v", err)
		}
	}
}
