package localsetup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/JDinSeattle/forge-runtime/internal/runner"
)

func TestFixtureProfilesPreservePythonAndSeparateTrustedGoVerifier(t *testing.T) {
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	f, err := LoadFixtures(context.Background(), repo)
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Sources) != 4 || len(f.Profiles) != 4 || len(f.Scripts) != 4 {
		t.Fatal("expected three Python sources plus one Go source")
	}
	for _, id := range []string{"clamp", "expiry", "ranges"} {
		source, ok := f.Sources[id]
		if !ok || source.ProfileID != "python-"+id || !source.HasTarget {
			t.Fatalf("Python fixture %s changed", id)
		}
		p := f.Profiles[source.ProfileID]
		if p.TmpfsExecutable {
			t.Fatal("Python must retain noexec tmpfs")
		}
		if !reflect.DeepEqual(p.TargetCommand, []string{"python", "-I", "-B", "/forge-tests/check.py", id, "target"}) || !reflect.DeepEqual(p.VerifyCommand, []string{"python", "-I", "-B", "/forge-tests/check.py", id, "regression"}) {
			t.Fatalf("Python verification behavior changed: %+v", p)
		}
	}
	goSource := f.Sources["go-ceil-div"]
	p := f.Profiles[goSource.ProfileID]
	if !goSource.HasTarget || p.ID != "go-ceil-div" || p.User != "1000:1000" || p.WorkspaceQuotaBytes != 256<<20 || p.MemoryBytes != 256<<20 || p.PIDs != 64 || !p.TmpfsExecutable {
		t.Fatal("Go profile lost fixed isolation bounds")
	}
	if p.TargetCommand[len(p.TargetCommand)-1] != "target" || p.VerifyCommand[len(p.VerifyCommand)-1] != "regression" || p.TrustedTestsDir != filepath.Join(repo, "testdata", "graders") {
		t.Fatal("Go target and regression must be independent trusted commands")
	}
	if _, err = os.Stat(filepath.Join(goSource.Path, "go-check.sh")); !os.IsNotExist(err) {
		t.Fatal("trusted grader entered writable source")
	}
}
func TestGoDemoPatchBindsBaselineAndProducesExpectedSnapshot(t *testing.T) {
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	f, err := LoadFixtures(context.Background(), repo)
	if err != nil {
		t.Fatal(err)
	}
	source := f.Sources["go-ceil-div"]
	before, err := os.ReadFile(filepath.Join(source.Path, "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	var patch runner.PatchArgs
	script := f.Scripts["go-ceil-div"][1]
	if err = json.Unmarshal([]byte(script.Chunks[1].Delta), &patch); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(before)
	if len(patch.Files) != 1 || patch.Files[0].Path != "main.go" || patch.Files[0].ExpectedSHA256 != hex.EncodeToString(digest[:]) || patch.Files[0].Content == nil {
		t.Fatal("demo patch lacks exact baseline binding")
	}
	candidate := t.TempDir()
	gomod, err := os.ReadFile(filepath.Join(source.Path, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(candidate, "go.mod"), gomod, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(candidate, "main.go"), []byte(*patch.Files[0].Content), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := runner.ComputeSourceHash(context.Background(), candidate, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	want, err := runner.ComputeSourceHash(context.Background(), filepath.Join(repo, "testdata", "repairs", "go-ceil-div", "expected"), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != want || got == source.Hash {
		t.Fatal("Go patch did not produce the expected different snapshot")
	}
}
