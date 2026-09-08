package localsetup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/JDinSeattle/forge-runtime/internal/configuration"
	"github.com/JDinSeattle/forge-runtime/internal/provider"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
)

// FixtureSet is trusted, repository-owned setup input. Loading it reads only
// source/expected fixture files; it does not run candidate code, contact Docker,
// write a live configuration, mint credentials or change database state.
type FixtureSet struct {
	Sources  map[string]configuration.Source `json:"sources"`
	Profiles map[string]sandbox.Profile      `json:"profiles"`
	Scripts  map[string][]provider.Script    `json:"fake_scripts"`
}

func LoadFixtures(ctx context.Context, repoDir string) (FixtureSet, error) {
	set := FixtureSet{Sources: map[string]configuration.Source{}, Profiles: map[string]sandbox.Profile{}, Scripts: map[string][]provider.Script{}}
	type definition struct {
		id, file string
		profile  sandbox.Profile
	}
	definitions := []definition{}
	for _, id := range []string{"clamp", "expiry", "ranges"} {
		profileID := "python-" + id
		definitions = append(definitions, definition{id, "app.py", sandbox.Profile{ID: profileID, Image: "python:3.12.13-slim", TargetCommand: []string{"python", "-I", "-B", "/forge-tests/check.py", id, "target"}, VerifyCommand: []string{"python", "-I", "-B", "/forge-tests/check.py", id, "regression"}}})
	}
	goCommand := func(suite string) []string {
		return []string{"env", "-i", "PATH=/usr/local/go/bin:/usr/bin:/bin", "sh", "/forge-tests/go-check.sh", suite}
	}
	definitions = append(definitions, definition{"go-ceil-div", "main.go", sandbox.Profile{ID: "go-ceil-div", Image: "golang:1.26-bookworm", TmpfsExecutable: true, TargetCommand: goCommand("target"), VerifyCommand: goCommand("regression")}})
	for _, d := range definitions {
		source := filepath.Join(repoDir, "testdata", "repairs", d.id, "source")
		hash, err := runner.ComputeSourceHash(ctx, source, 0, 0)
		if err != nil {
			return FixtureSet{}, err
		}
		p := d.profile
		p.TrustedTestsDir = filepath.Join(repoDir, "testdata", "graders")
		p.MemoryBytes = 256 << 20
		p.WorkspaceQuotaBytes = 256 << 20
		p.CPUs = 1
		p.PIDs = 64
		p.User = "1000:1000"
		set.Sources[d.id] = configuration.Source{Path: source, Hash: hash, ProfileID: p.ID, HasTarget: true}
		set.Profiles[p.ID] = p
		before, err := os.ReadFile(filepath.Join(source, d.file))
		if err != nil {
			return FixtureSet{}, err
		}
		after, err := os.ReadFile(filepath.Join(repoDir, "testdata", "repairs", d.id, "expected", d.file))
		if err != nil {
			return FixtureSet{}, err
		}
		digest := sha256.Sum256(before)
		content := string(after)
		patch, err := json.Marshal(runner.PatchArgs{Files: []runner.FileEdit{{Path: d.file, ExpectedSHA256: hex.EncodeToString(digest[:]), Content: &content}}})
		if err != nil {
			return FixtureSet{}, err
		}
		read, err := json.Marshal(runner.ReadArgs{Path: d.file})
		if err != nil {
			return FixtureSet{}, err
		}
		set.Scripts[d.id] = []provider.Script{tool("read", "read_file", string(read)), tool("patch", "apply_patch", string(patch)), tool("finish", "finish", `{}`)}
	}
	return set, nil
}
