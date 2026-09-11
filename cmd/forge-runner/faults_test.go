package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/runner"
)

func planFixture(t *testing.T) (string, string, faultPlan) {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "operator-faults")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return root, filepath.Join(dir, "case.json"), faultPlan{Point: "after_prepared", Action: "delay", TenantID: "tenant", RunID: "run", WorkspaceID: "workspace", OperationID: "operation", Epoch: 1, ExpiresAt: time.Now().Add(time.Minute), DelayMillis: 1}
}
func writePlan(t *testing.T, path string, p faultPlan) {
	t.Helper()
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
}
func TestFaultPlanRejectsUnsafeScopeAndLocation(t *testing.T) {
	for _, what := range []string{"point", "operation", "epoch", "expired", "long_expiry", "long_delay", "public", "symlink", "outside"} {
		t.Run(what, func(t *testing.T) {
			root, file, p := planFixture(t)
			switch what {
			case "point":
				p.Point = "any"
			case "operation":
				p.OperationID = ""
			case "epoch":
				p.Epoch = 0
			case "expired":
				p.ExpiresAt = time.Now().Add(-time.Second)
			case "long_expiry":
				p.ExpiresAt = time.Now().Add(time.Hour)
			case "long_delay":
				p.DelayMillis = 10001
			}
			writePlan(t, file, p)
			switch what {
			case "public":
				os.Chmod(file, 0644)
			case "symlink":
				next := file + ".link"
				os.Symlink(file, next)
				file = next
			case "outside":
				root = filepath.Join(root, "other")
			}
			if _, err := loadFaultController(file, root); err == nil {
				t.Fatal("unsafe plan accepted")
			}
		})
	}
}
func TestFaultPlanExactScopeOneShotAcrossReload(t *testing.T) {
	root, file, p := planFixture(t)
	writePlan(t, file, p)
	c, err := loadFaultController(file, root)
	if err != nil {
		t.Fatal(err)
	}
	r := runner.OperationRequest{WorkspaceRequest: runner.WorkspaceRequest{TenantID: p.TenantID, RunID: p.RunID, WorkspaceID: p.WorkspaceID, Epoch: 2, Grant: "must not leak"}, OperationID: p.OperationID}
	if err = c.hit(p.Point, r); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(file + ".used"); !os.IsNotExist(err) {
		t.Fatal("mismatched epoch armed fault")
	}
	r.Epoch = 1
	if err = c.hit(p.Point, r); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(file + ".used")
	if err != nil {
		t.Fatal(err)
	}
	c, err = loadFaultController(file, root)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.hit(p.Point, r); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(file + ".used")
	if string(after) != string(before) {
		t.Fatal("fault replayed after reload")
	}
	var evidence map[string]any
	if err = json.Unmarshal(after, &evidence); err != nil {
		t.Fatal(err)
	}
	if _, ok := evidence["grant"]; ok {
		t.Fatal("grant leaked")
	}
}
