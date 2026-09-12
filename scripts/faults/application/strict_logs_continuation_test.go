//go:build linux

package applicationfaults

import (
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
)

// SIGTERM source and log execution are independent dimensions. Historical
// evidence is never relabeled with the binary used for a later log execution.
func slExecutionPaths(a slAcceptance, input, execution string) (string, string, error) {
	attempt, err := lifecycleAttempt(a.ScopeRoot, a.EvidenceDir)
	if err != nil {
		return "", "", err
	}
	if execution == "" {
		execution = "01"
	}
	name := "acceptance.json"
	if attempt == "02" {
		name = "acceptance-02.json"
	}
	private := "strict-logs-private"
	if execution == "02" && attempt == "02" {
		name, private = "acceptance-logs-02.json", "strict-logs-private-02"
	} else if execution != "01" {
		return "", "", fmt.Errorf("unsupported log execution or historical source")
	}
	if input != filepath.Join(a.ScopeRoot, name) {
		return "", "", fmt.Errorf("log execution must select its explicit frozen manifest")
	}
	return filepath.Join(a.ScopeRoot, "evidence", "logs-"+execution), filepath.Join(a.ScopeRoot, "runtime", private), nil
}

func slHistoricalBinding(current, historical, sealed slAcceptance, newer bool) error {
	if historical != sealed {
		return fmt.Errorf("historical E50 input differs from its original sealed manifest")
	}
	if !newer {
		if current != historical {
			return fmt.Errorf("original logs execution requires original E50 binaries")
		}
		return nil
	}
	if current.EvidenceDir != filepath.Join(current.ScopeRoot, "evidence", "sigterm-02") || current.TestBinary == historical.TestBinary {
		return fmt.Errorf("second logs execution requires new binaries and SIGTERM02")
	}
	projected := current
	projected.RunnerBinary, projected.RunnerSHA = historical.RunnerBinary, historical.RunnerSHA
	projected.WorkerBinary, projected.WorkerSHA = historical.WorkerBinary, historical.WorkerSHA
	projected.TestBinary, projected.TestSHA = historical.TestBinary, historical.TestSHA
	if projected != historical {
		return fmt.Errorf("new logs execution changed historical non-binary authority")
	}
	return nil
}

func slHistoricalAuthority(current, historical slAcceptance, c slRunnerConfig, newer bool) (map[string]string, error) {
	if err := slValidateContract(historical, c); err != nil {
		return nil, err
	}
	attempt, err := lifecycleAttempt(current.ScopeRoot, current.EvidenceDir)
	if err != nil {
		return nil, err
	}
	name := "acceptance.json"
	if attempt == "02" {
		name = "acceptance-02.json"
	}
	path := filepath.Join(current.ScopeRoot, name)
	var sealed slAcceptance
	if err := slReadJSON(path, &sealed); err != nil {
		return nil, err
	}
	if err := slHistoricalBinding(current, historical, sealed, newer); err != nil {
		return nil, err
	}
	hashes := map[string]string{}
	for _, item := range []struct{ path, hash string }{
		{path, ""},
		{filepath.Join(current.EvidenceDir, "worker-runner-sigterm", "acceptance-input.json"), ""},
		{historical.RunnerBinary, historical.RunnerSHA},
		{historical.WorkerBinary, historical.WorkerSHA},
		{historical.TestBinary, historical.TestSHA},
	} {
		raw, err := slRead(item.path, 128<<20)
		if err != nil {
			return nil, err
		}
		hash := slSHA(raw)
		if item.hash != "" && item.hash != hash {
			return nil, fmt.Errorf("historical E50 executable bytes changed")
		}
		hashes[item.path] = hash
	}
	return hashes, nil
}

func TestStrictLogsExecutionAndHistoricalAuthority(t *testing.T) {
	scope := "/fixture/lifecycle-rehearsals/lr20260912_a"
	old := slAcceptance{ScopeRoot: scope, EvidenceDir: filepath.Join(scope, "evidence/sigterm-02"), RunnerConfig: "fixed-config", PoolRoot: "fixed-pool", RunnerBinary: "old-runner", RunnerSHA: "old-r", WorkerBinary: "old-worker", WorkerSHA: "old-w", TestBinary: "old-test", TestSHA: "old-t"}
	next := old
	next.RunnerBinary, next.RunnerSHA, next.WorkerBinary, next.WorkerSHA, next.TestBinary, next.TestSHA = "new-runner", "new-r", "new-worker", "new-w", "new-test", "new-t"
	for _, execution := range []string{"", "01", "02"} {
		name, logLeaf, private := "acceptance-02.json", "logs-01", "strict-logs-private"
		if execution == "02" {
			name, logLeaf, private = "acceptance-logs-02.json", "logs-02", "strict-logs-private-02"
		}
		d, p, err := slExecutionPaths(next, filepath.Join(scope, name), execution)
		if err != nil || d != filepath.Join(scope, "evidence", logLeaf) || p != filepath.Join(scope, "runtime", private) {
			t.Fatalf("execution %q: %s %s %v", execution, d, p, err)
		}
	}
	for _, bad := range []struct{ input, execution string }{{"acceptance-02.json", "02"}, {"acceptance-logs-02.json", "01"}, {"acceptance-02.json", "03"}, {"acceptance-logs-cleanup-01.json", "02"}} {
		if _, _, err := slExecutionPaths(next, filepath.Join(scope, bad.input), bad.execution); err == nil {
			t.Fatal("accepted mismatched execution", bad)
		}
	}
	if err := slHistoricalBinding(next, old, old, true); err != nil {
		t.Fatal(err)
	}
	if err := slHistoricalBinding(old, old, old, false); err != nil {
		t.Fatal(err)
	}
	if err := slHistoricalBinding(next, old, old, false); err == nil {
		t.Fatal("original logs silently changed binaries")
	}
	if err := slHistoricalBinding(next, next, old, true); err == nil {
		t.Fatal("relabelled historical binaries accepted")
	}
	for _, field := range []string{"Purpose", "FixtureID", "ScopeRoot", "PoolRoot", "RunnerConfig", "EvidenceDir"} {
		changed := next
		reflect.ValueOf(&changed).Elem().FieldByName(field).SetString("changed")
		if err := slHistoricalBinding(changed, old, old, true); err == nil {
			t.Fatal("changed shared authority accepted", field)
		}
	}
}
