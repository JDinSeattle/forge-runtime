//go:build linux

package applicationfaults

import (
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
)

// The second attempt failed after creating one private queued run, before any
// runner or worker start. Keep its complete failed evidence and read-only final
// observation separate from the third attempt's fresh schema and processes.
func slLogs02Prerequisite(a slAcceptance, c slRunnerConfig, j slJournal) (map[string]string, error) {
	hashes := map[string]string{}
	failed := filepath.Join(a.ScopeRoot, "evidence", "logs-02")
	observed := filepath.Join(a.ScopeRoot, "evidence", "logs-02-before-runner-observation")
	for _, item := range []struct{ dir, hash string }{
		{failed, "fb68b27b8a05a22f7c572d64788b044f81fdc4111c3c07a0dda9a724d1842da2"},
		{observed, "9ac0d6f20dcd256af3b8feaedef407b41e98cb481f532e37248284fd32217e2c"},
	} {
		if err := slCleanupManifest(item.dir, item.hash, hashes); err != nil {
			return nil, err
		}
	}
	var prior struct {
		Acceptance slAcceptance `json:"acceptance"`
		Journal    slJournal    `json:"journal"`
	}
	if err := slReadJSON(filepath.Join(failed, "preflight.json"), &prior); err != nil {
		return nil, err
	}
	if err := slValidateContract(prior.Acceptance, c); err != nil {
		return nil, err
	}
	if err := slHistoricalBinding(a, prior.Acceptance, prior.Acceptance, true); err != nil {
		return nil, err
	}
	var recorded slJournal
	if err := slReadJSON(filepath.Join(observed, "journal.json"), &recorded); err != nil {
		return nil, err
	}
	if !slCleanupSameJournal(prior.Journal, j) || !slCleanupSameJournal(recorded, j) {
		return nil, fmt.Errorf("logs02 before-runner journal changed")
	}
	for _, item := range []struct{ path, hash string }{
		{filepath.Join(a.ScopeRoot, "acceptance-logs-02.json"), "5998c98594c5bd466fb58625931ad7ecf8c3a57ed0d26c46b111868e6dd14a3e"},
		{filepath.Join(a.ScopeRoot, "evidence/host-logs-02-retry/intent.json"), "3aebaaa6e1148e26c9403859358a5f304746d0849088c69e4637662e2fa53bae"},
		{filepath.Join(a.ScopeRoot, "evidence/host-logs-02-retry/result.json"), "b7bb3fa5f5baddc1e9b988d1aaeefd69f9c93eec16f36b920b6eb0e200e30f30"},
		{filepath.Join(c.RootDir, "operator-faults/strict-log-publication.json"), "d9a34f57a0da180085d0e8afe4a52f5b6889daaf4a8aa48a24c26973c4c3d2cd"},
		{filepath.Join(c.RootDir, "operator-faults/strict-log-publication.json.used"), "2f9b1402d5885a4dfbdf98db97d1b53170688cc03383c20950b669ee31f9cc52"},
		{prior.Acceptance.RunnerBinary, prior.Acceptance.RunnerSHA},
		{prior.Acceptance.WorkerBinary, prior.Acceptance.WorkerSHA},
		{prior.Acceptance.TestBinary, prior.Acceptance.TestSHA},
	} {
		if _, err := slCleanupHashFile(item.path, hashes, item.hash); err != nil {
			return nil, err
		}
	}
	return hashes, nil
}

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
	if (execution == "02" || execution == "03") && attempt == "02" {
		name, private = "acceptance-logs-"+execution+".json", "strict-logs-private-"+execution
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
	for _, execution := range []string{"", "01", "02", "03"} {
		name, logLeaf, private := "acceptance-02.json", "logs-01", "strict-logs-private"
		if execution == "02" || execution == "03" {
			name, logLeaf, private = "acceptance-logs-"+execution+".json", "logs-"+execution, "strict-logs-private-"+execution
		}
		d, p, err := slExecutionPaths(next, filepath.Join(scope, name), execution)
		if err != nil || d != filepath.Join(scope, "evidence", logLeaf) || p != filepath.Join(scope, "runtime", private) {
			t.Fatalf("execution %q: %s %s %v", execution, d, p, err)
		}
	}
	for _, bad := range []struct{ input, execution string }{{"acceptance-02.json", "02"}, {"acceptance-logs-02.json", "01"}, {"acceptance-02.json", "03"}, {"acceptance-logs-cleanup-01.json", "02"}, {"acceptance-logs-03.json", "02"}, {"acceptance-logs-03.json", "04"}} {
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
