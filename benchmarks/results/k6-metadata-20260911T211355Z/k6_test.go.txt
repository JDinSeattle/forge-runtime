package benchmarks

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/persistence"
)

func TestK6MetadataEvidence(t *testing.T) {
	if os.Getenv("FORGE_RUN_K6_BENCHMARK") != "1" {
		t.Skip("opt-in actual k6 loopback workload")
	}
	t.Setenv("FORGE_RUN_HTTP_BENCHMARK", "1")
	TestHTTPMetadataEvidence(t)
}

func runK6Metadata(t *testing.T, admin *persistence.Store, endpoint, token, project, run, base string) {
	t.Helper()
	binary, err := exec.LookPath("k6")
	if err != nil {
		t.Fatal("install the documented k6 v2.2.0 first")
	}
	version, err := exec.Command(binary, "version").CombinedOutput()
	if err != nil || !strings.HasPrefix(string(version), "k6 v2.2.0 ") {
		t.Fatalf("expected pinned k6 v2.2.0, got %q: %v", version, err)
	}
	output := os.Getenv("FORGE_K6_EVIDENCE_DIR")
	if output == "" {
		output = filepath.Join("results", "k6-metadata-"+time.Now().UTC().Format("20060102T150405Z"))
	}
	output, err = filepath.Abs(output)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(output, 0700); err != nil {
		t.Fatal(err)
	}
	write := func(name string, data []byte) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(output, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("k6-version.txt", version)
	write("empty-config.json", []byte("{}\n"))
	// Exclude fixed warm-up reads from the measured k6 sample set.
	client := &http.Client{Timeout: 3 * time.Second}
	defer client.CloseIdleConnections()
	for range 100 {
		req, _ := http.NewRequest("GET", endpoint+"/v1/runs/"+run, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("X-Forge-Tenant", "http-tenant")
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, drain := io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != 200 || drain != nil {
			t.Fatalf("warmup failed: %d %v", res.StatusCode, drain)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	args := []string{"--config", filepath.Join(output, "empty-config.json"), "run", "--quiet", "--no-color", "--no-usage-report", "--summary-export", filepath.Join(output, "summary.json"), "--out", "json=" + filepath.Join(output, "metrics.jsonl"), "k6/metadata.js"}
	command := exec.CommandContext(ctx, binary, args...)
	command.Env = []string{"PATH=" + os.Getenv("PATH"), "LANG=C.UTF-8", "TZ=UTC", "FORGE_K6_URL=" + endpoint, "FORGE_K6_TOKEN=" + token, "FORGE_K6_PROJECT=" + project, "FORGE_K6_RUN=" + run, "FORGE_K6_BASE=" + base}
	started := time.Now()
	log, runErr := command.CombinedOutput()
	write("execution.log", log)
	var runs, keys, created, attempts, effects int
	if err := admin.Pool.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM runs),(SELECT count(*) FROM idempotency_keys),(SELECT count(*) FROM run_events WHERE type='run.created'),(SELECT count(*) FROM model_attempts),(SELECT count(*) FROM effects)`).Scan(&runs, &keys, &created, &attempts, &effects); err != nil {
		t.Fatal(err)
	}
	script, _ := os.ReadFile("k6/metadata.js")
	scriptHash := sha256.Sum256(script)
	binaryBytes, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	binaryHash := sha256.Sum256(binaryBytes)
	write("metadata.js.txt", script)
	for _, name := range []string{"k6_test.go", "http_test.go"} {
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		write(name+".txt", body)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	testBinary, err := os.ReadFile(executable)
	if err != nil {
		t.Fatal(err)
	}
	testHash := sha256.Sum256(testBinary)
	report := map[string]any{"scope": "actual k6 and TCP/private PG metadata only; no worker, runner, model or container", "machine": machine(), "source_sha256": sourceHashes(t), "k6_version": strings.TrimSpace(string(version)), "k6_binary_sha256": hex.EncodeToString(binaryHash[:]), "script_sha256": hex.EncodeToString(scriptHash[:]), "arguments": args, "elapsed_seconds": time.Since(started).Seconds(), "warmup_reads": 100, "offered_rps": 50, "window_seconds": 20, "read_write_ratio": "90:10", "max_vus": 16, "api_pool_max": 16, "nonowner_rls": true, "persisted_runs_including_seed": runs, "idempotency_keys_including_seed": keys, "run_created_events_including_seed": created, "model_attempts": attempts, "effects": effects, "process_success": runErr == nil}
	report["go_test_binary_sha256"] = hex.EncodeToString(testHash[:])
	body, _ := json.MarshalIndent(report, "", "  ")
	write("report.json", append(body, '\n'))
	t.Logf("k6 evidence=%s process_success=%v persisted_runs=%d", output, runErr == nil, runs)
	if runErr != nil || runs != 101 || keys != 101 || created != 101 || attempts != 0 || effects != 0 {
		t.Fatalf("k6 workload/ledger failed: %v runs=%d keys=%d created=%d attempts=%d effects=%d", runErr, runs, keys, created, attempts, effects)
	}
}
