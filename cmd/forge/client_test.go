package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	api "github.com/JDinSeattle/forge-runtime/internal/httpcontract"
)

func commandEnv(t *testing.T, endpoint string) {
	t.Helper()
	t.Setenv("FORGE_API_URL", endpoint)
	t.Setenv("FORGE_TOKEN", "local-test-token")
	t.Setenv("FORGE_TENANT", "tenant_test")
	t.Setenv("FORGE_STATE_DIR", t.TempDir())
}
func execute(args ...string) (string, string, error) {
	cmd := newCommand()
	var out, diagnostics bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&diagnostics)
	cmd.SetArgs(args)
	err := cmd.ExecuteContext(context.Background())
	return out.String(), diagnostics.String(), err
}
func serveJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func TestSubmitUncertaintyRetainsKeyWithoutAutomaticReplay(t *testing.T) {
	var requests atomic.Int32
	var mu sync.Mutex
	var keys []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/projects/project_test/runs" || r.Header.Get("Authorization") != "Bearer local-test-token" || r.Header.Get("X-Forge-Tenant") != "tenant_test" || r.URL.RawQuery != "" {
			t.Errorf("request headers/path wrong")
		}
		var body api.Submit
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body.Task != "fix fixture" || body.ConfigId != "demo" || body.Budget.MaxModelRounds == nil || *body.Budget.MaxModelRounds != 3 || body.Budget.MaxNoProgressBatches == nil || *body.Budget.MaxNoProgressBatches != 2 {
			t.Errorf("body=%+v", body)
		}
		mu.Lock()
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		mu.Unlock()
		if requests.Add(1) == 1 {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			conn.Close()
			return
		}
		serveJSON(w, 202, api.SubmitResult{RunId: "run_saved", Reused: true})
	}))
	defer server.Close()
	commandEnv(t, server.URL)
	args := []string{"run", "submit", "project_test", "--task", "fix fixture", "--base", "base", "--config", "demo", "--max-rounds", "3", "--max-no-progress-batches", "2"}
	_, diagnostics, err := execute(args...)
	if err == nil || requests.Load() != 1 {
		t.Fatalf("ambiguous mutation was retried: count=%d err=%v", requests.Load(), err)
	}
	if !strings.Contains(diagnostics, "Idempotency-Key:") {
		t.Fatal("key was not displayed")
	}
	files, err := filepath.Glob(filepath.Join(os.Getenv("FORGE_STATE_DIR"), "*.json"))
	if err != nil || len(files) != 1 {
		t.Fatal("key not saved", files, err)
	}
	info, err := os.Stat(files[0])
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("receipt is not private", err)
	}
	out, _, err := execute(args...)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "run_saved") {
		t.Fatal(out)
	}
	if _, _, err = execute(args...); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if requests.Load() != 2 || len(keys) != 2 || keys[0] == "" || keys[0] != keys[1] {
		t.Fatalf("duplicate run risk: count=%d keys=%v", requests.Load(), keys)
	}
}

func TestExplicitIdempotencyKeyBindsBody(t *testing.T) {
	s := settings{endpoint: "http://127.0.0.1:8080", tenant: "tenant", stateDir: t.TempDir()}
	r, _, err := s.receipt("/route", map[string]string{"task": "one"}, "my-key")
	if err != nil || r.Key != "my-key" {
		t.Fatal(r, err)
	}
	if _, _, err = s.receipt("/route", map[string]string{"task": "two"}, "my-key"); err == nil {
		t.Fatal("same explicit key accepted changed payload")
	}
}

func TestWatchReconnectDeduplicatesAndStopsAtFinished(t *testing.T) {
	var count atomic.Int32
	var mu sync.Mutex
	var cursors []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		cursors = append(cursors, r.Header.Get("Last-Event-ID"))
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		if count.Add(1) == 1 {
			fmt.Fprint(w, eventWire(1, "run.created")+eventWire(2, "model.text_delta"))
			return
		}
		fmt.Fprint(w, eventWire(2, "model.text_delta")+eventWire(3, "run.finished"))
	}))
	defer server.Close()
	s := settings{endpoint: server.URL, tenant: "tenant", token: "test"}
	client, err := s.client()
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	err = watch(context.Background(), client, "run_test", &out, io.Discard, watchOptions{maxReconnects: 3, idleTimeout: time.Second, wait: func(context.Context, time.Duration) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(cursors) != 2 || cursors[0] != "0" || cursors[1] != "2" {
		t.Fatalf("cursors=%v", cursors)
	}
	if strings.Count(out.String(), "\n") != 3 {
		t.Fatal("duplicate event was printed", out.String())
	}
}
func eventWire(seq uint64, kind string) string {
	raw, _ := json.Marshal(api.Event{RunId: "run_test", Seq: seq, Type: kind, SchemaVersion: 1, Payload: map[string]interface{}{}, CreatedAt: time.Now().UTC()})
	return fmt.Sprintf("id: %d\nevent: %s\ndata: %s\n\n", seq, kind, raw)
}

func TestWatchExpiredCursorFetchesSnapshot(t *testing.T) {
	var count atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/runs/run_test/events":
			if count.Add(1) == 1 {
				serveJSON(w, 410, api.APIError{Code: "reset_required"})
				return
			}
			if r.Header.Get("Last-Event-ID") != "5" {
				t.Error("snapshot cursor not used")
			}
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, eventWire(6, "run.finished"))
		case "/v1/runs/run_test/snapshot":
			serveJSON(w, 200, api.Run{Id: "run_test", CoveredSeq: 5, State: api.State{RunId: "run_test", Status: api.RunStatusRunning}})
		default:
			t.Error("unexpected request", r.URL.Path)
		}
	}))
	defer server.Close()
	client, err := (settings{endpoint: server.URL, tenant: "tenant", token: "test"}).client()
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err = watch(context.Background(), client, "run_test", &out, io.Discard, watchOptions{after: 1, maxReconnects: 3, idleTimeout: time.Second, wait: func(context.Context, time.Duration) error { return nil }}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "snapshot.reset") || !strings.Contains(out.String(), "run.finished") {
		t.Fatal(out.String())
	}
}

func TestSSEBoundsAndPartialFrame(t *testing.T) {
	for _, tc := range []struct {
		name, input string
		wantError   bool
		wantSeq     uint64
	}{
		{"partial", strings.TrimSuffix(eventWire(1, "run.created"), "\n"), false, 0},
		{"oversize", "data: " + strings.Repeat("x", maxEventBytes+1), true, 0},
		{"identity", strings.ReplaceAll(eventWire(1, "run.created"), "run_test", "other"), true, 0},
		{"gap", eventWire(2, "run.created"), true, 0},
		{"heartbeat", ": keepalive\n\n" + eventWire(1, "run.finished"), false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cursor uint64
			_, err := consumeEvents(strings.NewReader(tc.input), "run_test", &cursor, io.Discard)
			if errors.Is(err, errStreamProtocol) != tc.wantError || cursor != tc.wantSeq {
				t.Fatalf("cursor=%d err=%v", cursor, err)
			}
		})
	}
}

func TestCredentialRedirectIsRefused(t *testing.T) {
	var destinationCalls atomic.Int32
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { destinationCalls.Add(1) }))
	defer destination.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, destination.URL, 302) }))
	defer origin.Close()
	commandEnv(t, origin.URL)
	if _, _, err := execute("run", "snapshot", "run_test"); err == nil {
		t.Fatal("redirect accepted")
	}
	if destinationCalls.Load() != 0 {
		t.Fatal("credentials followed redirect")
	}
	for _, endpoint := range []string{"https://user:token@example.com", "https://example.com?token=secret", "http://example.com"} {
		if _, err := (settings{endpoint: endpoint, tenant: "tenant", token: "secret"}).client(); err == nil {
			t.Fatal("unsafe credential endpoint accepted", endpoint)
		}
	}
}

func TestDownloadRejectsCorruptionAndPreservesDestination(t *testing.T) {
	content := []byte("patch contents\n")
	sum := sha256.Sum256(content)
	goodHash := hex.EncodeToString(sum[:])
	var correct atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", fmt.Sprint(len(content)))
		hash := strings.Repeat("0", 64)
		if correct.Load() {
			hash = goodHash
		}
		w.Header().Set("ETag", "\""+hash+"\"")
		w.Write(content)
	}))
	defer server.Close()
	commandEnv(t, server.URL)
	output := filepath.Join(t.TempDir(), "patch.diff")
	if _, _, err := execute("run", "download", "artifact_test", "--output", output); err == nil {
		t.Fatal("corrupt artifact accepted")
	}
	if _, err := os.Stat(output); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("corrupt file published")
	}
	correct.Store(true)
	if _, _, err := execute("run", "download", "artifact_test", "--output", output); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(output)
	if err != nil || !bytes.Equal(raw, content) {
		t.Fatal("valid artifact missing", err)
	}
	if _, _, err = execute("run", "download", "artifact_test", "--output", output); err == nil {
		t.Fatal("existing destination overwritten")
	}
}

func TestApprovalUsesSavedBindingInsteadOfFetchingNewPermission(t *testing.T) {
	var got api.ApprovalDecisionBody
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != "POST" || r.URL.Path != "/v1/approvals/approval_test/decision" {
			t.Error("unreviewed binding fetched")
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Error(err)
		}
		serveJSON(w, 200, api.Run{Id: "run_test"})
	}))
	defer server.Close()
	commandEnv(t, server.URL)
	approval := api.Approval{Id: "approval_test", RunId: "run_test", Binding: api.ApprovalBinding{EffectId: "effect_test", ArgsHash: "saved-hash", Version: 4, WorkspaceRevision: 9, PolicyVersion: "policy_1"}}
	raw, _ := json.Marshal(approval)
	file := filepath.Join(t.TempDir(), "approval.json")
	if err := os.WriteFile(file, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := execute("run", "approve", "approval_test", "--binding-file", file, "--allow"); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || got.Binding.ArgsHash != "saved-hash" || got.Binding.Version != 4 || !got.Approve {
		t.Fatalf("approval=%+v", got)
	}
}

func TestSnapshotPreservesLargeNumbersInToolArguments(t *testing.T) {
	raw := `{"id":"run_test","state":{"pending_effect":{"args":{"counter":9007199254740993}}}}`
	response := &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(raw))}
	var run api.Run
	if err := decodeResponse(response, &run, 200); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(run)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"counter":9007199254740993`) {
		t.Fatal("tool argument integer rounded while displaying snapshot", string(encoded))
	}
}
