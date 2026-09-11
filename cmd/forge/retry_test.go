package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	api "github.com/JDinSeattle/forge-runtime/internal/httpcontract"
)

func TestSubmitParentBindingIsIncludedInIdempotencyReceipt(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var body api.Submit
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body.ParentRunId == nil || *body.ParentRunId != "run_parent" {
			t.Errorf("parent binding=%v", body.ParentRunId)
		}
		serveJSON(w, 202, api.SubmitResult{RunId: "run_child"})
	}))
	defer server.Close()
	commandEnv(t, server.URL)
	args := []string{"run", "submit", "project_test", "--task", "same task", "--base", "same_base", "--config", "fixture", "--idempotency-key", "child-retry", "--parent-run"}
	for range 2 {
		if out, _, err := execute(append(args, "run_parent")...); err != nil || !strings.Contains(out, "run_child") {
			t.Fatalf("same child replay: %s %v", out, err)
		}
	}
	if _, _, err := execute(append(args, "run_other")...); err == nil {
		t.Fatal("receipt accepted changed parent")
	}
	if calls.Load() != 1 {
		t.Fatalf("unexpected submissions: %d", calls.Load())
	}
}
