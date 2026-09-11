package review_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/httpapi"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestReviewReservedPriorityIsBoundedAndPreservesFIFO(t *testing.T) {
	ctx, owner, api := reviewAPIStore(t)
	if err := owner.BootstrapTenant(ctx, "priority_tenant", "priority_user", "developer"); err != nil {
		t.Fatal(err)
	}
	if err := owner.RegisterRunner(ctx, "priority_runner", "http://runner.invalid", 2); err != nil {
		t.Fatal(err)
	}
	project, err := owner.CreateProject(ctx, "priority_tenant", "priority", "fixture", "python")
	if err != nil {
		t.Fatal(err)
	}
	token, err := owner.IssueToken(ctx, "priority_user", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	cfg := persistence.Config{Provider: "fake", Model: "fixture", MaxModelRounds: 2, MaxToolCalls: 3, MaxCost: 1000, MaxRuntimeSeconds: 120}
	server := httptest.NewServer((&httpapi.Server{Store: api, Sources: map[string]httpapi.Source{"fixture": {BaseCommit: "fixed"}}, Configs: map[string]persistence.Config{"fixture": cfg}}).Handler())
	defer server.Close()
	post := func(key string, priority any, want int) domain.ID {
		t.Helper()
		body := map[string]any{"task": "repair", "base_commit": "fixed", "config_id": "fixture", "budget": map[string]any{}}
		if priority != "omitted" {
			body["priority"] = priority
		}
		raw, _ := json.Marshal(body)
		req, err := http.NewRequestWithContext(ctx, "POST", server.URL+"/v1/projects/"+string(project.ID)+"/runs", bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("X-Forge-Tenant", "priority_tenant")
		req.Header.Set("Idempotency-Key", key)
		req.Header.Set("Content-Type", "application/json")
		response, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != want {
			t.Fatalf("priority %v: status=%d want=%d", priority, response.StatusCode, want)
		}
		var answer struct {
			RunID domain.ID `json:"run_id"`
		}
		if err := json.NewDecoder(response.Body).Decode(&answer); err != nil {
			t.Fatal(err)
		}
		return answer.RunID
	}
	first := post("first", -2, 202)
	second := post("second", 2, 202)
	ordinary := post("ordinary", "omitted", 202)
	if got := post("first", -2, 202); got != first {
		t.Fatal("priority replay created a new run")
	}
	post("first", 2, 409)
	if got := post("ordinary", 0, 202); got != ordinary {
		t.Fatal("explicit zero differs from the default priority")
	}
	for _, invalid := range []any{-3, 3, 1.5, "high", nil} {
		post("invalid", invalid, 400)
	}
	for id, want := range map[domain.ID]int{first: -2, second: 2, ordinary: 0} {
		run, err := api.GetRun(ctx, "priority_tenant", id)
		if err != nil || run.Priority != want {
			t.Fatalf("stored priority=%d want=%d err=%v", run.Priority, want, err)
		}
	}
	for _, expected := range []domain.ID{first, second} {
		claimed, err := owner.Claim(ctx, "priority_worker", time.Minute)
		if err != nil || claimed.ID != expected {
			t.Fatalf("reserved priority changed FIFO: got=%s want=%s err=%v", claimed.ID, expected, err)
		}
	}
	_, err = owner.Pool.Exec(ctx, `UPDATE runs SET priority=3 WHERE tenant_id=$1 AND id=$2`, "priority_tenant", ordinary)
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) || pgerr.Code != "23514" {
		t.Fatalf("database allowed an out-of-range priority: %v", err)
	}
	var runs, keys int
	if err := owner.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM runs),(SELECT count(*) FROM idempotency_keys)`).Scan(&runs, &keys); err != nil || runs != 3 || keys != 3 {
		t.Fatalf("invalid priority consumed admission: runs=%d keys=%d err=%v", runs, keys, err)
	}
}
