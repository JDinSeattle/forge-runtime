package httpapi_test

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/dependency"
	"github.com/JDinSeattle/forge-runtime/internal/eventstream"
	"github.com/JDinSeattle/forge-runtime/internal/httpapi"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/testutil"
)

func TestSSESurvivesIndependentDatabaseDeadline(t *testing.T) {
	s := testutil.Database(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.BootstrapTenant(ctx, "deadline_tenant", "deadline_user", "developer"); err != nil {
		t.Fatal(err)
	}
	p, err := s.CreateProject(ctx, "deadline_tenant", "deadline fixture", "source", "profile")
	if err != nil {
		t.Fatal(err)
	}
	r, _, err := s.Submit(ctx, persistence.SubmitRequest{TenantID: "deadline_tenant", PrincipalID: "deadline_user", ProjectID: p.ID, Task: "long SSE", BaseCommit: "fixed", Config: persistence.Config{Provider: "fake", Model: "fake", MaxModelRounds: 1, MaxToolCalls: 1, MaxRuntimeSeconds: 30}}, "initial")
	if err != nil {
		t.Fatal(err)
	}
	token, err := s.IssueToken(ctx, r.PrincipalID, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	streams := eventstream.New(ctx, s, eventstream.Config{})
	defer streams.Close()
	api := &httpapi.Server{Store: s, Streams: streams}
	server := httptest.NewServer(api.Handler())
	defer server.Close()
	req, err := http.NewRequestWithContext(ctx, "GET", server.URL+"/v1/runs/"+string(r.ID)+"/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Forge-Tenant", string(r.TenantID))
	response, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatalf("SSE HTTP %d", response.StatusCode)
	}
	ids := make(chan string, 16)
	done := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(response.Body)
		for scanner.Scan() {
			if strings.HasPrefix(scanner.Text(), "id: ") {
				ids <- scanner.Text()
			}
		}
		done <- scanner.Err()
	}()
	select {
	case id := <-ids:
		if id != "id: 1" {
			t.Fatal(id)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	timer := time.NewTimer(dependency.DatabaseTimeout + 150*time.Millisecond)
	select {
	case <-timer.C:
	case err := <-done:
		timer.Stop()
		t.Fatalf("DB/auth deadline killed SSE: %v", err)
	}
	_, _, err = s.AddMessage(ctx, persistence.Identity{TenantID: r.TenantID, PrincipalID: r.PrincipalID, Role: "developer"}, r.ID, "after database deadline", "later")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-ids:
		if id != fmt.Sprint("id: ", 2) {
			t.Fatal(id)
		}
	case err := <-done:
		t.Fatalf("stream ended early: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	_ = response.Body.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reader did not stop")
	}
}
