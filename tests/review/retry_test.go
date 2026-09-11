package review_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/httpapi"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	flow "github.com/JDinSeattle/forge-runtime/internal/runtime"
)

// Real HTTP and nonowner/RLS PostgreSQL; no model or runner is dispatched.
func TestReviewWholeTaskRetryCreatesOneChildWithoutReopeningParent(t *testing.T) {
	ctx, owner, api := reviewAPIStore(t)
	parent := reviewRun(t, ctx, owner)
	token, err := owner.IssueToken(ctx, parent.PrincipalID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer((&httpapi.Server{
		Store:   api,
		Sources: map[string]httpapi.Source{"local-fixture": {BaseCommit: parent.BaseCommit}},
		Configs: map[string]persistence.Config{"retry-fixture": parent.Config},
	}).Handler())
	defer server.Close()
	body := httpapi.SubmitBody{Task: parent.Task, BaseCommit: parent.BaseCommit, ConfigID: "retry-fixture", ParentRunID: parent.ID}
	request := func(method, path, key, bearer string, value any) (int, []byte, error) {
		raw, err := json.Marshal(value)
		if err != nil {
			return 0, nil, err
		}
		req, err := http.NewRequestWithContext(ctx, method, server.URL+path, bytes.NewReader(raw))
		if err != nil {
			return 0, nil, err
		}
		req.Header.Set("Authorization", "Bearer "+bearer)
		req.Header.Set("X-Forge-Tenant", string(parent.TenantID))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", key)
		res, err := server.Client().Do(req)
		if err != nil {
			return 0, nil, err
		}
		defer res.Body.Close()
		data, err := io.ReadAll(res.Body)
		return res.StatusCode, data, err
	}
	path := "/v1/projects/" + string(parent.ProjectID) + "/runs"
	if status, raw, err := request("POST", path, "retry-child", token, body); err != nil || status != 409 {
		t.Fatalf("nonterminal parent accepted: status=%d body=%s err=%v", status, raw, err)
	}
	identity := persistence.Identity{TenantID: parent.TenantID, PrincipalID: parent.PrincipalID, Role: "developer"}
	parent, err = owner.Cancel(ctx, identity, parent.ID)
	if err != nil || parent.State.Status != domain.StatusCancelRequested {
		t.Fatalf("cancel unused parent: %+v %v", parent.State, err)
	}
	parent, err = owner.ClaimOnRunner(ctx, "retry-fixture-stop", time.Minute, "review_runner")
	if err != nil {
		t.Fatal(err)
	}
	// A control-plane fixture stop receipt, not evidence of a real container
	// cancellation. This run has never prepared a workspace or dispatched work.
	ref := domain.ID("retry_fixture_no_execution")
	if err := owner.PublishArtifact(ctx, persistence.Artifact{TenantID: parent.TenantID, RunID: parent.ID, ID: ref, Kind: "stop_receipt", ObjectKey: string(parent.TenantID) + "/" + string(parent.ID) + "/fixture", SHA256: fmt.Sprintf("%064d", 0), ByteSize: 0}); err != nil {
		t.Fatal(err)
	}
	parent, err = owner.Advance(ctx, parent.TenantID, parent.ID, flow.Event{Kind: flow.EventCancellationConfirmed, ExpectedVersion: parent.State.Version, Owner: parent.State.Lease.Owner, Epoch: parent.State.Lease.Epoch, Stop: &flow.StopReceipt{Ref: string(ref), NoActiveOperations: true}})
	if err != nil || parent.State.Status != domain.StatusCancelled {
		t.Fatalf("terminal fixture: %+v %v", parent.State, err)
	}
	// Admission captures the immutable input itself; retry must copy those
	// exact bytes without reconstructing them from the mutable child workspace.
	var parentInput string
	if err := owner.Pool.QueryRow(ctx, `SELECT input_snapshot FROM runs WHERE tenant_id=$1 AND id=$2`, parent.TenantID, parent.ID).Scan(&parentInput); err != nil {
		t.Fatal(err)
	}
	var accepted struct {
		SchemaVersion int       `json:"schema_version"`
		ProjectID     domain.ID `json:"project_id"`
		Task          string    `json:"task"`
		BaseCommit    string    `json:"base_commit"`
		SourceID      string    `json:"source_id"`
		ProfileID     string    `json:"profile_id"`
	}
	if err := json.Unmarshal([]byte(parentInput), &accepted); err != nil || accepted.SchemaVersion != 1 || accepted.ProjectID != parent.ProjectID || accepted.Task != parent.Task || accepted.BaseCommit != parent.BaseCommit || accepted.SourceID != "local-fixture" || accepted.ProfileID != "python" {
		t.Fatalf("admission input snapshot=%q err=%v", parentInput, err)
	}
	parentBytes := func() []byte {
		var raw []byte
		if err := owner.Pool.QueryRow(ctx, `SELECT to_jsonb(r) FROM runs r WHERE tenant_id=$1 AND id=$2`, parent.TenantID, parent.ID).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		return raw
	}
	before := parentBytes()
	type answer struct {
		RunID  domain.ID `json:"run_id"`
		Reused bool      `json:"reused"`
	}
	const clients = 12
	answers := make(chan answer, clients)
	errors := make(chan error, clients)
	var wg sync.WaitGroup
	for range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, raw, err := request("POST", path, "retry-child", token, body)
			if err != nil || status != 202 {
				errors <- fmt.Errorf("retry status=%d body=%s err=%v", status, raw, err)
				return
			}
			var a answer
			if err := json.Unmarshal(raw, &a); err != nil {
				errors <- err
				return
			}
			answers <- a
		}()
	}
	wg.Wait()
	close(errors)
	close(answers)
	for err := range errors {
		t.Fatal(err)
	}
	var childID domain.ID
	newCount := 0
	for a := range answers {
		if childID == "" {
			childID = a.RunID
		}
		if a.RunID != childID || a.RunID == parent.ID {
			t.Fatal("retry identities differ or reuse parent")
		}
		if !a.Reused {
			newCount++
		}
	}
	if childID == "" || newCount != 1 {
		t.Fatalf("new children=%d id=%s", newCount, childID)
	}
	status, raw, err := request("GET", "/v1/runs/"+string(childID), "", token, nil)
	var child persistence.Run
	if err != nil || status != 200 || json.Unmarshal(raw, &child) != nil {
		t.Fatalf("child read status=%d body=%s err=%v", status, raw, err)
	}
	if child.ParentRunID != parent.ID || child.State.Status != domain.StatusQueued || child.State.Version != 1 || child.State.Lease.Epoch != 0 || child.State.WorkspaceRevision != 0 || child.State.ModelRounds != 0 || child.State.ToolCalls != 0 || child.State.Cost != 0 || child.RunnerID != "" {
		t.Fatalf("child did not receive fresh execution state: %+v", child)
	}
	if !child.State.Limits.Deadline.After(parent.State.Limits.Deadline) {
		t.Fatal("child reused parent runtime deadline")
	}
	var input string
	var count int
	if err := owner.Pool.QueryRow(ctx, `SELECT input_snapshot FROM runs WHERE id=$1`, child.ID).Scan(&input); err != nil || input != parentInput {
		t.Fatalf("input ref=%q err=%v", input, err)
	}
	for _, query := range []string{`SELECT count(*) FROM runs WHERE parent_run_id=$1`, `SELECT count(*) FROM run_events WHERE run_id=$2 AND type='run.created'`, `SELECT count(*) FROM idempotency_keys WHERE resource_id=$2`} {
		// Every query accepts both positions to keep the fixture IDs explicit.
		q := "SELECT n FROM (" + query + ") AS counts(n) WHERE $1::text IS NOT NULL AND $2::text IS NOT NULL"
		if err := owner.Pool.QueryRow(ctx, q, parent.ID, child.ID).Scan(&count); err != nil || count != 1 {
			t.Fatalf("child exactly-once count=%d err=%v", count, err)
		}
	}
	if !bytes.Equal(before, parentBytes()) {
		t.Fatal("retry changed terminal parent row")
	}
	if err := owner.BootstrapTenant(ctx, "retry_foreign", "retry_foreign_user", "developer"); err != nil {
		t.Fatal(err)
	}
	foreignProject, err := owner.CreateProject(ctx, "retry_foreign", "foreign", "local-fixture", "python")
	if err != nil {
		t.Fatal(err)
	}
	foreign, _, err := owner.Submit(ctx, persistence.SubmitRequest{TenantID: "retry_foreign", PrincipalID: "retry_foreign_user", ProjectID: foreignProject.ID, Task: parent.Task, BaseCommit: parent.BaseCommit, Config: parent.Config}, "foreign-parent")
	if err != nil {
		t.Fatal(err)
	}
	otherProject, err := owner.CreateProject(ctx, parent.TenantID, "other project", "local-fixture", "python")
	if err != nil {
		t.Fatal(err)
	}
	if status, raw, err := request("POST", "/v1/projects/"+string(otherProject.ID)+"/runs", "other-project-rejected", token, body); err != nil || status != 409 {
		t.Fatalf("parent crossed project: %d %s %v", status, raw, err)
	}
	if err := owner.BootstrapTenant(ctx, parent.TenantID, "retry_viewer", "viewer"); err != nil {
		t.Fatal(err)
	}
	viewer, err := owner.IssueToken(ctx, "retry_viewer", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if status, raw, err := request("POST", path, "viewer-rejected", viewer, body); err != nil || status != 403 {
		t.Fatalf("viewer created retry: %d %s %v", status, raw, err)
	}
	for _, tc := range []struct {
		name string
		body httpapi.SubmitBody
		want int
	}{
		{"changed task", httpapi.SubmitBody{Task: "different task", BaseCommit: parent.BaseCommit, ConfigID: body.ConfigID, ParentRunID: parent.ID}, 409},
		{"missing parent", httpapi.SubmitBody{Task: parent.Task, BaseCommit: parent.BaseCommit, ConfigID: body.ConfigID, ParentRunID: "run_missing"}, 404},
		{"foreign parent", httpapi.SubmitBody{Task: parent.Task, BaseCommit: parent.BaseCommit, ConfigID: body.ConfigID, ParentRunID: foreign.ID}, 404},
		{"unregistered base", httpapi.SubmitBody{Task: parent.Task, BaseCommit: "different_base", ConfigID: body.ConfigID, ParentRunID: parent.ID}, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, raw, err := request("POST", path, "rejected-"+tc.name, token, tc.body)
			if err != nil || status != tc.want {
				t.Fatalf("status=%d body=%s err=%v", status, raw, err)
			}
		})
	}
	for _, invalid := range []any{"", nil, 17, "run/invalid"} {
		value := map[string]any{"task": parent.Task, "base_commit": parent.BaseCommit, "config_id": body.ConfigID, "budget": map[string]any{}, "parent_run_id": invalid}
		if status, raw, err := request("POST", path, "invalid-parent", token, value); err != nil || status != 400 {
			t.Fatalf("invalid parent %v accepted: %d %s %v", invalid, status, raw, err)
		}
	}
	unknown := map[string]any{"task": parent.Task, "base_commit": parent.BaseCommit, "config_id": body.ConfigID, "budget": map[string]any{}, "parent_run_id": parent.ID, "unrecognized": true}
	if status, raw, err := request("POST", path, "invalid-parent", token, unknown); err != nil || status != 400 {
		t.Fatalf("unknown field accepted: %d %s %v", status, raw, err)
	}
	if status, raw, err := request("POST", "/v1/runs/"+string(parent.ID)+"/resume", "", token, map[string]any{"expected_version": parent.State.Version}); err != nil || status != 409 {
		t.Fatalf("terminal parent reopened: status=%d body=%s err=%v", status, raw, err)
	}
	// All rejected calls rolled back without even leaving an idempotency key.
	if err := owner.Pool.QueryRow(ctx, `SELECT count(*) FROM idempotency_keys WHERE tenant_id=$1`, parent.TenantID).Scan(&count); err != nil || count != 2 {
		t.Fatalf("unexpected accepted key count=%d err=%v", count, err)
	}
	if !bytes.Equal(before, parentBytes()) {
		t.Fatal("negative requests changed terminal parent")
	}
}
