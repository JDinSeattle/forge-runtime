package application

import (
	"context"
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
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/provider"
	"github.com/JDinSeattle/forge-runtime/internal/quota"
	flow "github.com/JDinSeattle/forge-runtime/internal/runtime"
	anthropicoption "github.com/anthropics/anthropic-sdk-go/option"
	openaioption "github.com/openai/openai-go/v3/option"
)

type handoffWireRequest struct {
	At        time.Time       `json:"at"`
	AttemptID string          `json:"attempt_id"`
	Body      json.RawMessage `json:"body"`
}

type handoffWire struct {
	mu        sync.Mutex
	requests  []handoffWireRequest
	responses [][]map[string]any
}

func (h *handoffWire) serve(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "fixture read", 500)
		return
	}
	h.mu.Lock()
	n := len(h.requests)
	h.requests = append(h.requests, handoffWireRequest{At: time.Now().UTC(), AttemptID: r.Header.Get("X-Client-Request-Id"), Body: body})
	var response []map[string]any
	if n < len(h.responses) {
		response = h.responses[n]
	}
	h.mu.Unlock()
	if response == nil {
		http.Error(w, "fixture exhausted", 400)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("x-request-id", fmt.Sprintf("local_request_%d", n))
	w.Header().Set("request-id", fmt.Sprintf("local_request_%d", n))
	for _, event := range response {
		raw, _ := json.Marshal(event)
		if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event["type"], raw); err != nil {
			return
		}
		w.(http.Flusher).Flush()
	}
}

func (h *handoffWire) captured() []handoffWireRequest {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]handoffWireRequest(nil), h.requests...)
}

// These are native SDK wire fixtures, not vendor/model quality observations.
func handoffEvents(name, id, tool, args, opaque string, interrupt bool) []map[string]any {
	type event = map[string]any
	if name == "openai" {
		events := []event{{"type": "response.created", "response": event{"id": id, "status": "in_progress"}}}
		output := []any{}
		if tool != "" {
			call := event{"id": "item_" + id, "type": "function_call", "call_id": id, "name": tool, "arguments": args, "status": "completed"}
			events = append(events, event{"type": "response.output_item.added", "output_index": 0, "item": event{"id": "item_" + id, "type": "function_call", "call_id": id, "name": tool, "arguments": ""}}, event{"type": "response.function_call_arguments.delta", "item_id": "item_" + id, "delta": args})
			if interrupt {
				return events
			}
			events = append(events, event{"type": "response.function_call_arguments.done", "item_id": "item_" + id, "arguments": args, "name": tool}, event{"type": "response.output_item.done", "output_index": 0, "item": call})
			output = append(output, call)
		} else {
			events = append(events, event{"type": "response.output_text.delta", "delta": "Ready for trusted verification."})
		}
		if opaque != "" {
			output = append(output, event{"id": "reason_" + id, "type": "reasoning", "encrypted_content": opaque, "summary": []any{}})
		}
		return append(events, event{"type": "response.completed", "response": event{"id": id, "status": "completed", "output": output, "usage": event{"input_tokens": 100, "output_tokens": 50, "input_tokens_details": event{"cached_tokens": 0}}}})
	}
	events := []event{{"type": "message_start", "message": event{"id": id, "type": "message", "role": "assistant", "model": "route-fixture", "content": []any{}, "usage": event{"input_tokens": 100, "output_tokens": 1, "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0}}}}
	index := 0
	if opaque != "" {
		events = append(events, event{"type": "content_block_start", "index": index, "content_block": event{"type": "thinking", "thinking": "", "signature": ""}}, event{"type": "content_block_delta", "index": index, "delta": event{"type": "thinking_delta", "thinking": "local fixture"}}, event{"type": "content_block_delta", "index": index, "delta": event{"type": "signature_delta", "signature": opaque}}, event{"type": "content_block_stop", "index": index})
		index++
	}
	reason := "end_turn"
	if tool != "" {
		reason = "tool_use"
		events = append(events, event{"type": "content_block_start", "index": index, "content_block": event{"type": "tool_use", "id": id, "name": tool, "input": event{}}}, event{"type": "content_block_delta", "index": index, "delta": event{"type": "input_json_delta", "partial_json": args}})
		if interrupt {
			return events
		}
	} else {
		events = append(events, event{"type": "content_block_start", "index": index, "content_block": event{"type": "text", "text": ""}}, event{"type": "content_block_delta", "index": index, "delta": event{"type": "text_delta", "text": "Ready for trusted verification."}})
	}
	return append(events, event{"type": "content_block_stop", "index": index}, event{"type": "message_delta", "delta": event{"stop_reason": reason, "stop_sequence": nil}, "usage": event{"output_tokens": 50}}, event{"type": "message_stop"})
}

func nativeHandoffProvider(t *testing.T, name string, h *handoffWire) provider.Provider {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(h.serve))
	t.Cleanup(server.Close)
	registry := provider.Registry{"route-fixture": {ToolCalling: true, ContextWindow: 128 << 10, MaxOutputTokens: 1024}}
	if name == "openai" {
		return provider.NewOpenAI(registry, provider.Limits{}, openaioption.WithBaseURL(server.URL), openaioption.WithAPIKey("local-fixture-only"))
	}
	return provider.NewAnthropic(registry, provider.Limits{}, anthropicoption.WithBaseURL(server.URL), anthropicoption.WithAPIKey("local-fixture-only"))
}

func claimHandoff(t *testing.T, d *Driver, owner string) persistence.Run {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	for {
		r, err := d.Store.Claim(ctx, owner, d.leaseDuration())
		if err == nil {
			return r
		}
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("runnable claim timed out")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func saveHandoffEvidence(t *testing.T, d *Driver, r persistence.Run, report map[string]any) {
	t.Helper()
	base := os.Getenv("FORGE_HANDOFF_EVIDENCE_DIR")
	if base == "" {
		return
	}
	ctx := context.Background()
	queries := map[string]string{
		"run":          `SELECT row_to_json(r) FROM (SELECT tenant_id,id,snapshot,config_snapshot,version,state,lease_epoch,lease_until FROM runs WHERE tenant_id=$1 AND id=$2) r`,
		"attempts":     `SELECT coalesce(json_agg(a ORDER BY step_seq,attempt),'[]') FROM model_attempts a WHERE tenant_id=$1 AND run_id=$2`,
		"effects":      `SELECT coalesce(json_agg(e ORDER BY step_seq,ordinal),'[]') FROM effects e WHERE tenant_id=$1 AND run_id=$2`,
		"events":       `SELECT coalesce(json_agg(e ORDER BY seq),'[]') FROM run_events e WHERE tenant_id=$1 AND run_id=$2`,
		"artifacts":    `SELECT coalesce(json_agg(a ORDER BY id),'[]') FROM artifacts a WHERE tenant_id=$1 AND run_id=$2`,
		"reservations": `SELECT coalesce(json_agg(q ORDER BY id),'[]') FROM quota_reservations q WHERE tenant_id=$1 AND run_id=$2`,
	}
	for key, query := range queries {
		var raw json.RawMessage
		if err := d.Store.Pool.QueryRow(ctx, query, r.TenantID, r.ID).Scan(&raw); err != nil {
			t.Error(err)
		} else {
			report[key] = raw
		}
	}
	rows, err := d.Store.Pool.Query(ctx, `SELECT id FROM artifacts WHERE tenant_id=$1 AND run_id=$2 ORDER BY id`, r.TenantID, r.ID)
	if err != nil {
		t.Error(err)
		return
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			t.Error(err)
		}
		ids = append(ids, id)
	}
	if err = rows.Err(); err != nil {
		t.Error(err)
	}
	rows.Close()
	objects := map[string]json.RawMessage{}
	bytes := map[string][]byte{}
	for _, id := range ids {
		meta, err := d.Store.GetArtifact(ctx, r.TenantID, domain.ID(id))
		if err != nil {
			t.Error(err)
			continue
		}
		reader, err := artifact.OpenWithDeadline(ctx, d.Artifacts, r.TenantID, r.ID, artifact.Ref{TenantID: r.TenantID, RunID: r.ID, Kind: meta.Kind, ObjectKey: meta.ObjectKey, SHA256: meta.SHA256, Size: meta.ByteSize})
		if err != nil {
			t.Error(err)
			continue
		}
		body, err := io.ReadAll(reader)
		closeErr := reader.Close()
		if err != nil || closeErr != nil {
			t.Errorf("object read %v close %v", err, closeErr)
			continue
		}
		bytes[id] = body
		if json.Valid(body) {
			objects[id] = json.RawMessage(body)
		}
	}
	report["objects"] = objects
	report["object_bytes_base64"] = bytes
	report["test"], report["failed"], report["captured_at"] = t.Name(), t.Failed(), time.Now().UTC()
	if err = os.MkdirAll(base, 0755); err != nil {
		t.Error(err)
		return
	}
	body, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Error(err)
		return
	}
	path := filepath.Join(base, strings.ReplaceAll(t.Name(), "/", "-")+".json")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		t.Error(err)
		return
	}
	defer f.Close()
	if _, err = f.Write(body); err != nil {
		t.Error(err)
	}
}

func TestNativeProviderHandoffDurableRepair(t *testing.T) {
	for _, primary := range []string{"openai", "anthropic"} {
		t.Run(primary, func(t *testing.T) {
			target := "anthropic"
			if primary == target {
				target = "openai"
			}
			d, r, scripts, path := repairSetupConfigured(t, func(c *persistence.Config) {
				c.Provider = primary
				c.Model = "route-fixture"
				c.Fallback = &persistence.ModelRoute{Provider: target, Model: "route-fixture"}
			})
			ctx := context.Background()
			primaryWire := &handoffWire{responses: [][]map[string]any{handoffEvents(primary, "source_read", "read_file", `{"path":"clamp.py"}`, "SOURCE_OPAQUE_SENTINEL", false), handoffEvents(primary, "uncommitted_patch", "apply_patch", `{"files":[`, "", true)}}
			targetWire := &handoffWire{responses: [][]map[string]any{handoffEvents(target, "target_patch", "apply_patch", scripts.scripts[1].Chunks[1].Delta, "TARGET_OPAQUE_SENTINEL", false), handoffEvents(target, "target_finish", "", "", "", false)}}
			d.Providers = map[string]provider.Provider{primary: nativeHandoffProvider(t, primary, primaryWire), target: nativeHandoffProvider(t, target, targetWire)}
			zero := domain.Money(0)
			for _, name := range []string{primary, target} {
				d.Models[name+"/route-fixture"] = ModelSpec{CredentialGroup: name, PriceVersion: "local-exact-fixture-v1", InputPrice: 1_000_000, OutputPrice: 1_000_000, CacheReadPrice: &zero, CacheWrite5mPrice: &zero, CacheWrite1hPrice: &zero, ExactPricing: true, ContextTokens: 64 << 10, MaxOutputTokens: 1024, RequestTimeout: 5 * time.Second}
				if err := d.Quota.Configure(ctx, quota.Config{CredentialGroup: name, MaxConcurrent: 4, MaxTokens: 1_000_000, MaxCost: 10_000_000, WindowDuration: time.Hour, FailureThreshold: 3, BreakerCooldown: time.Second}); err != nil {
					t.Fatal(err)
				}
			}
			report := map[string]any{"scope": "real local native SDK HTTP streams, private PG, production Driver and SQLite runner; TestBackend verification is protocol-only, no paid model or real Python execution", "primary": primary, "target": target}
			defer func() {
				report["primary_wire"] = primaryWire.captured()
				report["target_wire"] = targetWire.captured()
				saveHandoffEvidence(t, d, r, report)
			}()
			if err := d.Drive(ctx, claimHandoff(t, d, "primary-worker")); !errors.Is(err, errDeferred) {
				t.Fatalf("expected interrupted attempt deferral, got %v", err)
			}
			failed, err := d.Store.LatestAttempt(ctx, r.TenantID, r.ID, 2)
			if err != nil {
				t.Fatal(err)
			}
			policy, err := d.Store.AttemptFailure(ctx, failed)
			if err != nil {
				t.Fatal(err)
			}
			if failed.Status != "failed" || failed.ErrorCode != "stream_interrupted" || failed.Number != 1 || !policy.Retry {
				t.Fatalf("failure %+v policy %+v", failed, policy)
			}
			var effectCount int
			if err = d.Store.Pool.QueryRow(ctx, `SELECT count(*) FROM effects WHERE tenant_id=$1 AND run_id=$2 AND step_seq=2 AND ordinal>=0`, r.TenantID, r.ID).Scan(&effectCount); err != nil || effectCount != 0 {
				t.Fatalf("provisional tools executed: %d %v", effectCount, err)
			}
			unknown, err := d.Quota.Get(ctx, string(r.TenantID), string(failed.ID))
			if err != nil || unknown.Status != "unknown" || unknown.ActualCost != nil {
				t.Fatalf("unknown obligation %+v %v", unknown, err)
			}
			report["failed_obligation_before_handoff"] = unknown
			// Interrupt after the route/request/price transaction, before reservation.
			d.Fault = func(point string) error {
				if point == "after_model_pricing_before_reservation" {
					return context.Canceled
				}
				return nil
			}
			claimed := claimHandoff(t, d, "handoff-worker")
			if err = d.Drive(ctx, claimed); !errors.Is(err, context.Canceled) {
				t.Fatalf("prepared crash: %v", err)
			}
			prepared, err := d.Store.LatestAttempt(ctx, r.TenantID, r.ID, 2)
			if err != nil || prepared.Provider != target || prepared.Number != 2 || prepared.Status != "prepared" {
				t.Fatalf("prepared %+v %v", prepared, err)
			}
			var portable contextEnvelope
			if err = d.load(ctx, r, prepared.RequestRef, &portable); err != nil || portable.NativeState != nil || len(portable.Messages) < 4 {
				t.Fatalf("portable %+v %v", portable, err)
			}
			if len(targetWire.captured()) != 0 {
				t.Fatal("target dispatched before crash point")
			}
			report["prepared_before_crash"] = prepared
			d.Fault = nil
			// The recovered prepared attempt retains its original quote. Future turns
			// may use the new quote but cannot return to the primary route.
			spec := d.Models[target+"/route-fixture"]
			spec.PriceVersion = "changed-after-preparation"
			spec.InputPrice *= 2
			d.Models[target+"/route-fixture"] = spec
			recovered := reclaimPricingRun(t, d, claimed)
			if err = d.Drive(ctx, recovered); err != nil {
				t.Fatal(err)
			}
			final, err := d.Store.GetRun(ctx, r.TenantID, r.ID)
			if err != nil || final.State.Status != domain.StatusCompleted || final.State.ModelRounds != 3 {
				t.Fatalf("final %+v %v", final.State, err)
			}
			data, err := os.ReadFile(path)
			if err != nil || !strings.Contains(string(data), "max(low, min(value, high))") {
				t.Fatalf("patch missing %v", err)
			}
			completed, err := d.Store.LatestAttempt(ctx, r.TenantID, r.ID, 2)
			if err != nil || completed.ID != prepared.ID || completed.PriceVersion != prepared.PriceVersion || !completed.Deadline.Equal(prepared.Deadline) {
				t.Fatalf("prepared identity changed %+v %v", completed, err)
			}
			last, err := d.Store.LatestAttempt(ctx, r.TenantID, r.ID, 3)
			if err != nil || last.Provider != target || last.PriceVersion != spec.PriceVersion {
				t.Fatalf("route bounced %+v %v", last, err)
			}
			pw, tw := primaryWire.captured(), targetWire.captured()
			if len(pw) != 2 || len(tw) != 2 || tw[0].AttemptID != string(prepared.ID) || !strings.Contains(string(pw[1].Body), "SOURCE_OPAQUE_SENTINEL") || strings.Contains(string(tw[0].Body), "SOURCE_OPAQUE_SENTINEL") || !strings.Contains(string(tw[0].Body), "handoff_1") || !strings.Contains(string(tw[0].Body), "clamp.py") || !strings.Contains(string(tw[1].Body), "TARGET_OPAQUE_SENTINEL") {
				t.Fatal("wire route/native/portable continuity mismatch")
			}
			if tw[0].At.Before(policy.NotBefore) {
				t.Fatal("fallback bypassed durable Retry-After")
			}
			unknownAfter, err := d.Quota.Get(ctx, string(r.TenantID), string(failed.ID))
			if err != nil || unknownAfter.Status != "unknown" || unknownAfter.Cost != unknown.Cost || unknownAfter.ActualCost != nil {
				t.Fatalf("old obligation released %+v %v", unknownAfter, err)
			}
			report["failed_obligation_after_handoff"] = unknownAfter
			var handoffs, attempts, slots int
			if err = d.Store.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM run_events WHERE tenant_id=$1 AND run_id=$2 AND type='model.handoff_prepared'),(SELECT count(*) FROM model_attempts WHERE tenant_id=$1 AND run_id=$2),(SELECT reserved_slots FROM runners WHERE id='runner')`, r.TenantID, r.ID).Scan(&handoffs, &attempts, &slots); err != nil || handoffs != 1 || attempts != 4 || slots != 0 {
				t.Fatalf("counts handoff=%d attempts=%d slots=%d %v", handoffs, attempts, slots, err)
			}
		})
	}
}

func TestFallbackFailureAllowlist(t *testing.T) {
	for _, code := range []string{"rate_limited", "unavailable", "stream_interrupted"} {
		if !persistence.FallbackFailure(code) {
			t.Fatal(code)
		}
	}
	for _, code := range []string{"authentication", "invalid_request", "unsupported_capability", "deadline", "cancelled", "protocol", "stream_limit", "event_consumer", "unconfirmed_previous_dispatch", "request_expired_before_dispatch", "local_failure", ""} {
		if persistence.FallbackFailure(code) {
			t.Fatal(code)
		}
	}
}

func TestFallbackCompatibilityBudgetAndRetryBounds(t *testing.T) {
	for _, scenario := range []string{"compatible_fallback_fails", "unsupported_tools", "context_too_small", "missing_model", "unknown_cost_exhausts_budget"} {
		t.Run(scenario, func(t *testing.T) {
			d, r, primary, _ := repairSetupConfigured(t, func(c *persistence.Config) {
				c.Fallback = &persistence.ModelRoute{Provider: "fake", Model: "backup"}
				if scenario == "unknown_cost_exhausts_budget" {
					c.MaxCost = 10_000
				}
			})
			ctx := context.Background()
			failure := provider.Script{Failure: &provider.Error{Kind: provider.ErrUnavailable}}
			primary.scripts[0] = failure
			backup := provider.NewFake(failure, failure)
			caps := provider.Capabilities{ToolCalling: true, ContextWindow: 64 << 10, MaxOutputTokens: 1024}
			if scenario == "unsupported_tools" {
				caps.ToolCalling = false
			}
			if scenario == "context_too_small" {
				caps.ContextWindow = 2048
			}
			backup.Registry = provider.Registry{"backup": caps}
			d.ProviderFactory = func(selected persistence.Run) provider.Provider {
				if selected.Config.Model == "backup" {
					return backup
				}
				return primary
			}
			spec := d.Models["fake/fake"]
			spec.InputPrice, spec.OutputPrice = 1_000_000, 1_000_000
			d.Models["fake/fake"] = spec
			spec.CredentialGroup = "backup"
			d.Models["fake/backup"] = spec
			if scenario == "missing_model" {
				delete(d.Models, "fake/backup")
			}
			if err := d.Quota.Configure(ctx, quota.Config{CredentialGroup: "backup", MaxConcurrent: 4, MaxTokens: 1_000_000, MaxCost: 10_000_000, WindowDuration: time.Hour, FailureThreshold: 3, BreakerCooldown: time.Second}); err != nil {
				t.Fatal(err)
			}
			report := map[string]any{"scope": "private PG and SQLite protocol fixture; local fake-provider routing guards", "scenario": scenario}
			defer func() {
				report["primary_calls"] = primary.calls.Load()
				report["backup_requests"] = backup.Requests()
				saveHandoffEvidence(t, d, r, report)
			}()
			var final persistence.Run
			for n := 0; n < 4; n++ {
				claimed := claimHandoff(t, d, fmt.Sprintf("guard-worker-%d", n))
				err := d.Drive(ctx, claimed)
				if err != nil && !errors.Is(err, errDeferred) {
					t.Fatal(err)
				}
				final, err = d.Store.GetRun(ctx, r.TenantID, r.ID)
				if err != nil {
					t.Fatal(err)
				}
				if final.State.Status.Terminal() {
					break
				}
			}
			wantStatus := domain.StatusFailed
			wantPrimary, wantBackup, wantAttempts, wantHandoffs := int64(3), 0, 3, 0
			if scenario == "compatible_fallback_fails" {
				wantPrimary, wantBackup, wantHandoffs = 1, 2, 1
			}
			if scenario == "unknown_cost_exhausts_budget" {
				wantStatus = domain.StatusBudgetExhausted
				wantPrimary, wantAttempts = 1, 1
			}
			if final.State.Status != wantStatus || final.State.ModelRounds != 1 || primary.calls.Load() != wantPrimary || len(backup.Requests()) != wantBackup {
				t.Fatalf("status=%s rounds=%d primary=%d backup=%d", final.State.Status, final.State.ModelRounds, primary.calls.Load(), len(backup.Requests()))
			}
			var attempts, handoffs int
			if err := d.Store.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM model_attempts WHERE tenant_id=$1 AND run_id=$2),(SELECT count(*) FROM run_events WHERE tenant_id=$1 AND run_id=$2 AND type='model.handoff_prepared')`, r.TenantID, r.ID).Scan(&attempts, &handoffs); err != nil || attempts != wantAttempts || handoffs != wantHandoffs {
				t.Fatalf("attempts=%d handoffs=%d %v", attempts, handoffs, err)
			}
			first, err := d.Store.LatestAttempt(ctx, r.TenantID, r.ID, 1)
			if err != nil {
				t.Fatal(err)
			}
			if first.Number != wantAttempts {
				t.Fatal("retry budget reset during provider change")
			}
		})
	}
}

func TestFallbackRejectsStaleLeaseAndUnclosedEffects(t *testing.T) {
	d, r, primary, _ := repairSetupConfigured(t, func(c *persistence.Config) { c.Fallback = &persistence.ModelRoute{Provider: "fake", Model: "backup"} })
	ctx := context.Background()
	primary.scripts[0] = provider.Script{Failure: &provider.Error{Kind: provider.ErrUnavailable}}
	if err := d.Drive(ctx, claimHandoff(t, d, "failure-worker")); !errors.Is(err, errDeferred) {
		t.Fatal(err)
	}
	_ = claimHandoff(t, d, "next-worker")
	// Execute only adoption, if it is scheduled. No model attempt is prepared.
	current, err := d.Store.GetRun(ctx, r.TenantID, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Commands[0].Kind == flow.CommandAdoptWorkspace {
		e, err := d.execute(ctx, current, current.Commands[0])
		if err != nil {
			t.Fatal(err)
		}
		e.ExpectedVersion, e.Owner, e.Epoch = current.State.Version, current.State.Lease.Owner, current.State.Lease.Epoch
		if _, err = d.Store.Advance(ctx, r.TenantID, r.ID, e); err != nil {
			t.Fatal(err)
		}
	}
	current, err = d.Store.GetRun(ctx, r.TenantID, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	spec := d.Models["fake/fake"]
	price, err := freezePricing("fake", "backup", spec)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(price)
	unregistered, _ := freezePricing("fake", "unregistered", spec)
	unregisteredRaw, _ := json.Marshal(unregistered)
	forged := current
	forged.Config.Fallback = &persistence.ModelRoute{Provider: "fake", Model: "unregistered"}
	if _, _, err = d.Store.BeginPricedAttempt(ctx, forged, current.State.OutputRef, spec.PriceVersion, spec.RequestTimeout, unregisteredRaw); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("caller changed frozen route=%v", err)
	}
	stale := current
	stale.State.Lease.Epoch--
	if _, _, err = d.Store.BeginPricedAttempt(ctx, stale, current.State.OutputRef, spec.PriceVersion, spec.RequestTimeout, raw); !errors.Is(err, domain.ErrFenced) {
		t.Fatalf("stale route preparation=%v", err)
	}
	// Fault injection is limited to this disposable schema: a contradictory
	// unknown receipt ledger must block a route even though StageModel is set.
	if _, err = d.Store.Pool.Exec(ctx, `UPDATE effects SET status='unknown' WHERE tenant_id=$1 AND run_id=$2`, r.TenantID, r.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err = d.Store.BeginPricedAttempt(ctx, current, current.State.OutputRef, spec.PriceVersion, spec.RequestTimeout, raw); !errors.Is(err, domain.ErrReconciliation) {
		t.Fatalf("unclosed route preparation=%v", err)
	}
	if _, err = d.Store.Pool.Exec(ctx, `UPDATE effects SET status='succeeded',receipt_ref=NULL WHERE tenant_id=$1 AND run_id=$2`, r.TenantID, r.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err = d.Store.BeginPricedAttempt(ctx, current, current.State.OutputRef, spec.PriceVersion, spec.RequestTimeout, raw); !errors.Is(err, domain.ErrReconciliation) {
		t.Fatalf("missing receipt preparation=%v", err)
	}
	latest, err := d.Store.LatestAttempt(ctx, r.TenantID, r.ID, current.State.StepSeq)
	if err != nil || latest.Number != 1 || latest.Provider != "fake" || latest.Model != "fake" {
		t.Fatalf("unexpected attempt %+v %v", latest, err)
	}
}

func TestFallbackLegacySnapshotCannotDispatchOrReplayPrepared(t *testing.T) {
	d, r, primary, _ := repairSetupConfigured(t, func(c *persistence.Config) {
		c.Fallback = &persistence.ModelRoute{Provider: "fake", Model: "backup"}
	})
	if r.State.SchemaVersion != 2 {
		t.Fatal("new fallback submission is not protected from v1 readers")
	}
	ctx := context.Background()
	d.Fault = func(point string) error {
		if point == "after_model_pricing_before_reservation" {
			return context.Canceled
		}
		return nil
	}
	if err := d.Drive(ctx, claimHandoff(t, d, "prepare-worker")); !errors.Is(err, context.Canceled) {
		t.Fatalf("prepare barrier: %v", err)
	}
	current, err := d.Store.GetRun(ctx, r.TenantID, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := d.Store.LatestAttempt(ctx, r.TenantID, r.ID, current.State.StepSeq)
	if err != nil || prepared.Status != "prepared" || primary.calls.Load() != 0 {
		t.Fatalf("attempt not prepared before dispatch: %+v %v", prepared, err)
	}
	// Contradictory old-reader fixture, only in this private schema. Removing
	// progress fields makes this a valid legacy state; fallback is the sole new
	// feature that must prevent both dispatch and prepared-attempt replay.
	legacy := current
	legacy.State.SchemaVersion = 1
	legacy.State.Progress = nil
	legacy.State.Limits.MaxNoProgressBatches = 0
	legacy.Config.MaxNoProgressBatches = 0
	snapshot, _ := json.Marshal(legacy.State)
	config, _ := json.Marshal(legacy.Config)
	if _, err := d.Store.Pool.Exec(ctx, `UPDATE runs SET snapshot=$3,config_snapshot=$4 WHERE tenant_id=$1 AND id=$2`, r.TenantID, r.ID, snapshot, config); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Store.ActiveModelRoute(ctx, legacy); !errors.Is(err, domain.ErrReconciliation) {
		t.Fatalf("legacy active route accepted: %v", err)
	}
	d.Fault = nil
	if _, err := d.callModel(ctx, legacy); !errors.Is(err, domain.ErrReconciliation) {
		t.Fatalf("legacy dispatch accepted: %v", err)
	}
	frozen, err := d.Store.AttemptPricing(ctx, prepared)
	if err != nil {
		t.Fatal(err)
	}
	// Pass the original in-memory v2 state: the Store must consult the durable
	// v1 snapshot before returning an already prepared immutable attempt.
	if _, _, err := d.Store.BeginPricedAttempt(ctx, current, prepared.RequestRef, prepared.PriceVersion, time.Second, frozen); !errors.Is(err, domain.ErrReconciliation) {
		t.Fatalf("prepared replay ignored durable version: %v", err)
	}
	var attempts, reservations int
	if err := d.Store.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM model_attempts WHERE tenant_id=$1 AND run_id=$2),(SELECT count(*) FROM quota_reservations WHERE tenant_id=$1 AND run_id=$2)`, r.TenantID, r.ID).Scan(&attempts, &reservations); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || reservations != 0 || primary.calls.Load() != 0 {
		t.Fatalf("legacy guard wrote/dispatched work: attempts=%d reservations=%d calls=%d", attempts, reservations, primary.calls.Load())
	}
}
