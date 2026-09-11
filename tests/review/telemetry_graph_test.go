package review_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
)

func TestReviewTelemetryRetainedGraphRejectsBrokenBindings(t *testing.T) {
	root := filepath.Join("..", "..", "benchmarks", "results", "telemetry-chain-20260911-frozen")
	raw, err := os.ReadFile(filepath.Join(root, "collector-captured.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	reportRaw, err := os.ReadFile(filepath.Join(root, "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	var r struct {
		Run struct {
			ID domain.ID `json:"id"`
		} `json:"run"`
		Claims     int            `json:"claims"`
		Operations int            `json:"journal_operations"`
		Enqueue    string         `json:"pg_enqueue_traceparent"`
		Claim      string         `json:"pg_last_claim_traceparent"`
		Attempts   []chainAttempt `json:"pg_attempts"`
		IDs        []string       `json:"journal_operation_ids"`
	}
	if err = json.Unmarshal(reportRaw, &r); err != nil {
		t.Fatal(err)
	}
	graph, err := chainGraph(raw, r.Run.ID, r.Enqueue, r.Claims, r.Operations)
	if err != nil {
		t.Fatal(err)
	}
	if err = chainDurableGraph(graph, r.Attempts, r.IDs, r.Claim); err != nil {
		t.Fatal(err)
	}
	rewrite := func(fn func(map[string]any)) []byte {
		var out bytes.Buffer
		for _, line := range bytes.Split(raw, []byte("\n")) {
			if len(bytes.TrimSpace(line)) == 0 {
				continue
			}
			var doc map[string]any
			if e := json.Unmarshal(line, &doc); e != nil {
				t.Fatal(e)
			}
			for _, res := range doc["resourceSpans"].([]any) {
				for _, scope := range res.(map[string]any)["scopeSpans"].([]any) {
					for _, span := range scope.(map[string]any)["spans"].([]any) {
						fn(span.(map[string]any))
					}
				}
			}
			b, _ := json.Marshal(doc)
			out.Write(b)
			out.WriteByte('\n')
		}
		return out.Bytes()
	}
	for name, fn := range map[string]func(map[string]any){
		"missing_RPC_parent": func(s map[string]any) {
			if strings.HasPrefix(s["name"].(string), "forge.runner.server/") {
				s["parentSpanId"] = ""
			}
		},
		"wrong_run": func(s map[string]any) {
			if s["name"] == "forge.runner.operation" {
				for _, a := range s["attributes"].([]any) {
					v := a.(map[string]any)
					if v["key"] == "forge.run_id" {
						v["value"] = map[string]any{"stringValue": "wrong-run"}
					}
				}
			}
		},
		"wrong_attempt": func(s map[string]any) {
			if s["name"] == "forge.model.request" {
				for _, a := range s["attributes"].([]any) {
					v := a.(map[string]any)
					if v["key"] == "forge.attempt.id" {
						v["value"] = map[string]any{"stringValue": "wrong-attempt"}
					}
				}
			}
		},
		"dangling_link": func(s map[string]any) {
			for _, l := range listAny(s["links"]) {
				l.(map[string]any)["spanId"] = "0000000000000001"
			}
		},
		"removed_retry_link": func(s map[string]any) {
			var kept []any
			for _, l := range listAny(s["links"]) {
				retry := false
				for _, a := range l.(map[string]any)["attributes"].([]any) {
					v := a.(map[string]any)
					if v["key"] == "forge.relationship" && v["value"].(map[string]any)["stringValue"] == "provider_retry" {
						retry = true
					}
				}
				if !retry {
					kept = append(kept, l)
				}
			}
			s["links"] = kept
		},
	} {
		t.Run(name, func(t *testing.T) {
			g, e := chainGraph(rewrite(fn), r.Run.ID, r.Enqueue, r.Claims, r.Operations)
			if e == nil {
				e = chainDurableGraph(g, r.Attempts, r.IDs, r.Claim)
			}
			if e == nil {
				t.Fatal("tampered evidence accepted")
			}
		})
	}
	t.Run("duplicate_span", func(t *testing.T) {
		if _, e := chainGraph(append(append([]byte{}, raw...), raw...), r.Run.ID, r.Enqueue, r.Claims, r.Operations); e == nil {
			t.Fatal("duplicate spans accepted")
		}
	})
	t.Run("wrong_SQLite_identity", func(t *testing.T) {
		ids := append([]string{}, r.IDs...)
		ids[0] = "invented"
		if chainDurableGraph(graph, r.Attempts, ids, r.Claim) == nil {
			t.Fatal("invented operation accepted")
		}
	})
	t.Run("wrong_PG_attempt_context", func(t *testing.T) {
		as := append([]chainAttempt{}, r.Attempts...)
		as[0].Traceparent = r.Enqueue
		if chainDurableGraph(graph, as, r.IDs, r.Claim) == nil {
			t.Fatal("wrong persisted context accepted")
		}
	})
}
func listAny(v any) []any {
	if v == nil {
		return nil
	}
	return v.([]any)
}
