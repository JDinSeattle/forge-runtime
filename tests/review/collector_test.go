package review_test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/telemetry"
	"go.opentelemetry.io/otel/trace"
)

// An external collector writes this file. The test never implements a receiver
// or writes the collector output; a successful exporter flush alone is not PASS.
func TestRecoveryIndependentCollectorDelivery(t *testing.T) {
	endpoint, output := os.Getenv("FORGE_RECOVERY_OTLP_ENDPOINT"), os.Getenv("FORGE_RECOVERY_OTLP_OUTPUT")
	if endpoint == "" || output == "" {
		t.Skip("requires separately running collector and its file-exporter output")
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Hostname() != "127.0.0.1" || u.Scheme != "http" || !filepath.IsAbs(output) {
		t.Fatal("only explicit loopback collector and absolute output path are accepted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	observations, err := telemetry.Setup(ctx, "forge-recovery-acceptance", endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer observations.Shutdown(context.Background())
	traceIDs := make(chan string, 1)
	server := httptest.NewServer(observations.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Exercise propagation through the durable traceparent representation,
		// which is the same format the driver stores and resumes.
		parent := telemetry.ContextFromTraceparent(context.Background(), telemetry.Traceparent(r.Context()))
		run, endRun := observations.StartRun(parent)
		traceIDs <- trace.SpanContextFromContext(run).TraceID().String()
		_, endModel := observations.StartModel(run, "fake")
		endModel(telemetry.ModelObservation{Outcome: telemetry.Unknown})
		_, endEffect := observations.StartEffect(run, "run_command")
		endEffect(telemetry.Unknown)
		endRun(domain.StatusNeedsReconciliation)
		w.WriteHeader(http.StatusAccepted)
	})))
	defer server.Close()
	const canary = "RECOVERY_SECRET_CANARY_NOT_FOR_TELEMETRY_746d"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/runs?credential="+canary, strings.NewReader(canary))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+canary)
	response, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	server.Close()
	traceID := <-traceIDs
	if response.StatusCode != http.StatusAccepted {
		t.Fatal(response.StatusCode)
	}
	if err = observations.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	type span struct {
		TraceID  string `json:"traceId"`
		SpanID   string `json:"spanId"`
		ParentID string `json:"parentSpanId"`
		Name     string `json:"name"`
	}
	wanted := map[string]span{}
	for {
		raw, readErr := os.ReadFile(output)
		if readErr == nil {
			if strings.Contains(string(raw), canary) {
				t.Fatal("collector persisted a credential/payload canary")
			}
			scanner := bufio.NewScanner(strings.NewReader(string(raw)))
			scanner.Buffer(make([]byte, 4096), 4<<20)
			for scanner.Scan() {
				var record struct {
					ResourceSpans []struct {
						Resource struct {
							Attributes []struct {
								Key   string `json:"key"`
								Value struct {
									StringValue string `json:"stringValue"`
								} `json:"value"`
							} `json:"attributes"`
						} `json:"resource"`
						ScopeSpans []struct {
							Spans []span `json:"spans"`
						} `json:"scopeSpans"`
					} `json:"resourceSpans"`
				}
				if json.Unmarshal(scanner.Bytes(), &record) != nil {
					continue
				} // writer may be mid-line
				for _, resource := range record.ResourceSpans {
					service := ""
					for _, attr := range resource.Resource.Attributes {
						if attr.Key == "service.name" {
							service = attr.Value.StringValue
						}
					}
					if service != "forge-recovery-acceptance" {
						continue
					}
					for _, scope := range resource.ScopeSpans {
						for _, s := range scope.Spans {
							if s.TraceID == traceID {
								wanted[s.Name] = s
							}
						}
					}
				}
			}
			if err = scanner.Err(); err != nil {
				t.Fatal(err)
			}
		}
		if len(wanted) == 4 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("collector delivery incomplete: %v, read=%v", wanted, readErr)
		case <-time.After(100 * time.Millisecond):
		}
	}
	api, run, model, effect := wanted["POST unmatched"], wanted["forge.run.drive"], wanted["forge.model.request"], wanted["forge.effect.execute"]
	if api.SpanID == "" || run.ParentID != api.SpanID || model.ParentID != run.SpanID || effect.ParentID != run.SpanID {
		t.Fatalf("collector did not preserve trace topology: %+v", wanted)
	}
	recoveryJSON(t, filepath.Join(filepath.Dir(output), "delivery-report.json"), map[string]any{"trace_id": traceID, "spans": wanted, "collector_file": output, "separate_collector_delivery": true, "payload_canary_absent": true, "provider_or_command_actually_dispatched": false})
	t.Logf("independent collector persisted four correlated API/run/model/effect spans, trace=%s", traceID)
}
