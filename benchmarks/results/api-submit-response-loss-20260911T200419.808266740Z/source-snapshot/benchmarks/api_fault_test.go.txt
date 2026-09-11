package benchmarks

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/httpapi"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/testutil"
	"github.com/jackc/pgx/v5"
)

var errDropAcceptedResponse = errors.New("fixture drops a durably accepted response")

type acceptedSubmission struct {
	RunID  domain.ID `json:"run_id"`
	Reused bool      `json:"reused"`
}

type responseDropRecord struct {
	UpstreamResponseAt time.Time       `json:"upstream_response_at"`
	UpstreamStatus     int             `json:"upstream_status"`
	UpstreamLocation   string          `json:"upstream_location"`
	UpstreamBody       json.RawMessage `json:"upstream_body"`
	DurableProofAt     time.Time       `json:"durable_proof_at"`
	DurableProof       json.RawMessage `json:"durable_proof_before_connection_close"`
	ConnectionClosedAt time.Time       `json:"client_connection_closed_at"`
	SocketClosed       bool            `json:"client_socket_closed_without_http_response"`
	Error              string          `json:"fixture_error,omitempty"`
}

// This injector is a fixture reverse proxy, not a production API feature. It
// cannot drop a response until an independent observer proves its run durable.
// It matches one exact route and idempotency key, and acts at most once.
type submissionResponseDropper struct {
	path, key string
	prove     func(context.Context, acceptedSubmission) (json.RawMessage, error)
	armed     atomic.Bool
	mu        sync.Mutex
	record    responseDropRecord
}

func (d *submissionResponseDropper) snapshot() responseDropRecord {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.record
}
func (d *submissionResponseDropper) modify(resp *http.Response) error {
	if resp.StatusCode != http.StatusAccepted || resp.Request.Method != http.MethodPost || resp.Request.URL.Path != d.path || resp.Request.Header.Get("Idempotency-Key") != d.key || !d.armed.CompareAndSwap(false, true) {
		return nil
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 65537))
	if err != nil {
		return err
	}
	if len(body) > 65536 {
		return fmt.Errorf("fixture accepted body too large")
	}
	var accepted acceptedSubmission
	if err = json.Unmarshal(body, &accepted); err != nil {
		return err
	}
	if accepted.RunID.Validate() != nil || accepted.Reused {
		return fmt.Errorf("fixture expected original new acceptance")
	}
	record := responseDropRecord{UpstreamResponseAt: time.Now(), UpstreamStatus: resp.StatusCode, UpstreamLocation: resp.Header.Get("Location"), UpstreamBody: body}
	proof, err := d.prove(resp.Request.Context(), accepted)
	if err != nil {
		return fmt.Errorf("fixture could not prove committed acceptance: %w", err)
	}
	record.DurableProofAt = time.Now()
	record.DurableProof = proof
	d.mu.Lock()
	d.record = record
	d.mu.Unlock()
	return errDropAcceptedResponse
}
func (d *submissionResponseDropper) handleError(w http.ResponseWriter, r *http.Request, err error) {
	if !errors.Is(err, errDropAcceptedResponse) {
		d.mu.Lock()
		d.record.Error = err.Error()
		d.mu.Unlock()
		http.Error(w, "fixture proxy failure", http.StatusBadGateway)
		return
	}
	conn, _, hijackErr := http.NewResponseController(w).Hijack()
	if hijackErr != nil {
		d.mu.Lock()
		d.record.Error = hijackErr.Error()
		d.mu.Unlock()
		http.Error(w, "fixture cannot drop socket", http.StatusBadGateway)
		return
	}
	closeErr := conn.Close() // no WriteHeader, Write, or buffered response flush
	d.mu.Lock()
	d.record.ConnectionClosedAt = time.Now()
	d.record.SocketClosed = closeErr == nil
	if closeErr != nil {
		d.record.Error = closeErr.Error()
	}
	d.mu.Unlock()
}

type submissionAttempt struct {
	Label            string          `json:"label"`
	StartedAt        time.Time       `json:"started_at"`
	FinishedAt       time.Time       `json:"finished_at"`
	Tenant           string          `json:"tenant"`
	RequestSHA256    string          `json:"request_sha256"`
	Status           int             `json:"http_status"`
	ResponseReceived bool            `json:"http_response_received"`
	ResponseLocation string          `json:"response_location,omitempty"`
	ResponseBody     json.RawMessage `json:"response_body,omitempty"`
	Error            string          `json:"client_error,omitempty"`
}

func submissionLedger(ctx context.Context, store *persistence.Store) (json.RawMessage, error) {
	var raw []byte
	err := store.Pool.QueryRow(ctx, `SELECT jsonb_build_object(
 'captured_at',clock_timestamp(),
 'runs',coalesce((SELECT jsonb_agg(jsonb_build_object('tenant_id',tenant_id,'id',id,'project_id',project_id,'principal_id',principal_id,'state',state,'version',version,'lease_epoch',lease_epoch,'runner_id',runner_id,'covered_seq',next_event_seq-1,'created_at',created_at) ORDER BY id) FROM runs),'[]'::jsonb),
 'events',coalesce((SELECT jsonb_agg(to_jsonb(e) ORDER BY run_id,seq) FROM run_events e),'[]'::jsonb),
 'idempotency_keys',coalesce((SELECT jsonb_agg(to_jsonb(k) ORDER BY tenant_id,principal_id,route,key) FROM idempotency_keys k),'[]'::jsonb),
 'model_attempts',(SELECT count(*) FROM model_attempts),
 'effects',(SELECT count(*) FROM effects),
 'runner_allocations',(SELECT count(*) FROM runner_allocations))`).Scan(&raw)
	return raw, err
}

type submissionLedgerView struct {
	Runs []struct {
		Tenant    domain.ID `json:"tenant_id"`
		ID        domain.ID `json:"id"`
		Project   domain.ID `json:"project_id"`
		Principal domain.ID `json:"principal_id"`
		State     string    `json:"state"`
		Seq       uint64    `json:"covered_seq"`
		Epoch     uint64    `json:"lease_epoch"`
		Runner    *string   `json:"runner_id"`
	} `json:"runs"`
	Events []struct {
		Tenant domain.ID `json:"tenant_id"`
		Run    domain.ID `json:"run_id"`
		Seq    uint64    `json:"seq"`
		Type   string    `json:"type"`
	} `json:"events"`
	Keys []struct {
		Tenant    domain.ID `json:"tenant_id"`
		Principal domain.ID `json:"principal_id"`
		Route     string    `json:"route"`
		Key       string    `json:"key"`
		Resource  domain.ID `json:"resource_id"`
		Hash      string    `json:"request_hash"`
	} `json:"idempotency_keys"`
	Models      int `json:"model_attempts"`
	Effects     int `json:"effects"`
	Allocations int `json:"runner_allocations"`
}

func verifySingleSubmission(raw json.RawMessage, tenant, principal, project, run domain.ID, route, key string) error {
	var v submissionLedgerView
	if err := json.Unmarshal(raw, &v); err != nil {
		return err
	}
	if len(v.Runs) != 1 || len(v.Events) != 1 || len(v.Keys) != 1 || v.Models != 0 || v.Effects != 0 || v.Allocations != 0 {
		return fmt.Errorf("expected exactly one untouched creation/key/event and no execution")
	}
	r, e, k := v.Runs[0], v.Events[0], v.Keys[0]
	if r.Tenant != tenant || r.ID != run || r.Project != project || r.Principal != principal || r.State != "queued" || r.Seq != 1 || r.Epoch != 0 || r.Runner != nil || e.Tenant != tenant || e.Run != run || e.Seq != 1 || e.Type != "run.created" || k.Tenant != tenant || k.Principal != principal || k.Route != route || k.Key != key || k.Resource != run || len(k.Hash) != 64 {
		return fmt.Errorf("durable submission bindings or initial state differ")
	}
	return nil
}

// TestSubmissionResponseLossEvidence uses real loopback HTTP and PostgreSQL,
// with one exact fixture proxy fault after commit and before response forwarding.
func TestSubmissionResponseLossEvidence(t *testing.T) {
	if os.Getenv("FORGE_RUN_API_FAULT") != "1" {
		t.Skip("set FORGE_RUN_API_FAULT=1 and FORGE_TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	started := time.Now()
	out := sustainedOutput(t, started, "api-submit-response-loss")
	owner := testutil.Database(t)
	const tenant, principal, otherTenant, otherPrincipal domain.ID = "f01_tenant", "f01_developer", "f01_other_tenant", "f01_other_developer"
	for _, args := range [][2]domain.ID{{tenant, principal}, {otherTenant, otherPrincipal}} {
		if err := owner.BootstrapTenant(ctx, args[0], args[1], "developer"); err != nil {
			t.Fatal(err)
		}
	}
	api := sseNonOwnerPool(t, ctx, owner)
	var role, schema, version, fsync, synchronousCommit string
	if err := api.Pool.QueryRow(ctx, `SELECT current_user,current_schema()`).Scan(&role, &schema); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Pool.Exec(ctx, "GRANT INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA "+pgx.Identifier{schema}.Sanitize()+" TO "+pgx.Identifier{role}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	if err := api.CheckAPIRole(ctx); err != nil {
		t.Fatal(err)
	}
	if err := owner.Pool.QueryRow(ctx, `SELECT version(),current_setting('fsync'),current_setting('synchronous_commit')`).Scan(&version, &fsync, &synchronousCommit); err != nil {
		t.Fatal(err)
	}
	project, err := owner.CreateProject(ctx, tenant, "Response loss fault fixture", "f01_source", "f01_profile")
	if err != nil {
		t.Fatal(err)
	}
	token, err := owner.IssueToken(ctx, principal, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	base := strings.Repeat("a", 64)
	cfg := persistence.Config{Provider: "fake", Model: "not-called", MaxModelRounds: 1, MaxToolCalls: 1, MaxRuntimeSeconds: 60}
	handler := (&httpapi.Server{Store: api, Configs: map[string]persistence.Config{"f01": cfg}, Sources: map[string]httpapi.Source{"f01_source": {BaseCommit: base, ProfileID: "f01_profile"}}}).Handler()
	var upstreamCalls, proxyCalls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { upstreamCalls.Add(1); handler.ServeHTTP(w, r) }))
	defer upstream.Close()
	upstreamURL, _ := url.Parse(upstream.URL)
	route := "/v1/projects/" + string(project.ID) + "/runs"
	const key = "f01-response-lost-exact-key"
	dropper := &submissionResponseDropper{path: route, key: key, prove: func(proofCtx context.Context, reply acceptedSubmission) (json.RawMessage, error) {
		raw, err := submissionLedger(proofCtx, owner)
		if err == nil {
			err = verifySingleSubmission(raw, tenant, principal, project.ID, reply.RunID, route, key)
		}
		return raw, err
	}}
	reverse := httputil.NewSingleHostReverseProxy(upstreamURL)
	reverse.ModifyResponse = dropper.modify
	reverse.ErrorHandler = dropper.handleError
	proxyTransport := &http.Transport{ForceAttemptHTTP2: false, DisableKeepAlives: true}
	defer proxyTransport.CloseIdleConnections()
	reverse.Transport = proxyTransport
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { proxyCalls.Add(1); reverse.ServeHTTP(w, r) }))
	defer proxy.Close()
	// Fresh TCP connections prevent net/http from invisibly replaying an
	// idempotent POST on a reused broken connection before this client observes it.
	transport := &http.Transport{ForceAttemptHTTP2: false, DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	body, _ := json.Marshal(httpapi.SubmitBody{Task: "Response was lost after this synthetic task committed", BaseCommit: base, ConfigID: "f01"})
	attempt := func(label string, asTenant domain.ID, payload []byte) submissionAttempt {
		sum := sha256.Sum256(payload)
		a := submissionAttempt{Label: label, StartedAt: time.Now(), Tenant: string(asTenant), RequestSHA256: hex.EncodeToString(sum[:])}
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, proxy.URL+route, bytes.NewReader(payload))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("X-Forge-Tenant", string(asTenant))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", key)
		resp, err := client.Do(req)
		if resp != nil {
			a.ResponseReceived = true
			a.Status = resp.StatusCode
			a.ResponseLocation = resp.Header.Get("Location")
			raw, readErr := io.ReadAll(io.LimitReader(resp.Body, 65537))
			resp.Body.Close()
			if json.Valid(raw) {
				a.ResponseBody = raw
			}
			if err == nil {
				err = readErr
			}
		}
		if err != nil {
			a.Error = err.Error()
		}
		a.FinishedAt = time.Now()
		return a
	}
	first := attempt("committed_response_deliberately_lost", tenant, body)
	callsAfterFirst := map[string]int64{"proxy": proxyCalls.Load(), "upstream": upstreamCalls.Load()}
	proofAfterFirst, proofErr := submissionLedger(ctx, owner)
	retry := attempt("explicit_same_key_and_body_retry", tenant, body)
	var retried acceptedSubmission
	decodeErr := json.Unmarshal(retry.ResponseBody, &retried)
	proofAfterRetry, retryProofErr := submissionLedger(ctx, owner)
	changed, _ := json.Marshal(httpapi.SubmitBody{Task: "Different task with the same key must conflict", BaseCommit: base, ConfigID: "f01"})
	conflict := attempt("same_key_different_body", tenant, changed)
	forbidden := attempt("same_token_unrelated_tenant", otherTenant, body)
	finalProof, finalProofErr := submissionLedger(ctx, owner)
	record := dropper.snapshot()
	var original acceptedSubmission
	originalErr := json.Unmarshal(record.UpstreamBody, &original)
	var problems []string
	for _, err := range []error{proofErr, retryProofErr, finalProofErr, decodeErr, originalErr} {
		if err != nil {
			problems = append(problems, err.Error())
		}
	}
	if originalErr == nil {
		for _, raw := range []json.RawMessage{record.DurableProof, proofAfterFirst, proofAfterRetry, finalProof} {
			if err := verifySingleSubmission(raw, tenant, principal, project.ID, original.RunID, route, key); err != nil {
				problems = append(problems, err.Error())
			}
		}
	}
	ordered := !record.UpstreamResponseAt.IsZero() && !record.DurableProofAt.Before(record.UpstreamResponseAt) && !record.ConnectionClosedAt.Before(record.DurableProofAt) && !retry.StartedAt.Before(first.FinishedAt)
	passed := len(problems) == 0 && first.Error != "" && !first.ResponseReceived && first.Status == 0 && callsAfterFirst["proxy"] == 1 && callsAfterFirst["upstream"] == 1 && record.SocketClosed && record.Error == "" && ordered && original.RunID == retried.RunID && !original.Reused && retried.Reused && retry.Status == 202 && retry.Error == "" && retry.ResponseLocation == "/v1/runs/"+string(original.RunID) && first.RequestSHA256 == retry.RequestSHA256 && conflict.Status == 409 && forbidden.Status == 403 && proxyCalls.Load() == 4 && upstreamCalls.Load() == 4
	manifest := sustainedManifest(t)
	for _, source := range []string{"benchmarks/api_fault_test.go", "internal/httpapi/server.go", "internal/persistence/store.go", "internal/persistence/identity.go"} {
		raw, err := os.ReadFile(filepath.Join("..", source))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		manifest["source_sha256"].(map[string]string)[source] = hex.EncodeToString(sum[:])
	}
	for source, digest := range manifest["source_sha256"].(map[string]string) {
		raw, err := os.ReadFile(filepath.Join("..", source))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		if hex.EncodeToString(sum[:]) != digest {
			t.Fatal("source changed during evidence capture")
		}
		dest := filepath.Join(out, "source-snapshot", source+".txt")
		if err = os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(dest, raw, 0644); err != nil {
			t.Fatal(err)
		}
	}
	report := map[string]any{"schema_version": 1, "timestamp_utc": started.UTC(), "scope": "Actual HTTP API commits once; fixture reverse proxy confirms durable PG records, drops the accepted response before forwarding, then the client explicitly retries the same bound key and body", "manifest": manifest, "database": map[string]any{"version": version, "private_schema": schema, "fsync": fsync, "synchronous_commit": synchronousCommit, "api_role": "nonowner NOSUPERUSER NOBYPASSRLS NOINHERIT with private-schema CRUD grants", "owner_pool_max": owner.Pool.Config().MaxConns, "api_pool_max": api.Pool.Config().MaxConns}, "request": map[string]any{"method": "POST", "route": route, "tenant": tenant, "principal": principal, "key": key, "body": json.RawMessage(body), "authorization_header": "present; bearer token intentionally excluded"}, "attempts": []submissionAttempt{first, retry, conflict, forbidden}, "proxy_fault": record, "calls_after_first_client_failure": callsAfterFirst, "calls_at_end": map[string]int64{"proxy": proxyCalls.Load(), "upstream": upstreamCalls.Load()}, "pg_after_first_client_failure": proofAfterFirst, "pg_after_retry": proofAfterRetry, "pg_final": finalProof, "invariants": map[string]any{"passed": passed, "first_client_saw_unknown_transport_outcome": first.Error != "" && !first.ResponseReceived, "durable_commit_proved_before_socket_close": ordered, "retry_same_run": original.RunID == retried.RunID, "retry_reused": retried.Reused, "one_run_event_and_key": len(problems) == 0, "conflicting_body_rejected": conflict.Status == 409, "unrelated_tenant_rejected": forbidden.Status == 403}, "problems": problems, "limitations": []string{"This is a single deterministic after-commit/before-response transport cut. It is not a production reverse-proxy change, distributed packet-loss rate test or worker crash.", "The proxy's upstream response and independent PG proof are fixture observations unavailable to the failed client. The client retry uses only its original request/key, not that hidden run ID.", "Both HTTP legs use real loopback TCP. Fresh client/proxy connections avoid hidden automatic replay; per-hop call counters prove exactly one initial upstream call.", "Only the test's private schema and synthetic tenant/principal/key are used. Bearer tokens and DSNs are not retained.", "No worker, runner, executor or model is started. The single run remains queued with one run.created event; model/effect/allocation tables stay empty."}}
	sustainedWriteReport(t, out, report)
	t.Logf("passed=%v first_status=%d first_response=%v retry_status=%d reused=%v same_run=%v calls=%d", passed, first.Status, first.ResponseReceived, retry.Status, retried.Reused, original.RunID == retried.RunID, upstreamCalls.Load())
	if !passed {
		t.Error("F01 accepted-response-loss/retry boundary failed; preserve report")
	}
}

// The injector contract is tested with net.Pipe, without PostgreSQL or sockets.
type pipeHijacker struct {
	header http.Header
	conn   net.Conn
	writes int
	status int
}

func (w *pipeHijacker) Header() http.Header         { return w.header }
func (w *pipeHijacker) WriteHeader(code int)        { w.status = code }
func (w *pipeHijacker) Write(p []byte) (int, error) { w.writes += len(p); return len(p), nil }
func (w *pipeHijacker) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.conn, bufio.NewReadWriter(bufio.NewReader(w.conn), bufio.NewWriter(w.conn)), nil
}
func dropFixtureResponse(path, key string) *http.Response {
	r := httptest.NewRequest(http.MethodPost, path, nil)
	r.Header.Set("Idempotency-Key", key)
	return &http.Response{StatusCode: 202, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"run_id":"run_committed","reused":false}`)), Request: r}
}

func TestSubmissionDropFixtureRequiresDurabilityAndIsScoped(t *testing.T) {
	proofCalls := 0
	d := &submissionResponseDropper{path: "/v1/projects/proj/runs", key: "exact-key", prove: func(context.Context, acceptedSubmission) (json.RawMessage, error) {
		proofCalls++
		return json.RawMessage(`{"committed":true}`), nil
	}}
	wrong := dropFixtureResponse(d.path, "unrelated-key")
	if err := d.modify(wrong); err != nil || proofCalls != 0 {
		t.Fatal("wrong key triggered injector")
	}
	resp := dropFixtureResponse(d.path, d.key)
	if err := d.modify(resp); !errors.Is(err, errDropAcceptedResponse) || proofCalls != 1 {
		t.Fatalf("injection: %v", err)
	}
	server, client := net.Pipe()
	defer client.Close()
	writer := &pipeHijacker{header: make(http.Header), conn: server}
	d.handleError(writer, resp.Request, errDropAcceptedResponse)
	if n, err := client.Read(make([]byte, 1)); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("client saw response bytes: %d %v", n, err)
	}
	if writer.writes != 0 || writer.status != 0 || !d.snapshot().SocketClosed {
		t.Fatal("injector wrote HTTP response before disconnect")
	}
	if err := d.modify(dropFixtureResponse(d.path, d.key)); err != nil || proofCalls != 1 {
		t.Fatal("fault repeated on retry")
	}
}
func TestSubmissionDropFixtureNeverLosesResponseWithoutCommitProof(t *testing.T) {
	d := &submissionResponseDropper{path: "/v1/projects/proj/runs", key: "exact-key", prove: func(context.Context, acceptedSubmission) (json.RawMessage, error) {
		return nil, errors.New("durable record missing")
	}}
	resp := dropFixtureResponse(d.path, d.key)
	err := d.modify(resp)
	if err == nil || errors.Is(err, errDropAcceptedResponse) {
		t.Fatal("unproved commit entered deliberate response-loss path")
	}
	writer := httptest.NewRecorder()
	d.handleError(writer, resp.Request, err)
	if writer.Code != 502 || d.snapshot().SocketClosed {
		t.Fatal("proof failure manufactured an unknown transport outcome")
	}
}
