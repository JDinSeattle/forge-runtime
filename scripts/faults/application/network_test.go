package applicationfaults

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/eventstream"
	"github.com/JDinSeattle/forge-runtime/internal/httpapi"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	"github.com/JDinSeattle/forge-runtime/internal/runnerclient"
	"github.com/jackc/pgx/v5"
)

func networkMode(mode string) bool {
	return mode == "F10_cancel_first" || mode == "F10_complete_first" || mode == "F11" || mode == "F12"
}

// Gate files are private test-process controls. No runner RPC or production
// worker accepts them; releasing a gate never fabricates an operation result.
type networkRunner struct {
	runner.Service
	s      settings
	owner  string
	paused bool
}

func (n *networkRunner) gate(ctx context.Context, phase string, op runner.Operation) {
	if n.paused || n.owner != "worker1" {
		return
	}
	n.paused = true
	if err := save(filepath.Join(n.s.Evidence, "worker1-paused.json"), map[string]any{"pid": os.Getpid(), "at": time.Now().UTC(), "phase": phase, "operation": op}); err != nil {
		os.Exit(87)
	}
	for {
		if _, err := os.Stat(filepath.Join(n.s.Evidence, "release-worker1")); err == nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}
func (n *networkRunner) terminal(ctx context.Context, op runner.Operation, err error) (runner.Operation, error) {
	if strings.HasPrefix(n.s.Mode, "F10_") && err == nil && op.Request.OperationID == domain.ID(string(n.s.RunID)+"_4_final_diff") && op.Status == runner.Succeeded {
		n.gate(ctx, "final_diff_durable_before_driver_return", op)
	}
	return op, err
}
func (n *networkRunner) StartOperation(ctx context.Context, r runner.OperationRequest) (runner.Operation, error) {
	op, err := n.Service.StartOperation(ctx, r)
	if (n.s.Mode == "F11" || n.s.Mode == "F12") && r.OperationID == n.s.TargetOp && err == nil {
		if op.Status.Terminal() {
			return op, fmt.Errorf("fixture missed accepted-before-terminal Start window")
		}
		n.gate(ctx, "original_start_accepted_before_driver_return", op)
	}
	return n.terminal(ctx, op, err)
}
func (n *networkRunner) InspectOperation(ctx context.Context, r runner.InspectRequest) (runner.Operation, error) {
	op, err := n.Service.InspectOperation(ctx, r)
	return n.terminal(ctx, op, err)
}

func TestApplicationNetworkWorkerProcess(t *testing.T) {
	path := os.Getenv("FORGE_APP_NETWORK_SETTINGS")
	if path == "" {
		t.Skip("private network-matrix worker only")
	}
	var s settings
	var c runnerSettings
	if err := readJSON(path, &s); err != nil {
		t.Fatal(err)
	}
	if err := readJSON(s.RunnerConfig, &c); err != nil {
		t.Fatal(err)
	}
	owner, dsn := os.Getenv("FORGE_APP_WORKER_ID"), os.Getenv("FORGE_APP_FIXTURE_DSN")
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || u.Path != "/forge" || !strings.HasPrefix(u.Query().Get("search_path"), "appfault_") || (owner != "worker1" && owner != "worker2") || (!networkMode(s.Mode) && s.Mode != "network_recovery") {
		t.Fatal("exact private network fixture required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	client, err := runnerclient.Dial(ctx, runnerclient.ClientConfig{UnixSocket: s.Socket, RPCTimeout: 2 * time.Second, ReconcileTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	d, closeDriver, err := driver(ctx, s, c, dsn, &networkRunner{Service: client, s: s, owner: owner})
	if err != nil {
		t.Fatal(err)
	}
	defer closeDriver()
	if err = save(filepath.Join(s.Evidence, owner+"-ready.json"), map[string]any{"pid": os.Getpid(), "at": time.Now().UTC(), "owner": owner}); err != nil {
		t.Fatal(err)
	}
	err = d.RunWorker(ctx, owner, 1)
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}

func TestApplicationNetworkAPIProcess(t *testing.T) {
	path := os.Getenv("FORGE_APP_NETWORK_API_SETTINGS")
	if path == "" {
		t.Skip("private real HTTP/SSE process only")
	}
	var s settings
	var c runnerSettings
	if err := readJSON(path, &s); err != nil {
		t.Fatal(err)
	}
	if err := readJSON(s.RunnerConfig, &c); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(os.Getenv("FORGE_APP_API_DSN"))
	if err != nil || u.Hostname() != "127.0.0.1" || u.Path != "/forge" || !strings.HasPrefix(u.Query().Get("search_path"), "appfault_") || !networkMode(s.Mode) {
		t.Fatal("exact private API fixture required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	store, err := persistence.Open(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = store.CheckAPIRole(ctx); err != nil {
		t.Fatal(err)
	}
	objects, err := artifact.NewLocalStore(c.ArtifactRoot, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer objects.Close()
	streams := eventstream.New(ctx, store, eventstream.Config{})
	defer streams.Close()
	run, err := store.GetRun(ctx, s.Tenant, s.RunID)
	if err != nil {
		t.Fatal(err)
	}
	api := &httpapi.Server{Store: store, Artifacts: objects, Streams: streams, Configs: map[string]persistence.Config{"fixture": run.Config}, Sources: map[string]httpapi.Source{"clamp": {BaseCommit: s.SourceHash, ProfileID: "python-clamp"}}}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: api.Handler(), ReadHeaderTimeout: 2 * time.Second, IdleTimeout: 15 * time.Second, MaxHeaderBytes: 32 << 10}
	done := make(chan error, 1)
	go func() { done <- server.Serve(l) }()
	if err = save(filepath.Join(s.Evidence, "api-ready.json"), map[string]any{"pid": os.Getpid(), "at": time.Now().UTC(), "address": l.Addr().String(), "production_CheckAPIRole": "passed"}); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Fatal(err)
		}
	case <-ctx.Done():
	}
	closeCtx, end := context.WithTimeout(context.Background(), 2*time.Second)
	defer end()
	streams.Close()
	server.Shutdown(closeCtx)
}

type networkHarness struct {
	*harness
	api                            *process
	apiDSN, apiRole, token, apiURL string
	proxy                          *fixtureProxy
	workerDSNDirect                string
	httpLog                        []map[string]any
	sse                            *sseObservation
}

func (h *networkHarness) closeNetwork() {
	if h.sse != nil {
		h.sse.close()
	}
	h.stop(h.api)
	h.stopAll()
	if h.proxy != nil {
		h.proxy.close()
	}
}
func (h *networkHarness) workerNetwork(owner string) *process {
	p := h.launch(os.Args[0], []string{"-test.run=^TestApplicationNetworkWorkerProcess$", "-test.v"}, []string{"FORGE_APP_NETWORK_SETTINGS=" + h.workerSettings, "FORGE_APP_FIXTURE_DSN=" + h.workerDSN, "FORGE_APP_WORKER_ID=" + owner}, owner)
	h.workers = append(h.workers, p)
	h.wait(func() bool { _, err := os.Stat(filepath.Join(h.dir, owner+"-ready.json")); return err == nil }, 10*time.Second, "network worker ready")
	return p
}
func (h *networkHarness) setupAPI() {
	scoped, err := url.Parse(h.dsn)
	h.fatal(err)
	h.apiRole = "appfault_api_" + strings.ToLower(rand.Text())
	password := rand.Text() + rand.Text()
	role, schema := pgx.Identifier{h.apiRole}.Sanitize(), pgx.Identifier{h.schema}.Sanitize()
	for _, q := range []string{
		"CREATE ROLE " + role + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS PASSWORD '" + password + "'",
		"GRANT USAGE ON SCHEMA " + schema + " TO " + role,
		"GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA " + schema + " TO " + role,
		"REVOKE ALL ON " + schema + ".goose_db_version FROM " + role,
		"REVOKE INSERT,UPDATE,DELETE ON " + schema + ".api_tokens," + schema + ".memberships," + schema + ".tenants FROM " + role,
	} {
		_, err = h.db.Pool.Exec(h.ctx, q)
		h.fatal(err)
	}
	scoped.User = url.UserPassword(h.apiRole, password)
	h.apiDSN = scoped.String()
	api, err := persistence.Open(h.ctx, h.apiDSN)
	h.fatal(err)
	defer api.Close()
	h.fatal(api.CheckAPIRole(h.ctx))
	var current, session string
	var super, bypass, createRole, createDB, owner bool
	h.fatal(api.Pool.QueryRow(h.ctx, `SELECT current_user,session_user,rolsuper,rolbypassrls,rolcreaterole,rolcreatedb,pg_has_role(current_user,(SELECT relowner FROM pg_class WHERE oid=to_regclass('runs')),'MEMBER') FROM pg_roles WHERE rolname=current_user`).Scan(&current, &session, &super, &bypass, &createRole, &createDB, &owner))
	if current != h.apiRole || session != h.apiRole || super || bypass || createRole || createDB || owner {
		h.t.Fatal("restricted real API role preflight failed")
	}
	h.fatal(save(filepath.Join(h.dir, "api-role-preflight.json"), map[string]any{"current_user": current, "session_user": session, "superuser": super, "bypassrls": bypass, "create_role": createRole, "create_database": createDB, "member_of_runs_owner": owner, "production_CheckAPIRole": "passed"}))
	h.token, err = h.db.IssueToken(h.ctx, "fixture-operator", time.Hour)
	h.fatal(err)
	if h.s.Mode == "F11" {
		dbURL, err := url.Parse(h.dsn)
		h.fatal(err)
		port := dbURL.Port()
		if port == "" {
			port = "5432"
		}
		h.proxy, err = startFixtureProxy(net.JoinHostPort(dbURL.Hostname(), port))
		h.fatal(err)
		h.workerDSNDirect = h.workerDSN
		h.workerDSN = h.proxyURL(h.workerDSN)
		h.apiDSN = h.proxyURL(h.apiDSN)
	}
	h.api = h.launch(os.Args[0], []string{"-test.run=^TestApplicationNetworkAPIProcess$", "-test.v"}, []string{"FORGE_APP_NETWORK_API_SETTINGS=" + h.workerSettings, "FORGE_APP_API_DSN=" + h.apiDSN}, "api")
	until := time.Now().Add(10 * time.Second)
	for {
		var ready struct {
			Address string `json:"address"`
		}
		if readJSON(filepath.Join(h.dir, "api-ready.json"), &ready) == nil {
			h.apiURL = "http://" + ready.Address
			break
		}
		select {
		case err := <-h.api.done:
			h.api.exited = true
			raw, _ := os.ReadFile(h.api.log.Name())
			h.t.Fatalf("API child failed: %v: %s", err, raw)
		default:
		}
		if time.Now().After(until) {
			h.t.Fatal("API ready timeout")
		}
		time.Sleep(20 * time.Millisecond)
	}
	status, _ := h.request("GET", "/readyz", nil, "")
	if status != 200 {
		h.t.Fatal("real API not ready")
	}
}
func (h *networkHarness) proxyURL(dsn string) string {
	u, err := url.Parse(dsn)
	h.fatal(err)
	u.Host = h.proxy.listener.Addr().String()
	q := u.Query()
	q.Set("connect_timeout", "1")
	u.RawQuery = q.Encode()
	return u.String()
}
func (h *networkHarness) request(method, path string, body any, key string) (int, []byte) {
	return h.requestWithin(method, path, body, key, 5*time.Second)
}
func (h *networkHarness) requestWithin(method, path string, body any, key string, timeout time.Duration) (int, []byte) {
	var raw []byte
	var err error
	if body != nil {
		raw, err = json.Marshal(body)
		h.fatal(err)
	}
	ctx, cancel := context.WithTimeout(h.ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, h.apiURL+path, bytes.NewReader(raw))
	h.fatal(err)
	req.Header.Set("Authorization", "Bearer "+h.token)
	req.Header.Set("X-Forge-Tenant", string(h.s.Tenant))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	at := time.Now().UTC()
	resp, err := (&http.Client{Timeout: timeout}).Do(req)
	h.fatal(err)
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	h.fatal(err)
	h.httpLog = append(h.httpLog, map[string]any{"at": at, "method": method, "path": path, "status": resp.StatusCode, "response": json.RawMessage(data)})
	h.fatal(save(filepath.Join(h.dir, "http-observations.json"), h.httpLog))
	return resp.StatusCode, data
}
func (h *networkHarness) releaseWorker() {
	h.fatal(os.WriteFile(filepath.Join(h.dir, "release-worker1"), []byte("release literal test barrier\n"), 0600))
}

// Closing proxy sockets invalidates pooled connections asynchronously. Observe
// bounded read recovery; never retry submission/cancel to hide its first result.
func (h *networkHarness) waitAPIReadRecovery() {
	until := time.Now().Add(5 * time.Second)
	for {
		remaining := time.Until(until)
		if remaining <= 0 {
			h.t.Fatal("API read recovery exceeded five-second bound")
		}
		code, _ := h.requestWithin("GET", "/v1/runs/"+string(h.s.RunID), nil, "", remaining)
		if code == 200 {
			return
		}
		if code < 500 || code > 599 || time.Now().After(until) {
			h.t.Fatalf("API read did not recover within bound; last HTTP status=%d", code)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

type sseObservation struct {
	cancel context.CancelFunc
	body   io.ReadCloser
	done   chan struct{}
	mu     sync.Mutex
	events []persistence.Event
	err    error
}

func (h *networkHarness) startSSE() {
	ctx, cancel := context.WithCancel(h.ctx)
	req, err := http.NewRequestWithContext(ctx, "GET", h.apiURL+"/v1/runs/"+string(h.s.RunID)+"/events", nil)
	h.fatal(err)
	req.Header.Set("Authorization", "Bearer "+h.token)
	req.Header.Set("X-Forge-Tenant", string(h.s.Tenant))
	resp, err := http.DefaultClient.Do(req)
	h.fatal(err)
	if resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		resp.Body.Close()
		cancel()
		h.t.Fatal("real TCP SSE unavailable")
	}
	o := &sseObservation{cancel: cancel, body: resp.Body, done: make(chan struct{})}
	h.sse = o
	go func() {
		defer close(o.done)
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 4096), 1<<20)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data: ") {
				var event persistence.Event
				if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
					o.mu.Lock()
					o.err = err
					o.mu.Unlock()
					return
				}
				o.mu.Lock()
				o.events = append(o.events, event)
				o.mu.Unlock()
			}
		}
		o.mu.Lock()
		o.err = scanner.Err()
		o.mu.Unlock()
	}()
}
func (o *sseObservation) snapshot() ([]persistence.Event, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]persistence.Event(nil), o.events...), o.err
}
func (o *sseObservation) close() { o.cancel(); o.body.Close(); <-o.done }

func TestRealApplicationNetworkMatrix(t *testing.T) {
	config, binary, output, u, c := continuationInputs(t)
	for _, mode := range []string{"F10_cancel_first", "F10_complete_first", "F11", "F12"} {
		if !t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
			defer cancel()
			base := newContinuation(t, ctx, mode, config, binary, output, u, c)
			h := &networkHarness{harness: base}
			defer h.closeNetwork()
			h.runNetwork()
		}) {
			return
		}
	}
}

func (h *networkHarness) runNetwork() {
	h.runnerStart("")
	h.setupAPI()
	one := h.workerNetwork("worker1")
	proof := map[string]any{"mode": h.s.Mode, "schema": h.schema, "api_pid": h.api.cmd.Process.Pid, "worker1_pid": one.cmd.Process.Pid, "model": "deterministic fake; no actual provider fee"}
	h.wait(func() bool {
		r := h.getRun()
		if r.State.Status == domain.StatusWaitingApproval {
			h.approve(r)
		}
		_, err := os.Stat(filepath.Join(h.dir, "worker1-paused.json"))
		return err == nil
	}, 35*time.Second, "actual operation boundary before network/cancel fault")
	if strings.HasPrefix(h.s.Mode, "F10_") {
		h.runCancellationOrder(one, proof)
		return
	}
	// Ensure the real container wrote its single counter and committed a native
	// receipt while Driver still knows only the earlier Start acceptance.
	h.wait(func() bool {
		var status string
		err := h.journal.QueryRow(`SELECT status FROM operations WHERE id=?`, h.s.TargetOp).Scan(&status)
		return err == nil && status == "succeeded"
	}, 10*time.Second, "actual command exit and runner receipt")
	before := h.getRun()
	original := h.inspectContinuation(h.s.TargetOp, before.State.Lease.Epoch)
	h.fatal(save(filepath.Join(h.dir, "original-before-outage.json"), original))
	h.capture("before-outage")
	var status string
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT status FROM effects WHERE tenant_id=$1 AND operation_id=$2`, h.s.Tenant, h.s.TargetOp).Scan(&status))
	if status != "in_flight" {
		h.t.Fatal("fault missed original PG effect in flight")
	}
	if h.s.Mode == "F11" {
		h.runDatabaseOutage(one, proof)
	} else {
		h.runRunnerOutage(one, proof)
	}
}

func (h *networkHarness) runCancellationOrder(one *process, proof map[string]any) {
	before := h.getRun()
	h.capture("before-cancel-race")
	if h.s.Mode == "F10_cancel_first" {
		status, _ := h.request("POST", "/v1/runs/"+string(h.s.RunID)+"/cancel", nil, "")
		if status != 202 {
			h.t.Fatal("cancel not accepted")
		}
		if h.getRun().State.Status != domain.StatusCancelRequested {
			h.t.Fatal("cancel did not commit before releasing finalization")
		}
		h.capture("cancel-committed-before-finalization-return")
		h.releaseWorker()
	} else {
		h.releaseWorker()
		h.wait(func() bool { return h.getRun().State.Status == domain.StatusCompleted }, 15*time.Second, "completion committed before cancel")
		proof["completed_version_before_cancel"] = h.getRun().State.Version
	}
	h.wait(func() bool { return h.getRun().State.Status.Terminal() }, 15*time.Second, "versioned cancellation/completion terminal")
	terminal := h.getRun()
	wanted := domain.StatusCompleted
	if h.s.Mode == "F10_cancel_first" {
		wanted = domain.StatusCancelled
	}
	if terminal.State.Status != wanted {
		h.t.Fatalf("wrong terminal winner: %s", terminal.State.Status)
	}
	for range 2 {
		status, _ := h.request("POST", "/v1/runs/"+string(h.s.RunID)+"/cancel", nil, "")
		if status != 202 {
			h.t.Fatal("idempotent terminal cancel rejected")
		}
	}
	after := h.getRun()
	if after.State.Version != terminal.State.Version || after.State.Status != terminal.State.Status {
		h.t.Fatal("late cancel reopened or advanced terminal")
	}
	h.stop(one)
	h.capture("completed")
	var epoch uint64
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT epoch FROM effects WHERE tenant_id=$1 AND operation_id=$2`, h.s.Tenant, h.s.TargetOp).Scan(&epoch))
	for k, v := range h.verifyLedgers(after, epoch) {
		proof[k] = v
	}
	var unresolved int
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT count(*) FROM effects WHERE tenant_id=$1 AND run_id=$2 AND status IN ('in_flight','unknown')`, h.s.Tenant, h.s.RunID).Scan(&unresolved))
	if unresolved != 0 {
		h.t.Fatal("terminal cancel stranded unresolved actual effects")
	}
	proof["terminal_winner"], proof["terminal_version"], proof["late_cancel_did_not_advance"], proof["race_boundary_version"] = wanted, after.State.Version, true, before.State.Version
	h.finishNetwork(proof)
}

func (h *networkHarness) runDatabaseOutage(one *process, proof map[string]any) {
	var originalCount int
	h.fatal(h.journal.QueryRow(`SELECT count(*) FROM operations WHERE workspace_id=?`, h.s.RunID).Scan(&originalCount))
	cut := h.proxy.block(true)
	proof["database_cut"] = cut
	h.releaseWorker()
	r := h.getRun()
	submission := httpapi.SubmitBody{Task: "This must not be accepted while this fixture database connection is unavailable.", BaseCommit: h.s.SourceHash, ConfigID: "fixture"}
	code, _ := h.request("POST", "/v1/projects/"+string(r.ProjectID)+"/runs", submission, "outage-unaccepted-submit")
	if code < 500 || code > 599 {
		h.t.Fatalf("unpersistable admission returned %d", code)
	}
	code, _ = h.request("GET", "/readyz", nil, "")
	if code < 500 {
		h.t.Fatal("DB-dependent readiness stayed successful during cut")
	}
	time.Sleep(2 * time.Second)
	var runs, created int
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT count(*) FROM runs`).Scan(&runs))
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT count(*) FROM run_events WHERE type='run.created'`).Scan(&created))
	if runs != 1 || created != 1 {
		h.t.Fatal("failed API request created an unpersistable run/event")
	}
	var currentCount int
	h.fatal(h.journal.QueryRow(`SELECT count(*) FROM operations WHERE workspace_id=?`, h.s.RunID).Scan(&currentCount))
	if currentCount != originalCount {
		h.t.Fatal("new runner operation dispatched during DB outage")
	}
	h.capture("during-database-outage")
	proof["worker1_sigkill_at"] = h.killContinuation(one)
	atDeath := h.getRun()
	proof["original_epoch"], proof["original_lease_until"] = atDeath.State.Lease.Epoch, atDeath.State.Lease.Until
	proof["recovery_trigger"] = "database connectivity restored; same original runner and next available persisted lease"
	proof["database_restore"] = h.proxy.block(false)
	h.fatal(save(filepath.Join(h.dir, "database-proxy-events.json"), h.proxy.log()))
	h.waitAPIReadRecovery()
	two := h.workerNetwork("worker2")
	proof["worker2_pid"] = two.cmd.Process.Pid
	h.waitReplacement(atDeath, proof)
	proof["admission_failed_without_new_run"] = true
	proof["operation_count_during_outage"] = currentCount
	h.completeNetwork(two, proof)
}

func (h *networkHarness) runRunnerOutage(one *process, proof map[string]any) {
	h.startSSE()
	proof["runner1_pid"] = h.runner.cmd.Process.Pid
	proof["runner_sigkill_at"] = h.killContinuation(h.runner)
	h.releaseWorker()
	h.wait(func() bool { return h.getRun().State.Status == domain.StatusNeedsReconciliation }, 12*time.Second, "unavailable original runner recorded unknown")
	r := h.getRun()
	var status string
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT status FROM effects WHERE tenant_id=$1 AND operation_id=$2`, h.s.Tenant, h.s.TargetOp).Scan(&status))
	if status != "unknown" {
		h.t.Fatal("runner outage did not retain unknown original effect")
	}
	code, _ := h.request("GET", "/readyz", nil, "")
	if code != 200 {
		h.t.Fatal("healthy DB API not ready during runner outage")
	}
	code, body := h.request("GET", "/v1/runs/"+string(h.s.RunID), nil, "")
	if code != 200 {
		h.t.Fatal("control read unavailable with runner down")
	}
	var observed persistence.Run
	h.fatal(json.Unmarshal(body, &observed))
	if observed.State.Status != domain.StatusNeedsReconciliation || observed.RunnerID != r.RunnerID {
		h.t.Fatal("control snapshot hid/moved original unknown run")
	}
	code, _ = h.request("POST", "/v1/runs/"+string(h.s.RunID)+"/messages", map[string]string{"text": "Fixture control event while the original runner is offline."}, "runner-outage-message")
	if code != 202 {
		h.t.Fatal("control event write failed with runner down")
	}
	h.wait(func() bool {
		events, err := h.sse.snapshot()
		if err != nil {
			h.t.Fatal(err)
		}
		for _, event := range events {
			if event.Type == "run.message_added" {
				return true
			}
		}
		return false
	}, 5*time.Second, "actual TCP SSE delivers control event while runner stays dead")
	events, _ := h.sse.snapshot()
	for i, event := range events {
		if event.Seq != uint64(i+1) {
			h.t.Fatal("SSE event sequence gap during outage")
		}
	}
	h.fatal(save(filepath.Join(h.dir, "sse-during-runner-outage.json"), events))
	h.capture("during-runner-outage")
	var active, slots int
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT active_count FROM tenant_runtime WHERE tenant_id=$1`, h.s.Tenant).Scan(&active))
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT reserved_slots FROM runners WHERE id='application-fault-runner'`).Scan(&slots))
	if active != 1 || slots != 1 || r.RunnerID != "application-fault-runner" {
		h.t.Fatal("unknown work released capacity or moved runner")
	}
	proof["worker1_sigkill_at"] = h.killContinuation(one)
	atDeath := h.getRun()
	proof["original_epoch"], proof["original_lease_until"] = atDeath.State.Lease.Epoch, atDeath.State.Lease.Until
	proof["recovery_trigger"] = "original runner restored after normal Driver deferral; no lease row edited by harness"
	h.runnerStart("")
	proof["runner2_pid"] = h.runner.cmd.Process.Pid
	two := h.workerNetwork("worker2")
	proof["worker2_pid"] = two.cmd.Process.Pid
	h.waitReplacement(atDeath, proof)
	proof["http_and_sse_available_during_runner_outage"], proof["unknown_original_allocation_retained"] = true, true
	h.completeNetwork(two, proof)
}
func (h *networkHarness) waitReplacement(before persistence.Run, proof map[string]any) {
	h.wait(func() bool { return h.getRun().State.Lease.Epoch > before.State.Lease.Epoch }, 15*time.Second, "natural replacement claim on original runner")
	var raw, owner string
	var epoch uint64
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT input_event->>'at',input_event->>'owner',(input_event->>'epoch')::bigint FROM run_snapshots WHERE tenant_id=$1 AND run_id=$2 AND input_event->>'kind'='claimed' AND (input_event->>'epoch')::bigint>$3 ORDER BY version LIMIT 1`, h.s.Tenant, h.s.RunID, before.State.Lease.Epoch).Scan(&raw, &owner, &epoch))
	at, err := time.Parse(time.RFC3339Nano, raw)
	h.fatal(err)
	if owner != "worker2-0" || epoch != before.State.Lease.Epoch+1 || at.Before(before.State.Lease.Until) {
		h.t.Fatal("replacement was not the next natural worker lease")
	}
	proof["replacement_claim_db_time"], proof["first_replacement_epoch"] = at, epoch
}
func (h *networkHarness) completeNetwork(worker *process, proof map[string]any) {
	h.wait(func() bool {
		r := h.getRun()
		if r.State.Status == domain.StatusWaitingApproval {
			h.approve(r)
		}
		return r.State.Status.Terminal()
	}, 40*time.Second, "verified repair after dependency restoration")
	r := h.getRun()
	if r.State.Status != domain.StatusCompleted || r.State.Verification != domain.VerificationVerified {
		h.t.Fatalf("restored repair failed %s/%s: %s", r.State.Status, r.State.Verification, r.State.FailureReason)
	}
	h.stop(worker)
	h.capture("completed")
	var epoch uint64
	h.fatal(h.db.Pool.QueryRow(h.ctx, `SELECT epoch FROM effects WHERE tenant_id=$1 AND operation_id=$2`, h.s.Tenant, h.s.TargetOp).Scan(&epoch))
	for k, v := range h.verifyLedgers(r, epoch) {
		proof[k] = v
	}
	var old runner.Operation
	h.fatal(readJSON(filepath.Join(h.dir, "original-before-outage.json"), &old))
	newOp := h.inspectContinuation(h.s.TargetOp, r.State.Lease.Epoch)
	left, _ := json.Marshal(old)
	right, _ := json.Marshal(newOp)
	if !bytes.Equal(left, right) {
		h.t.Fatal("original receipt/operation changed across outage")
	}
	if _, err := h.db.Heartbeat(h.ctx, h.s.Tenant, h.s.RunID, "worker1-0", epoch, 5*time.Second); !errors.Is(err, domain.ErrFenced) {
		h.t.Fatal("old worker heartbeat not fenced")
	}
	proof["original_receipt_preserved"], proof["old_epoch_heartbeat_fenced"] = true, true
	h.finishNetwork(proof)
}
func (h *networkHarness) finishNetwork(proof map[string]any) {
	if h.sse != nil {
		events, _ := h.sse.snapshot()
		h.fatal(save(filepath.Join(h.dir, "sse-observed.json"), events))
	}
	dsn := h.workerDSN
	if h.workerDSNDirect != "" {
		dsn = h.workerDSNDirect
	}
	d, closeDriver, err := driver(h.ctx, h.s, h.c, dsn, h.client)
	h.fatal(err)
	defer closeDriver()
	first, err := d.CleanupWorkspace(h.ctx, h.s.Tenant, h.s.RunID, "network-cleanup", 0)
	h.fatal(err)
	second, err := d.CleanupWorkspace(h.ctx, h.s.Tenant, h.s.RunID, "network-cleanup-repeat", 0)
	h.fatal(err)
	if first.Phase != "released" || first.SnapshotRef == "" || first.ID != second.ID || first.SnapshotRef != second.SnapshotRef || second.Phase != "released" {
		h.t.Fatal("cleanup did not preserve one snapshot/release")
	}
	proof["cleanup"], proof["repeated_cleanup_same_record"], proof["passed"] = first, true, true
	h.capture("cleanup-released")
	h.archiveArtifacts()
	h.fatal(save(filepath.Join(h.dir, "acceptance.json"), proof))
}

func TestApplicationNetworkPreflight(t *testing.T) {
	for _, mode := range []string{"F10_cancel_first", "F10_complete_first", "F11", "F12"} {
		if !networkMode(mode) {
			t.Fatal("missing declared network fixture")
		}
	}
	for _, mode := range []string{"F02", "F06", "F09", "arbitrary"} {
		if networkMode(mode) {
			t.Fatal("network child accepted unrelated fixture")
		}
	}
	if domain.ID("fixture_4_final_diff").Validate() != nil {
		t.Fatal("invalid fixed finalization operation binding")
	}
}

// This optional private-PG/loopback check starts only an API test process. It
// does not start a runner or worker, allocate a real workspace, or stop services.
func TestApplicationNetworkAPIPrivilegePreflight(t *testing.T) {
	if os.Getenv("FORGE_APP_NETWORK_API_PREFLIGHT") != "1" {
		t.Skip("explicit private PG/API preflight required; no runner process used")
	}
	config, binary, output, u, c := continuationInputs(t)
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	base := newContinuation(t, ctx, "F11", config, binary, output, u, c)
	h := &networkHarness{harness: base}
	defer h.closeNetwork()
	h.setupAPI()
	h.startSSE()
	code, _ := h.request("POST", "/v1/runs/"+string(h.s.RunID)+"/messages", map[string]string{"text": "Isolated API privilege preflight."}, "preflight-message")
	if code != 202 {
		t.Fatal("private API role failed real message write")
	}
	h.wait(func() bool {
		events, err := h.sse.snapshot()
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range events {
			if e.Type == "run.message_added" {
				return true
			}
		}
		return false
	}, 3*time.Second, "preflight real TCP SSE event")
	h.sse.close()
	h.sse = nil
	h.proxy.block(true)
	r := h.getRun()
	code, _ = h.request("POST", "/v1/projects/"+string(r.ProjectID)+"/runs", httpapi.SubmitBody{Task: "Must remain unaccepted while fixture DB is unreachable.", BaseCommit: h.s.SourceHash, ConfigID: "fixture"}, "preflight-unaccepted")
	if code < 500 || code > 599 {
		t.Fatalf("blocked private DB admission returned %d", code)
	}
	h.proxy.block(false)
	h.waitAPIReadRecovery()
	var count int
	h.fatal(h.db.Pool.QueryRow(ctx, `SELECT count(*) FROM runs`).Scan(&count))
	if count != 1 {
		t.Fatal("failed admission created a run")
	}
	h.fatal(h.journal.QueryRow(`SELECT count(*) FROM volume_leases WHERE workspace_id=?`, h.s.RunID).Scan(&count))
	if count != 0 {
		t.Fatal("API-only preflight allocated a workspace")
	}
	h.capture("api-preflight")
	h.fatal(save(filepath.Join(h.dir, "api-preflight.json"), map[string]any{"passed": true, "no_runner_or_worker_started": true, "private_api_role_verified": true, "actual_http_sse": true, "proxy_cut_recovered": true, "failed_submit_created_no_run": true, "workspace_volume_leases": 0, "run_id": h.s.RunID, "schema": h.schema}))
}
