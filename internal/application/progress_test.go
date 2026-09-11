package application

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	flow "github.com/JDinSeattle/forge-runtime/internal/runtime"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
)

func observedFixture(kind, args string, result any) (persistence.StoredEffect, runner.Operation) {
	a, _ := domain.CanonicalJSON([]byte(args))
	h := sha256.Sum256(a)
	digest := hex.EncodeToString(h[:])
	b, _ := json.Marshal(result)
	e := persistence.StoredEffect{Effect: flow.Effect{ID: "effect_1", Kind: kind, Args: a, ArgsHash: digest, ExpectedRevision: 1, DispatchEpoch: 1, PolicyVersion: "workspace-v1", Status: flow.EffectSucceeded, ReceiptRef: "receipt_1"}}
	op := runner.Operation{Request: runner.OperationRequest{OperationID: e.Effect.ID, Kind: kind, Args: a, ArgsHash: digest, ExpectedRevision: 1, PolicyVersion: "workspace-v1"}, Status: runner.Succeeded, BeforeHash: strings.Repeat("a", 64), AfterHash: strings.Repeat("a", 64), AfterRevision: 1, Result: b}
	op.Request.Epoch = 1
	return e, op
}
func TestProgressNormalizesActualReceiptFacts(t *testing.T) {
	result := map[string]any{"path": "a.py", "sha256": strings.Repeat("a", 64), "start_line": 1, "content": "one\ntwo"}
	e, a := observedFixture("read_file", `{"path":"a.py"}`, result)
	f, b := observedFixture("read_file", `{"path":"a.py","start_line":1,"end_line":2}`, result)
	x, err := observation(e, a)
	if err != nil {
		t.Fatal(err)
	}
	y, err := observation(f, b)
	if err != nil {
		t.Fatal(err)
	}
	if x.Fingerprint != y.Fingerprint {
		t.Fatal("default args manufactured novelty")
	}
	for _, field := range []string{"path", "sha256", "content", "start_line"} {
		changed := map[string]any{}
		for k, v := range result {
			changed[k] = v
		}
		changed[field] = "different"
		f, b = observedFixture("read_file", `{"path":"a.py"}`, changed)
		y, err = observation(f, b)
		if err != nil || y.Fingerprint == x.Fingerprint {
			t.Fatal("lost new read evidence", field)
		}
	}
	e, a = observedFixture("run_command", `{"command":["test"]}`, sandbox.Job{ID: "job_a", Started: true, ExitCode: 1, Output: []byte("failure")})
	f, b = observedFixture("run_command", `{"command":["test"]}`, sandbox.Job{ID: "job_b", Started: true, ExitCode: 1, Output: []byte("failure")})
	f.Effect.ID = "effect_2"
	b.Request.OperationID = f.Effect.ID
	f.Effect.DispatchEpoch = 2
	b.Request.Epoch = 2
	f.Effect.ExpectedRevision = 99
	b.Request.ExpectedRevision = 99
	x, err = observation(e, a)
	if err != nil {
		t.Fatal(err)
	}
	y, err = observation(f, b)
	if err != nil {
		t.Fatal(err)
	}
	if x.Fingerprint != y.Fingerprint {
		t.Fatal("identity/revision/job ID manufactured novelty")
	}
	b.Result, _ = json.Marshal(sandbox.Job{ID: "job_b", Started: true, ExitCode: 1, Output: []byte("failure"), Truncated: true})
	y, err = observation(f, b)
	if err != nil || !y.Indeterminate || y.Fingerprint != "" {
		t.Fatal("truncated evidence counted")
	}

	b.Result, _ = json.Marshal(sandbox.Job{Started: true, ExitCode: 1, Output: []byte(strings.Repeat("x", 17000))})
	y, err = observation(f, b)
	if err != nil || !y.Indeterminate {
		t.Fatal("context truncation was ignored")
	}
	b.Request.ArgsHash = strings.Repeat("f", 64)
	if _, err = observation(f, b); err == nil {
		t.Fatal("unbound receipt accepted")
	}
}
