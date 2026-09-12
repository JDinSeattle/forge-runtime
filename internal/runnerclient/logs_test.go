package runnerclient

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/runner"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
)

func TestStrictLogRPCPreviewPreservesPolicyMetadata(t *testing.T) {
	p, _ := (sandbox.LogPolicy{}).Normalize()
	job := sandbox.Job{ID: "job", Started: true, ExitCode: 137, Output: bytes.Repeat([]byte{0xff}, p.PreviewBytes), Truncated: true, Log: &sandbox.LogSummary{SchemaVersion: 1, Policy: p, Complete: true, Truncated: true, Reason: "output_limit", TerminationRequested: true, TerminationObserved: true}}
	raw, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	original := runner.Operation{Request: runner.OperationRequest{WorkspaceRequest: runner.WorkspaceRequest{TenantID: "tenant", RunID: "run", WorkspaceID: "workspace", Epoch: 3, Grant: "private-not-returned"}, OperationID: "operation", Kind: "run_command", Args: json.RawMessage(`{"command":["fixture"]}`), ArgsHash: "hash", PolicyVersion: "v1", ExpectedRevision: 7, Deadline: time.Now().Add(time.Minute).UTC()}, Status: runner.Failed, Result: raw}
	wire, err := operation(original)
	if err != nil {
		t.Fatal(err)
	}
	if wire.ResultTruncated || len(wire.ResultJson) == 0 || len(wire.ResultJson) > 1<<20 {
		t.Fatal("bounded preview lost whole result")
	}
	roundtrip, err := fromOperation(wire)
	if err != nil {
		t.Fatal(err)
	}
	if roundtrip.Request.Grant != "" || roundtrip.Request.OperationID != original.Request.OperationID || roundtrip.Request.WorkspaceRequest.Epoch != 3 || !bytes.Equal(roundtrip.Request.Args, original.Request.Args) {
		t.Fatal("binding roundtrip failed")
	}
	var returned sandbox.Job
	if json.Unmarshal(wire.ResultJson, &returned) != nil || returned.Log == nil || returned.Log.Reason != "output_limit" || !bytes.Equal(returned.Output, job.Output) {
		t.Fatal("log evidence lost in RPC mapping")
	}
}
