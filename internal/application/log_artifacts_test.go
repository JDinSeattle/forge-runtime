package application

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
)

func TestStrictLogArtifactReceiptBindingAndBytes(t *testing.T) {
	ctx := context.Background()
	store, err := artifact.NewLocalStore(filepath.Join(t.TempDir(), "artifacts"), 2<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	d := &Driver{Artifacts: store}
	p, _ := (sandbox.LogPolicy{}).Normalize()
	ref, err := store.Put(ctx, "tenant", "run", "operation_log", bytes.NewReader([]byte("framed retained bytes")))
	if err != nil {
		t.Fatal(err)
	}
	job := sandbox.Job{StrictLogs: true, Log: &sandbox.LogSummary{SchemaVersion: 1, Policy: p, OperationID: "operation", RetainedBytes: ref.Size, Complete: true, Artifact: ref}}
	result, _ := json.Marshal(job)
	original := runner.Operation{Request: runner.OperationRequest{WorkspaceRequest: runner.WorkspaceRequest{TenantID: "tenant", RunID: "run", WorkspaceID: "workspace", Epoch: 1}, OperationID: "operation", Kind: "run_command", Args: json.RawMessage(`{"command":["fixture"]}`), ArgsHash: "fixed", PolicyVersion: "v1", Deadline: time.Now().Add(time.Minute).UTC()}, Status: runner.Succeeded, Result: result}
	bind := func(op runner.Operation) runner.Operation {
		raw, _ := json.Marshal(op)
		receipt, err := store.Put(ctx, "tenant", "run", "operation_receipt", bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		op.Receipt = receipt
		return op
	}
	good := bind(original)
	for n := 0; n < 3; n++ {
		if got, err := d.verifiedLogArtifact(ctx, good); err != nil || got != ref {
			t.Fatal("repeat validation changed artifact", got, err)
		}
	}
	for name, mutate := range map[string]func(*sandbox.Job){"cross_run": func(j *sandbox.Job) { j.Log.Artifact.RunID = "other" }, "cross_tenant": func(j *sandbox.Job) { j.Log.Artifact.TenantID = "other" }, "wrong_kind": func(j *sandbox.Job) { j.Log.Artifact.Kind = "workspace_snapshot" }, "wrong_size": func(j *sandbox.Job) { j.Log.Artifact.Size++ }, "missing_object": func(j *sandbox.Job) {
		j.Log.Artifact.SHA256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		j.Log.Artifact.ObjectKey = "tenant/run/" + j.Log.Artifact.SHA256
	}, "operation_binding": func(j *sandbox.Job) { j.Log.OperationID = "other" }, "missing_log": func(j *sandbox.Job) { j.Log = nil }} {
		t.Run(name, func(t *testing.T) {
			var j sandbox.Job
			json.Unmarshal(result, &j)
			mutate(&j)
			changed := original
			changed.Result, _ = json.Marshal(j)
			// Bind the malformed ref into a new valid immutable receipt too: this proves
			// tenant/kind/size checks do not merely rely on receipt equality.
			changed = bind(changed)
			if _, err := d.verifiedLogArtifact(ctx, changed); err == nil {
				t.Fatal("malformed bound log accepted")
			}
		})
	}
	t.Run("substitute_after_receipt", func(t *testing.T) {
		j := job
		ref2, err := store.Put(ctx, "tenant", "run", "operation_log", bytes.NewReader([]byte("other retained bytes!")))
		if err != nil {
			t.Fatal(err)
		}
		copied := *j.Log
		j.Log = &copied
		j.Log.Artifact = ref2
		j.Log.RetainedBytes = ref2.Size
		changed := good
		changed.Result, _ = json.Marshal(j)
		if _, err = d.verifiedLogArtifact(ctx, changed); err == nil {
			t.Fatal("unreceipted substitution accepted")
		}
	})
	t.Run("strip_strict_to_legacy", func(t *testing.T) {
		changed := good
		changed.Result = json.RawMessage(`{"id":"job","started":true}`)
		if _, err := d.verifiedLogArtifact(ctx, changed); err == nil {
			t.Fatal("stripped RPC metadata hid authenticated strict receipt")
		}
	})
	t.Run("authentic_legacy_truncated_rpc", func(t *testing.T) {
		legacy := original
		legacy.Result = json.RawMessage(`{"id":"job","output":"bGVnYWN5"}`)
		legacy = bind(legacy)
		legacy.Result = nil
		legacy.ResultTruncated = true
		if got, err := d.verifiedLogArtifact(ctx, legacy); err != nil || got.ObjectKey != "" {
			t.Fatal("authentic legacy receipt rejected", err)
		}
	})
	if err = store.Delete(ctx, "tenant", "run", ref); err != nil {
		t.Fatal(err)
	}
	if _, err = d.verifiedLogArtifact(ctx, good); err == nil {
		t.Fatal("missing retained bytes accepted")
	}
}
