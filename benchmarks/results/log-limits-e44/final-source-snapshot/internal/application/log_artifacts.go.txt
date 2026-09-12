package application

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
)

// A new log ref is trusted only after the immutable receipt authenticates both
// the operation binding and exact result. Stat verifies its bytes before READY.
func (d *Driver) verifiedLogArtifact(ctx context.Context, op runner.Operation) (artifact.Ref, error) {
	var job sandbox.Job
	if op.Request.Kind != "run_command" && op.Request.Kind != "verify" {
		return artifact.Ref{}, nil
	}
	reject := func() (artifact.Ref, error) { return artifact.Ref{}, domain.ErrUntrusted }
	receipt := op.Receipt
	if receipt.TenantID != op.Request.TenantID || receipt.RunID != op.Request.RunID || receipt.Kind != "operation_receipt" || receipt.Size < 0 || receipt.Size > 2<<20 {
		return reject()
	}
	input, err := d.Artifacts.Open(ctx, op.Request.TenantID, op.Request.RunID, receipt)
	if err != nil {
		return artifact.Ref{}, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(input, (2<<20)+1))
	closeErr := input.Close()
	if readErr != nil {
		return artifact.Ref{}, readErr
	}
	if closeErr != nil {
		return artifact.Ref{}, closeErr
	}
	if len(raw) > 2<<20 {
		return reject()
	}
	var original runner.Operation
	if json.Unmarshal(raw, &original) != nil {
		return reject()
	}
	requested := op.Request
	requested.Grant = ""
	expected, _ := json.Marshal(requested)
	bound, _ := json.Marshal(original.Request)
	if !bytes.Equal(expected, bound) || original.Status != op.Status || original.JobID != op.JobID || original.AfterRevision != op.AfterRevision || original.AfterHash != op.AfterHash {
		return reject()
	}
	// Legacy classification comes only from the authenticated original receipt,
	// never from mutable/reduced RPC metadata. Legacy RPC truncation remains valid.
	if len(original.Result) > 0 && json.Unmarshal(original.Result, &job) != nil {
		return reject()
	}
	if job.Log == nil {
		if job.StrictLogs {
			return reject()
		}
		return artifact.Ref{}, nil
	}
	if !bytes.Equal(original.Result, op.Result) {
		return reject()
	}
	log := job.Log
	p, err := log.Policy.Normalize()
	if err != nil || p != log.Policy || log.SchemaVersion != 1 || log.OperationID != op.Request.OperationID || !job.StrictLogs {
		return reject()
	}
	ref := log.Artifact
	if ref.TenantID != op.Request.TenantID || ref.RunID != op.Request.RunID || ref.Kind != "operation_log" || ref.Size != log.RetainedBytes || ref.Size < 0 || ref.Size > int64(p.OperationBytes) {
		return reject()
	}
	if _, err = d.Artifacts.Stat(ctx, op.Request.TenantID, op.Request.RunID, ref); err != nil {
		return artifact.Ref{}, err
	}
	return ref, nil
}
