package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/provider"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	flow "github.com/JDinSeattle/forge-runtime/internal/runtime"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
)

func progressDigest(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	b, err = domain.CanonicalJSON(b)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}

// Normalize only known structural wrappers. Never remove arbitrary substrings
// from user output to manufacture equality. Truly truncated output is unknown.
func observation(e persistence.StoredEffect, op runner.Operation) (flow.ProgressObservation, error) {
	o := flow.ProgressObservation{EffectID: e.Effect.ID, ReceiptRef: e.Effect.ReceiptRef, BeforeHash: op.BeforeHash, AfterHash: op.AfterHash}
	r := op.Request
	if r.OperationID != e.Effect.ID || r.Kind != e.Effect.Kind || r.ArgsHash != e.Effect.ArgsHash || r.ExpectedRevision != e.Effect.ExpectedRevision || r.Epoch != e.Effect.DispatchEpoch || r.PolicyVersion != e.Effect.PolicyVersion || flow.EffectStatus(op.Status) != e.Effect.Status || !op.Status.Terminal() {
		return o, domain.ErrUntrusted
	}
	var args, result any
	canonical, err := domain.CanonicalJSON(r.Args)
	if err != nil {
		return o, err
	}
	h := sha256.Sum256(canonical)
	if hex.EncodeToString(h[:]) != r.ArgsHash {
		return o, domain.ErrUntrusted
	}
	args = r.ArgsHash
	result = op.Result
	contextBytes := len(op.Result)
	if op.Error != "" {
		contextBytes += len(op.Error) + 8
	}
	o.Indeterminate = op.ResultTruncated || contextBytes > 16000
	switch r.Kind {
	case "read_file":
		// The actual result includes path, whole-file SHA, normalized first line
		// and returned content. Omitted end/start and equivalent explicit args
		// must not fabricate a fresh observation of the same slice.
		if op.Status == runner.Succeeded {
			args = nil
		}
	case "run_command", "verify":
		if len(op.Result) != 0 {
			var job sandbox.Job
			if err := json.Unmarshal(op.Result, &job); err != nil {
				return o, err
			}
			o.Indeterminate = o.Indeterminate || job.Truncated || job.Running || (r.Kind == "verify" && len(job.Output) > 4000)
			job.ID = ""
			result = job
		}
	case "list_files", "search_code", "get_diff", "apply_patch":
		if r.Kind == "search_code" && len(op.Result) != 0 {
			var v struct {
				Truncated bool `json:"truncated"`
			}
			if err := json.Unmarshal(op.Result, &v); err != nil {
				return o, err
			}
			o.Indeterminate = o.Indeterminate || v.Truncated
		}
	default:
		return o, domain.ErrUntrusted
	}
	if !o.Indeterminate {
		// Tree hashes are content identities, not revision counters. Excluding
		// read-only workspace context lets reading A remain known after editing B.
		var inputTree, outputTree string
		if r.Kind == "run_command" || r.Kind == "verify" || r.Kind == "apply_patch" {
			inputTree, outputTree = op.BeforeHash, op.AfterHash
		}
		o.Fingerprint, err = progressDigest(struct {
			Kind                         string
			Args, Result                 any
			Status                       runner.Status
			Error, InputTree, OutputTree string
		}{r.Kind, args, result, op.Status, op.Error, inputTree, outputTree})
	}
	return o, err
}

func (d *Driver) contextProgress(ctx context.Context, r persistence.Run, contextRef string, messageSeq uint64) (flow.Event, error) {
	event := flow.Event{Kind: flow.EventContextBuilt, OutputRef: contextRef}
	if r.State.Limits.MaxNoProgressBatches == 0 {
		return event, nil
	}
	f := &flow.ProgressFrame{PolicyVersion: flow.ProgressPolicyVersion, StepSeq: r.State.StepSeq, MessageSeq: messageSeq, Observations: []flow.ProgressObservation{}}
	if f.StepSeq != 0 {
		a, err := d.Store.LatestAttempt(ctx, r.TenantID, r.ID, r.State.StepSeq)
		if err != nil {
			return event, err
		}
		if a.Status != "completed" {
			return event, domain.ErrReconciliation
		}
		f.AttemptID = a.ID
		var turn provider.ModelTurn
		if err = d.load(ctx, r, a.RawRef, &turn); err != nil {
			return event, err
		}
		if turn.RunID != string(r.ID) || turn.StepID != fmt.Sprint(r.State.StepSeq) || turn.AttemptID != string(a.ID) {
			return event, domain.ErrUntrusted
		}
		finish := len(turn.ToolCalls) == 0 || (len(turn.ToolCalls) == 1 && turn.ToolCalls[0].Name == "finish")
		items, err := d.Store.Effects(ctx, r.TenantID, r.ID, r.State.StepSeq)
		if err != nil {
			return event, err
		}
		var verification flow.VerificationEvidence
		var verificationRefs []string
		if finish {
			f.VerificationRef = r.State.VerificationReportRef
			if f.VerificationRef == "" || r.State.VerificationRevision != r.State.WorkspaceRevision {
				return event, domain.ErrUntrusted
			}
			var report struct {
				Evidence flow.VerificationEvidence `json:"evidence"`
				Receipts []string                  `json:"receipts"`
			}
			if err = d.load(ctx, r, f.VerificationRef, &report); err != nil {
				return event, err
			}
			if !report.Evidence.Trusted || report.Evidence.WorkspaceRevision != r.State.WorkspaceRevision {
				return event, domain.ErrUntrusted
			}
			verification, verificationRefs = report.Evidence, report.Receipts
		}
		failed, seen := false, 0
		for _, e := range items {
			if finish {
				if e.Ordinal != -2 && e.Ordinal != -3 {
					continue
				}
			} else {
				if e.Ordinal < 0 {
					continue
				}
				if e.Ordinal != seen || e.Ordinal >= len(turn.ToolCalls) || e.Effect.Kind != turn.ToolCalls[e.Ordinal].Name {
					return event, domain.ErrUntrusted
				}
				seen++
				if failed {
					if e.Effect.Status != flow.EffectPlanned || e.Effect.ReceiptRef != "" {
						return event, domain.ErrReconciliation
					}
					continue // never-executed remainder of a failed batch
				}
			}
			var op runner.Operation
			if e.Effect.ReceiptRef == "" {
				return event, domain.ErrReconciliation
			}
			if err = d.load(ctx, r, e.Effect.ReceiptRef, &op); err != nil {
				return event, err
			}
			if op.Request.TenantID != r.TenantID || op.Request.RunID != r.ID || op.Request.WorkspaceID != r.ID {
				return event, domain.ErrUntrusted
			}
			o, err := observation(e, op)
			if err != nil {
				return event, err
			}
			if finish {
				if !slices.Contains(verificationRefs, e.Effect.ReceiptRef) {
					return event, domain.ErrUntrusted
				}
				if !o.Indeterminate {
					o.Fingerprint, err = progressDigest(struct {
						Result                       string
						Baseline, Target, Regression bool
					}{o.Fingerprint, verification.BaselineTargetFailed, verification.TargetPassed, verification.RegressionPassed})
					if err != nil {
						return event, err
					}
				}
			}
			f.Observations = append(f.Observations, o)
			failed = e.Effect.Status != flow.EffectSucceeded
		}
		if finish && len(f.Observations) != len(verificationRefs)-1 {
			return event, domain.ErrUntrusted
		}
		if !finish && seen != len(turn.ToolCalls) {
			return event, domain.ErrUntrusted
		}
		if len(f.Observations) == 0 {
			return event, domain.ErrUntrusted
		}
	}
	// The report is bound to this exact immutable context by both the stored
	// artifact bytes and the event committed with it.
	f.ContextRef = contextRef
	ref, err := d.put(ctx, r, "progress_report", f)
	if err != nil {
		return event, err
	}
	event.Progress, event.ProgressRef = f, ref
	if err = d.fault("after_progress_report_before_transition"); err != nil {
		return event, err
	}
	return event, nil
}
