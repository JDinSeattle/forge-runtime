package benchmarks

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	flow "github.com/JDinSeattle/forge-runtime/internal/runtime"
)

// This simulated controller must satisfy the v2 closed-batch persistence
// contract. Its observation remains explicitly synthetic: only Driver tests
// exercise production semantic normalization of actual runner operations.
func (h *terminalHarness) simulatedClosedContext(ctx context.Context, r persistence.Run, contextRef string, receipt flow.EffectReceipt) (flow.Event, error) {
	if !receipt.Settled || receipt.Status != flow.EffectSucceeded {
		return flow.Event{}, domain.ErrUntrusted
	}
	a, err := h.store.LatestAttempt(ctx, r.TenantID, r.ID, r.State.StepSeq)
	if err != nil {
		return flow.Event{}, err
	}
	if a.Status != "completed" {
		return flow.Event{}, domain.ErrReconciliation
	}
	hash := sha256.Sum256([]byte("simulation-only-receipt:" + receipt.ArgsHash))
	f := &flow.ProgressFrame{
		ContextRef: contextRef, PolicyVersion: flow.ProgressPolicyVersion,
		StepSeq: r.State.StepSeq, AttemptID: a.ID,
		Observations: []flow.ProgressObservation{{
			EffectID: receipt.EffectID, ReceiptRef: receipt.Ref,
			Fingerprint: hex.EncodeToString(hash[:]),
			BeforeHash:  strings.Repeat("0", 64), AfterHash: strings.Repeat("0", 64),
		}},
	}
	ref, err := h.publish(ctx, r, "progress_report", f, "")
	return flow.Event{Kind: flow.EventContextBuilt, OutputRef: contextRef, Progress: f, ProgressRef: ref}, err
}
