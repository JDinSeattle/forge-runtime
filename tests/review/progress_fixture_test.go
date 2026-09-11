package review_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	flow "github.com/JDinSeattle/forge-runtime/internal/runtime"
)

// reviewContext is for existing declarative Store-only lifecycle fixtures. Its
// observations are explicitly synthetic; real semantic receipt normalization is
// separately exercised by application.TestProgress* with the production Driver.
// Preserve the new message/report/ledger contract instead of disabling v2 guards.
func reviewContext(t *testing.T, ctx context.Context, s *persistence.Store, r persistence.Run, ref string) flow.Event {
	t.Helper()
	messages, err := s.ConsumeMessages(ctx, r.TenantID, r.ID, r.State.Lease.Owner, r.State.Lease.Epoch, r.State.StepSeq)
	if err != nil {
		t.Fatal(err)
	}
	f := &flow.ProgressFrame{ContextRef: ref, PolicyVersion: flow.ProgressPolicyVersion, StepSeq: r.State.StepSeq, Observations: []flow.ProgressObservation{}}
	for _, m := range messages {
		f.MessageSeq = m.Seq
	}
	if f.StepSeq > 0 {
		a, err := s.LatestAttempt(ctx, r.TenantID, r.ID, f.StepSeq)
		if err != nil {
			t.Fatal(err)
		}
		f.AttemptID = a.ID
		effects, err := s.Effects(ctx, r.TenantID, r.ID, f.StepSeq)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range effects {
			if e.Ordinal < 0 {
				continue
			}
			if e.Effect.Status != flow.EffectSucceeded || e.Effect.ReceiptRef == "" {
				t.Fatal("fixture boundary is not settled")
			}
			h := sha256.Sum256([]byte("synthetic-control-fixture:" + e.Effect.Kind + ":" + e.Effect.ArgsHash))
			f.Observations = append(f.Observations, flow.ProgressObservation{EffectID: e.Effect.ID, ReceiptRef: e.Effect.ReceiptRef, Fingerprint: hex.EncodeToString(h[:]), BeforeHash: strings.Repeat("0", 64), AfterHash: strings.Repeat("0", 64)})
		}
	}
	objects, err := artifact.NewLocalStore(t.TempDir(), 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = objects.Close() })
	body, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := objects.Put(ctx, r.TenantID, r.ID, "progress_report", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return flow.Event{Kind: flow.EventContextBuilt, OutputRef: ref, Progress: f, ProgressRef: recoveryPublish(t, ctx, s, proof)}
}
