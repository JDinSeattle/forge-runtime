package runtime

import (
	"encoding/hex"
	"fmt"
	"slices"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
)

const DefaultNoProgressBatches = 3
const ProgressPolicyVersion = 1
const ProgressEvidenceWindow = 512
const ProgressTreeWindow = 32

// ProgressState is a bounded history of observations, not an estimate of model
// intelligence. Eviction conservatively treats an old observation as new again.
type ProgressState struct {
	LastCheckedStep uint64   `json:"last_checked_step"`
	MessageSeq      uint64   `json:"message_seq"`
	RepeatedBatches uint64   `json:"repeated_batches"`
	Classification  string   `json:"classification"`
	RecentEvidence  []string `json:"recent_evidence"`
	RecentTrees     []string `json:"recent_trees"`
}

type ProgressObservation struct {
	EffectID      domain.ID `json:"effect_id"`
	ReceiptRef    string    `json:"receipt_ref"`
	Fingerprint   string    `json:"fingerprint,omitempty"`
	BeforeHash    string    `json:"before_hash"`
	AfterHash     string    `json:"after_hash"`
	Indeterminate bool      `json:"indeterminate,omitempty"`
}

// The driver derives this frame from complete, authenticated receipt artifacts.
// Persistence binds its step/attempt/receipts and context message watermark in
// the same transaction as the reducer decision. Never built from stream deltas.
type ProgressFrame struct {
	ContextRef      string                `json:"context_ref"`
	PolicyVersion   uint32                `json:"policy_version"`
	StepSeq         uint64                `json:"step_seq"`
	MessageSeq      uint64                `json:"message_seq"`
	AttemptID       domain.ID             `json:"attempt_id,omitempty"`
	VerificationRef string                `json:"verification_ref,omitempty"`
	Observations    []ProgressObservation `json:"observations"`
}

func validProgressHash(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == 32 && hex.EncodeToString(b) == s
}

func validateProgressState(s State) error {
	if s.Limits.MaxNoProgressBatches > 20 || (s.SchemaVersion == 1 && (s.Limits.MaxNoProgressBatches != 0 || s.Progress != nil)) {
		return fmt.Errorf("%w: progress policy/snapshot version", domain.ErrInvalid)
	}
	p := s.Progress
	if p == nil {
		return nil
	}
	if s.Limits.MaxNoProgressBatches == 0 || p.LastCheckedStep > s.StepSeq || p.RepeatedBatches > s.Limits.MaxNoProgressBatches || p.MessageSeq > 64 || len(p.RecentEvidence) > ProgressEvidenceWindow || len(p.RecentTrees) > ProgressTreeWindow {
		return fmt.Errorf("%w: progress state bound", domain.ErrInvalid)
	}
	for _, list := range [][]string{p.RecentEvidence, p.RecentTrees} {
		seen := map[string]bool{}
		for _, hash := range list {
			if !validProgressHash(hash) || seen[hash] {
				return domain.ErrInvalid
			}
			seen[hash] = true
		}
	}
	switch p.Classification {
	case "initial", "message", "new_evidence", "repeated", "indeterminate":
	default:
		return domain.ErrInvalid
	}
	return nil
}

// remember adds only first observations, in deterministic receipt order.
func remember(list *[]string, value string, capacity int) bool {
	if slices.Contains(*list, value) {
		return false
	}
	*list = append(*list, value)
	if len(*list) > capacity {
		*list = (*list)[len(*list)-capacity:]
	}
	return true
}

func observeProgress(s *State, e Event) error {
	if s.Limits.MaxNoProgressBatches == 0 {
		return nil
	}
	f := e.Progress
	// Initial context without messages has no completed batch to assess. This
	// also keeps existing internal callers constructing initial contexts valid;
	// the transaction still checks the durable message watermark (zero).
	if f == nil && s.StepSeq == 0 {
		f = &ProgressFrame{PolicyVersion: ProgressPolicyVersion}
	}
	if f == nil || (f.ContextRef != "" && f.ContextRef != e.OutputRef) || f.PolicyVersion != ProgressPolicyVersion || f.StepSeq != s.StepSeq || f.MessageSeq > 64 || len(f.Observations) > 130 || s.PendingEffect != nil || len(s.RemainingEffects) != 0 {
		return fmt.Errorf("%w: incomplete progress boundary", domain.ErrInvalid)
	}
	if s.Progress != nil && (f.MessageSeq < s.Progress.MessageSeq || f.StepSeq <= s.Progress.LastCheckedStep) {
		return domain.ErrConflict
	}
	if f.StepSeq == 0 {
		if f.AttemptID != "" || len(f.Observations) != 0 || f.VerificationRef != "" {
			return domain.ErrInvalid
		}
		s.Progress = &ProgressState{MessageSeq: f.MessageSeq, Classification: "initial", RecentEvidence: []string{}, RecentTrees: []string{}}
		return nil
	}
	if f.AttemptID == "" || len(f.Observations) == 0 || e.ProgressRef == "" {
		return domain.ErrUntrusted
	}
	if s.Progress == nil {
		return fmt.Errorf("%w: missing earlier context progress", domain.ErrInvalid)
	}
	p := s.Progress
	novel, uncertain := false, false
	seen := map[domain.ID]bool{}
	for _, o := range f.Observations {
		if o.EffectID.Validate() != nil || o.ReceiptRef == "" || seen[o.EffectID] || !validProgressHash(o.BeforeHash) || !validProgressHash(o.AfterHash) || (!o.Indeterminate && !validProgressHash(o.Fingerprint)) || (o.Indeterminate && o.Fingerprint != "") {
			return domain.ErrUntrusted
		}
		seen[o.EffectID] = true
		// The first before-tree is a baseline, not novel progress. Afterwards
		// returning to an old tree cannot reset the limit via revision numbers.
		if len(p.RecentTrees) == 0 {
			remember(&p.RecentTrees, o.BeforeHash, ProgressTreeWindow)
		}
		if remember(&p.RecentTrees, o.AfterHash, ProgressTreeWindow) {
			novel = true
		}
		if o.Indeterminate {
			uncertain = true
		} else if remember(&p.RecentEvidence, o.Fingerprint, ProgressEvidenceWindow) {
			novel = true
		}
	}
	p.LastCheckedStep = f.StepSeq
	switch {
	case f.MessageSeq > p.MessageSeq:
		p.RepeatedBatches, p.Classification = 0, "message"
	case novel:
		p.RepeatedBatches, p.Classification = 0, "new_evidence"
	case uncertain:
		p.RepeatedBatches, p.Classification = 0, "indeterminate"
	default:
		p.RepeatedBatches++
		p.Classification = "repeated"
	}
	p.MessageSeq = f.MessageSeq
	return nil
}
