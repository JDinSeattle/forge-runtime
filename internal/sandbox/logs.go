package sandbox

import (
	"errors"
	"fmt"

	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
)

var ErrLogLimit = errors.New("operation_log_limit")
var ErrCaptureGap = errors.New("log_capture_incomplete")

// LogPolicy is trusted runner configuration. Limits count encoded spool bytes,
// including record headers. Reservations are cumulative for the run lifetime.
type LogPolicy struct {
	EntryBytes     int `json:"entry_bytes"`
	OperationBytes int `json:"operation_bytes"`
	RunBytes       int `json:"run_bytes"`
	MaxOperations  int `json:"max_operations"`
	PreviewBytes   int `json:"preview_bytes"`
}

func (p LogPolicy) Normalize() (LogPolicy, error) {
	values := []*int{&p.EntryBytes, &p.OperationBytes, &p.RunBytes, &p.MaxOperations, &p.PreviewBytes}
	maxima := []int{16 << 10, 512 << 10, 16 << 20, 32, 64 << 10}
	for i, v := range values {
		if *v == 0 {
			*v = maxima[i]
		}
		if *v < 1 || *v > maxima[i] {
			return p, fmt.Errorf("%w: log policy limit", domain.ErrInvalid)
		}
	}
	if p.EntryBytes < 64 || p.OperationBytes < p.EntryBytes || p.RunBytes < p.OperationBytes || p.PreviewBytes > p.OperationBytes {
		return p, fmt.Errorf("%w: inconsistent log policy", domain.ErrInvalid)
	}
	return p, nil
}

type LogSummary struct {
	SchemaVersion        int          `json:"schema_version"`
	Policy               LogPolicy    `json:"policy"`
	OperationID          domain.ID    `json:"operation_id"`
	BindingHash          string       `json:"binding_hash"`
	StdoutSeen           uint64       `json:"stdout_seen"`
	StderrSeen           uint64       `json:"stderr_seen"`
	RetainedBytes        int64        `json:"retained_bytes"`
	RetainedPayload      int64        `json:"retained_payload"`
	Records              uint64       `json:"records"`
	Complete             bool         `json:"complete"`
	DroppedKnown         bool         `json:"dropped_known"`
	DroppedBytes         uint64       `json:"dropped_bytes"`
	Truncated            bool         `json:"truncated"`
	Reason               string       `json:"reason,omitempty"`
	TerminationRequested bool         `json:"termination_requested"`
	TerminationObserved  bool         `json:"termination_observed"`
	Artifact             artifact.Ref `json:"artifact"`
}

// LogCapture stores trusted binary records. Write must consume/discard input
// after its first failure so that a log limit never stops pipe drainage.
// Finish is called only after the attach reader has stopped; complete means an
// EOF and an independently observed stopped container, not socket EOF alone.
type LogCapture interface {
	Write(byte, []byte) error
	Finish(complete bool, reason string, requested, observed bool) error
	Snapshot() (LogSummary, []byte)
}

func VerificationLogValid(job Job) bool {
	return job.Log == nil || (job.Log.Complete && !job.Log.Truncated && job.Log.Reason == "" && !job.Log.TerminationRequested)
}
