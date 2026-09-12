// Package runner owns local operation facts independently of worker leases.
package runner

import (
	"context"
	"encoding/json"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
	"github.com/JDinSeattle/forge-runtime/internal/telemetry"
)

type Config struct {
	Telemetry *telemetry.Telemetry
	// Nil preserves legacy embedding compatibility. The executable supplies the
	// normalized strict policy by default for every newly admitted process.
	Logs        *sandbox.LogPolicy
	VolumeSlots []sandbox.VolumeSpec
	// TestVolumeVerifier is only accepted with the explicit no-process TestBackend.
	TestVolumeVerifier func(context.Context, sandbox.VolumeSpec) error
	RootDir            string
	JournalPath        string
	Artifacts          artifact.Store
	Backend            sandbox.Backend
	Signer             *Signer
	Profiles           map[string]sandbox.Profile
	Sources            map[string]string
	Now                func() time.Time
	MaxFileBytes       int64
	MaxWorkspaceBytes  int64
	// Fault is an explicit test hook at durable boundaries. It is nil in normal
	// deployment and is not configurable by requests or repository content.
	Fault func(string) error
	// OperatorFault is wired only by the local runner executable explicit fault CLI.
	OperatorFault func(string, OperationRequest) error
}

type WorkspaceRequest struct {
	TenantID    domain.ID `json:"tenant_id"`
	RunID       domain.ID `json:"run_id"`
	WorkspaceID domain.ID `json:"workspace_id"`
	Epoch       uint64    `json:"epoch"`
	Grant       string    `json:"grant"`
}
type PrepareRequest struct {
	WorkspaceRequest
	SourceID  string `json:"source_id"`
	ProfileID string `json:"profile_id"`
}
type OperationRequest struct {
	WorkspaceRequest
	OperationID      domain.ID       `json:"operation_id"`
	ExpectedRevision uint64          `json:"expected_revision"`
	Kind             string          `json:"kind"`
	Args             json.RawMessage `json:"args"`
	ArgsHash         string          `json:"args_hash"`
	PolicyVersion    string          `json:"policy_version"`
	Deadline         time.Time       `json:"deadline"`
}
type InspectRequest struct {
	WorkspaceRequest
	OperationID domain.ID `json:"operation_id"`
}
type Workspace struct {
	TenantID        domain.ID `json:"tenant_id"`
	RunID           domain.ID `json:"run_id"`
	ID              domain.ID `json:"id"`
	SourceID        string    `json:"source_id"`
	ProfileID       string    `json:"profile_id"`
	Epoch           uint64    `json:"epoch"`
	Revision        uint64    `json:"revision"`
	ActiveOperation domain.ID `json:"active_operation,omitempty"`
	Stopped         bool      `json:"stopped"`
	Adopting        bool      `json:"adopting"`
	Released        bool      `json:"released"`
	BaselineHash    string    `json:"baseline_hash"`
}
type Status string

const (
	Prepared  Status = "prepared"
	Running   Status = "running"
	Succeeded Status = "succeeded"
	Failed    Status = "failed"
	Cancelled Status = "cancelled"
	Unknown   Status = "unknown"
)

func (s Status) Terminal() bool { return s == Succeeded || s == Failed || s == Cancelled }

type Operation struct {
	Request           OperationRequest `json:"request"`
	Status            Status           `json:"status"`
	JobID             string           `json:"job_id"`
	BeforeHash        string           `json:"before_hash"`
	ExpectedAfterHash string           `json:"expected_after_hash,omitempty"`
	AfterHash         string           `json:"after_hash,omitempty"`
	AfterRevision     uint64           `json:"after_revision"`
	Result            json.RawMessage  `json:"result,omitempty"`
	ResultTruncated   bool             `json:"result_truncated,omitempty"`
	Receipt           artifact.Ref     `json:"receipt"`
	Error             string           `json:"error,omitempty"`
	CancelRequested   bool             `json:"cancel_requested"`
}
type StopReceipt struct {
	Workspace          Workspace    `json:"workspace"`
	NoActiveOperations bool         `json:"no_active_operations"`
	Ref                artifact.Ref `json:"ref"`
}
type Snapshot struct {
	Workspace Workspace    `json:"workspace"`
	Hash      string       `json:"hash"`
	Artifact  artifact.Ref `json:"artifact"`
}
type ReleaseResult struct {
	WorkspaceID domain.ID `json:"workspace_id"`
	Released    bool      `json:"released"`
}

type Service interface {
	PrepareWorkspace(context.Context, PrepareRequest) (Workspace, error)
	AdoptWorkspace(context.Context, WorkspaceRequest) (StopReceipt, error)
	StartOperation(context.Context, OperationRequest) (Operation, error)
	InspectOperation(context.Context, InspectRequest) (Operation, error)
	CancelOperation(context.Context, InspectRequest) (Operation, error)
	StopWorkspace(context.Context, WorkspaceRequest) (StopReceipt, error)
	SealSnapshot(context.Context, WorkspaceRequest) (Snapshot, error)
	ReleaseWorkspace(context.Context, WorkspaceRequest) (ReleaseResult, error)
}
