// Package runtime implements the finite, deterministic control flow of one
// coding run. Commands are intentions: a driver must persist the transition and
// operation ID before performing I/O. Replaying an event must never execute a
// command a second time without consulting the effect ledger.
package runtime

import (
	"encoding/json"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
)

const SnapshotSchemaVersion = 1

type EffectStatus string

const (
	EffectPlanned   EffectStatus = "planned"
	EffectInFlight  EffectStatus = "in_flight"
	EffectSucceeded EffectStatus = "succeeded"
	EffectFailed    EffectStatus = "failed"
	EffectCancelled EffectStatus = "cancelled"
	EffectUnknown   EffectStatus = "unknown"
)

type Effect struct {
	ID               domain.ID       `json:"operation_id"`
	Kind             string          `json:"kind"`
	Args             json.RawMessage `json:"args"`
	ArgsHash         string          `json:"args_hash"`
	ExpectedRevision uint64          `json:"expected_revision"`
	DispatchEpoch    uint64          `json:"dispatch_epoch"`
	PolicyVersion    string          `json:"policy_version"`
	RequiresApproval bool            `json:"requires_approval"`
	Status           EffectStatus    `json:"status"`
	ReceiptRef       string          `json:"receipt_ref,omitempty"`
}

// ApprovalBinding is immutable while pending. The decision must match every
// field, not merely the approval ID. Permission checks occur in the application
// transaction before this trusted control event is emitted.
type ApprovalBinding struct {
	EffectID          domain.ID `json:"effect_id"`
	ArgsHash          string    `json:"args_hash"`
	WorkspaceRevision uint64    `json:"workspace_revision"`
	PolicyVersion     string    `json:"policy_version"`
	Version           uint64    `json:"version"`
	Granted           bool      `json:"granted"`
}

type Limits struct {
	MaxModelRounds uint64       `json:"max_model_rounds"`
	MaxToolCalls   uint64       `json:"max_tool_calls"`
	MaxCost        domain.Money `json:"max_cost_microusd"`
	Deadline       time.Time    `json:"deadline"`
}

type State struct {
	SchemaVersion          uint32                    `json:"schema_version"`
	RunID                  domain.ID                 `json:"run_id"`
	TenantID               domain.ID                 `json:"tenant_id"`
	Version                uint64                    `json:"version"`
	Status                 domain.RunStatus          `json:"status"`
	Stage                  domain.Stage              `json:"stage"`
	ResumeStage            domain.Stage              `json:"resume_stage,omitempty"`
	Lease                  domain.Lease              `json:"lease"`
	StepSeq                uint64                    `json:"step_seq"`
	WorkspaceRevision      uint64                    `json:"workspace_revision"`
	ReconciliationRevision uint64                    `json:"reconciliation_revision,omitempty"`
	PendingEffect          *Effect                   `json:"pending_effect,omitempty"`
	RemainingEffects       []Effect                  `json:"remaining_effects,omitempty"`
	Approval               *ApprovalBinding          `json:"approval,omitempty"`
	OutputRef              string                    `json:"output_ref,omitempty"`
	Verification           domain.VerificationStatus `json:"verification_status"`
	VerificationReportRef  string                    `json:"verification_report_ref,omitempty"`
	VerificationRevision   uint64                    `json:"verification_revision"`
	ModelRounds            uint64                    `json:"model_rounds"`
	ToolCalls              uint64                    `json:"tool_calls"`
	Cost                   domain.Money              `json:"cost_microusd"`
	Limits                 Limits                    `json:"limits"`
	FailureReason          string                    `json:"failure_reason,omitempty"`
	// StopTarget is recorded before stopping active work on budget/failure.
	// A terminal state is committed only after runner stop confirmation.
	StopTarget domain.RunStatus `json:"stop_target,omitempty"`
}

type EventKind string

const (
	EventClaimed               EventKind = "claimed"
	EventWorkspaceReady        EventKind = "workspace_ready"
	EventWorkspaceAdopted      EventKind = "workspace_adopted"
	EventContextBuilt          EventKind = "context_built"
	EventModelCompleted        EventKind = "model_completed"
	EventToolsValidated        EventKind = "tools_validated"
	EventEffectCompleted       EventKind = "effect_completed"
	EventEffectUncertain       EventKind = "effect_uncertain"
	EventResultsIngested       EventKind = "results_ingested"
	EventApprovalDecided       EventKind = "approval_decided"
	EventVerificationCompleted EventKind = "verification_completed"
	EventFinalized             EventKind = "finalized"
	EventCancelRequested       EventKind = "cancel_requested"
	EventCancellationConfirmed EventKind = "cancellation_confirmed"
	EventResumeRequested       EventKind = "resume_requested"
	EventReconciled            EventKind = "reconciled"
	EventFailed                EventKind = "failed"
	EventBudgetReached         EventKind = "budget_reached"
)

// Event is a versioned envelope. At must be trusted database time. Owner/Epoch
// identify the current advancing worker; API control events use application
// authorization and ExpectedVersion instead. OutputRef refers to a READY,
// durable artifact, never a partial model stream or provisional SSE text.
// The reducer validates structure; the transaction/driver authenticates sources
// and proves persistence of referenced evidence before committing an event.
type Event struct {
	Kind              EventKind             `json:"kind"`
	ExpectedVersion   uint64                `json:"expected_version"`
	Owner             string                `json:"owner"`
	Epoch             uint64                `json:"epoch"`
	At                time.Time             `json:"at"`
	Lease             *domain.Lease         `json:"lease,omitempty"`
	WorkspaceRevision uint64                `json:"workspace_revision,omitempty"`
	OutputRef         string                `json:"output_ref,omitempty"`
	Complete          bool                  `json:"complete,omitempty"`
	Finish            bool                  `json:"finish,omitempty"`
	Effects           []Effect              `json:"effects,omitempty"`
	Receipt           *EffectReceipt        `json:"receipt,omitempty"`
	Approval          *ApprovalDecision     `json:"approval,omitempty"`
	Verification      *VerificationEvidence `json:"verification,omitempty"`
	Stop              *StopReceipt          `json:"stop,omitempty"`
	Cost              domain.Money          `json:"cost_microusd,omitempty"`
	Reason            string                `json:"reason,omitempty"`
}

type EffectReceipt struct {
	EffectID       domain.ID    `json:"effect_id"`
	ArgsHash       string       `json:"args_hash"`
	Epoch          uint64       `json:"epoch"`
	Status         EffectStatus `json:"status"`
	BeforeRevision uint64       `json:"before_revision"`
	AfterRevision  uint64       `json:"after_revision"`
	Ref            string       `json:"ref"`
	// Settled proves the executor has no process still mutating the workspace.
	Settled bool `json:"settled"`
}

type ApprovalDecision struct {
	Binding ApprovalBinding `json:"binding"`
	Approve bool            `json:"approve"`
}

// VerificationEvidence is emitted only by the trusted RepoProfile verifier.
// Models and user-supplied tool outputs cannot author this event. TargetPassed
// alone is insufficient: the baseline must have demonstrated the target failure
// and original regression checks must still pass against this exact revision.
type VerificationEvidence struct {
	Trusted              bool   `json:"trusted"`
	ReportRef            string `json:"report_ref"`
	WorkspaceRevision    uint64 `json:"workspace_revision"`
	BaselineTargetFailed bool   `json:"baseline_target_failed"`
	TargetPassed         bool   `json:"target_passed"`
	RegressionPassed     bool   `json:"regression_passed"`
}

// StopReceipt must be authenticated and durably recorded by the driver. For a
// never-started run, the scheduler can prove NoActiveOperations from its ledger;
// otherwise this evidence must come from the assigned runner's journal.
type StopReceipt struct {
	Ref                string `json:"ref"`
	NoActiveOperations bool   `json:"no_active_operations"`
	WorkspaceRevision  uint64 `json:"workspace_revision"`
}

type CommandKind string

const (
	CommandInitializeWorkspace CommandKind = "initialize_workspace"
	CommandAdoptWorkspace      CommandKind = "adopt_workspace"
	CommandBuildContext        CommandKind = "build_context"
	CommandCallModel           CommandKind = "call_model"
	CommandValidateTools       CommandKind = "validate_tools"
	CommandExecuteEffect       CommandKind = "execute_effect"
	CommandRequestApproval     CommandKind = "request_approval"
	CommandIngestResults       CommandKind = "ingest_results"
	CommandVerify              CommandKind = "verify"
	CommandFinalize            CommandKind = "finalize"
	CommandInspectEffect       CommandKind = "inspect_effect"
	CommandStopExecution       CommandKind = "stop_execution"
	CommandPublishTerminal     CommandKind = "publish_terminal"
)

type Command struct {
	Kind    CommandKind      `json:"kind"`
	Effect  *Effect          `json:"effect,omitempty"`
	Binding *ApprovalBinding `json:"binding,omitempty"`
}

func NewState(tenantID, runID domain.ID, limits Limits) State {
	return State{
		SchemaVersion: SnapshotSchemaVersion, RunID: runID, TenantID: tenantID,
		Version: 1, Status: domain.StatusQueued, Stage: domain.StageInitialize,
		Verification: domain.VerificationUnverified, Limits: limits,
	}
}
