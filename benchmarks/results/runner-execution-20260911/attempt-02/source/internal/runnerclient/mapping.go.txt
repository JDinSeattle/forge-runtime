// Package runnerclient exposes the runner's typed internal protocol over gRPC.
// Transport identity supplements, and never replaces, per-operation grants.
package runnerclient

import (
	"encoding/json"
	"fmt"

	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	pb "github.com/JDinSeattle/forge-runtime/proto/runner/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var kindToProto = map[string]pb.ToolKind{"list_files": pb.ToolKind_TOOL_KIND_LIST_FILES, "read_file": pb.ToolKind_TOOL_KIND_READ_FILE, "search_code": pb.ToolKind_TOOL_KIND_SEARCH_CODE, "apply_patch": pb.ToolKind_TOOL_KIND_APPLY_PATCH, "run_command": pb.ToolKind_TOOL_KIND_RUN_COMMAND, "get_diff": pb.ToolKind_TOOL_KIND_GET_DIFF, "verify": pb.ToolKind_TOOL_KIND_VERIFY}
var statusToProto = map[runner.Status]pb.OperationStatus{runner.Prepared: pb.OperationStatus_OPERATION_STATUS_PREPARED, runner.Running: pb.OperationStatus_OPERATION_STATUS_RUNNING, runner.Succeeded: pb.OperationStatus_OPERATION_STATUS_SUCCEEDED, runner.Failed: pb.OperationStatus_OPERATION_STATUS_FAILED, runner.Cancelled: pb.OperationStatus_OPERATION_STATUS_CANCELLED, runner.Unknown: pb.OperationStatus_OPERATION_STATUS_UNKNOWN}

func id(v domain.ID) *pb.ResourceID {
	if v == "" {
		return nil
	}
	return &pb.ResourceID{Value: string(v)}
}
func fromID(v *pb.ResourceID) domain.ID {
	if v == nil {
		return ""
	}
	return domain.ID(v.Value)
}
func binding(v runner.WorkspaceRequest) *pb.WorkspaceBinding {
	return &pb.WorkspaceBinding{TenantId: id(v.TenantID), RunId: id(v.RunID), WorkspaceId: id(v.WorkspaceID), Epoch: v.Epoch, CapabilityGrant: v.Grant}
}
func fromBinding(v *pb.WorkspaceBinding) (runner.WorkspaceRequest, error) {
	if v == nil {
		return runner.WorkspaceRequest{}, domain.ErrInvalid
	}
	r := runner.WorkspaceRequest{TenantID: fromID(v.TenantId), RunID: fromID(v.RunId), WorkspaceID: fromID(v.WorkspaceId), Epoch: v.Epoch, Grant: v.CapabilityGrant}
	if r.TenantID.Validate() != nil || r.RunID.Validate() != nil || r.WorkspaceID.Validate() != nil || r.Epoch == 0 || len(r.Grant) > 8192 {
		return r, domain.ErrInvalid
	}
	return r, nil
}
func startRequest(r runner.OperationRequest) (*pb.StartOperationRequest, error) {
	kind, ok := kindToProto[r.Kind]
	if !ok {
		return nil, domain.ErrInvalid
	}
	return &pb.StartOperationRequest{Binding: binding(r.WorkspaceRequest), OperationId: id(r.OperationID), ExpectedRevision: r.ExpectedRevision, ToolKind: kind, CanonicalArgsJson: append([]byte(nil), r.Args...), ArgsSha256: r.ArgsHash, PolicyVersion: r.PolicyVersion, Deadline: timestamppb.New(r.Deadline)}, nil
}
func fromStart(v *pb.StartOperationRequest) (runner.OperationRequest, error) {
	if v == nil || v.Deadline == nil || v.Deadline.CheckValid() != nil {
		return runner.OperationRequest{}, domain.ErrInvalid
	}
	b, err := fromBinding(v.Binding)
	if err != nil {
		return runner.OperationRequest{}, err
	}
	kind := ""
	for name, value := range kindToProto {
		if value == v.ToolKind {
			kind = name
			break
		}
	}
	if kind == "" || fromID(v.OperationId).Validate() != nil || len(v.CanonicalArgsJson) > 256<<10 || !json.Valid(v.CanonicalArgsJson) {
		return runner.OperationRequest{}, domain.ErrInvalid
	}
	return runner.OperationRequest{WorkspaceRequest: b, OperationID: fromID(v.OperationId), ExpectedRevision: v.ExpectedRevision, Kind: kind, Args: append(json.RawMessage(nil), v.CanonicalArgsJson...), ArgsHash: v.ArgsSha256, PolicyVersion: v.PolicyVersion, Deadline: v.Deadline.AsTime()}, nil
}
func workspace(w runner.Workspace) *pb.Workspace {
	return &pb.Workspace{TenantId: id(w.TenantID), RunId: id(w.RunID), Id: id(w.ID), SourceId: w.SourceID, ProfileId: w.ProfileID, Epoch: w.Epoch, Revision: w.Revision, ActiveOperationId: id(w.ActiveOperation), Adopting: w.Adopting, Released: w.Released, BaselineSha256: w.BaselineHash, Stopped: w.Stopped}
}
func fromWorkspace(w *pb.Workspace) (runner.Workspace, error) {
	if w == nil {
		return runner.Workspace{}, domain.ErrInvalid
	}
	return runner.Workspace{TenantID: fromID(w.TenantId), RunID: fromID(w.RunId), ID: fromID(w.Id), SourceID: w.SourceId, ProfileID: w.ProfileId, Epoch: w.Epoch, Revision: w.Revision, ActiveOperation: fromID(w.ActiveOperationId), Adopting: w.Adopting, Released: w.Released, BaselineHash: w.BaselineSha256, Stopped: w.Stopped}, nil
}
func artifactRef(r artifact.Ref) *pb.ArtifactRef {
	return &pb.ArtifactRef{TenantId: id(r.TenantID), RunId: id(r.RunID), Kind: r.Kind, ObjectKey: r.ObjectKey, Sha256: r.SHA256, ByteSize: r.Size}
}
func fromArtifact(r *pb.ArtifactRef) artifact.Ref {
	if r == nil {
		return artifact.Ref{}
	}
	return artifact.Ref{TenantID: fromID(r.TenantId), RunID: fromID(r.RunId), Kind: r.Kind, ObjectKey: r.ObjectKey, SHA256: r.Sha256, Size: r.ByteSize}
}
func operation(o runner.Operation) (*pb.Operation, error) {
	o.Request.Grant = ""
	r, err := startRequest(o.Request)
	if err != nil {
		return nil, err
	}
	status, ok := statusToProto[o.Status]
	if !ok {
		return nil, domain.ErrInvalid
	}
	truncated := o.ResultTruncated
	result := o.Result
	if len(result) > 1<<20 {
		result = nil
		truncated = true
	}
	return &pb.Operation{Request: r, Status: status, JobId: o.JobID, BeforeSha256: o.BeforeHash, ExpectedAfterSha256: o.ExpectedAfterHash, AfterSha256: o.AfterHash, AfterRevision: o.AfterRevision, ResultJson: append([]byte(nil), result...), ResultTruncated: truncated, Receipt: artifactRef(o.Receipt), Error: o.Error, CancelRequested: o.CancelRequested}, nil
}
func fromOperation(v *pb.Operation) (runner.Operation, error) {
	if v == nil {
		return runner.Operation{}, domain.ErrInvalid
	}
	r, err := fromStart(v.Request)
	if err != nil {
		return runner.Operation{}, err
	}
	status := runner.Status("")
	for key, value := range statusToProto {
		if value == v.Status {
			status = key
			break
		}
	}
	if status == "" {
		return runner.Operation{}, fmt.Errorf("%w: unknown operation status", domain.ErrInvalid)
	}
	return runner.Operation{Request: r, Status: status, JobID: v.JobId, BeforeHash: v.BeforeSha256, ExpectedAfterHash: v.ExpectedAfterSha256, AfterHash: v.AfterSha256, AfterRevision: v.AfterRevision, Result: append(json.RawMessage(nil), v.ResultJson...), ResultTruncated: v.ResultTruncated, Receipt: fromArtifact(v.Receipt), Error: v.Error, CancelRequested: v.CancelRequested}, nil
}
func stopReceipt(r runner.StopReceipt) *pb.StopReceipt {
	return &pb.StopReceipt{Workspace: workspace(r.Workspace), NoActiveOperations: r.NoActiveOperations, Receipt: artifactRef(r.Ref)}
}
func fromStop(v *pb.StopReceipt) (runner.StopReceipt, error) {
	if v == nil {
		return runner.StopReceipt{}, domain.ErrInvalid
	}
	w, err := fromWorkspace(v.Workspace)
	return runner.StopReceipt{Workspace: w, NoActiveOperations: v.NoActiveOperations, Ref: fromArtifact(v.Receipt)}, err
}
