package runnerclient

import (
	"context"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	pb "github.com/JDinSeattle/forge-runtime/proto/runner/v1"
)

type rpcServer struct {
	pb.UnimplementedRunnerServiceServer
	service          runner.Service
	maxOperationTime time.Duration
}

func (s *rpcServer) PrepareWorkspace(ctx context.Context, v *pb.PrepareWorkspaceRequest) (*pb.Workspace, error) {
	if v == nil {
		return nil, domain.ErrInvalid
	}
	r, err := fromBinding(v.Binding)
	if err != nil {
		return nil, err
	}
	w, err := s.service.PrepareWorkspace(ctx, runner.PrepareRequest{WorkspaceRequest: r, SourceID: v.SourceId, ProfileID: v.ProfileId})
	if err != nil {
		return nil, err
	}
	return workspace(w), nil
}
func (s *rpcServer) AdoptWorkspace(ctx context.Context, v *pb.WorkspaceRequest) (*pb.StopReceipt, error) {
	if v == nil {
		return nil, domain.ErrInvalid
	}
	r, err := fromBinding(v.Binding)
	if err != nil {
		return nil, err
	}
	receipt, err := s.service.AdoptWorkspace(ctx, r)
	if err != nil {
		return nil, err
	}
	return stopReceipt(receipt), nil
}
func (s *rpcServer) StartOperation(ctx context.Context, v *pb.StartOperationRequest) (*pb.Operation, error) {
	r, err := fromStart(v)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	if !r.Deadline.After(now) || r.Deadline.After(now.Add(s.maxOperationTime)) {
		return nil, domain.ErrInvalid
	}
	o, err := s.service.StartOperation(ctx, r)
	if err != nil {
		return nil, err
	}
	return operation(o)
}
func (s *rpcServer) InspectOperation(ctx context.Context, v *pb.InspectOperationRequest) (*pb.Operation, error) {
	r, err := inspectRequest(v)
	if err != nil {
		return nil, err
	}
	o, err := s.service.InspectOperation(ctx, r)
	if err != nil {
		return nil, err
	}
	return operation(o)
}
func (s *rpcServer) CancelOperation(ctx context.Context, v *pb.InspectOperationRequest) (*pb.Operation, error) {
	r, err := inspectRequest(v)
	if err != nil {
		return nil, err
	}
	o, err := s.service.CancelOperation(ctx, r)
	if err != nil {
		return nil, err
	}
	return operation(o)
}
func (s *rpcServer) StopWorkspace(ctx context.Context, v *pb.WorkspaceRequest) (*pb.StopReceipt, error) {
	if v == nil {
		return nil, domain.ErrInvalid
	}
	r, err := fromBinding(v.Binding)
	if err != nil {
		return nil, err
	}
	receipt, err := s.service.StopWorkspace(ctx, r)
	if err != nil {
		return nil, err
	}
	return stopReceipt(receipt), nil
}
func (s *rpcServer) SealSnapshot(ctx context.Context, v *pb.WorkspaceRequest) (*pb.Snapshot, error) {
	if v == nil {
		return nil, domain.ErrInvalid
	}
	r, err := fromBinding(v.Binding)
	if err != nil {
		return nil, err
	}
	snapshot, err := s.service.SealSnapshot(ctx, r)
	if err != nil {
		return nil, err
	}
	return &pb.Snapshot{Workspace: workspace(snapshot.Workspace), Sha256: snapshot.Hash, Artifact: artifactRef(snapshot.Artifact)}, nil
}
func (s *rpcServer) ReleaseWorkspace(ctx context.Context, v *pb.WorkspaceRequest) (*pb.ReleaseResult, error) {
	if v == nil {
		return nil, domain.ErrInvalid
	}
	r, err := fromBinding(v.Binding)
	if err != nil {
		return nil, err
	}
	result, err := s.service.ReleaseWorkspace(ctx, r)
	if err != nil {
		return nil, err
	}
	return &pb.ReleaseResult{WorkspaceId: id(result.WorkspaceID), Released: result.Released}, nil
}
func inspectRequest(v *pb.InspectOperationRequest) (runner.InspectRequest, error) {
	if v == nil || fromID(v.OperationId).Validate() != nil {
		return runner.InspectRequest{}, domain.ErrInvalid
	}
	r, err := fromBinding(v.Binding)
	if err != nil {
		return runner.InspectRequest{}, err
	}
	return runner.InspectRequest{WorkspaceRequest: r, OperationID: fromID(v.OperationId)}, nil
}
