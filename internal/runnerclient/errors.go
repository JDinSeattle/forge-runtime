package runnerclient

import (
	"context"
	"errors"
	"fmt"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrUnavailable = errors.New("runner_unavailable")
var ErrUncertainStart = errors.New("runner_start_uncertain")

func rpcError(err error) error {
	if err == nil {
		return nil
	}
	code, reason, message := codes.Internal, "internal", "runner internal error"
	for _, row := range []struct {
		err    error
		code   codes.Code
		reason string
	}{{domain.ErrInvalid, codes.InvalidArgument, "invalid_argument"}, {domain.ErrForbidden, codes.PermissionDenied, "forbidden"}, {domain.ErrNotFound, codes.NotFound, "not_found"}, {domain.ErrConflict, codes.Aborted, "conflict"}, {domain.ErrFenced, codes.FailedPrecondition, "lease_fenced"}, {domain.ErrReconciliation, codes.FailedPrecondition, "reconciliation_required"}, {domain.ErrUntrusted, codes.FailedPrecondition, "untrusted_evidence"}, {domain.ErrCapacity, codes.ResourceExhausted, "capacity_exhausted"}, {sandbox.ErrUnavailable, codes.Unavailable, "sandbox_unavailable"}, {context.Canceled, codes.Canceled, "cancelled"}, {context.DeadlineExceeded, codes.DeadlineExceeded, "deadline_exceeded"}} {
		if errors.Is(err, row.err) {
			code, reason, message = row.code, row.reason, row.reason
			break
		}
	}
	s := status.New(code, message)
	if detailed, e := s.WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: "forge.runner"}); e == nil {
		s = detailed
	}
	return s.Err()
}
func fromRPC(err error) error {
	if err == nil {
		return nil
	}
	s, ok := status.FromError(err)
	if !ok {
		return err
	}
	reason := ""
	for _, d := range s.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.Domain == "forge.runner" {
			reason = info.Reason
		}
	}
	known := map[string]error{"invalid_argument": domain.ErrInvalid, "forbidden": domain.ErrForbidden, "not_found": domain.ErrNotFound, "conflict": domain.ErrConflict, "lease_fenced": domain.ErrFenced, "reconciliation_required": domain.ErrReconciliation, "untrusted_evidence": domain.ErrUntrusted, "capacity_exhausted": domain.ErrCapacity, "sandbox_unavailable": sandbox.ErrUnavailable, "cancelled": context.Canceled, "deadline_exceeded": context.DeadlineExceeded}
	if mapped := known[reason]; mapped != nil {
		return fmt.Errorf("%w: %s", mapped, s.Message())
	}
	switch s.Code() {
	case codes.Canceled:
		return fmt.Errorf("%w: %s", context.Canceled, s.Message())
	case codes.DeadlineExceeded:
		return fmt.Errorf("%w: %s", context.DeadlineExceeded, s.Message())
	case codes.Unavailable:
		return fmt.Errorf("%w: %s", ErrUnavailable, s.Message())
	case codes.ResourceExhausted:
		return fmt.Errorf("%w: %s", domain.ErrCapacity, s.Message())
	case codes.InvalidArgument:
		return fmt.Errorf("%w: %s", domain.ErrInvalid, s.Message())
	}
	return err
}
