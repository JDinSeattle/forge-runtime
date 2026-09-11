package runnerclient

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	"github.com/JDinSeattle/forge-runtime/internal/telemetry"
	pb "github.com/JDinSeattle/forge-runtime/proto/runner/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type ClientConfig struct {
	Telemetry        *telemetry.Telemetry `json:"-"`
	UnixSocket       string
	TCPAddress       string
	TLS              TLSFiles
	MaxMessageBytes  int
	RPCTimeout       time.Duration
	ReconcileTimeout time.Duration
}
type Client struct {
	conn                      *grpc.ClientConn
	rpc                       pb.RunnerServiceClient
	timeout, reconcileTimeout time.Duration
}

func Dial(ctx context.Context, config ClientConfig) (*Client, error) {
	if (config.UnixSocket == "") == (config.TCPAddress == "") {
		return nil, domain.ErrInvalid
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = 8 << 20
	}
	if config.MaxMessageBytes < 1024 || config.MaxMessageBytes > 64<<20 {
		return nil, domain.ErrInvalid
	}
	if config.RPCTimeout == 0 {
		config.RPCTimeout = 15 * time.Second
	}
	if config.ReconcileTimeout == 0 {
		config.ReconcileTimeout = 5 * time.Second
	}
	if config.RPCTimeout < time.Millisecond || config.RPCTimeout > time.Minute || config.ReconcileTimeout < time.Millisecond || config.ReconcileTimeout > 30*time.Second {
		return nil, domain.ErrInvalid
	}
	options := []grpc.DialOption{grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(config.MaxMessageBytes), grpc.MaxCallSendMsgSize(config.MaxMessageBytes)), grpc.WithDisableRetry(), grpc.WithUnaryInterceptor(traceClient(config.Telemetry))}
	target := config.TCPAddress
	if config.UnixSocket != "" {
		if !filepath.IsAbs(config.UnixSocket) {
			return nil, domain.ErrInvalid
		}
		target = "passthrough:///forge-runner-unix"
		options = append(options, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", config.UnixSocket)
		}))
	} else {
		tlsConfig, err := loadTLS(config.TLS, false)
		if err != nil {
			return nil, err
		}
		options = append(options, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	}
	// Blocking establishment makes wrong CA/SPIFFE identity fail at construction.
	// DialContext remains supported by grpc-go and avoids a lazy insecure-looking
	// client being returned before the transport handshake is verified.
	options = append(options, grpc.WithBlock())
	dialCtx, cancel := context.WithTimeout(ctx, config.RPCTimeout)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, target, options...)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, rpc: pb.NewRunnerServiceClient(conn), timeout: config.RPCTimeout, reconcileTimeout: config.ReconcileTimeout}, nil
}
func (c *Client) Close() error { return c.conn.Close() }
func (c *Client) PrepareWorkspace(ctx context.Context, r runner.PrepareRequest) (runner.Workspace, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	v, err := c.rpc.PrepareWorkspace(ctx, &pb.PrepareWorkspaceRequest{Binding: binding(r.WorkspaceRequest), SourceId: r.SourceID, ProfileId: r.ProfileID})
	if err != nil {
		return runner.Workspace{}, fromRPC(err)
	}
	return fromWorkspace(v)
}
func (c *Client) AdoptWorkspace(ctx context.Context, r runner.WorkspaceRequest) (runner.StopReceipt, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	v, err := c.rpc.AdoptWorkspace(ctx, &pb.WorkspaceRequest{Binding: binding(r)})
	if err != nil {
		return runner.StopReceipt{}, fromRPC(err)
	}
	return fromStop(v)
}

// StartOperation never retries the mutation automatically. An ambiguous RPC
// failure is followed by one bounded Inspect of the SAME operation ID using a
// detached observation context; an RPC timeout is not business cancellation.
func (c *Client) StartOperation(ctx context.Context, r runner.OperationRequest) (runner.Operation, error) {
	request, err := startRequest(r)
	if err != nil {
		return runner.Operation{}, err
	}
	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	v, err := c.rpc.StartOperation(callCtx, request)
	cancel()
	if err == nil {
		return fromOperation(v)
	}
	switch status.Code(err) {
	case codes.DeadlineExceeded, codes.Canceled, codes.Unavailable:
		inspectCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), c.reconcileTimeout)
		defer stop()
		found, inspectErr := c.rpc.InspectOperation(inspectCtx, &pb.InspectOperationRequest{Binding: binding(r.WorkspaceRequest), OperationId: id(r.OperationID)})
		if inspectErr == nil {
			return fromOperation(found)
		}
		return runner.Operation{}, fmt.Errorf("%w: operation %s must be inspected on the original runner; start=%v, inspect=%v", ErrUncertainStart, r.OperationID, fromRPC(err), fromRPC(inspectErr))
	default:
		return runner.Operation{}, fromRPC(err)
	}
}
func (c *Client) InspectOperation(ctx context.Context, r runner.InspectRequest) (runner.Operation, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	v, err := c.rpc.InspectOperation(ctx, &pb.InspectOperationRequest{Binding: binding(r.WorkspaceRequest), OperationId: id(r.OperationID)})
	if err != nil {
		return runner.Operation{}, fromRPC(err)
	}
	return fromOperation(v)
}
func (c *Client) CancelOperation(ctx context.Context, r runner.InspectRequest) (runner.Operation, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	v, err := c.rpc.CancelOperation(ctx, &pb.InspectOperationRequest{Binding: binding(r.WorkspaceRequest), OperationId: id(r.OperationID)})
	if err != nil {
		return runner.Operation{}, fromRPC(err)
	}
	return fromOperation(v)
}
func (c *Client) StopWorkspace(ctx context.Context, r runner.WorkspaceRequest) (runner.StopReceipt, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	v, err := c.rpc.StopWorkspace(ctx, &pb.WorkspaceRequest{Binding: binding(r)})
	if err != nil {
		return runner.StopReceipt{}, fromRPC(err)
	}
	return fromStop(v)
}
func (c *Client) SealSnapshot(ctx context.Context, r runner.WorkspaceRequest) (runner.Snapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	v, err := c.rpc.SealSnapshot(ctx, &pb.WorkspaceRequest{Binding: binding(r)})
	if err != nil {
		return runner.Snapshot{}, fromRPC(err)
	}
	if v == nil {
		return runner.Snapshot{}, domain.ErrInvalid
	}
	w, err := fromWorkspace(v.Workspace)
	return runner.Snapshot{Workspace: w, Hash: v.Sha256, Artifact: fromArtifact(v.Artifact)}, err
}
func (c *Client) ReleaseWorkspace(ctx context.Context, r runner.WorkspaceRequest) (runner.ReleaseResult, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	v, err := c.rpc.ReleaseWorkspace(ctx, &pb.WorkspaceRequest{Binding: binding(r)})
	if err != nil {
		return runner.ReleaseResult{}, fromRPC(err)
	}
	if v == nil {
		return runner.ReleaseResult{}, domain.ErrInvalid
	}
	return runner.ReleaseResult{WorkspaceID: fromID(v.WorkspaceId), Released: v.Released}, nil
}

var _ runner.Service = (*Client)(nil)
