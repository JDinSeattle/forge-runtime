package runnerclient

import (
	"context"
	"strings"

	"github.com/JDinSeattle/forge-runtime/internal/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func traceClient(t *telemetry.Telemetry) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		end := func(telemetry.Outcome) {}
		if t != nil {
			ctx, end = t.StartRPC(ctx, method[strings.LastIndex(method, "/")+1:], false)
		}
		outcome := telemetry.Unknown
		defer func() { end(outcome) }()
		md, _ := metadata.FromOutgoingContext(ctx)
		md = md.Copy()
		md.Delete("traceparent")
		md.Delete("tracestate")
		md.Delete("baggage")
		if parent := telemetry.Traceparent(ctx); parent != "" {
			md.Set("traceparent", parent)
		}
		err := invoke(metadata.NewOutgoingContext(ctx, md), method, req, reply, cc, opts...)
		if err == nil {
			outcome = telemetry.Success
		}
		return err
	}
}
func traceServerContext(ctx context.Context, t *telemetry.Telemetry, method string) (context.Context, func(telemetry.Outcome)) {
	md, _ := metadata.FromIncomingContext(ctx)
	parents := md.Get("traceparent")
	if len(parents) == 1 {
		ctx = telemetry.ContextFromTraceparent(ctx, parents[0])
	}
	if t != nil {
		return t.StartRPC(ctx, method[strings.LastIndex(method, "/")+1:], true)
	}
	return ctx, func(telemetry.Outcome) {}
}
