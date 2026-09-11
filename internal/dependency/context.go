// Package dependency provides separate budgets for short dependency operations.
// These contexts never replace the lifetime of a worker, model call or SSE stream.
package dependency

import (
	"context"
	"time"
)

const (
	DatabaseTimeout = 3 * time.Second
	ArtifactTimeout = 10 * time.Second
	CleanupTimeout  = time.Second
)

func Database(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, DatabaseTimeout)
}

func Artifact(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, ArtifactTimeout)
}
