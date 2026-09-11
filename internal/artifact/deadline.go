package artifact

import (
	"context"
	"io"
	"sync"

	"github.com/JDinSeattle/forge-runtime/internal/dependency"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
)

// OpenWithDeadline also bounds consumption of the returned reader. Store
// implementations must honor their context while opening. Close must unblock a
// concurrent Read; the local file and HTTP response implementations support it.
func OpenWithDeadline(ctx context.Context, store Store, tenant, run domain.ID, ref Ref) (io.ReadCloser, error) {
	ctx, cancel := dependency.Artifact(ctx)
	reader, err := store.Open(ctx, tenant, run, ref)
	if err != nil {
		cancel()
		return nil, err
	}
	return deadlineReader(ctx, cancel, reader), nil
}

type operationReader struct {
	ctx    context.Context
	cancel context.CancelFunc
	reader io.ReadCloser
	stop   func() bool
	once   sync.Once
	err    error
}

func deadlineReader(ctx context.Context, cancel context.CancelFunc, reader io.ReadCloser) *operationReader {
	r := &operationReader{ctx: ctx, cancel: cancel, reader: reader}
	// The callback does not access stop: cancellation may run before AfterFunc
	// returns. sync.Once makes normal Close and cancellation close exactly once.
	r.stop = context.AfterFunc(ctx, func() { r.closeReader() })
	return r
}
func (r *operationReader) closeReader() { r.once.Do(func() { r.err = r.reader.Close() }) }
func (r *operationReader) Close() error { r.stop(); r.cancel(); r.closeReader(); return r.err }
func (r *operationReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.reader.Read(p)
	if contextErr := r.ctx.Err(); contextErr != nil {
		return n, contextErr
	}
	return n, err
}

// A plain io.Reader cannot be forcibly interrupted safely. For closable input,
// cancellation closes it to unblock Read, and cleanup waits for that callback.
// Successful Put does not take ownership of or close the caller's input.
func interruptInputOnCancel(ctx context.Context, input io.Reader) func() {
	closer, ok := input.(io.Closer)
	if !ok {
		return func() {}
	}
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(done); _ = closer.Close() })
	return func() {
		if !stop() {
			<-done
		}
	}
}
