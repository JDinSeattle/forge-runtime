package artifact

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPutDeadlineUnblocksClosableInputAndReleasesPublication(t *testing.T) {
	root := t.TempDir()
	store, err := NewLocalStore(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	input, writer := io.Pipe()
	defer writer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := store.Put(ctx, "tenant", "run", "test", input); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked input outlived deadline")
	}
	files, err := filepath.Glob(filepath.Join(root, "tenant", "run", ".staging-*"))
	if err != nil || len(files) != 0 {
		t.Fatalf("staging leaked: %v %v", files, err)
	}
	lockCtx, end := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer end()
	unlock, err := store.lockCollection(lockCtx, true)
	if err != nil {
		t.Fatalf("publication lock leaked: %v", err)
	}
	unlock()
	if _, err = writer.Write([]byte("late")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("input was not closed on timeout: %v", err)
	}
}

type countedReadCloser struct {
	*io.PipeReader
	closes atomic.Int32
}

func (r *countedReadCloser) Close() error { r.closes.Add(1); return r.PipeReader.Close() }
func TestArtifactReaderCancellationClosesOnceAndUnblocksRead(t *testing.T) {
	pipe, writer := io.Pipe()
	defer writer.Close()
	input := &countedReadCloser{PipeReader: pipe}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	reader := deadlineReader(ctx, cancel, input)
	done := make(chan error, 1)
	go func() { _, err := reader.Read(make([]byte, 1)); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("read=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("read leaked")
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { _ = reader.Close() })
	}
	wg.Wait()
	if input.closes.Load() != 1 {
		t.Fatalf("close count=%d", input.closes.Load())
	}
}

func TestLocalOpenBudgetLivesUntilReaderClose(t *testing.T) {
	store, err := NewLocalStore(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ref, err := store.Put(context.Background(), "tenant", "run", "test", strings.NewReader("bounded object"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	reader, err := OpenWithDeadline(ctx, store, "tenant", "run", ref)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil || string(data) != "bounded object" {
		t.Fatalf("Open canceled its returned reader: %s %v", data, err)
	}
	<-ctx.Done()
	if _, err = reader.Read(make([]byte, 1)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("reader escaped operation budget: %v", err)
	}
}
