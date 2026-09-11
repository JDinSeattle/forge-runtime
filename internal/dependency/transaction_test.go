package dependency

import (
	"context"
	"testing"
	"time"
)

func TestTransactionOperationPreservesDeadlineCause(t *testing.T) {
	// Both contexts expire at the same instant. Exercise competing timers:
	// an expired transaction must remain a timeout when SQL gets Background.
	for range 100 {
		parent, cancelParent := context.WithTimeout(context.Background(), time.Millisecond)
		tx := transaction{ctx: parent}
		ctx, cancel := tx.operation(context.Background())
		<-ctx.Done()
		if ctx.Err() != context.DeadlineExceeded {
			t.Errorf("transaction timeout became %v", ctx.Err())
		}
		cancel()
		cancelParent()
	}
}

func TestTransactionOperationPropagatesExplicitCancellation(t *testing.T) {
	for _, beforeOperation := range []bool{false, true} {
		parent, cancelParent := context.WithTimeout(context.Background(), time.Hour)
		if beforeOperation {
			cancelParent()
		}
		tx := transaction{ctx: parent}
		ctx, cancel := tx.operation(context.Background())
		cancelParent()
		select {
		case <-ctx.Done():
			if ctx.Err() != context.Canceled {
				t.Errorf("explicit cancellation became %v", ctx.Err())
			}
		case <-time.After(time.Second):
			t.Error("transaction cancellation did not reach the operation")
		}
		cancel()
	}
	parent, cancelParent := context.WithTimeout(context.Background(), time.Hour)
	defer cancelParent()
	caller, cancelCaller := context.WithCancel(context.Background())
	tx := transaction{ctx: parent}
	ctx, cancel := tx.operation(caller)
	defer cancel()
	cancelCaller()
	if ctx.Err() != context.Canceled {
		t.Fatalf("caller cancellation lost: %v", ctx.Err())
	}
	if parent.Err() != nil {
		t.Fatal("operation cancellation ended the transaction")
	}
}
