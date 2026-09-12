//go:build linux

package applicationfaults

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"syscall"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/runner"
)

type slPressureStage struct {
	Chunk   int    `json:"chunk_bytes"`
	Written int64  `json:"written_bytes"`
	Error   string `json:"error"`
	Synced  bool   `json:"synced"`
}
type slPressureFill struct {
	Written int64             `json:"written_bytes"`
	Stages  []slPressureStage `json:"stages"`
}

// A failed large append can leave enough space for the spool. Refine on the
// same open file, ending only after a one-byte write returns actual ENOSPC.
// Every stage is synced, all bytes count against one fixed bound, and other
// errors (including delayed-allocation sync errors) fail closed.
func slFillPressure(w io.Writer, sync func() error, maximum int64) (slPressureFill, error) {
	var out slPressureFill
	block := bytes.Repeat([]byte{0x5a}, 1<<20)
	for _, chunk := range []int{1 << 20, 4096, 1} {
		stage := slPressureStage{Chunk: chunk}
		for {
			if out.Written+int64(chunk) > maximum {
				return out, fmt.Errorf("physical pressure bound exhausted before one-byte ENOSPC")
			}
			n, err := w.Write(block[:chunk])
			if n < 0 || n > chunk {
				return out, fmt.Errorf("invalid pressure write count")
			}
			out.Written += int64(n)
			stage.Written += int64(n)
			if err != nil {
				stage.Error = err.Error()
				out.Stages = append(out.Stages, stage)
				if !errors.Is(err, syscall.ENOSPC) {
					return out, err
				}
				if err = sync(); err != nil {
					return out, err
				}
				out.Stages[len(out.Stages)-1].Synced = true
				break
			}
			if n != chunk {
				return out, io.ErrShortWrite
			}
		}
	}
	return out, nil
}

func slENOSPCStop(raw []byte, request runner.OperationRequest, observed time.Time) error {
	var rows []struct {
		State struct {
			Running, OOMKilled    bool
			ExitCode              int
			StartedAt, FinishedAt time.Time
		}
	}
	if err := json.Unmarshal(raw, &rows); err != nil || len(rows) != 1 {
		return fmt.Errorf("missing ENOSPC Docker stop proof")
	}
	s := rows[0].State
	// Strictly below the 8 + 300 * .05 second natural minimum; allow no
	// near-deadline observation that could instead be an operation timeout.
	if s.Running || s.OOMKilled || s.ExitCode == 0 || s.StartedAt.IsZero() || !s.FinishedAt.After(s.StartedAt) || s.FinishedAt.Sub(s.StartedAt) >= 20*time.Second || s.FinishedAt.After(observed) || !request.Deadline.After(observed.Add(time.Second)) {
		return fmt.Errorf("ENOSPC did not establish an early non-OOM stop before the unchanged deadline")
	}
	return nil
}

// This filesystem model rejects oversized requests without consuming the
// remaining tail, reproducing the failed 1 MiB pressure oracle deterministically.
type slTailWriter struct {
	left    int
	partial bool
}

func (w *slTailWriter) Write(b []byte) (int, error) {
	if len(b) > w.left {
		n := 0
		if w.partial {
			n, w.left = w.left, 0
		}
		return n, syscall.ENOSPC
	}
	w.left -= len(b)
	return len(b), nil
}

type slWriteFunc func([]byte) (int, error)

func (f slWriteFunc) Write(b []byte) (int, error) { return f(b) }

func TestStrictLogsPressureExhaustsSmallWriteTail(t *testing.T) {
	for _, partial := range []bool{false, true} {
		w := &slTailWriter{left: (2 << 20) + 430080 + 37, partial: partial}
		initial, syncs := w.left, 0
		got, err := slFillPressure(w, func() error { syncs++; return nil }, 4<<20)
		if err != nil || w.left != 0 || got.Written != int64(initial) || len(got.Stages) != 3 || syncs != 3 {
			t.Fatalf("partial=%v: %+v left=%d syncs=%d: %v", partial, got, w.left, syncs, err)
		}
		if got.Stages[2].Chunk != 1 || !got.Stages[2].Synced || got.Stages[2].Error == "" {
			t.Fatal("one-byte ENOSPC evidence missing")
		}
	}
}

func TestStrictLogsPressureRefusesOtherFailuresAndUnboundedWrites(t *testing.T) {
	for name, w := range map[string]io.Writer{
		"EIO":       slWriteFunc(func([]byte) (int, error) { return 0, syscall.EIO }),
		"short":     slWriteFunc(func([]byte) (int, error) { return 0, nil }),
		"invalid":   slWriteFunc(func([]byte) (int, error) { return -1, syscall.ENOSPC }),
		"unbounded": io.Discard,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := slFillPressure(w, func() error { return nil }, 2<<20); err == nil {
				t.Fatal("unsafe pressure accepted")
			}
		})
	}
	if _, err := slFillPressure(&slTailWriter{}, func() error { return syscall.ENOSPC }, 2<<20); !errors.Is(err, syscall.ENOSPC) {
		t.Fatal("sync failure not preserved", err)
	}
}

func TestStrictLogsENOSPCStopRejectsNaturalExitAndOtherKillCauses(t *testing.T) {
	start := time.Date(2026, 9, 12, 1, 0, 0, 0, time.UTC)
	request := runner.OperationRequest{Deadline: start.Add(time.Minute)}
	state := map[string]any{"Running": false, "OOMKilled": false, "ExitCode": 137, "StartedAt": start, "FinishedAt": start.Add(9 * time.Second)}
	raw := func() []byte { return slJSON([]any{map[string]any{"State": state}}) }
	if err := slENOSPCStop(raw(), request, start.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]any{"Running": true, "OOMKilled": true, "ExitCode": 0, "FinishedAt": start.Add(23 * time.Second)} {
		old := state[key]
		state[key] = value
		if slENOSPCStop(raw(), request, start.Add(25*time.Second)) == nil {
			t.Fatal("ambiguous stop accepted", key)
		}
		state[key] = old
	}
	request.Deadline = start.Add(10 * time.Second)
	if slENOSPCStop(raw(), request, start.Add(10*time.Second)) == nil {
		t.Fatal("deadline stop accepted")
	}
}
