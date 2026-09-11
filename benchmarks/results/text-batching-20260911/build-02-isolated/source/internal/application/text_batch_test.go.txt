package application

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/provider"
)

func TestTextBatchConfigDefaultsAndBounds(t *testing.T) {
	c, err := (TextBatchConfig{}).Normalize()
	if err != nil || c.Interval != 100*time.Millisecond || c.MaxBytes != 16<<10 {
		t.Fatalf("defaults: %+v %v", c, err)
	}
	for _, valid := range []TextBatchConfig{{10 * time.Millisecond, 4}, {time.Second, 16 << 10}, {MaxBytes: 7}, {Interval: 25 * time.Millisecond}} {
		if _, err := valid.Normalize(); err != nil {
			t.Fatal(err)
		}
	}
	for _, invalid := range []TextBatchConfig{{Interval: -1}, {Interval: 9 * time.Millisecond}, {Interval: time.Second + 1}, {MaxBytes: -1}, {MaxBytes: 3}, {MaxBytes: (16 << 10) + 1}} {
		if _, err := invalid.Normalize(); !errors.Is(err, domain.ErrInvalid) {
			t.Fatalf("accepted invalid config %+v: %v", invalid, err)
		}
		// Invalid deployment policy must fail before even touching Store/Runner.
		d := &Driver{TextBatch: invalid}
		if err := d.RunWorker(context.Background(), "worker", 1); !errors.Is(err, domain.ErrInvalid) {
			t.Fatalf("worker accepted invalid config: %v", err)
		}
		if err := d.Drive(context.Background(), persistence.Run{}); !errors.Is(err, domain.ErrInvalid) {
			t.Fatalf("driver accepted invalid config: %v", err)
		}
	}
}

func TestTextBatchExactUnicodeAndEscaping(t *testing.T) {
	for name, input := range map[string]string{
		"original_chinese_boundary": strings.Repeat("a", (16<<10)-1) + "你",
		"emoji_boundary":            strings.Repeat("a", (16<<10)-2) + "👩🏽‍💻界🙂",
		"html_expansion":            strings.Repeat("<", 16<<10),
		"all_escaping":              strings.Repeat("\x00\b\f\n\r\t\"\\<>&\u2028\u2029中文🙂", 2048),
	} {
		t.Run(name, func(t *testing.T) {
			for _, maximum := range []int{4, 7, 1024, 16 << 10} {
				ctx, cancel := context.WithCancel(context.Background())
				var mu sync.Mutex
				var bodies [][]byte
				s := newTextSink(ctx, cancel, "attempt_fixture", TextBatchConfig{time.Second, maximum}, func(_ context.Context, body json.RawMessage) error {
					mu.Lock()
					defer mu.Unlock()
					bodies = append(bodies, append([]byte{}, body...))
					return nil
				})
				if err := s.emit(provider.ModelEvent{Type: provider.EventTextDelta, Delta: input}); err != nil {
					t.Fatal(err)
				}
				var closers sync.WaitGroup
				for range 8 {
					closers.Go(func() {
						if err := s.close(); err != nil {
							t.Error(err)
						}
					})
				}
				closers.Wait()
				cancel()
				var reconstructed strings.Builder
				for _, body := range bodies {
					var got textPayload
					if len(body) > 32<<10 || !utf8.Valid(body) || json.Unmarshal(body, &got) != nil || !got.Provisional || got.AttemptID != "attempt_fixture" || len(got.Delta) == 0 || len(got.Delta) > maximum || !utf8.ValidString(got.Delta) {
						t.Fatalf("invalid delivered frame with max %d, encoded size %d", maximum, len(body))
					}
					reconstructed.WriteString(got.Delta)
				}
				if reconstructed.String() != input {
					t.Fatalf("changed original text with max %d: got %d bytes, want %d", maximum, reconstructed.Len(), len(input))
				}
				if err := s.emit(provider.ModelEvent{Type: provider.EventTextDelta, Delta: "late"}); !errors.Is(err, io.ErrClosedPipe) {
					t.Fatalf("emit after close: %v", err)
				}
			}
		})
	}
}

func TestTextBatchCloseAndConsumerFailureNeverReplay(t *testing.T) {
	want := errors.New("durable append outcome unknown")
	for _, trigger := range []string{"size", "close", "timer"} {
		t.Run(trigger, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			cfg := TextBatchConfig{10 * time.Millisecond, 16 << 10}
			if trigger == "size" {
				cfg.MaxBytes = 4
			}
			s := newTextSink(ctx, cancel, "attempt", cfg, func(context.Context, json.RawMessage) error { calls++; return want })
			defer s.close()
			err := s.emit(provider.ModelEvent{Type: provider.EventTextDelta, Delta: "abcd"})
			if trigger == "size" && !errors.Is(err, want) {
				t.Fatalf("size append did not return consumer error: %v", err)
			}
			if trigger == "close" {
				if err = s.close(); !errors.Is(err, want) {
					t.Fatal(err)
				}
			}
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
				t.Fatal("consumer failure did not cancel provider context")
			}
			if !errors.Is(s.close(), want) || !errors.Is(s.close(), want) || !errors.Is(s.emit(provider.ModelEvent{Type: provider.EventTextDelta, Delta: "more"}), want) {
				t.Fatal("first error lost")
			}
			if calls != 1 {
				t.Fatalf("uncertain append retried %d times", calls)
			}
		})
	}
}

func TestTextBatchRejectsInvalidUTF8WithoutReplacement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writes := 0
	s := newTextSink(ctx, cancel, "attempt", TextBatchConfig{}, func(context.Context, json.RawMessage) error { writes++; return nil })
	if err := s.emit(provider.ModelEvent{Type: provider.EventTextDelta, Delta: string([]byte{0xf0, 0x9f})}); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("invalid bytes accepted: %v", err)
	}
	if !errors.Is(s.close(), domain.ErrInvalid) || ctx.Err() == nil || writes != 0 {
		t.Fatal("invalid bytes reached persistence or failed to cancel")
	}
}
