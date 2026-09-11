package application

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/JDinSeattle/forge-runtime/internal/provider"
)

// TextBatchConfig is operator-owned delivery policy, not model/run input.
// MaxBytes bounds unencoded pending text. JSON escaping may require several
// smaller events to satisfy the independent persistence payload ceiling.
type TextBatchConfig struct {
	Interval time.Duration `json:"interval_ns,omitempty"`
	MaxBytes int           `json:"max_bytes,omitempty"`
}

const textEventPayloadLimit = 32 << 10 // AppendWorkerEvent's JSON payload limit.

// Normalize fills omitted fields without mutating shared worker configuration.
func (c TextBatchConfig) Normalize() (TextBatchConfig, error) {
	if c.Interval == 0 {
		c.Interval = 100 * time.Millisecond
	}
	if c.MaxBytes == 0 {
		c.MaxBytes = 16 << 10
	}
	if c.Interval < 10*time.Millisecond || c.Interval > time.Second || c.MaxBytes < utf8.UTFMax || c.MaxBytes > 16<<10 {
		return c, fmt.Errorf("%w: text batch interval must be 10ms..1s and max_bytes 4..16384", domain.ErrInvalid)
	}
	return c, nil
}

type textPayload struct {
	AttemptID   domain.ID `json:"attempt_id"`
	Provisional bool      `json:"provisional"`
	Delta       string    `json:"delta"`
}

// textPayloadPrefix returns the largest UTF-8 prefix whose actual JSON encoding
// fits the fixed payload ceiling. The binary search operates on rune counts,
// so even escaped control characters and surrogate-pair-independent UTF-8 text
// are split without altering the decoded concatenation.
func textPayloadPrefix(id domain.ID, text string) ([]byte, int, error) {
	encode := func(n int) ([]byte, error) { return json.Marshal(textPayload{id, true, text[:n]}) }
	body, err := encode(len(text))
	if err != nil || len(body) <= textEventPayloadLimit {
		return body, len(text), err
	}
	ends := make([]int, 0, len(text)+1)
	ends = append(ends, 0)
	for at, r := range text {
		ends = append(ends, at+utf8.RuneLen(r))
	}
	low, high := 0, len(ends)-1
	for low < high {
		mid := (low + high + 1) / 2
		body, err = encode(ends[mid])
		if err != nil {
			return nil, 0, err
		}
		if len(body) <= textEventPayloadLimit {
			low = mid
		} else {
			high = mid - 1
		}
	}
	if low == 0 {
		return nil, 0, fmt.Errorf("%w: text event identity leaves no payload capacity", domain.ErrCapacity)
	}
	body, err = encode(ends[low])
	return body, ends[low], err
}

type streamSink struct {
	mu      sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
	attempt domain.ID
	config  TextBatchConfig
	append  func(context.Context, json.RawMessage) error
	pending string
	err     error
	closed  bool
	done    chan struct{}
	once    sync.Once
	wg      sync.WaitGroup
}

func newSink(ctx context.Context, cancel context.CancelFunc, d *Driver, r persistence.Run, a persistence.ModelAttempt) *streamSink {
	return newTextSink(ctx, cancel, a.ID, d.TextBatch, func(ctx context.Context, body json.RawMessage) error {
		return d.Store.AppendWorkerEvent(ctx, r, "text.delta", body)
	})
}

func newTextSink(ctx context.Context, cancel context.CancelFunc, attempt domain.ID, config TextBatchConfig, appendEvent func(context.Context, json.RawMessage) error) *streamSink {
	config, err := config.Normalize()
	s := &streamSink{ctx: ctx, cancel: cancel, attempt: attempt, config: config, append: appendEvent, err: err, done: make(chan struct{})}
	if err != nil {
		cancel()
		return s
	}
	s.wg.Go(func() {
		ticker := time.NewTicker(config.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.mu.Lock()
				s.flush()
				s.mu.Unlock()
			}
		}
	})
	return s
}

func (s *streamSink) fail(err error) error {
	if s.err == nil {
		s.err = err
		s.cancel()
	}
	return s.err
}

// flush runs under mu. A failed append is never retried by this sink: its
// outcome could be unknown, and cancelling the model preserves that distinction.
func (s *streamSink) flush() {
	for s.err == nil && s.pending != "" {
		body, size, err := textPayloadPrefix(s.attempt, s.pending)
		if err == nil {
			ctx, cancel := context.WithTimeout(s.ctx, 3*time.Second)
			err = s.append(ctx, body)
			cancel()
		}
		if err != nil {
			s.fail(err)
			return
		}
		s.pending = s.pending[size:]
	}
}

func (s *streamSink) emit(e provider.ModelEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	if s.closed {
		return io.ErrClosedPipe
	}
	if e.Type != provider.EventTextDelta {
		return nil
	}
	if !utf8.ValidString(e.Delta) {
		return s.fail(fmt.Errorf("%w: invalid UTF-8 text delta", domain.ErrInvalid))
	}
	for len(e.Delta) > 0 {
		size := min(s.config.MaxBytes-len(s.pending), len(e.Delta))
		for size > 0 && size < len(e.Delta) && !utf8.RuneStart(e.Delta[size]) {
			size--
		}
		if size == 0 {
			s.flush()
		} else {
			s.pending += e.Delta[:size]
			e.Delta = e.Delta[size:]
			if len(s.pending) == s.config.MaxBytes {
				s.flush()
			}
		}
		if s.err != nil {
			return s.err
		}
	}
	return nil
}

func (s *streamSink) close() error {
	s.once.Do(func() {
		close(s.done)
		s.wg.Wait()
		s.mu.Lock()
		defer s.mu.Unlock()
		s.closed = true
		s.flush()
	})
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}
