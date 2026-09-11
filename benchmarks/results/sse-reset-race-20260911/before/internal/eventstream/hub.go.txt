// Package eventstream shares live reads and bounds every subscriber queue.
// Durable SQL events remain authoritative; notifications are only latency hints.
package eventstream

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/persistence"
)

var ErrReset = domain.ErrReset
var ErrSlow = errors.New("slow_subscriber")

type Source interface {
	GetRun(context.Context, domain.ID, domain.ID) (persistence.Run, error)
	Events(context.Context, domain.ID, domain.ID, uint64, int) ([]persistence.Event, error)
}
type Config struct {
	PollInterval                                    time.Duration
	QueueSize, HistorySize, MaxHubs, MaxSubscribers int
	Slow                                            func()
}
type Manager struct {
	mu     sync.Mutex
	source Source
	cfg    Config
	ctx    context.Context
	cancel context.CancelFunc
	hubs   map[string]*hub
}
type Subscription struct {
	Events     <-chan persistence.Event
	Errors     <-chan error
	CoveredSeq uint64
	Close      func()
}
type subscriber struct {
	events chan persistence.Event
	errors chan error
	after  uint64
	closed bool
}
type hub struct {
	manager    *Manager
	key        string
	tenant, id domain.ID
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	cursor     uint64
	history    []persistence.Event
	subs       map[*subscriber]struct{}
}

func New(ctx context.Context, source Source, cfg Config) *Manager {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 128
	}
	if cfg.HistorySize <= 0 {
		cfg.HistorySize = 512
	}
	if cfg.MaxHubs <= 0 {
		cfg.MaxHubs = 128
	}
	if cfg.MaxSubscribers <= 0 {
		cfg.MaxSubscribers = 256
	}
	ctx, cancel := context.WithCancel(ctx)
	return &Manager{source: source, cfg: cfg, ctx: ctx, cancel: cancel, hubs: map[string]*hub{}}
}
func (m *Manager) Close() { m.cancel() }

// Subscribe registers for live changes before the handler replays SQL history.
// CoveredSeq divides replay from live delivery with no observation gap.
func (m *Manager) Subscribe(ctx context.Context, tenant, id domain.ID, after uint64) (Subscription, error) {
	r, err := m.source.GetRun(ctx, tenant, id)
	if err != nil {
		return Subscription{}, err
	}
	if after > r.CoveredSeq {
		return Subscription{}, domain.ErrConflict
	}
	if r.RetainedFromSeq > 0 && after < r.RetainedFromSeq-1 {
		return Subscription{}, ErrReset
	}
	key := string(tenant) + "/" + string(id)
	m.mu.Lock()
	h := m.hubs[key]
	if h == nil {
		if len(m.hubs) >= m.cfg.MaxHubs {
			m.mu.Unlock()
			return Subscription{}, domain.ErrCapacity
		}
		hctx, cancel := context.WithCancel(m.ctx)
		h = &hub{manager: m, key: key, tenant: tenant, id: id, ctx: hctx, cancel: cancel, cursor: r.CoveredSeq, subs: map[*subscriber]struct{}{}}
		m.hubs[key] = h
		go h.run()
	}
	h.mu.Lock()
	m.mu.Unlock()
	if len(h.subs) >= m.cfg.MaxSubscribers {
		h.mu.Unlock()
		return Subscription{}, domain.ErrCapacity
	}
	sub := &subscriber{events: make(chan persistence.Event, m.cfg.QueueSize), errors: make(chan error, 1), after: r.CoveredSeq}
	if h.cursor > r.CoveredSeq && (len(h.history) == 0 || h.history[0].Seq > r.CoveredSeq+1) {
		h.mu.Unlock()
		return Subscription{}, ErrReset
	}
	for _, e := range h.history {
		if e.Seq > sub.after {
			if len(sub.events) == cap(sub.events) {
				h.mu.Unlock()
				return Subscription{}, ErrReset
			}
			sub.events <- e
			sub.after = e.Seq
		}
	}
	h.subs[sub] = struct{}{}
	h.mu.Unlock()
	var once sync.Once
	closeSub := func() {
		once.Do(func() {
			m.mu.Lock()
			h.mu.Lock()
			h.remove(sub, nil)
			empty := len(h.subs) == 0
			if empty && m.hubs[key] == h {
				delete(m.hubs, key)
				h.cancel()
			}
			h.mu.Unlock()
			m.mu.Unlock()
		})
	}
	return Subscription{Events: sub.events, Errors: sub.errors, CoveredSeq: r.CoveredSeq, Close: closeSub}, nil
}
func (h *hub) remove(s *subscriber, err error) {
	if s.closed {
		return
	}
	s.closed = true
	delete(h.subs, s)
	if err != nil {
		s.errors <- err
	}
	close(s.errors)
	close(s.events)
}
func (h *hub) run() {
	ticker := time.NewTicker(h.manager.cfg.PollInterval)
	defer ticker.Stop()
	defer func() {
		h.manager.mu.Lock()
		h.mu.Lock()
		if h.manager.hubs[h.key] == h {
			delete(h.manager.hubs, h.key)
		}
		h.cancel()
		for s := range h.subs {
			h.remove(s, context.Canceled)
		}
		h.mu.Unlock()
		h.manager.mu.Unlock()
	}()
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
		}
		h.mu.Lock()
		after := h.cursor
		h.mu.Unlock()
		ctx, cancel := context.WithTimeout(h.ctx, 3*time.Second)
		events, err := h.manager.source.Events(ctx, h.tenant, h.id, after, 256)
		cancel()
		if err != nil {
			if errors.Is(err, domain.ErrReset) {
				h.mu.Lock()
				for s := range h.subs {
					h.remove(s, ErrReset)
				}
				h.mu.Unlock()
				return
			}
			if errors.Is(err, domain.ErrNotFound) {
				return
			}
			continue
		}
		h.mu.Lock()
		for _, e := range events {
			if e.Seq != h.cursor+1 {
				for s := range h.subs {
					h.remove(s, ErrReset)
				}
				h.mu.Unlock()
				return
			}
			h.cursor = e.Seq
			h.history = append(h.history, e)
			if len(h.history) > h.manager.cfg.HistorySize {
				h.history = append([]persistence.Event(nil), h.history[len(h.history)-h.manager.cfg.HistorySize:]...)
			}
			for s := range h.subs {
				if e.Seq <= s.after {
					continue
				}
				select {
				case s.events <- e:
					s.after = e.Seq
				default:
					h.remove(s, ErrSlow)
					if h.manager.cfg.Slow != nil {
						h.manager.cfg.Slow()
					}
				}
			}
		}
		h.mu.Unlock()
	}
}
