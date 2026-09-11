package applicationfaults

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// fixtureProxy only owns the loopback connections explicitly routed through it.
// It neither administers PostgreSQL nor inspects/logs protocol or credential bytes.
type fixtureProxy struct {
	listener        net.Listener
	upstream        string
	mu              sync.Mutex
	blocked, closed bool
	connections     map[net.Conn]bool
	workers         sync.WaitGroup
	events          []proxyEvent
}
type proxyEvent struct {
	At          time.Time `json:"at"`
	Action      string    `json:"action"`
	Connections int       `json:"connections"`
}

func startFixtureProxy(upstream string) (*fixtureProxy, error) {
	host, _, err := net.SplitHostPort(upstream)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return nil, fmt.Errorf("numeric loopback upstream required")
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &fixtureProxy{listener: l, upstream: upstream, connections: map[net.Conn]bool{}}
	p.workers.Add(1)
	go func() {
		defer p.workers.Done()
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			p.workers.Add(1)
			go func() { defer p.workers.Done(); p.forward(c) }()
		}
	}()
	return p, nil
}
func (p *fixtureProxy) forward(down net.Conn) {
	defer down.Close()
	p.mu.Lock()
	blocked := p.blocked || p.closed
	p.mu.Unlock()
	if blocked {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	up, err := (&net.Dialer{}).DialContext(ctx, "tcp", p.upstream)
	if err != nil {
		return
	}
	defer up.Close()
	p.mu.Lock()
	if p.blocked || p.closed {
		p.mu.Unlock()
		return
	}
	p.connections[down], p.connections[up] = true, true
	p.mu.Unlock()
	defer func() { p.mu.Lock(); delete(p.connections, down); delete(p.connections, up); p.mu.Unlock() }()
	done := make(chan struct{})
	go func() { io.Copy(up, down); up.Close(); close(done) }()
	io.Copy(down, up)
	down.Close()
	<-done
}
func (p *fixtureProxy) block(blocked bool) proxyEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blocked = blocked
	e := proxyEvent{At: time.Now().UTC(), Action: "restore", Connections: len(p.connections)}
	if blocked {
		e.Action = "cut_all_and_refuse_new"
		for c := range p.connections {
			c.Close()
		}
	}
	p.events = append(p.events, e)
	return e
}
func (p *fixtureProxy) close() {
	p.mu.Lock()
	p.closed = true
	for c := range p.connections {
		c.Close()
	}
	p.mu.Unlock()
	p.listener.Close()
	p.workers.Wait()
}
func (p *fixtureProxy) log() []proxyEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]proxyEvent(nil), p.events...)
}

func TestFixtureProxyCutsExistingAndNewConnections(t *testing.T) {
	echo, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer echo.Close()
	go func() {
		for {
			c, err := echo.Accept()
			if err != nil {
				return
			}
			go func() { defer c.Close(); io.Copy(c, c) }()
		}
	}()
	p, err := startFixtureProxy(echo.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	dial := func() net.Conn {
		t.Helper()
		c, err := net.DialTimeout("tcp", p.listener.Addr().String(), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		c.SetDeadline(time.Now().Add(time.Second))
		return c
	}
	exchange := func(c net.Conn) error {
		if _, err := c.Write([]byte("probe")); err != nil {
			return err
		}
		b := make([]byte, 5)
		_, err := io.ReadFull(c, b)
		if err == nil && string(b) != "probe" {
			return fmt.Errorf("incorrect forwarded data")
		}
		return err
	}
	old := dial()
	defer old.Close()
	if err = exchange(old); err != nil {
		t.Fatal(err)
	}
	cut := p.block(true)
	if cut.Connections < 2 {
		t.Fatal("test did not cut an established pair")
	}
	if err = exchange(old); err == nil {
		t.Fatal("established connection survived cut")
	}
	fresh := dial()
	defer fresh.Close()
	if err = exchange(fresh); err == nil {
		t.Fatal("new connection passed while blocked")
	}
	p.block(false)
	restored := dial()
	defer restored.Close()
	if err = exchange(restored); err != nil {
		t.Fatal(err)
	}
}
