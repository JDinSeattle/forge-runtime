package telemetry

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"
)

// ServeMetrics admits only a numeric loopback listener. It exposes this
// process's bounded-label registry, never task contents or credentials.
func (t *Telemetry) ServeMetrics(address string) (func(context.Context) error, error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return nil, errors.New("metrics listener must be numeric loopback")
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	server := &http.Server{Handler: t.Handler(), ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	go func() { _ = server.Serve(listener) }()
	return server.Shutdown, nil
}
