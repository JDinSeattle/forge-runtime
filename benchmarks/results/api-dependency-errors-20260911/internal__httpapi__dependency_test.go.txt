package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/persistence"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestDependencyErrorsSeparateAvailabilityFromPermanentFailures(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{"connection", fmt.Errorf("wrapped: %w", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("secret-canary")}), 503, "dependency_unavailable"},
		{"lost response", io.ErrUnexpectedEOF, 503, "dependency_unavailable"},
		{"restart", &pgconn.PgError{Code: "57P01", Message: "secret-canary"}, 503, "dependency_unavailable"},
		{"timeout", context.DeadlineExceeded, 503, "dependency_timeout"},
		{"sql bug", &pgconn.PgError{Code: "42601", Message: "secret-canary"}, 500, "internal"},
		{"database authentication", &pgconn.PgError{Code: "28P01", Message: "secret-canary"}, 500, "internal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			(&Server{}).fail(response, httptest.NewRequest("GET", "/readyz", nil), tc.err)
			var body APIError
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if response.Code != tc.status || body.Code != tc.code || body.Retryable != (tc.status == 503) || strings.Contains(response.Body.String(), "secret-canary") {
				t.Fatalf("unexpected public classification: %d %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestReadinessClassifiesActualDatabaseTransportLoss(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()
	defer func() { _ = listener.Close(); <-done }()
	config, err := pgxpool.ParseConfig("postgres://fixture@" + listener.Addr().String() + "/fixture?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.ConnectTimeout = time.Second
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	server := httptest.NewServer((&Server{Store: &persistence.Store{Pool: pool}}).Handler())
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/readyz", nil)
	res, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var body APIError
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != 503 || body.Code != "dependency_unavailable" || !body.Retryable || body.RequestID == "" {
		t.Fatalf("actual dropped database connection: status=%d body=%+v", res.StatusCode, body)
	}
}
