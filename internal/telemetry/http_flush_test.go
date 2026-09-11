package telemetry

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

type failingFlushWriter struct {
	*httptest.ResponseRecorder
	cancel   context.CancelFunc
	flushes  int
	deadline time.Time
}

func (w *failingFlushWriter) Flush() { _ = w.FlushError() }
func (w *failingFlushWriter) FlushError() error {
	w.flushes++
	w.cancel() // net/http can cancel the request when a socket write fails.
	return os.ErrDeadlineExceeded
}
func (w *failingFlushWriter) SetWriteDeadline(at time.Time) error { w.deadline = at; return nil }

func TestHTTPMiddlewarePreservesFlushErrorAfterRequestCancellation(t *testing.T) {
	m, _ := recording(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer := &failingFlushWriter{ResponseRecorder: httptest.NewRecorder(), cancel: cancel}
	deadline := time.Now().Add(5 * time.Second)
	h := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := w.(http.Flusher); !ok {
			t.Fatal("flushing capability hidden")
		}
		controller := http.NewResponseController(w)
		if err := controller.SetWriteDeadline(deadline); err != nil {
			t.Fatal(err)
		}
		if err := controller.Flush(); !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("flush timeout swallowed: %v", err)
		}
		if r.Context().Err() == nil {
			t.Fatal("fixture did not reproduce request cancellation")
		}
	}))
	h.ServeHTTP(writer, httptest.NewRequest("GET", "/events", nil).WithContext(ctx))
	if writer.flushes != 1 || !writer.deadline.Equal(deadline) {
		t.Fatalf("wrong flush/deadline delegation: %+v", writer)
	}
	if metricValue(m.httpRequests.WithLabelValues("unmatched", "GET", "2xx")) != 1 {
		t.Fatal("flush without explicit write lost HTTP status accounting")
	}
}

type plainResponseWriter struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (w *plainResponseWriter) Header() http.Header    { return w.header }
func (w *plainResponseWriter) WriteHeader(status int) { w.status = status }
func (w *plainResponseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = 200
	}
	return w.body.Write(p)
}

func TestHTTPMiddlewarePlainResponseWriterDoesNotInventFlushing(t *testing.T) {
	m, _ := recording(t)
	writer := &plainResponseWriter{header: make(http.Header)}
	h := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := w.(http.Flusher); ok {
			t.Fatal("plain writer advertised unsupported Flusher")
		}
		if err := http.NewResponseController(w).Flush(); !errors.Is(err, http.ErrNotSupported) {
			t.Fatalf("unsupported flush=%v", err)
		}
		w.WriteHeader(204)
	}))
	h.ServeHTTP(writer, httptest.NewRequest("GET", "/plain", nil))
	if writer.status != 204 {
		t.Fatalf("status=%d", writer.status)
	}
}

var errTestHijack = errors.New("test hijack")
var errTestPush = errors.New("test push")

type fullResponseWriter struct{ *httptest.ResponseRecorder }

func (w *fullResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return nil, nil, errTestHijack
}
func (w *fullResponseWriter) Push(string, *http.PushOptions) error { return errTestPush }
func (w *fullResponseWriter) ReadFrom(r io.Reader) (int64, error) {
	return io.Copy(w.ResponseRecorder, r)
}

func TestHTTPMiddlewareKeepsExistingWriterCapabilities(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(string(rune('0'+version)), func(t *testing.T) {
			m, _ := recording(t)
			writer := &fullResponseWriter{httptest.NewRecorder()}
			h := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if _, ok := w.(http.Flusher); !ok {
					t.Fatal("lost Flusher")
				}
				if version == 1 {
					hijacker, ok := w.(http.Hijacker)
					if !ok {
						t.Fatal("lost existing Hijacker")
					}
					if _, _, err := hijacker.Hijack(); !errors.Is(err, errTestHijack) {
						t.Fatal(err)
					}
					reader, ok := w.(io.ReaderFrom)
					if !ok {
						t.Fatal("lost existing ReaderFrom")
					}
					if n, err := reader.ReadFrom(bytes.NewBufferString("payload")); err != nil || n != 7 {
						t.Fatalf("ReadFrom %d: %v", n, err)
					}
				} else {
					pusher, ok := w.(http.Pusher)
					if !ok {
						t.Fatal("lost existing Pusher")
					}
					if err := pusher.Push("/asset", nil); !errors.Is(err, errTestPush) {
						t.Fatal(err)
					}
				}
				if err := http.NewResponseController(w).Flush(); err != nil {
					t.Fatal(err)
				}
			}))
			req := httptest.NewRequest("GET", "/capabilities", nil)
			req.ProtoMajor = version
			h.ServeHTTP(writer, req)
		})
	}
}
