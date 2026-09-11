package provider

import (
	"io"
	"net/http"
)

// Bound the raw HTTP body before SDK SSE decoding. This prevents a single huge
// event (or unlimited SSE comments) from allocating memory before our semantic
// argument/text limits run. Framing is budgeted separately from native JSON.
func boundResponse(nativeBytes int) func(*http.Request, func(*http.Request) (*http.Response, error)) (*http.Response, error) {
	return func(req *http.Request, next func(*http.Request) (*http.Response, error)) (*http.Response, error) {
		response, err := next(req)
		if err == nil && response != nil && response.Body != nil {
			response.Body = &boundedBody{ReadCloser: response.Body, remaining: int64(nativeBytes) + (1 << 20)}
		}
		return response, err
	}
}

type boundedBody struct {
	io.ReadCloser
	remaining int64
}

func (b *boundedBody) Read(p []byte) (int, error) {
	if b.remaining <= 0 {
		return 0, &Error{Kind: ErrLimit, Detail: "HTTP response exceeded stream byte budget"}
	}
	if int64(len(p)) > b.remaining {
		p = p[:b.remaining]
	}
	n, err := b.ReadCloser.Read(p)
	b.remaining -= int64(n)
	return n, err
}
