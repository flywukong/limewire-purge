package chain

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestIsTimeout(t *testing.T) {
	// what net/http returns when http.Client.Timeout fires, as flattened by the query stack
	httpTimeout := errors.New(`rpc error: code = Unknown desc = post failed: Post "http://127.0.0.1:26750": context deadline exceeded (Client.Timeout exceeded while awaiting headers)`)
	if !isTimeout(httpTimeout) {
		t.Fatal("http client timeout not recognised")
	}
	notFound := errors.New("rpc error: code = Unknown desc = No such object: unknown request")
	if isTimeout(notFound) {
		t.Fatal("not-found must not be retried as a timeout")
	}
}

func TestTransientQueryErrors(t *testing.T) {
	for _, msg := range []string{"connection refused", "connection reset by peer", "unexpected EOF", "Post: EOF", "temporary RPC HTTP status 429", "temporary RPC HTTP status 503", "i/o timeout"} {
		if !isTransient(errors.New("rpc error: code = Unknown desc = " + msg)) {
			t.Errorf("should retry %q", msg)
		}
	}
	for _, err := range []error{context.Canceled, errors.New("No such object: unknown request"), errors.New("401 Unauthorized"), errors.New("403 AccessDenied"), errors.New("invalid request")} {
		if isTransient(err) {
			t.Errorf("must not retry %v", err)
		}
	}
}

type fakeRoundTripper func(*http.Request) (*http.Response, error)

func (f fakeRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestHTTPTransientStatusesSurviveFlatteningAndRecover(t *testing.T) {
	var calls atomic.Int32
	transport := transientStatusTransport{base: fakeRoundTripper(func(*http.Request) (*http.Response, error) {
		status := 200
		switch calls.Add(1) {
		case 1:
			status = 429
		case 2:
			status = 503
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader("test"))}, nil
	})}
	value, err := call(context.Background(), &Client{timeout: time.Second}, "test query", func(ctx context.Context) (int, error) {
		req, _ := http.NewRequestWithContext(ctx, "POST", "http://unused.invalid", nil)
		resp, err := transport.RoundTrip(req)
		if err != nil {
			return 0, fmt.Errorf("rpc error: code = Unknown desc = %v", err)
		}
		resp.Body.Close()
		return 7, nil
	})
	if err != nil || value != 7 || calls.Load() != 3 {
		t.Fatalf("value=%d calls=%d err=%v", value, calls.Load(), err)
	}
}

func TestTransientQueriesHaveBoundedAttempts(t *testing.T) {
	var calls atomic.Int32
	_, err := call(context.Background(), &Client{timeout: time.Second}, "test bounded", func(context.Context) (int, error) {
		calls.Add(1)
		return 0, errors.New("connection refused")
	})
	if err == nil || calls.Load() != 3 {
		t.Fatalf("calls=%d err=%v", calls.Load(), err)
	}
}

func TestPermanentHTTPStatusIsNotMarkedTransient(t *testing.T) {
	for _, status := range []int{400, 401, 403, 404} {
		transport := transientStatusTransport{base: fakeRoundTripper(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader("denied"))}, nil
		})}
		resp, err := transport.RoundTrip(&http.Request{})
		if err != nil || resp.StatusCode != status {
			t.Fatalf("status=%d err=%v", status, err)
		}
		resp.Body.Close()
	}
}
