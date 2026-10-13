// Package deadline bounds calls that may never return. The Greenfield SDK's
// RPC client has no timeout of its own and a few of its calls (client set-up,
// GetRedundancyParams) ignore the caller's context, so a context deadline alone
// is not enough: the call runs in its own goroutine and is abandoned once the
// deadline passes.
package deadline

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"
)

// ErrTimeout marks a call that did not finish within its deadline.
var ErrTimeout = errors.New("timed out")

// Call runs fn with a per-call deadline d and returns when fn finishes, the
// deadline passes, or ctx is cancelled, whichever is first. fn should honour its
// context; if it does not, its goroutine is left behind when the deadline hits.
func Call[T any](ctx context.Context, d time.Duration, fn func(context.Context) (T, error)) (T, error) {
	cctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	type result struct {
		v   T
		err error
	}
	ch := make(chan result, 1)
	go func() {
		v, err := fn(cctx)
		ch <- result{v, err}
	}()
	var zero T
	select {
	case r := <-ch:
		if r.err != nil && ctx.Err() == nil && errors.Is(cctx.Err(), context.DeadlineExceeded) {
			return zero, fmt.Errorf("%w after %s: %v", ErrTimeout, d, r.err)
		}
		return r.v, r.err
	case <-cctx.Done():
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		return zero, fmt.Errorf("%w after %s", ErrTimeout, d)
	}
}

// Retry is Call with up to attempts tries; only timeouts are retried, any other
// error is returned at once. what names the call in the retry log line.
func Retry[T any](ctx context.Context, d time.Duration, attempts int, what string, fn func(context.Context) (T, error)) (T, error) {
	return RetryIf(ctx, d, attempts, what, func(err error) bool { return errors.Is(err, ErrTimeout) }, fn)
}

// RetryIf also supports transient errors explicitly recognized by the caller.
func RetryIf[T any](ctx context.Context, d time.Duration, attempts int, what string, retryable func(error) bool, fn func(context.Context) (T, error)) (T, error) {
	if attempts < 1 {
		attempts = 1
	}
	for i := 1; ; i++ {
		if err := ctx.Err(); err != nil {
			var zero T
			return zero, err
		}
		v, err := Call(ctx, d, fn)
		if err == nil || !retryable(err) || i >= attempts || ctx.Err() != nil {
			return v, err
		}
		log.Printf("%s: %v (attempt %d/%d), retrying", what, err, i, attempts)
		select {
		case <-ctx.Done():
			return v, ctx.Err()
		case <-time.After(time.Duration(i) * time.Second):
		}
	}
}
