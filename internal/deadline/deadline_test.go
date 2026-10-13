package deadline

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestCallReturnsResult(t *testing.T) {
	v, err := Call(context.Background(), time.Second, func(context.Context) (int, error) { return 7, nil })
	if err != nil || v != 7 {
		t.Fatalf("v=%d err=%v", v, err)
	}
}

// A call that ignores its context (like the SDK's client set-up) must not hold
// the caller past the deadline.
func TestCallAbandonsCallThatIgnoresContext(t *testing.T) {
	block := make(chan struct{})
	defer close(block)
	start := time.Now()
	_, err := Call(context.Background(), 50*time.Millisecond, func(context.Context) (int, error) {
		<-block
		return 0, nil
	})
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("want ErrTimeout, got %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("returned after %s", time.Since(start))
	}
}

// A call that honours its context and fails because of the deadline is reported
// as a timeout, not as the raw context error.
func TestCallMapsDeadlineErrorToTimeout(t *testing.T) {
	_, err := Call(context.Background(), 20*time.Millisecond, func(ctx context.Context) (int, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	})
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("want ErrTimeout, got %v", err)
	}
}

// Cancelling the caller's context is an interruption, not a timeout.
func TestCallCancelledIsNotTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Call(ctx, time.Second, func(c context.Context) (int, error) {
		<-c.Done()
		return 0, c.Err()
	})
	if errors.Is(err, ErrTimeout) || !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}

func TestRetryOnlyRetriesTimeouts(t *testing.T) {
	var calls atomic.Int32
	_, err := Retry(context.Background(), 20*time.Millisecond, 3, "test", func(ctx context.Context) (int, error) {
		calls.Add(1)
		return 0, errors.New("not found")
	})
	if err == nil || calls.Load() != 1 {
		t.Fatalf("non-timeout error must not be retried: calls=%d err=%v", calls.Load(), err)
	}
}

func TestRetryRecoversAfterTimeout(t *testing.T) {
	var calls atomic.Int32
	v, err := Retry(context.Background(), 20*time.Millisecond, 3, "test", func(ctx context.Context) (int, error) {
		if calls.Add(1) == 1 {
			<-ctx.Done()
			return 0, ctx.Err()
		}
		return 5, nil
	})
	if err != nil || v != 5 || calls.Load() != 2 {
		t.Fatalf("v=%d calls=%d err=%v", v, calls.Load(), err)
	}
}

func TestRetryGivesUpAfterAttempts(t *testing.T) {
	var calls atomic.Int32
	_, err := Retry(context.Background(), 10*time.Millisecond, 2, "test", func(ctx context.Context) (int, error) {
		calls.Add(1)
		<-ctx.Done()
		return 0, ctx.Err()
	})
	if !errors.Is(err, ErrTimeout) || calls.Load() != 2 {
		t.Fatalf("calls=%d err=%v", calls.Load(), err)
	}
}
