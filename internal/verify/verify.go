// Package verify independently checks physical residue under both piece prefixes.
// It never deletes and never assumes purge's DONE means verification PASS. The
// SP's own metadata (SpDB) is out of scope: purge keeps it.
package verify

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"golang.org/x/time/rate"
	"limewire-purge/internal/pieceop"
	"limewire-purge/internal/progress"
)

type Store interface {
	Empty(context.Context, string) (bool, error)
}
type Progress interface {
	VerifyPending(context.Context, progress.Subtask, uint64, int) ([]progress.VerifyRow, error)
	RecordVerify(context.Context, progress.Subtask, uint64, string, string) error
}
type Runner struct {
	Store    Store
	Progress Progress
	Task     progress.Subtask
	Check    func(context.Context) error
	QPS      float64
	MaxRetry int
	Timeout  time.Duration
	// Backoff is overridable in unit tests; production defaults to capped seconds.
	Backoff func(context.Context, int) error
}

func (r *Runner) Run(ctx context.Context) error {
	if r.QPS <= 0 || r.MaxRetry < 0 || r.Timeout <= 0 {
		return errors.New("positive QPS/timeout and nonnegative max-retry required")
	}
	lim := rate.NewLimiter(rate.Limit(r.QPS), 1)
	var after, checked uint64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		batch, err := r.Progress.VerifyPending(ctx, r.Task, after, 1000)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}
		for _, row := range batch {
			if r.Check != nil {
				if err := r.Check(ctx); err != nil {
					return err
				}
			}
			status, detail := r.checkObject(ctx, lim, row.OID)
			if err := ctx.Err(); err != nil {
				return err
			} // incomplete checks are never PASS
			if err := r.Progress.RecordVerify(ctx, r.Task, row.OID, status, detail); err != nil {
				return err
			}
			after = row.OID
			checked++
			if status != "PASS" || checked%100 == 0 {
				log.Printf("verify task=%d oid=%d checked=%d status=%s %s", r.Task.ID, row.OID, checked, status, detail)
			}
		}
	}
}

func (r *Runner) checkObject(ctx context.Context, lim *rate.Limiter, oid uint64) (string, string) {
	var residue, problems []string
	for _, prefix := range pieceop.Prefixes(oid) {
		var empty bool
		err := r.retry(ctx, lim, func(ctx context.Context) error { var e error; empty, e = r.Store.Empty(ctx, prefix); return e })
		if err != nil {
			problems = append(problems, prefix+": "+err.Error())
		} else if !empty {
			residue = append(residue, prefix)
		}
	}
	// Prefer RESIDUE if any check proved residue; detail retains failed queries.
	// This enables purge to requeue the object. The next verify still checks all.
	if len(residue) > 0 {
		return "RESIDUE", clip(strings.Join(append(residue, problems...), "; "))
	}
	if len(problems) > 0 {
		return "ERROR", clip(strings.Join(problems, "; "))
	}
	return "PASS", ""
}

func clip(s string) string {
	if len(s) > 3900 {
		return string([]rune(s)[:min(len([]rune(s)), 900)])
	}
	return s
}

func (r *Runner) retry(ctx context.Context, lim *rate.Limiter, fn func(context.Context) error) error {
	var err error
	for attempt := 0; attempt <= r.MaxRetry; attempt++ {
		if err = lim.Wait(ctx); err != nil {
			return err
		}
		if r.Check != nil {
			if err = r.Check(ctx); err != nil {
				return err
			}
		}
		cctx, cancel := context.WithTimeout(ctx, r.Timeout)
		err = fn(cctx)
		cancel()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt == r.MaxRetry {
			break
		}
		if r.Backoff != nil {
			if e := r.Backoff(ctx, attempt); e != nil {
				return e
			}
		} else {
			t := time.NewTimer(time.Duration(min(attempt+1, 5)) * time.Second)
			select {
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			case <-t.C:
			}
		}
	}
	return fmt.Errorf("after %d attempts: %w", r.MaxRetry+1, err)
}
