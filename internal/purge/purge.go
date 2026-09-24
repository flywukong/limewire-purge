// Package purge is the per-object state machine. Everything it touches sits behind
// small interfaces so the whole loop can be exercised offline with fakes.
package purge

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"limewire-purge/internal/pieceop"
	"limewire-purge/internal/s3store"
)

// ObjectStore is the object-storage side: list by prefix, delete a batch.
type ObjectStore interface {
	List(ctx context.Context, prefix string, max int32) ([]s3store.Object, error)
	ListAll(ctx context.Context, prefix string, fn func([]s3store.Object) error) error
	Empty(ctx context.Context, prefix string) (bool, error)
	Delete(ctx context.Context, keys []string) (deleted []string, failed []s3store.KeyError, err error)
}

// Chain is the pre-delete ownership check.
type Chain interface {
	HeadObjectBucket(ctx context.Context, oid uint64) (bucketName string, err error)
}

// Meta is the SP's local metadata database.
type Meta interface {
	Delete(ctx context.Context, oid uint64) error
	Exists(ctx context.Context, oid uint64) (bool, error)
}

// Claimed is one object handed to a worker, with the chain payload size bsdb
// recorded for it (used only for reconciliation, never to decide deletion).
type Claimed struct {
	OID         uint64
	PayloadSize uint64
}

// Progress is the tool's own progress table.
type Progress interface {
	Claim(ctx context.Context, after uint64, limit int, retryFailed bool) ([]Claimed, error)
	Done(ctx context.Context, oid uint64, keys, bytes uint64, check SizeCheck) error
	Fail(ctx context.Context, oid uint64, keys, bytes uint64, reason string) error
}

type Options struct {
	Bucket      string // Greenfield bucket name the object must belong to
	Concurrency int
	QPS         float64
	MaxRetry    int  // consecutive no-progress rounds (list/delete errors or all-keys-failed) before the object is failed
	DryRun      bool // list only, never delete, never write progress
	RetryFailed bool
	// EC layout used to compute a secondary's expected bytes; 0 disables the
	// secondary size check (primary is still checked against payload_size)
	DataChunks  uint32
	SegmentSize uint64
}

type Runner struct {
	Store    ObjectStore
	Chain    Chain
	Meta     Meta
	Progress Progress
	Opt      Options

	lim       *rate.Limiter
	processed atomic.Uint64
	failed    atomic.Uint64
	keys      atomic.Uint64
	bytes     atomic.Uint64
	mismatch  atomic.Uint64
	total     uint64
}

// Result is what one object attempt produced. SKeys/EKeys split Keys by prefix
// (s<oid>_ vs e<oid>_) so the SP's role for the object can be inferred.
type Result struct {
	Keys, Bytes  uint64
	SKeys, EKeys uint64
	Err          error
}

// Run drains the progress table with Opt.Concurrency workers.
func (r *Runner) Run(ctx context.Context, total uint64) error {
	if r.Opt.Concurrency <= 0 {
		r.Opt.Concurrency = 1
	}
	if r.Opt.QPS <= 0 {
		r.Opt.QPS = 50
	}
	if r.Opt.MaxRetry <= 0 {
		r.Opt.MaxRetry = 10
	}
	r.total = total
	r.lim = rate.NewLimiter(rate.Limit(r.Opt.QPS), 1)

	ids := make(chan Claimed, r.Opt.Concurrency*2)
	var wg sync.WaitGroup
	for i := 0; i < r.Opt.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range ids {
				r.handle(ctx, c)
			}
		}()
	}

	// heartbeat: overall progress every 30s, so a long-running large object or a
	// slow bucket never looks like a hang
	started := time.Now()
	hbStop := make(chan struct{})
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-hbStop:
				return
			case <-t.C:
				log.Printf("progress: %d/%d processed, %d failed, keys=%d bytes=%d, elapsed=%s",
					r.processed.Load(), r.total, r.failed.Load(), r.keys.Load(), r.bytes.Load(),
					time.Since(started).Truncate(time.Second))
			}
		}
	}()
	defer close(hbStop)

	var after uint64
	var claimErr error
feed:
	for {
		batch, err := r.Progress.Claim(ctx, after, r.Opt.Concurrency*4, r.Opt.RetryFailed)
		if err != nil {
			claimErr = err
			break
		}
		if len(batch) == 0 {
			break
		}
		for _, c := range batch {
			select {
			case ids <- c:
			case <-ctx.Done():
				claimErr = ctx.Err()
				break feed
			}
			after = c.OID
		}
	}
	close(ids)
	wg.Wait()
	log.Printf("finished: processed=%d failed=%d deleted_keys=%d deleted_bytes=%d size_mismatch=%d",
		r.processed.Load(), r.failed.Load(), r.keys.Load(), r.bytes.Load(), r.mismatch.Load())
	return claimErr
}

func (r *Runner) handle(ctx context.Context, c Claimed) {
	oid := c.OID
	start := time.Now()
	log.Printf("oid=%d start", oid)
	res := r.ProcessOne(ctx, oid)
	n := r.processed.Add(1)
	r.keys.Add(res.Keys)
	r.bytes.Add(res.Bytes)
	check := r.checkSize(res, c.PayloadSize)
	verdict := r.verdict(oid, res, check, c.PayloadSize)

	if r.Opt.DryRun {
		log.Printf("[dry-run %d/%d] oid=%d keys=%d bytes=%d %s size=%s err=%v", n, r.total, oid, res.Keys, res.Bytes, check, verdict, res.Err)
		return
	}
	if res.Err != nil {
		r.failed.Add(1)
		log.Printf("[%d/%d] oid=%d FAILED keys=%d bytes=%d %.1fs: %v", n, r.total, oid, res.Keys, res.Bytes, time.Since(start).Seconds(), res.Err)
		if err := r.Progress.Fail(ctx, oid, res.Keys, res.Bytes, res.Err.Error()); err != nil {
			log.Printf("oid=%d: cannot record failure: %v", oid, err)
		}
		return
	}
	log.Printf("[%d/%d] oid=%d done keys=%d bytes=%d %s size=%s %.1fs", n, r.total, oid, res.Keys, res.Bytes, check, verdict, time.Since(start).Seconds())
	if err := r.Progress.Done(ctx, oid, res.Keys, res.Bytes, check); err != nil {
		log.Printf("oid=%d: cannot record completion: %v", oid, err)
	}
}

// verdict compares this attempt's bytes with the expectation and logs a warning
// on mismatch. A mismatch on a resumed object can be a false alarm: bytes removed
// by an earlier interrupted run were never recorded, so this attempt sees less.
func (r *Runner) verdict(oid uint64, res Result, check SizeCheck, payload uint64) string {
	switch {
	case res.Err != nil:
		return "n/a"
	case check.Role == "mixed":
		r.mismatch.Add(1)
		log.Printf("WARNING oid=%d: data under both s%d_ and e%d_ on this SP (primary and secondary at once) — unexpected, inspect manually", oid, oid, oid)
		return "ANOMALY"
	case !check.Known:
		return "skip"
	case r.Opt.RetryFailed:
		// this attempt only removes what an earlier attempt left; the cumulative
		// comparison in `status` is the authoritative one
		return "see-status"
	case res.Bytes == check.Expected:
		return "ok"
	default:
		r.mismatch.Add(1)
		log.Printf("WARNING oid=%d: size mismatch, %s holds %d bytes but chain payload %d implies %d (earlier interrupted run, wrong bucket/config, or corrupt data)",
			oid, check.Role, res.Bytes, payload, check.Expected)
		return "MISMATCH"
	}
}

// ProcessOne runs the full sequence for a single object:
// chain check → delete both prefixes until empty → re-check empty → clear metadata.
// Intermediate state lives only in the returned counters.
func (r *Runner) ProcessOne(ctx context.Context, oid uint64) Result {
	var res Result

	// ② the object must belong to the target bucket
	bucket, err := r.Chain.HeadObjectBucket(ctx, oid)
	if err != nil {
		res.Err = fmt.Errorf("chain check: %w", err)
		return res
	}
	if bucket != r.Opt.Bucket {
		res.Err = fmt.Errorf("not in bucket %s (chain says %q); nothing deleted", r.Opt.Bucket, bucket)
		return res
	}

	// ③ both prefixes (index 0 is s<oid>_, 1 is e<oid>_)
	for i, prefix := range pieceop.Prefixes(oid) {
		k, b, err := r.clearPrefix(ctx, oid, prefix)
		res.Keys += k
		res.Bytes += b
		if i == 0 {
			res.SKeys += k
		} else {
			res.EKeys += k
		}
		if err != nil {
			res.Err = fmt.Errorf("prefix %s: %w", prefix, err)
			return res
		}
	}
	if r.Opt.DryRun {
		return res
	}

	// ④ read-only re-check
	for _, prefix := range pieceop.Prefixes(oid) {
		if err := r.wait(ctx); err != nil {
			res.Err = err
			return res
		}
		empty, err := r.Store.Empty(ctx, prefix)
		if err != nil {
			res.Err = fmt.Errorf("re-check %s: %w", prefix, err)
			return res
		}
		if !empty {
			res.Err = fmt.Errorf("re-check %s: residue", prefix)
			return res
		}
	}

	// ⑤ local metadata
	if err := r.Meta.Delete(ctx, oid); err != nil {
		res.Err = fmt.Errorf("metadata delete: %w", err)
		return res
	}
	exists, err := r.Meta.Exists(ctx, oid)
	if err != nil {
		res.Err = fmt.Errorf("metadata re-check: %w", err)
		return res
	}
	if exists {
		res.Err = errors.New("metadata re-check: rows still present")
	}
	return res
}

// clearPrefix lists from the start of prefix and deletes what it sees, until the
// listing comes back empty. Whatever a failed batch leaves behind is simply listed
// again next round, so no per-key bookkeeping is needed.
func (r *Runner) clearPrefix(ctx context.Context, oid uint64, prefix string) (keys, bytes uint64, err error) {
	if r.Opt.DryRun {
		err = r.Store.ListAll(ctx, prefix, func(objs []s3store.Object) error {
			for _, o := range objs {
				if got, ok := pieceop.ParseOID(o.Key); ok && got == oid {
					keys++
					bytes += uint64(o.Size)
				}
			}
			return r.wait(ctx)
		})
		return keys, bytes, err
	}

	// attempts counts consecutive rounds that made no progress (a failed list, a
	// failed delete request, or a delete where every key errored). Any round that
	// deletes at least one key resets it to 0. Re-listing from the prefix start
	// means each retry naturally targets only the keys still left, i.e. the
	// unsuccessful portion. Only after MaxRetry consecutive stuck rounds is the
	// object given up as failed; the remaining keys stay in the bucket for a
	// later run. Intermediate failures are logged, not written to the DB.
	attempts := 0
	round := 0
	giveUp := func(format string, args ...any) error {
		return fmt.Errorf(format, args...)
	}
	backoff := func() error {
		if r.lim == nil { // rate limiting disabled (offline tests) — don't sleep
			return ctx.Err()
		}
		shift := attempts
		if shift > 6 {
			shift = 6 // cap growth
		}
		d := time.Duration(1<<shift) * 500 * time.Millisecond
		if d > 30*time.Second {
			d = 30 * time.Second
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d):
			return nil
		}
	}
	for {
		round++
		if err := r.wait(ctx); err != nil {
			return keys, bytes, err
		}
		objs, lerr := r.Store.List(ctx, prefix, 1000)
		if lerr != nil {
			attempts++
			if attempts > r.Opt.MaxRetry {
				return keys, bytes, giveUp("list %s failed after %d retries: %w", prefix, r.Opt.MaxRetry, lerr)
			}
			log.Printf("oid=%d %s: list error (retry %d/%d): %v", oid, prefix, attempts, r.Opt.MaxRetry, lerr)
			if err := backoff(); err != nil {
				return keys, bytes, err
			}
			continue
		}
		if len(objs) == 0 {
			return keys, bytes, nil
		}
		size := make(map[string]int64, len(objs))
		all := make([]string, 0, len(objs))
		for _, o := range objs {
			all = append(all, o.Key)
			size[o.Key] = o.Size
		}
		batch, dropped := pieceop.KeepOnly(all, oid) // fresh slice every round
		if len(dropped) > 0 {
			// a correctness violation, not a transient failure — never retry
			return keys, bytes, fmt.Errorf("listing under %s returned keys of another object (%s); refusing to continue", prefix, strings.Join(dropped[:min(3, len(dropped))], ","))
		}
		if err := r.wait(ctx); err != nil {
			return keys, bytes, err
		}
		deleted, failed, derr := r.Store.Delete(ctx, batch)
		for _, k := range deleted {
			keys++
			bytes += uint64(size[k])
		}
		if len(deleted) > 0 {
			attempts = 0 // progress made this round
		}
		// one line per round so a large object visibly advances instead of looking hung
		log.Printf("oid=%d %s: round %d listed=%d deleted=%d failed=%d (prefix total keys=%d bytes=%d)",
			oid, prefix, round, len(batch), len(deleted), len(failed), keys, bytes)
		if derr != nil {
			if len(deleted) == 0 {
				attempts++
			}
			if attempts > r.Opt.MaxRetry {
				return keys, bytes, giveUp("delete %s failed after %d retries: %w", prefix, r.Opt.MaxRetry, derr)
			}
			log.Printf("oid=%d %s: delete request error (retry %d/%d): %v", oid, prefix, attempts, r.Opt.MaxRetry, derr)
			if err := backoff(); err != nil {
				return keys, bytes, err
			}
			continue
		}
		if len(failed) == 0 {
			continue // whole batch deleted; re-list for the next page
		}
		if len(deleted) == 0 {
			attempts++
		}
		if attempts > r.Opt.MaxRetry {
			return keys, bytes, giveUp("%d keys under %s keep failing after %d retries, first: %s", len(failed), prefix, r.Opt.MaxRetry, failed[0])
		}
		log.Printf("oid=%d %s: %d/%d keys failed (retry %d/%d), first: %s", oid, prefix, len(failed), len(batch), attempts, r.Opt.MaxRetry, failed[0])
		if err := backoff(); err != nil {
			return keys, bytes, err
		}
	}
}

func (r *Runner) wait(ctx context.Context) error {
	if r.lim == nil {
		return nil
	}
	return r.lim.Wait(ctx)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
