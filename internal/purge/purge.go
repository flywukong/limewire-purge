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

// ObjectStore is the object-storage side. The unversioned methods (List/ListAll/
// Empty/Delete) address objects by key; the versioned ones address them by
// Key+VersionId and also see delete markers. Which pair is used is decided once,
// from the bucket's versioning status — see Options.Versioned.
type ObjectStore interface {
	List(ctx context.Context, prefix string, max int32) ([]s3store.Object, error)
	ListAll(ctx context.Context, prefix string, fn func([]s3store.Object) error) error
	Empty(ctx context.Context, prefix string) (bool, error)
	Delete(ctx context.Context, keys []string) (deleted []string, failed []s3store.KeyError, err error)

	ListVersions(ctx context.Context, prefix string, max int32) ([]s3store.Version, error)
	ListAllVersions(ctx context.Context, prefix string, fn func([]s3store.Version) error) error
	EmptyVersions(ctx context.Context, prefix string) (bool, error)
	DeleteVersions(ctx context.Context, vs []s3store.Version) (deleted []s3store.Version, failed []s3store.KeyError, err error)
}

// entry is one deletable thing: a key on an unversioned bucket, or a
// key+version (possibly a delete marker) on a versioned one.
type entry struct {
	key       string
	versionID string
	size      int64
	marker    bool
}

func (e entry) label() string {
	if e.versionID == "" {
		return e.key
	}
	return e.key + "@" + e.versionID
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

// Progress is the tool's own progress table.
type Progress interface {
	Claim(ctx context.Context, after uint64, limit int, retryFailed bool) ([]uint64, error)
	Done(ctx context.Context, oid uint64, keys, bytes uint64) error
	Fail(ctx context.Context, oid uint64, keys, bytes uint64, reason string) error
}

type Options struct {
	Bucket      string // Greenfield bucket name the object must belong to
	Concurrency int
	QPS         float64
	MaxRetry    int  // consecutive no-progress rounds (list/delete errors or all-keys-failed) before the object is failed
	DryRun      bool // list only, never delete, never write progress
	RetryFailed bool
	// Versioned selects the version-aware path: list every version and delete
	// marker under the prefix and delete each by Key+VersionId. Required on a
	// bucket whose versioning is Enabled or Suspended, where a plain key delete
	// only adds a delete marker and leaves the bytes (and any historical version)
	// in place while an unversioned listing reports the prefix as empty.
	Versioned bool
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
	total     uint64
}

// Result is what one object attempt produced.
type Result struct {
	Keys, Bytes uint64
	Err         error
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

	ids := make(chan uint64, r.Opt.Concurrency*2)
	var wg sync.WaitGroup
	for i := 0; i < r.Opt.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for oid := range ids {
				r.handle(ctx, oid)
			}
		}()
	}

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
		for _, oid := range batch {
			select {
			case ids <- oid:
			case <-ctx.Done():
				claimErr = ctx.Err()
				break feed
			}
			after = oid
		}
	}
	close(ids)
	wg.Wait()
	log.Printf("finished: processed=%d failed=%d deleted_keys=%d deleted_bytes=%d",
		r.processed.Load(), r.failed.Load(), r.keys.Load(), r.bytes.Load())
	return claimErr
}

func (r *Runner) handle(ctx context.Context, oid uint64) {
	start := time.Now()
	res := r.ProcessOne(ctx, oid)
	n := r.processed.Add(1)
	r.keys.Add(res.Keys)
	r.bytes.Add(res.Bytes)

	if r.Opt.DryRun {
		log.Printf("[dry-run %d/%d] oid=%d keys=%d bytes=%d err=%v", n, r.total, oid, res.Keys, res.Bytes, res.Err)
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
	log.Printf("[%d/%d] oid=%d done keys=%d bytes=%d %.1fs", n, r.total, oid, res.Keys, res.Bytes, time.Since(start).Seconds())
	if err := r.Progress.Done(ctx, oid, res.Keys, res.Bytes); err != nil {
		log.Printf("oid=%d: cannot record completion: %v", oid, err)
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

	// ③ both prefixes
	for _, prefix := range pieceop.Prefixes(oid) {
		k, b, err := r.clearPrefix(ctx, oid, prefix)
		res.Keys += k
		res.Bytes += b
		if err != nil {
			res.Err = fmt.Errorf("prefix %s: %w", prefix, err)
			return res
		}
	}
	if r.Opt.DryRun {
		return res
	}

	// ④ read-only re-check (on a versioned bucket a leftover delete marker counts
	// as residue, so this must use the same addressing mode as the deletes)
	for _, prefix := range pieceop.Prefixes(oid) {
		if err := r.wait(ctx); err != nil {
			res.Err = err
			return res
		}
		var empty bool
		var err error
		if r.Opt.Versioned {
			empty, err = r.Store.EmptyVersions(ctx, prefix)
		} else {
			empty, err = r.Store.Empty(ctx, prefix)
		}
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
		if r.Opt.Versioned {
			err = r.Store.ListAllVersions(ctx, prefix, func(vs []s3store.Version) error {
				for _, v := range vs {
					if got, ok := pieceop.ParseOID(v.Key); ok && got == oid {
						keys++
						bytes += uint64(v.Size) // delete markers carry no size
					}
				}
				return r.wait(ctx)
			})
			return keys, bytes, err
		}
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

	// one listing/deleting pair, chosen by the bucket's versioning mode
	list := func() ([]entry, error) {
		objs, lerr := r.Store.List(ctx, prefix, 1000)
		if lerr != nil {
			return nil, lerr
		}
		out := make([]entry, 0, len(objs))
		for _, o := range objs {
			out = append(out, entry{key: o.Key, size: o.Size})
		}
		return out, nil
	}
	del := func(batch []entry) (deleted []entry, failed []s3store.KeyError, derr error) {
		ks := make([]string, 0, len(batch))
		for _, e := range batch {
			ks = append(ks, e.key)
		}
		size := make(map[string]int64, len(batch))
		for _, e := range batch {
			size[e.key] = e.size
		}
		okKeys, failed, derr := r.Store.Delete(ctx, ks)
		for _, k := range okKeys {
			deleted = append(deleted, entry{key: k, size: size[k]})
		}
		return deleted, failed, derr
	}
	if r.Opt.Versioned {
		list = func() ([]entry, error) {
			vs, lerr := r.Store.ListVersions(ctx, prefix, 1000)
			if lerr != nil {
				return nil, lerr
			}
			out := make([]entry, 0, len(vs))
			for _, v := range vs {
				out = append(out, entry{key: v.Key, versionID: v.VersionID, size: v.Size, marker: v.IsDeleteMarker})
			}
			return out, nil
		}
		del = func(batch []entry) (deleted []entry, failed []s3store.KeyError, derr error) {
			vs := make([]s3store.Version, 0, len(batch))
			for _, e := range batch {
				vs = append(vs, s3store.Version{Key: e.key, VersionID: e.versionID, Size: e.size, IsDeleteMarker: e.marker})
			}
			okVs, failed, derr := r.Store.DeleteVersions(ctx, vs)
			for _, v := range okVs {
				deleted = append(deleted, entry{key: v.Key, versionID: v.VersionID, size: v.Size, marker: v.IsDeleteMarker})
			}
			return deleted, failed, derr
		}
	}

	// attempts counts consecutive rounds that made no progress (a failed list, a
	// failed delete request, or a delete where every key errored). Any round that
	// deletes at least one key resets it to 0. Re-listing from the prefix start
	// means each retry naturally targets only the keys still left, i.e. the
	// unsuccessful portion. Only after MaxRetry consecutive stuck rounds is the
	// object given up as failed; the remaining keys stay in the bucket for a
	// later run. Intermediate failures are logged, not written to the DB.
	attempts := 0
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
		if err := r.wait(ctx); err != nil {
			return keys, bytes, err
		}
		listed, lerr := list()
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
		if len(listed) == 0 {
			return keys, bytes, nil
		}
		batch := make([]entry, 0, len(listed)) // fresh slice every round
		var dropped []string
		for _, e := range listed {
			if got, ok := pieceop.ParseOID(e.key); ok && got == oid {
				batch = append(batch, e)
			} else {
				dropped = append(dropped, e.label())
			}
		}
		if len(dropped) > 0 {
			// a correctness violation, not a transient failure — never retry
			return keys, bytes, fmt.Errorf("listing under %s returned keys of another object (%s); refusing to continue", prefix, strings.Join(dropped[:min(3, len(dropped))], ","))
		}
		if err := r.wait(ctx); err != nil {
			return keys, bytes, err
		}
		deleted, failed, derr := del(batch)
		for _, e := range deleted {
			keys++
			bytes += uint64(e.size)
		}
		if len(deleted) > 0 {
			attempts = 0 // progress made this round
		}
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
