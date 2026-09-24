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
	MaxRetry    int  // retries per piece (and consecutive list retries per prefix) before the object is failed
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

	retryPass  atomic.Bool
	mu         sync.Mutex
	failedList []Claimed // objects that failed in the current pass
}

// Result is what one object attempt produced. SKeys/EKeys split Keys by prefix
// (s<oid>_ vs e<oid>_) so the SP's role for the object can be inferred.
type Result struct {
	Keys, Bytes  uint64
	SKeys, EKeys uint64
	Err          error
}

// Run drains the progress table with Opt.Concurrency workers, then re-runs the
// objects that failed in this run once more (they are usually transient errors).
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
				log.Printf("progress(%s): %d/%d processed, %d failed, keys=%d bytes=%d, elapsed=%s",
					r.passName(), r.processed.Load(), r.total, r.failed.Load(), r.keys.Load(), r.bytes.Load(),
					time.Since(started).Truncate(time.Second))
			}
		}
	}()
	defer close(hbStop)

	claimErr := r.work(ctx, func(ids chan<- Claimed) error {
		var after uint64
		for {
			batch, err := r.Progress.Claim(ctx, after, r.Opt.Concurrency*4, r.Opt.RetryFailed)
			if err != nil {
				return fmt.Errorf("claim from progress table: %w", err)
			}
			if len(batch) == 0 {
				return nil
			}
			for _, c := range batch {
				select {
				case ids <- c:
				case <-ctx.Done():
					return ctx.Err()
				}
				after = c.OID
			}
		}
	})
	log.Printf("main pass finished: processed=%d failed=%d deleted_keys=%d deleted_bytes=%d size_mismatch=%d",
		r.processed.Load(), r.failed.Load(), r.keys.Load(), r.bytes.Load(), r.mismatch.Load())
	if claimErr != nil {
		log.Printf("ERROR main pass stopped early: %v", claimErr)
	}

	r.mu.Lock()
	retry := r.failedList
	r.failedList = nil
	r.mu.Unlock()
	switch {
	case len(retry) == 0:
		return claimErr
	case r.Opt.DryRun:
		return claimErr
	case ctx.Err() != nil:
		log.Printf("retry pass skipped: %v; %d failed objects stay FAILED (use --retry-failed later)", ctx.Err(), len(retry))
		return claimErr
	}

	firstFailed := len(retry)
	log.Printf("retry pass: re-running %d objects that failed in the main pass", firstFailed)
	r.retryPass.Store(true)
	r.total = uint64(firstFailed)
	r.processed.Store(0)
	r.failed.Store(0)
	retryErr := r.work(ctx, func(ids chan<- Claimed) error {
		for _, c := range retry {
			select {
			case ids <- c:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	still := r.failed.Load()
	log.Printf("retry pass finished: retried=%d recovered=%d still_failed=%d", r.processed.Load(), r.processed.Load()-still, still)
	if retryErr != nil {
		log.Printf("ERROR retry pass stopped early: %v", retryErr)
	}
	r.mu.Lock()
	left := r.failedList
	r.mu.Unlock()
	for _, c := range left {
		log.Printf("still FAILED oid=%d (reason in purge_progress.fail_reason; rerun with --retry-failed)", c.OID)
	}
	log.Printf("finished: deleted_keys=%d deleted_bytes=%d size_mismatch=%d failed_after_retry=%d",
		r.keys.Load(), r.bytes.Load(), r.mismatch.Load(), still)
	if claimErr != nil {
		return claimErr
	}
	return retryErr
}

// work runs Opt.Concurrency workers over whatever feed pushes into the channel.
func (r *Runner) work(ctx context.Context, feed func(chan<- Claimed) error) error {
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
	err := feed(ids)
	close(ids)
	wg.Wait()
	return err
}

func (r *Runner) passName() string {
	if r.retryPass.Load() {
		return "retry"
	}
	return "main"
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
		r.mu.Lock()
		r.failedList = append(r.failedList, c)
		r.mu.Unlock()
		log.Printf("[%s %d/%d] oid=%d FAILED keys=%d bytes=%d %.1fs: %v", r.passName(), n, r.total, oid, res.Keys, res.Bytes, time.Since(start).Seconds(), res.Err)
		if err := r.Progress.Fail(ctx, oid, res.Keys, res.Bytes, res.Err.Error()); err != nil {
			log.Printf("ERROR oid=%d: cannot record failure in progress table: %v", oid, err)
		}
		return
	}
	log.Printf("[%s %d/%d] oid=%d done keys=%d bytes=%d %s size=%s %.1fs", r.passName(), n, r.total, oid, res.Keys, res.Bytes, check, verdict, time.Since(start).Seconds())
	if err := r.Progress.Done(ctx, oid, res.Keys, res.Bytes, check); err != nil {
		log.Printf("ERROR oid=%d: cannot record completion in progress table: %v", oid, err)
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
	case r.Opt.RetryFailed || r.retryPass.Load():
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
// again next round.
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

	// Retries are counted per piece: fails[key] is how many times that key has
	// failed to delete (a whole-request error counts once for every key in the
	// batch). Other keys succeeding does not reset it. Once a key exceeds MaxRetry
	// the object is given up as failed; every other deletable piece has been
	// removed by then, the stuck ones stay for a later run. List errors have their
	// own consecutive counter, reset by any successful list. Re-listing from the
	// prefix start each round means a retry targets only the keys still present.
	fails := map[string]int{}
	listFails := 0
	round := 0
	backoff := func(n int) error {
		if r.lim == nil { // rate limiting disabled (offline tests) — don't sleep
			return ctx.Err()
		}
		if n > 6 {
			n = 6 // cap growth
		}
		d := time.Duration(1<<n) * 500 * time.Millisecond
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
			listFails++
			if listFails > r.Opt.MaxRetry {
				log.Printf("oid=%d %s: list error, giving up after %d retries: %v", oid, prefix, r.Opt.MaxRetry, lerr)
				return keys, bytes, fmt.Errorf("list %s failed after %d retries: %w", prefix, r.Opt.MaxRetry, lerr)
			}
			log.Printf("oid=%d %s: list error (retry %d/%d): %v", oid, prefix, listFails, r.Opt.MaxRetry, lerr)
			if err := backoff(listFails); err != nil {
				return keys, bytes, err
			}
			continue
		}
		listFails = 0
		if len(objs) == 0 {
			if len(fails) > 0 {
				log.Printf("oid=%d %s: cleared; %d pieces needed retries", oid, prefix, len(fails))
			}
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
			log.Printf("ERROR oid=%d %s: listing returned %d keys of another object, e.g. %s; refusing to continue", oid, prefix, len(dropped), strings.Join(dropped[:min(3, len(dropped))], ","))
			return keys, bytes, fmt.Errorf("listing under %s returned keys of another object (%s); refusing to continue", prefix, strings.Join(dropped[:min(3, len(dropped))], ","))
		}
		if err := r.wait(ctx); err != nil {
			return keys, bytes, err
		}
		deleted, failed, derr := r.Store.Delete(ctx, batch)
		ok := make(map[string]bool, len(deleted))
		for _, k := range deleted {
			ok[k] = true
			keys++
			bytes += uint64(size[k])
			delete(fails, k)
		}
		// one line per round so a large object visibly advances instead of looking hung
		log.Printf("oid=%d %s: round %d listed=%d deleted=%d failed=%d (prefix total keys=%d bytes=%d)",
			oid, prefix, round, len(batch), len(deleted), len(batch)-len(deleted), keys, bytes)

		// what failed this round, and why
		reason := map[string]string{}
		if derr != nil {
			log.Printf("oid=%d %s: delete request error, all %d undeleted keys in the batch count one failure: %v", oid, prefix, len(batch)-len(deleted), derr)
			for _, k := range batch {
				if !ok[k] {
					reason[k] = derr.Error()
				}
			}
		} else {
			for _, e := range failed {
				reason[e.Key] = e.Code + " " + e.Message
			}
		}
		if len(reason) == 0 {
			continue // whole batch deleted; re-list for the next page
		}
		worst, logged := 0, 0
		var stuck []string
		for _, k := range batch { // batch order keeps the log stable
			why, bad := reason[k]
			if !bad {
				continue
			}
			fails[k]++
			if fails[k] > worst {
				worst = fails[k]
			}
			if fails[k] > r.Opt.MaxRetry {
				stuck = append(stuck, k)
			}
			if logged < 5 {
				log.Printf("oid=%d piece %s delete failed (attempt %d/%d): %s", oid, k, fails[k], r.Opt.MaxRetry+1, why)
				logged++
			}
		}
		if len(reason) > logged {
			log.Printf("oid=%d %s: ... and %d more pieces failed this round", oid, prefix, len(reason)-logged)
		}
		if len(stuck) > 0 {
			for _, k := range stuck[:min(10, len(stuck))] {
				log.Printf("oid=%d piece %s: giving up after %d attempts, last error: %s", oid, k, fails[k], reason[k])
			}
			return keys, bytes, fmt.Errorf("%d pieces under %s still failing after %d retries, first %s: %s",
				len(stuck), prefix, r.Opt.MaxRetry, stuck[0], reason[stuck[0]])
		}
		if err := backoff(worst); err != nil {
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
