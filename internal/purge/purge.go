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
	"limewire-purge/internal/spdb"
)

// ObjectStore is the object-storage side: list by prefix, delete a batch.
type ObjectStore interface {
	List(ctx context.Context, prefix string, max int32) ([]s3store.Object, error)
	ListAll(ctx context.Context, prefix string, fn func([]s3store.Object) error) error
	Empty(ctx context.Context, prefix string) (bool, error)
	// Delete must refuse (s3store.ErrUnsafeKeys) any key that is not a
	// well-formed piece of object oid.
	Delete(ctx context.Context, oid uint64, keys []string) (deleted []string, failed []s3store.KeyError, err error)
}

// ErrSafety marks a failure that puts the task's scope in doubt rather than a
// transient fault: the chain disagrees with the approved object list, or storage
// holds keys this tool does not recognise. One such failure halts the whole run:
// no further objects are claimed, in-flight ones are cancelled, and nothing is
// retried. Delete requests already sent cannot be recalled.
var ErrSafety = errors.New("safety violation")

// ObjectHead is what the chain says about one object right now.
type ObjectHead struct {
	ID          uint64
	Bucket      string
	Status      string // SEALED / CREATED / ... (OBJECT_STATUS_ prefix stripped)
	Version     int64
	PayloadSize uint64
}

// Chain is the pre-delete identity check.
type Chain interface {
	HeadObject(ctx context.Context, oid uint64) (ObjectHead, error)
}

// Meta is the SP's local metadata database.
type Meta interface {
	Delete(ctx context.Context, oid uint64) error
	Exists(ctx context.Context, oid uint64) (bool, error)
}

// Claimed is one object handed to a worker, with the snapshot scan recorded for
// it from bsdb. The chain must still agree with the snapshot before anything is
// deleted; PayloadSize also drives the size reconciliation.
type Claimed struct {
	OID         uint64
	PayloadSize uint64
	Status      string
	Version     int64
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
	// Check runs at request boundaries; OnSafety durably latches the task hold.
	Check    func(context.Context) error
	OnSafety func(context.Context, uint64, error) error

	lim         *rate.Limiter
	processed   atomic.Uint64
	failed      atomic.Uint64
	interrupted atomic.Uint64
	keys        atomic.Uint64
	bytes       atomic.Uint64
	mismatch    atomic.Uint64
	total       atomic.Uint64

	mu         sync.Mutex
	failedList []Claimed         // failed objects retained for the final summary
	unrecorded map[uint64]string // oid -> progress write error, until a later write for it succeeds

	haltOnce   sync.Once
	haltErr    error // first safety violation; set once, read after the workers stop
	haltCancel context.CancelCauseFunc
	stopOnce   sync.Once
	stopErr    error      // ordinary failure; never persists a SAFETY_HOLD
	startMu    sync.Mutex // linearizes starting an object against ordinary stop
	stopCh     chan struct{}
}

// writeTimeout bounds one progress-table write. Writes use a context detached
// from the run's, so an object finished just as the operator hits Ctrl+C is
// still recorded instead of being redone.
const writeTimeout = 30 * time.Second

// Result is what one object attempt produced. SKeys/EKeys split Keys by prefix
// (s<oid>_ vs e<oid>_) so the SP's role for the object can be inferred.
type Result struct {
	Keys, Bytes  uint64
	SKeys, EKeys uint64
	Err          error
}

// Run processes objects with bounded request/piece retries. The first object
// that still fails stops the run; operators fix the cause and rerun the command.
// There is no automatic second object pass and no ordinary-failure safety hold.
func (r *Runner) Run(parent context.Context, total uint64) error {
	r.unrecorded = map[uint64]string{}
	r.stopCh = make(chan struct{})
	// Ordinary failures stop admission only. Safety violations, operator
	// cancellation and execution-lock loss still cancel in-flight work.
	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)
	r.haltCancel = cancel
	if r.Opt.Concurrency <= 0 {
		r.Opt.Concurrency = 1
	}
	if r.Opt.QPS <= 0 {
		r.Opt.QPS = 50
	}
	if r.Opt.MaxRetry < 0 {
		r.Opt.MaxRetry = 10
	}
	r.total.Store(total)
	r.lim = rate.NewLimiter(rate.Limit(r.Opt.QPS), 1)

	// heartbeat: overall progress every 30s, so a long-running large object or a
	// slow bucket never looks like a hang
	started := time.Now()
	hbStop := make(chan struct{})
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-hbStop:
				return
			case <-t.C:
				log.Printf("progress(%s): %d/%d processed, %d failed, keys=%d bytes=%d, elapsed=%s",
					r.passName(), r.processed.Load(), r.total.Load(), r.failed.Load(), r.keys.Load(), r.bytes.Load(),
					time.Since(started).Truncate(time.Second))
			}
		}
	}()
	defer func() { close(hbStop); <-hbDone }()

	claimErr := r.work(ctx, func(ids chan<- Claimed) error {
		var after uint64
		for {
			select {
			case <-r.stopCh:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			batch, err := r.Progress.Claim(ctx, after, r.Opt.Concurrency*4, r.Opt.RetryFailed)
			if err != nil {
				r.stop(0, fmt.Errorf("claim from progress table: %w", err))
				return fmt.Errorf("claim from progress table: %w", err)
			}
			if len(batch) == 0 {
				return nil
			}
			for _, c := range batch {
				select {
				case ids <- c:
				case <-r.stopCh:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
				after = c.OID
			}
		}
	})
	log.Printf("main pass finished: processed=%d failed=%d deleted_keys=%d deleted_bytes=%d size_mismatch=%d",
		r.processed.Load(), r.failed.Load(), r.keys.Load(), r.bytes.Load(), r.mismatch.Load())
	if r.haltErr != nil || r.stopErr != nil {
		return r.finish(parent, nil) // the claim error is just the cancellation
	}
	if claimErr != nil {
		log.Printf("ERROR main pass stopped early: %v", claimErr)
	}

	return r.finish(parent, claimErr)
}

// stop prevents new objects from starting but lets admitted objects finish and
// record their results. It does not invoke OnSafety or cancel their contexts.
func (r *Runner) stop(oid uint64, err error) {
	r.startMu.Lock()
	defer r.startMu.Unlock()
	r.stopOnce.Do(func() {
		r.stopErr = fmt.Errorf("oid=%d: %w", oid, err)
		close(r.stopCh)
		log.Printf("!!! STOP: deletion run failed: %v; stopping new objects, waiting for in-flight objects to finish and record results; no automatic second pass. Fix the cause and rerun the same purge command; no task resume needed for this ordinary failure.", r.stopErr)
	})
}

// halt stops the run on the first safety violation: the feed stops claiming,
// in-flight objects are cancelled (left unrecorded), and no retry pass follows.
func (r *Runner) halt(oid uint64, err error) {
	r.haltOnce.Do(func() {
		r.haltErr = fmt.Errorf("oid %d: %w", oid, err)
		log.Printf("HALT: safety violation on oid=%d: %v; claiming no further objects, cancelling in-flight ones, no retry pass", oid, err)
		r.haltCancel(r.haltErr)
		if r.OnSafety != nil {
			ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
			defer cancel()
			if e := r.OnSafety(ctx, oid, err); e != nil {
				r.haltErr = errors.Join(r.haltErr, fmt.Errorf("cannot persist SAFETY_HOLD: %w", e))
			}
		}
	})
}

// finish logs what is left over and turns it into Run's error. failedList holds
// the objects of the last pass that failed or whose result was not recorded.
// ctx is the operator's context, not the one a halt cancels.
func (r *Runner) finish(ctx context.Context, claimErr error) error {
	r.mu.Lock()
	left := r.failedList
	unrecorded := len(r.unrecorded)
	for _, c := range left {
		if why, ok := r.unrecorded[c.OID]; ok {
			log.Printf("NOT RECORDED oid=%d: result missing from purge_progress (%s); a plain rerun picks it up again", c.OID, why)
		} else {
			log.Printf("still FAILED oid=%d (reason in purge_progress.fail_reason; fix the cause and rerun the same purge command)", c.OID)
		}
	}
	r.mu.Unlock()
	still := r.failed.Load()
	log.Printf("finished: deleted_keys=%d deleted_bytes=%d size_mismatch=%d failed=%d not_recorded=%d interrupted=%d",
		r.keys.Load(), r.bytes.Load(), r.mismatch.Load(), still, unrecorded, r.interrupted.Load())

	var problems []string
	if r.haltErr != nil {
		problems = append(problems, fmt.Sprintf("HALTED on %v; %d in-flight objects cancelled and left unrecorded; inspect before running again",
			r.haltErr, r.interrupted.Load()))
	}
	if claimErr != nil {
		problems = append(problems, fmt.Sprintf("main pass stopped early: %v", claimErr))
	}
	if r.stopErr != nil {
		problems = append(problems, fmt.Sprintf("STOPPED on %v; fix the cause and rerun purge", r.stopErr))
	}
	if still > 0 {
		problems = append(problems, fmt.Sprintf("%d objects still FAILED", still))
	}
	if unrecorded > 0 {
		problems = append(problems, fmt.Sprintf("%d results not recorded in the progress table", unrecorded))
	}
	if ctx.Err() != nil {
		problems = append(problems, fmt.Sprintf("interrupted (%v); %d in-flight objects left unrecorded for the next run", ctx.Err(), r.interrupted.Load()))
	}
	if len(problems) > 0 {
		return errors.Join(r.haltErr, r.stopErr, errors.New(strings.Join(problems, "; ")))
	}
	return nil
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
				r.startMu.Lock()
				stopped := ctx.Err() != nil
				select {
				case <-r.stopCh:
					stopped = true
				default:
				}
				r.startMu.Unlock()
				if stopped {
					continue
				}
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
	return "main"
}

func (r *Runner) handle(ctx context.Context, c Claimed) {
	if ctx.Err() != nil {
		return // interrupted before this object started; it stays unprocessed for the next run
	}
	oid := c.OID
	start := time.Now()
	log.Printf("oid=%d start", oid)
	res := r.ProcessOne(ctx, c)
	n := r.processed.Add(1)
	r.keys.Add(res.Keys)
	r.bytes.Add(res.Bytes)
	check := r.checkSize(res, c.PayloadSize)
	verdict := r.verdict(oid, res, check, c.PayloadSize)

	if r.Opt.DryRun {
		// a preview lists every anomaly instead of stopping at the first
		log.Printf("[dry-run %d/%d] oid=%d keys=%d bytes=%d %s size=%s err=%v", n, r.total.Load(), oid, res.Keys, res.Bytes, check, verdict, res.Err)
		if res.Err != nil {
			r.failed.Add(1)
		}
		return
	}
	safety := errors.Is(res.Err, ErrSafety)
	if res.Err != nil && !safety && ctx.Err() != nil {
		// cut short by the interrupt or a halt, not a verdict on the object: leave it
		// without a result so the next invocation reconciles its actual residue.
		r.interrupted.Add(1)
		log.Printf("[%s %d/%d] oid=%d interrupted keys=%d bytes=%d: %v; not recorded, the next run redoes it", r.passName(), n, r.total.Load(), oid, res.Keys, res.Bytes, context.Cause(ctx))
		return
	}
	if safety {
		r.halt(oid, res.Err) // stop the others first; recording can wait a moment
	} else if res.Err != nil {
		r.stop(oid, res.Err)
	}

	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), writeTimeout)
	defer cancel()
	if res.Err != nil {
		r.failed.Add(1)
		if !safety {
			r.rememberFailure(c)
		}
		log.Printf("[%s %d/%d] oid=%d FAILED keys=%d bytes=%d %.1fs: %v", r.passName(), n, r.total.Load(), oid, res.Keys, res.Bytes, time.Since(start).Seconds(), res.Err)
		if err := r.Progress.Fail(wctx, oid, res.Keys, res.Bytes, res.Err.Error()); err != nil {
			r.notRecorded(oid, err)
			log.Printf("ERROR oid=%d: cannot record failure in progress table: %v", oid, err)
			return
		}
		r.recorded(oid)
		return
	}
	if err := r.Progress.Done(wctx, oid, res.Keys, res.Bytes, check); err != nil {
		// The pieces are gone but progress is missing. Stop and let a manual
		// rerun reconcile the actual residue instead of blindly processing more.
		r.notRecorded(oid, err)
		r.rememberFailure(c)
		r.stop(oid, fmt.Errorf("completion NOT recorded in progress table: %w", err))
		log.Printf("ERROR [%s %d/%d] oid=%d deleted keys=%d bytes=%d but completion NOT recorded in progress table: %v", r.passName(), n, r.total.Load(), oid, res.Keys, res.Bytes, err)
		return
	}
	r.recorded(oid)
	log.Printf("[%s %d/%d] oid=%d done keys=%d bytes=%d %s size=%s %.1fs", r.passName(), n, r.total.Load(), oid, res.Keys, res.Bytes, check, verdict, time.Since(start).Seconds())
}

func (r *Runner) rememberFailure(c Claimed) {
	r.mu.Lock()
	r.failedList = append(r.failedList, c)
	r.mu.Unlock()
}

func (r *Runner) notRecorded(oid uint64, err error) {
	r.mu.Lock()
	r.unrecorded[oid] = err.Error()
	r.mu.Unlock()
}

func (r *Runner) recorded(oid uint64) {
	r.mu.Lock()
	delete(r.unrecorded, oid)
	r.mu.Unlock()
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
func (r *Runner) ProcessOne(ctx context.Context, c Claimed) Result {
	var res Result
	oid := c.OID

	// ② the chain must confirm the object's full identity; a failed lookup is a
	// failure, never a reason to fall back on the local snapshot
	if r.Check != nil {
		if err := r.Check(ctx); err != nil {
			res.Err = err
			return res
		}
	}
	h, err := r.Chain.HeadObject(ctx, oid)
	if err != nil {
		res.Err = fmt.Errorf("chain check: %w; nothing deleted", err)
		return res
	}
	if diff := identityDiff(c, h, r.Opt.Bucket); len(diff) > 0 {
		res.Err = fmt.Errorf("%w: chain identity check failed (%s); nothing deleted", ErrSafety, strings.Join(diff, "; "))
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
	if r.Check != nil {
		if err := r.Check(ctx); err != nil {
			res.Err = err
			return res
		}
	}
	if err := spdb.Retry(ctx, fmt.Sprintf("oid=%d metadata delete", oid), func(cctx context.Context) error {
		if r.Check != nil {
			if err := r.Check(cctx); err != nil {
				return err
			}
		}
		return r.Meta.Delete(cctx, oid)
	}); err != nil {
		res.Err = fmt.Errorf("metadata delete: %w", err)
		return res
	}
	var exists bool
	err = spdb.Retry(ctx, fmt.Sprintf("oid=%d metadata re-check", oid), func(cctx context.Context) error {
		if r.Check != nil {
			if err := r.Check(cctx); err != nil {
				return err
			}
		}
		var e error
		exists, e = r.Meta.Exists(cctx, oid)
		return e
	})
	if err != nil {
		res.Err = fmt.Errorf("metadata re-check: %w", err)
		return res
	}
	if exists {
		res.Err = errors.New("metadata re-check: rows still present")
	}
	return res
}

// identityDiff lists every way the chain's view of the object departs from what
// this task approved: the object asked for, in the target bucket (whose id was
// checked against the chain at start-up), unchanged since the scan snapshot.
func identityDiff(c Claimed, h ObjectHead, bucket string) []string {
	var d []string
	if h.ID != c.OID {
		d = append(d, fmt.Sprintf("chain returned object id %d for %d", h.ID, c.OID))
	}
	if h.Bucket != bucket {
		d = append(d, fmt.Sprintf("not in bucket %s (chain says %q)", bucket, h.Bucket))
	}
	if h.Status != c.Status {
		d = append(d, fmt.Sprintf("status %s at scan, %s now", c.Status, h.Status))
	}
	if h.Version != c.Version {
		d = append(d, fmt.Sprintf("version %d at scan, %d now", c.Version, h.Version))
	}
	if h.PayloadSize != c.PayloadSize {
		d = append(d, fmt.Sprintf("payload %d at scan, %d now", c.PayloadSize, h.PayloadSize))
	}
	return d
}

// unknownKeysErr reports keys under an object's prefix that are not well-formed
// pieces of it. They are never deleted; the object fails so someone looks.
func unknownKeysErr(oid uint64, prefix string, rejected []string) error {
	eg := strings.Join(rejected[:min(3, len(rejected))], ",")
	log.Printf("ERROR oid=%d %s: %d keys are not well-formed pieces of this object, e.g. %s; deleting none of them, inspect manually", oid, prefix, len(rejected), eg)
	return fmt.Errorf("%w: %d keys under %s are not well-formed pieces of object %d (%s); left in place for manual inspection", ErrSafety, len(rejected), prefix, oid, eg)
}

// clearPrefix lists from the start of prefix and deletes what it sees, until the
// listing comes back empty. Whatever a failed batch leaves behind is simply listed
// again next round.
func (r *Runner) clearPrefix(ctx context.Context, oid uint64, prefix string) (keys, bytes uint64, err error) {
	if r.Opt.DryRun {
		var rejected []string
		err = r.Store.ListAll(ctx, prefix, func(objs []s3store.Object) error {
			for _, o := range objs {
				if p, ok := pieceop.Parse(o.Key); ok && p.OID == oid {
					keys++
					bytes += uint64(o.Size)
				} else {
					rejected = append(rejected, o.Key)
				}
			}
			return r.wait(ctx)
		})
		if err == nil && len(rejected) > 0 {
			err = unknownKeysErr(oid, prefix, rejected)
		}
		return keys, bytes, err
	}

	// Retries are counted per piece: fails[key] is how many times that key has
	// failed to delete (a whole-request error counts once for every key in the
	// batch). Other keys succeeding does not reset it. Once a key exceeds MaxRetry
	// the object is given up as failed at once; each round has deleted what it could
	// in its listing window, but pieces beyond it and keys still below the limit
	// stay for a later run. List errors have their
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
		batch, rejected := pieceop.KeepOnly(all, oid) // fresh slice every round
		if len(rejected) > 0 {
			// a correctness violation, not a transient failure — never retry, and
			// delete nothing from this round so the prefix is left as found
			return keys, bytes, unknownKeysErr(oid, prefix, rejected)
		}
		if err := r.wait(ctx); err != nil {
			return keys, bytes, err
		}
		deleted, failed, derr := r.Store.Delete(ctx, oid, batch)
		if errors.Is(derr, s3store.ErrUnsafeKeys) {
			return keys, bytes, fmt.Errorf("%w: %w", ErrSafety, derr)
		}
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
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.Check != nil {
		if err := r.Check(ctx); err != nil {
			return err
		}
	}
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
