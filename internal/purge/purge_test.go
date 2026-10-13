package purge

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"limewire-purge/internal/s3store"
)

type transientMeta struct{ deletes, reads int }

func (m *transientMeta) Delete(context.Context, uint64) error {
	m.deletes++
	if m.deletes == 1 {
		return &mysql.MySQLError{Number: 1213}
	}
	return nil
}
func (m *transientMeta) Exists(context.Context, uint64) (bool, error) {
	m.reads++
	if m.reads == 1 {
		return false, &mysql.MySQLError{Number: 2013}
	}
	return false, nil
}

func TestPurgeRetriesMetadataDeleteAndRecheck(t *testing.T) {
	meta := &transientMeta{}
	r := &Runner{Store: newFakeStore("s1_s0"), Chain: fakeChain{bucketOf: map[uint64]string{1: "limewire"}}, Meta: meta, Opt: Options{Bucket: "limewire"}}
	result := r.ProcessOne(context.Background(), sealed(1))
	if result.Err != nil || meta.deletes != 2 || meta.reads != 2 {
		t.Fatalf("result=%+v delete=%d reads=%d", result, meta.deletes, meta.reads)
	}
}

type ordinaryStopChain struct{ started, release chan struct{} }

func (c ordinaryStopChain) HeadObject(ctx context.Context, oid uint64) (ObjectHead, error) {
	if oid == 1 {
		close(c.started)
		select {
		case <-ctx.Done():
			return ObjectHead{}, ctx.Err()
		case <-c.release:
			return ObjectHead{ID: oid, Bucket: "limewire", Status: "SEALED"}, nil
		}
	}
	if oid == 2 {
		select {
		case <-c.started:
			return ObjectHead{}, errors.New("RPC unavailable after client retries")
		case <-ctx.Done():
			return ObjectHead{}, ctx.Err()
		}
	}
	return ObjectHead{ID: oid, Bucket: "limewire", Status: "SEALED"}, nil
}

func TestOrdinaryFailureDrainsInFlightAndLogsCause(t *testing.T) {
	var logs bytes.Buffer
	writer := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(writer) })
	ch := ordinaryStopChain{started: make(chan struct{}), release: make(chan struct{})}
	st := newFakeStore("s1_s0", "s2_s0", "s3_s0")
	pg := &fakeProgress{pending: []uint64{1, 2, 3}, done: map[uint64][2]uint64{}, failed: map[uint64]string{}}
	r := &Runner{Store: st, Chain: ch, Meta: &fakeMeta{rows: map[uint64]bool{}}, Progress: pg,
		Opt: Options{Bucket: "limewire", Concurrency: 2, QPS: 1e6},
		OnSafety: func(context.Context, uint64, error) error {
			t.Error("ordinary failure triggered safety hold")
			return nil
		}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- r.Run(ctx, 3) }()
	select {
	case <-ch.started:
	case <-ctx.Done():
		t.Fatal("worker did not start")
	}
	select {
	case <-r.stopCh:
	case <-ctx.Done():
		t.Fatal("ordinary stop not triggered")
	}
	select {
	case err := <-result:
		t.Fatalf("returned before in-flight object finished: %v", err)
	default:
	}
	close(ch.release)
	err := <-result
	if err == nil || !strings.Contains(err.Error(), "STOPPED") || ctx.Err() != nil {
		t.Fatalf("did not stop promptly: %v context=%v", err, ctx.Err())
	}
	if len(pg.failed) != 1 || pg.failed[2] == "" || len(pg.done) != 1 || len(st.objs) != 2 || st.tries["s1_s0"] != 1 || st.tries["s3_s0"] != 0 {
		t.Fatalf("failed=%v done=%v remaining=%v", pg.failed, pg.done, st.objs)
	}
	if !strings.Contains(logs.String(), "!!! STOP") || !strings.Contains(logs.String(), "oid=2") || !strings.Contains(logs.String(), "RPC unavailable") {
		t.Fatal(logs.String())
	}
}

func TestOperatorCancellationInterruptsOrdinaryDrain(t *testing.T) {
	ch := ordinaryStopChain{started: make(chan struct{}), release: make(chan struct{})}
	pg := &fakeProgress{pending: []uint64{1, 2, 3}, done: map[uint64][2]uint64{}, failed: map[uint64]string{}}
	r := &Runner{Store: newFakeStore("s1_s0"), Chain: ch, Meta: &fakeMeta{rows: map[uint64]bool{}}, Progress: pg,
		Opt: Options{Bucket: "limewire", Concurrency: 2, QPS: 1e6}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- r.Run(ctx, 3) }()
	select {
	case <-ch.started:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not start")
	}
	select {
	case <-r.stopCh:
	case <-time.After(5 * time.Second):
		t.Fatal("no stop")
	}
	cancel()
	select {
	case err := <-result:
		if err == nil || len(pg.done) != 0 || r.interrupted.Load() != 1 {
			t.Fatalf("err=%v done=%v interrupted=%d", err, pg.done, r.interrupted.Load())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not interrupt draining")
	}
}

// ---- fakes -------------------------------------------------------------

type fakeStore struct {
	mu         sync.Mutex
	objs       map[string]int64
	failKeys   map[string]int // key -> remaining failures to inject
	deleteErrN int            // remaining whole-request delete errors to inject (-1 = forever)
	maxBatch   int            // largest batch seen
	lists      int
	listErrN   int            // remaining List errors to inject
	tries      map[string]int // key -> delete attempts seen
}

func newFakeStore(keys ...string) *fakeStore {
	f := &fakeStore{objs: map[string]int64{}, failKeys: map[string]int{}, tries: map[string]int{}}
	for _, k := range keys {
		f.objs[k] = 10
	}
	return f
}

func (f *fakeStore) sorted(prefix string) []string {
	var ks []string
	for k := range f.objs {
		if strings.HasPrefix(k, prefix) {
			ks = append(ks, k)
		}
	}
	sort.Strings(ks)
	return ks
}

func (f *fakeStore) List(_ context.Context, prefix string, max int32) ([]s3store.Object, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lists++
	if f.listErrN > 0 {
		f.listErrN--
		return nil, errors.New("SlowDown: injected")
	}
	var out []s3store.Object
	for _, k := range f.sorted(prefix) {
		if int32(len(out)) >= max {
			break
		}
		out = append(out, s3store.Object{Key: k, Size: f.objs[k]})
	}
	return out, nil
}

func (f *fakeStore) ListAll(ctx context.Context, prefix string, fn func([]s3store.Object) error) error {
	f.mu.Lock()
	var out []s3store.Object
	for _, k := range f.sorted(prefix) {
		out = append(out, s3store.Object{Key: k, Size: f.objs[k]})
	}
	f.mu.Unlock()
	return fn(out)
}

func (f *fakeStore) Empty(ctx context.Context, prefix string) (bool, error) {
	objs, err := f.List(ctx, prefix, 1)
	return len(objs) == 0, err
}

func (f *fakeStore) Delete(_ context.Context, oid uint64, keys []string) ([]string, []s3store.KeyError, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := s3store.CheckKeys(oid, keys); err != nil { // same guard as the real store
		return nil, nil, err
	}
	if f.deleteErrN != 0 { // inject whole-request errors
		if f.deleteErrN > 0 {
			f.deleteErrN--
		}
		return nil, nil, errors.New("RequestTimeout: injected")
	}
	if len(keys) > 1000 {
		return nil, nil, errors.New("MalformedXML: more than 1000 keys")
	}
	if len(keys) > f.maxBatch {
		f.maxBatch = len(keys)
	}
	var deleted []string
	var failed []s3store.KeyError
	for _, k := range keys {
		f.tries[k]++
		if n := f.failKeys[k]; n > 0 {
			f.failKeys[k] = n - 1
			failed = append(failed, s3store.KeyError{Key: k, Code: "InternalError", Message: "injected"})
			continue
		}
		delete(f.objs, k)
		deleted = append(deleted, k)
	}
	return deleted, failed, nil
}

// fakeChain reports every known object as SEALED, version 0, with the payload
// from payload (missing means 0) — the same as sealed() and fakeProgress claim.
type fakeChain struct {
	bucketOf map[uint64]string
	payload  map[uint64]uint64
}

func (c fakeChain) HeadObject(_ context.Context, oid uint64) (ObjectHead, error) {
	b, ok := c.bucketOf[oid]
	if !ok {
		return ObjectHead{}, errors.New("not found")
	}
	return ObjectHead{ID: oid, Bucket: b, Status: "SEALED", PayloadSize: c.payload[oid]}, nil
}

// sealed is the scan snapshot of an object fakeChain knows, payload 0.
func sealed(oid uint64) Claimed { return Claimed{OID: oid, Status: "SEALED"} }

type fakeMeta struct {
	mu   sync.Mutex
	rows map[uint64]bool
}

func (m *fakeMeta) Delete(_ context.Context, oid uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.rows, oid)
	return nil
}
func (m *fakeMeta) Exists(_ context.Context, oid uint64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rows[oid], nil
}

type fakeProgress struct {
	mu       sync.Mutex
	pending  []uint64
	payload  map[uint64]uint64 // oid -> payload_size; missing means 0
	done     map[uint64][2]uint64
	checks   map[uint64]SizeCheck
	failed   map[uint64]string
	doneErrN map[uint64]int // oid -> remaining Done write errors to inject (-1 = forever)
}

func (p *fakeProgress) Claim(_ context.Context, after uint64, limit int, retryFailed bool) ([]Claimed, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []Claimed
	for _, id := range p.pending {
		if id > after {
			out = append(out, Claimed{OID: id, PayloadSize: p.payload[id], Status: "SEALED"})
		}
		if len(out) == limit {
			break
		}
	}
	return out, nil
}
func (p *fakeProgress) Done(ctx context.Context, oid, k, b uint64, check SizeCheck) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if n := p.doneErrN[oid]; n != 0 {
		if n > 0 {
			p.doneErrN[oid] = n - 1
		}
		return errors.New("Error 2013: Lost connection to MySQL server (injected)")
	}
	p.done[oid] = [2]uint64{k, b}
	if p.checks != nil {
		p.checks[oid] = check
	}
	return nil
}
func (p *fakeProgress) Fail(_ context.Context, oid, k, b uint64, reason string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failed[oid] = reason
	return nil
}

func newRunner(st *fakeStore, ch fakeChain, meta *fakeMeta) *Runner {
	return &Runner{Store: st, Chain: ch, Meta: meta, Opt: Options{Bucket: "limewire", MaxRetry: 3, QPS: 1e6}}
}

// ---- the L0 cases ------------------------------------------------------

// Nine keys: four belong to oid 42268 (incl. a _v2 version and an EC piece),
// five are neighbours that must survive.
func TestPrefixBoundary(t *testing.T) {
	st := newFakeStore(
		"s42268_s0", "s42268_s1", "s42268_s0_v2", "e42268_s0_p3",
		"s422680_s0", "e422680_s0_p3", "s4226_s0", "s99999_s0", "e99999_s0_p1",
	)
	meta := &fakeMeta{rows: map[uint64]bool{42268: true, 99999: true}}
	r := newRunner(st, fakeChain{bucketOf: map[uint64]string{42268: "limewire", 99999: "other-bucket"}}, meta)
	r.lim = nil

	res := r.ProcessOne(context.Background(), sealed(42268))
	if res.Err != nil {
		t.Fatalf("unexpected error: %v", res.Err)
	}
	if res.Keys != 4 || res.Bytes != 40 {
		t.Fatalf("keys=%d bytes=%d", res.Keys, res.Bytes)
	}
	left := st.sorted("")
	want := []string{"e422680_s0_p3", "e99999_s0_p1", "s4226_s0", "s422680_s0", "s99999_s0"}
	sort.Strings(want)
	if fmt.Sprint(left) != fmt.Sprint(want) {
		t.Fatalf("survivors %v, want %v", left, want)
	}
	if meta.rows[42268] || !meta.rows[99999] {
		t.Fatalf("metadata rows: %v", meta.rows)
	}

	// 99999 is in the list but the chain says another bucket → nothing deleted, failed
	res = r.ProcessOne(context.Background(), sealed(99999))
	if res.Err == nil || !strings.Contains(res.Err.Error(), "not in bucket") || res.Keys != 0 {
		t.Fatalf("expected not-in-bucket failure, got %+v", res)
	}
	if len(st.sorted("s99999_")) != 1 {
		t.Fatal("99999 keys must survive")
	}
}

// 2500 keys under one prefix: three rounds of ≤1000, never a >1000 request.
func Test2500KeysBatched(t *testing.T) {
	var keys []string
	for i := 0; i < 2500; i++ {
		keys = append(keys, fmt.Sprintf("s7_s%d", i))
	}
	st := newFakeStore(keys...)
	r := newRunner(st, fakeChain{bucketOf: map[uint64]string{7: "limewire"}}, &fakeMeta{rows: map[uint64]bool{}})
	r.lim = nil
	res := r.ProcessOne(context.Background(), sealed(7))
	if res.Err != nil || res.Keys != 2500 {
		t.Fatalf("res=%+v", res)
	}
	if st.maxBatch > 1000 {
		t.Fatalf("batch of %d exceeded 1000", st.maxBatch)
	}
	if len(st.objs) != 0 {
		t.Fatalf("%d keys left", len(st.objs))
	}
}

// Partial failure: 10 of 1000 keys fail once, then succeed on the next round.
func TestPartialFailureRetried(t *testing.T) {
	var keys []string
	for i := 0; i < 1000; i++ {
		keys = append(keys, fmt.Sprintf("s8_s%d", i))
	}
	st := newFakeStore(keys...)
	for i := 0; i < 10; i++ {
		st.failKeys[fmt.Sprintf("s8_s%d", i)] = 1
	}
	r := newRunner(st, fakeChain{bucketOf: map[uint64]string{8: "limewire"}}, &fakeMeta{rows: map[uint64]bool{}})
	r.lim = nil
	res := r.ProcessOne(context.Background(), sealed(8))
	if res.Err != nil || res.Keys != 1000 || len(st.objs) != 0 {
		t.Fatalf("res=%+v left=%d", res, len(st.objs))
	}
}

// A key that never stops failing: the object is failed once that key used up its
// 1+MaxRetry attempts, and what did get deleted is still counted.
func TestPersistentFailureGivesUp(t *testing.T) {
	st := newFakeStore("s9_s0", "s9_s1", "s9_s2")
	st.failKeys["s9_s2"] = 1000
	r := newRunner(st, fakeChain{bucketOf: map[uint64]string{9: "limewire"}}, &fakeMeta{rows: map[uint64]bool{}})
	r.lim = nil
	res := r.ProcessOne(context.Background(), sealed(9))
	if res.Err == nil || !strings.Contains(res.Err.Error(), "still failing after 3 retries") {
		t.Fatalf("expected give-up error, got %+v", res)
	}
	if st.tries["s9_s2"] != 4 {
		t.Fatalf("stuck key tried %d times, want 1+MaxRetry=4", st.tries["s9_s2"])
	}
	if res.Keys != 2 {
		t.Fatalf("partial progress not counted: %+v", res)
	}
	if len(st.sorted("s9_")) != 1 {
		t.Fatal("the failing key must remain for the next attempt")
	}
}

// Retries are per piece: 2500 keys, one never deletes. Every other key is removed,
// the stuck one is tried exactly 1+MaxRetry times even though other keys kept
// succeeding in the same rounds.
func TestPerPieceRetryCount(t *testing.T) {
	var keys []string
	for i := 0; i < 2500; i++ {
		keys = append(keys, fmt.Sprintf("s13_s%d", i))
	}
	st := newFakeStore(keys...)
	st.failKeys["s13_s0"] = 1000 // sorts first, so it is in every listing window
	r := newRunner(st, fakeChain{bucketOf: map[uint64]string{13: "limewire"}}, &fakeMeta{rows: map[uint64]bool{}})
	r.lim = nil
	res := r.ProcessOne(context.Background(), sealed(13))
	if res.Err == nil || res.Keys != 2499 {
		t.Fatalf("res=%+v", res)
	}
	if st.tries["s13_s0"] != 1+r.Opt.MaxRetry {
		t.Fatalf("stuck key tried %d times, want %d", st.tries["s13_s0"], 1+r.Opt.MaxRetry)
	}
	if left := st.sorted("s13_"); len(left) != 1 || left[0] != "s13_s0" {
		t.Fatalf("left=%v", left)
	}
}

// List errors have their own counter: a few are retried, then the object completes.
func TestListErrorRetried(t *testing.T) {
	st := newFakeStore("s14_s0")
	st.listErrN = 3 // MaxRetry is 3, so exactly at the limit still recovers
	r := newRunner(st, fakeChain{bucketOf: map[uint64]string{14: "limewire"}}, &fakeMeta{rows: map[uint64]bool{}})
	r.lim = nil
	res := r.ProcessOne(context.Background(), sealed(14))
	if res.Err != nil || res.Keys != 1 {
		t.Fatalf("res=%+v", res)
	}
	st.objs["s14_s1"] = 10
	st.listErrN = 4 // one more than MaxRetry → give up
	res = r.ProcessOne(context.Background(), sealed(14))
	if res.Err == nil || !strings.Contains(res.Err.Error(), "list") {
		t.Fatalf("expected list give-up, got %+v", res)
	}
}

// Transient whole-request delete errors are retried (not an immediate fail) and
// the object still completes once they stop.
func TestRequestErrorRetriedThenSucceeds(t *testing.T) {
	st := newFakeStore("s11_s0", "s11_s1")
	st.deleteErrN = 3 // first 3 delete requests error out, then succeed
	r := newRunner(st, fakeChain{bucketOf: map[uint64]string{11: "limewire"}}, &fakeMeta{rows: map[uint64]bool{}})
	r.lim = nil
	res := r.ProcessOne(context.Background(), sealed(11))
	if res.Err != nil || res.Keys != 2 || len(st.objs) != 0 {
		t.Fatalf("res=%+v left=%d", res, len(st.objs))
	}
}

// A persistent whole-request delete error gives up after MaxRetry, as FAILED.
func TestRequestErrorGivesUp(t *testing.T) {
	st := newFakeStore("s12_s0")
	st.deleteErrN = -1 // always error
	r := newRunner(st, fakeChain{bucketOf: map[uint64]string{12: "limewire"}}, &fakeMeta{rows: map[uint64]bool{}})
	r.lim = nil
	res := r.ProcessOne(context.Background(), sealed(12))
	if res.Err == nil || !strings.Contains(res.Err.Error(), "after") {
		t.Fatalf("expected give-up after retries, got %+v", res)
	}
	if len(st.sorted("s12_")) != 1 {
		t.Fatal("key must remain after give-up")
	}
}

// Resume: an object whose first attempt failed half-way is finished by re-listing.
func TestResumeAfterInterruption(t *testing.T) {
	st := newFakeStore("s5_s0", "s5_s1", "s5_s2", "s5_s3")
	st.failKeys["s5_s3"] = 1000
	meta := &fakeMeta{rows: map[uint64]bool{5: true}}
	r := newRunner(st, fakeChain{bucketOf: map[uint64]string{5: "limewire"}}, meta)
	r.lim = nil
	first := r.ProcessOne(context.Background(), sealed(5))
	if first.Err == nil || first.Keys != 3 {
		t.Fatalf("first attempt: %+v", first)
	}
	if !meta.rows[5] {
		t.Fatal("metadata must not be cleared while pieces remain")
	}
	st.failKeys["s5_s3"] = 0 // the cause is fixed
	second := r.ProcessOne(context.Background(), sealed(5))
	if second.Err != nil || second.Keys != 1 {
		t.Fatalf("second attempt: %+v", second)
	}
	if meta.rows[5] {
		t.Fatal("metadata should now be gone")
	}
}

// Dry-run counts everything and deletes nothing.
func TestDryRun(t *testing.T) {
	st := newFakeStore("s6_s0", "e6_s0_p1", "s60_s0")
	r := newRunner(st, fakeChain{bucketOf: map[uint64]string{6: "limewire"}}, &fakeMeta{rows: map[uint64]bool{6: true}})
	r.Opt.DryRun = true
	r.lim = nil
	res := r.ProcessOne(context.Background(), sealed(6))
	if res.Err != nil || res.Keys != 2 || len(st.objs) != 3 {
		t.Fatalf("res=%+v left=%d", res, len(st.objs))
	}
}

// Run drives Claim → workers → Done/Fail end to end with the fakes.
func TestRunEndToEnd(t *testing.T) {
	st := newFakeStore("s1_s0", "s2_s0", "e2_s0_p0", "s3_s0")
	pg := &fakeProgress{pending: []uint64{1, 2, 3}, done: map[uint64][2]uint64{}, failed: map[uint64]string{}}
	// The chain does not know oid 3: an ordinary failure stops the run.
	r := newRunner(st, fakeChain{bucketOf: map[uint64]string{1: "limewire", 2: "limewire"}}, &fakeMeta{rows: map[uint64]bool{}})
	r.Progress = pg
	r.Opt.Concurrency = 1
	err := r.Run(context.Background(), 3)
	if err == nil || !strings.Contains(err.Error(), "1 objects still FAILED") || strings.Contains(err.Error(), "HALTED") {
		t.Fatalf("want still-FAILED error without a halt, got %v", err)
	}
	if len(pg.done) != 2 || pg.done[2][0] != 2 || len(pg.failed) != 1 {
		t.Fatalf("done=%v failed=%v", pg.done, pg.failed)
	}
	if len(st.sorted("s3_")) != 1 {
		t.Fatal("an object the chain cannot confirm must be untouched")
	}
}

// countingChain counts lookups per oid on top of fakeChain.
type countingChain struct {
	fakeChain
	mu    sync.Mutex
	calls map[uint64]int
}

func (c *countingChain) HeadObject(ctx context.Context, oid uint64) (ObjectHead, error) {
	c.mu.Lock()
	c.calls[oid]++
	c.mu.Unlock()
	return c.fakeChain.HeadObject(ctx, oid)
}

// A chain identity mismatch halts the whole run: the offending object is FAILED
// and never retried, later objects are not touched, and the run fails.
func TestRunHaltsOnIdentityMismatch(t *testing.T) {
	st := newFakeStore("s1_s0", "s2_s0", "s3_s0")
	pg := &fakeProgress{pending: []uint64{1, 2, 3}, done: map[uint64][2]uint64{}, failed: map[uint64]string{}}
	ch := &countingChain{fakeChain: fakeChain{bucketOf: map[uint64]string{1: "limewire-bak", 2: "limewire", 3: "limewire"}}, calls: map[uint64]int{}}
	r := &Runner{Store: st, Chain: ch, Meta: &fakeMeta{rows: map[uint64]bool{}}, Progress: pg,
		Opt: Options{Bucket: "limewire", MaxRetry: 3, QPS: 1e6, Concurrency: 1}}
	err := r.Run(context.Background(), 3)
	if err == nil || !strings.Contains(err.Error(), "HALTED") || !strings.Contains(err.Error(), "oid 1") {
		t.Fatalf("want HALTED on oid 1, got %v", err)
	}
	if !strings.Contains(pg.failed[1], "safety violation") {
		t.Fatalf("oid 1 must be recorded FAILED as a safety violation: %v", pg.failed)
	}
	if ch.calls[1] != 1 {
		t.Fatalf("oid 1 looked up %d times; a safety violation must not be retried", ch.calls[1])
	}
	if len(pg.done) != 0 || len(st.objs) != 3 {
		t.Fatalf("nothing after the halt may be deleted: done=%v keys=%v", pg.done, st.objs)
	}
}

// Unknown keys under a prefix halt the run the same way.
func TestRunHaltsOnUnknownKey(t *testing.T) {
	st := newFakeStore("s1_backup", "s1_s0", "s2_s0")
	pg := &fakeProgress{pending: []uint64{1, 2}, done: map[uint64][2]uint64{}, failed: map[uint64]string{}}
	r := &Runner{Store: st, Chain: fakeChain{bucketOf: map[uint64]string{1: "limewire", 2: "limewire"}}, Meta: &fakeMeta{rows: map[uint64]bool{}},
		Progress: pg, Opt: Options{Bucket: "limewire", MaxRetry: 3, QPS: 1e6, Concurrency: 1}}
	err := r.Run(context.Background(), 2)
	if err == nil || !strings.Contains(err.Error(), "HALTED") {
		t.Fatalf("want HALTED, got %v", err)
	}
	if len(st.objs) != 3 || len(pg.done) != 0 {
		t.Fatalf("nothing may be deleted: keys=%v done=%v", st.objs, pg.done)
	}
}

// A dry-run reports every anomaly instead of halting at the first.
func TestDryRunDoesNotHalt(t *testing.T) {
	st := newFakeStore("s1_s0", "s2_s0")
	ch := &countingChain{fakeChain: fakeChain{bucketOf: map[uint64]string{1: "limewire-bak", 2: "limewire-bak"}}, calls: map[uint64]int{}}
	r := &Runner{Store: st, Chain: ch, Meta: &fakeMeta{rows: map[uint64]bool{}},
		Progress: &fakeProgress{pending: []uint64{1, 2}, done: map[uint64][2]uint64{}, failed: map[uint64]string{}},
		Opt:      Options{Bucket: "limewire", MaxRetry: 3, QPS: 1e6, Concurrency: 1, DryRun: true}}
	if err := r.Run(context.Background(), 2); err == nil {
		t.Fatal("dry-run with anomalies must return an error, while checking all objects")
	}
	if ch.calls[1] != 1 || ch.calls[2] != 1 {
		t.Fatalf("dry-run must look at every object: %v", ch.calls)
	}
}

// A piece exhausts its budget: stop without a second pass or touching later OIDs.
// A new invocation resumes the failed object and remaining work after repair.
func TestRunStopsAfterPieceRetriesAndResumesOnRerun(t *testing.T) {
	st := newFakeStore("s1_s0", "s2_s0", "s3_s0")
	st.failKeys["s2_s0"] = 2 // MaxRetry=1: fails both attempts
	pg := &fakeProgress{pending: []uint64{1, 2, 3}, done: map[uint64][2]uint64{}, failed: map[uint64]string{}}
	r := newRunner(st, fakeChain{bucketOf: map[uint64]string{1: "limewire", 2: "limewire", 3: "limewire"}}, &fakeMeta{rows: map[uint64]bool{}})
	r.Progress = pg
	r.Opt.MaxRetry = 1
	r.Opt.Concurrency = 1
	r.OnSafety = func(context.Context, uint64, error) error {
		t.Error("ordinary failure must not persist a safety hold")
		return nil
	}
	if err := r.Run(context.Background(), 3); err == nil || !strings.Contains(err.Error(), "1 objects still FAILED") {
		t.Fatalf("want still-FAILED error, got %v", err)
	}
	if _, ok := pg.failed[2]; !ok || len(pg.done) != 1 {
		t.Fatalf("only oid 1 done, oid 2 failed: done=%v failed=%v", pg.done, pg.failed)
	}
	if st.tries["s2_s0"] != 2 || st.tries["s3_s0"] != 0 {
		t.Fatalf("no second pass or later deletions: tries=%v", st.tries)
	}
	// Simulate the real progress adapter selecting failed and pending only.
	pg.pending = []uint64{2, 3}
	next := newRunner(st, fakeChain{bucketOf: map[uint64]string{2: "limewire", 3: "limewire"}}, &fakeMeta{rows: map[uint64]bool{}})
	next.Progress = pg
	if err := next.Run(context.Background(), 2); err != nil {
		t.Fatalf("rerun should resume after the cause is fixed: %v", err)
	}
	if len(pg.done) != 3 || len(st.objs) != 0 || st.tries["s1_s0"] != 1 {
		t.Fatalf("rerun lost progress: done=%v keys=%v tries=%v", pg.done, st.objs, st.tries)
	}
}

// A progress write failure stops now; a manual rerun reconciles and records it.
func TestRunDoneWriteFailureRecoveredOnManualRerun(t *testing.T) {
	st := newFakeStore("s1_s0", "s2_s0")
	pg := &fakeProgress{pending: []uint64{1, 2}, done: map[uint64][2]uint64{}, failed: map[uint64]string{},
		doneErrN: map[uint64]int{2: 1}}
	r := newRunner(st, fakeChain{bucketOf: map[uint64]string{1: "limewire", 2: "limewire"}}, &fakeMeta{rows: map[uint64]bool{}})
	r.Progress = pg
	r.Opt.Concurrency = 1
	if err := r.Run(context.Background(), 2); err == nil || !strings.Contains(err.Error(), "completion NOT recorded") {
		t.Fatalf("run should stop on lost progress: %v", err)
	}
	if _, ok := pg.done[2]; ok {
		t.Fatal("must not automatically retry the failed write")
	}
	pg.pending = []uint64{2}
	next := newRunner(st, fakeChain{bucketOf: map[uint64]string{2: "limewire"}}, &fakeMeta{rows: map[uint64]bool{}})
	next.Progress = pg
	if err := next.Run(context.Background(), 1); err != nil {
		t.Fatalf("manual rerun should reconcile progress: %v", err)
	}
	if _, ok := pg.done[2]; !ok || len(st.objs) != 0 {
		t.Fatalf("done=%v left=%v", pg.done, st.objs)
	}
}

// A completion the progress table never accepts must fail the run, even though
// the pieces were deleted and no object failed.
func TestRunDoneWriteFailureFailsRun(t *testing.T) {
	st := newFakeStore("s1_s0", "s2_s0")
	pg := &fakeProgress{pending: []uint64{1, 2}, done: map[uint64][2]uint64{}, failed: map[uint64]string{},
		doneErrN: map[uint64]int{2: -1}}
	r := newRunner(st, fakeChain{bucketOf: map[uint64]string{1: "limewire", 2: "limewire"}}, &fakeMeta{rows: map[uint64]bool{}})
	r.Progress = pg
	err := r.Run(context.Background(), 2)
	if err == nil || !strings.Contains(err.Error(), "1 results not recorded") {
		t.Fatalf("want not-recorded error, got %v", err)
	}
	if _, ok := pg.done[2]; ok {
		t.Fatal("oid 2 must not be recorded")
	}
	if len(pg.failed) != 0 {
		t.Fatalf("a write failure is not an object failure: %v", pg.failed)
	}
}

// A key under the object's prefix in a layout the SP never writes is not guessed
// at: nothing from that listing is deleted, the object fails, metadata stays.
func TestUnknownKeyLayoutLeftForInspection(t *testing.T) {
	st := newFakeStore("s5_backup", "s5_s0", "s5_s1")
	meta := &fakeMeta{rows: map[uint64]bool{5: true}}
	r := newRunner(st, fakeChain{bucketOf: map[uint64]string{5: "limewire"}}, meta)
	r.lim = nil
	res := r.ProcessOne(context.Background(), sealed(5))
	if !errors.Is(res.Err, ErrSafety) || !strings.Contains(res.Err.Error(), "not well-formed pieces") || res.Keys != 0 {
		t.Fatalf("want unknown-layout safety failure with nothing deleted, got %+v", res)
	}
	if len(st.sorted("s5_")) != 3 || !meta.rows[5] {
		t.Fatalf("prefix and metadata must be left as found: keys=%v meta=%v", st.sorted("s5_"), meta.rows)
	}

	// dry-run reports it the same way
	r.Opt.DryRun = true
	if res := r.ProcessOne(context.Background(), sealed(5)); res.Err == nil || !strings.Contains(res.Err.Error(), "not well-formed pieces") {
		t.Fatalf("dry-run must flag the unknown key, got %+v", res)
	}
}

// The chain must confirm the full identity recorded at scan time; any difference
// fails the object before a single key is touched.
func TestChainIdentityMismatchDeletesNothing(t *testing.T) {
	cases := map[string]struct {
		head ObjectHead
		want string
	}{
		"other id":        {ObjectHead{ID: 77, Bucket: "limewire", Status: "SEALED", PayloadSize: 10}, "object id 77"},
		"other bucket":    {ObjectHead{ID: 7, Bucket: "limewire-bak", Status: "SEALED", PayloadSize: 10}, "not in bucket"},
		"status changed":  {ObjectHead{ID: 7, Bucket: "limewire", Status: "DISCONTINUED", PayloadSize: 10}, "status SEALED at scan"},
		"version bumped":  {ObjectHead{ID: 7, Bucket: "limewire", Status: "SEALED", Version: 1, PayloadSize: 10}, "version 0 at scan"},
		"payload differs": {ObjectHead{ID: 7, Bucket: "limewire", Status: "SEALED", PayloadSize: 11}, "payload 10 at scan"},
	}
	for name, c := range cases {
		st := newFakeStore("s7_s0")
		meta := &fakeMeta{rows: map[uint64]bool{7: true}}
		r := &Runner{Store: st, Chain: fixedChain{c.head}, Meta: meta, Opt: Options{Bucket: "limewire", MaxRetry: 3}}
		res := r.ProcessOne(context.Background(), Claimed{OID: 7, Status: "SEALED", PayloadSize: 10})
		if !errors.Is(res.Err, ErrSafety) || !strings.Contains(res.Err.Error(), c.want) || !strings.Contains(res.Err.Error(), "nothing deleted") {
			t.Errorf("%s: got %v", name, res.Err)
		}
		if len(st.objs) != 1 || !meta.rows[7] || st.lists != 0 {
			t.Errorf("%s: storage or metadata touched (keys=%v meta=%v lists=%d)", name, st.objs, meta.rows, st.lists)
		}
	}

	// a failed lookup is a failure too, never "trust the snapshot and delete"
	st := newFakeStore("s7_s0")
	r := &Runner{Store: st, Chain: fakeChain{}, Meta: &fakeMeta{rows: map[uint64]bool{}}, Opt: Options{Bucket: "limewire", MaxRetry: 3}}
	res := r.ProcessOne(context.Background(), sealed(7))
	if res.Err == nil || len(st.objs) != 1 {
		t.Fatalf("lookup failure must delete nothing: %+v keys=%v", res, st.objs)
	}
	if errors.Is(res.Err, ErrSafety) {
		t.Fatalf("a lookup failure is an ordinary (retryable) failure, not a safety violation: %v", res.Err)
	}
}

type fixedChain struct{ h ObjectHead }

func (c fixedChain) HeadObject(context.Context, uint64) (ObjectHead, error) { return c.h, nil }

// cancelChain interrupts the run while the chain check of oid at is in flight.
type cancelChain struct {
	cancel context.CancelFunc
	at     uint64
}

func (c cancelChain) HeadObject(ctx context.Context, oid uint64) (ObjectHead, error) {
	if oid == c.at {
		c.cancel()
		return ObjectHead{}, ctx.Err()
	}
	return ObjectHead{ID: oid, Bucket: "limewire", Status: "SEALED"}, nil
}

// An interrupted run fails; the object cut short and the ones never started have
// no result, so a plain rerun (not --retry-failed) picks them up.
func TestRunInterrupted(t *testing.T) {
	st := newFakeStore("s1_s0", "s2_s0", "s3_s0")
	pg := &fakeProgress{pending: []uint64{1, 2, 3}, done: map[uint64][2]uint64{}, failed: map[uint64]string{}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &Runner{Store: st, Chain: cancelChain{cancel: cancel, at: 2}, Meta: &fakeMeta{rows: map[uint64]bool{}},
		Progress: pg, Opt: Options{Bucket: "limewire", MaxRetry: 3, QPS: 1e6, Concurrency: 1}}
	err := r.Run(ctx, 3)
	if err == nil || !strings.Contains(err.Error(), "interrupted") {
		t.Fatalf("want interrupted error, got %v", err)
	}
	if _, ok := pg.done[1]; !ok || len(pg.done) != 1 {
		t.Fatalf("only oid 1 should be done: %v", pg.done)
	}
	if len(pg.failed) != 0 {
		t.Fatalf("interrupted objects must not be recorded as FAILED: %v", pg.failed)
	}
	if len(st.sorted("s2_")) != 1 || len(st.sorted("s3_")) != 1 {
		t.Fatal("objects 2 and 3 must be untouched")
	}
}
