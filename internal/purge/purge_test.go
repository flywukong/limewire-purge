package purge

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"limewire-purge/internal/s3store"
)

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

func (f *fakeStore) Delete(_ context.Context, keys []string) ([]string, []s3store.KeyError, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
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

type fakeChain struct{ bucketOf map[uint64]string }

func (c fakeChain) HeadObjectBucket(_ context.Context, oid uint64) (string, error) {
	b, ok := c.bucketOf[oid]
	if !ok {
		return "", errors.New("not found")
	}
	return b, nil
}

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
	mu      sync.Mutex
	pending []uint64
	payload map[uint64]uint64 // oid -> payload_size; missing means 0
	done    map[uint64][2]uint64
	checks  map[uint64]SizeCheck
	failed  map[uint64]string
}

func (p *fakeProgress) Claim(_ context.Context, after uint64, limit int, retryFailed bool) ([]Claimed, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []Claimed
	for _, id := range p.pending {
		if id > after {
			out = append(out, Claimed{OID: id, PayloadSize: p.payload[id]})
		}
		if len(out) == limit {
			break
		}
	}
	return out, nil
}
func (p *fakeProgress) Done(_ context.Context, oid, k, b uint64, check SizeCheck) error {
	p.mu.Lock()
	defer p.mu.Unlock()
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
	r := newRunner(st, fakeChain{map[uint64]string{42268: "limewire", 99999: "other-bucket"}}, meta)
	r.lim = nil

	res := r.ProcessOne(context.Background(), 42268)
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
	res = r.ProcessOne(context.Background(), 99999)
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
	r := newRunner(st, fakeChain{map[uint64]string{7: "limewire"}}, &fakeMeta{rows: map[uint64]bool{}})
	r.lim = nil
	res := r.ProcessOne(context.Background(), 7)
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
	r := newRunner(st, fakeChain{map[uint64]string{8: "limewire"}}, &fakeMeta{rows: map[uint64]bool{}})
	r.lim = nil
	res := r.ProcessOne(context.Background(), 8)
	if res.Err != nil || res.Keys != 1000 || len(st.objs) != 0 {
		t.Fatalf("res=%+v left=%d", res, len(st.objs))
	}
}

// A key that never stops failing: the object is failed once that key used up its
// 1+MaxRetry attempts, and what did get deleted is still counted.
func TestPersistentFailureGivesUp(t *testing.T) {
	st := newFakeStore("s9_s0", "s9_s1", "s9_s2")
	st.failKeys["s9_s2"] = 1000
	r := newRunner(st, fakeChain{map[uint64]string{9: "limewire"}}, &fakeMeta{rows: map[uint64]bool{}})
	r.lim = nil
	res := r.ProcessOne(context.Background(), 9)
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
		keys = append(keys, fmt.Sprintf("s13_s%04d", i))
	}
	st := newFakeStore(keys...)
	st.failKeys["s13_s0000"] = 1000
	r := newRunner(st, fakeChain{map[uint64]string{13: "limewire"}}, &fakeMeta{rows: map[uint64]bool{}})
	r.lim = nil
	res := r.ProcessOne(context.Background(), 13)
	if res.Err == nil || res.Keys != 2499 {
		t.Fatalf("res=%+v", res)
	}
	if st.tries["s13_s0000"] != 1+r.Opt.MaxRetry {
		t.Fatalf("stuck key tried %d times, want %d", st.tries["s13_s0000"], 1+r.Opt.MaxRetry)
	}
	if left := st.sorted("s13_"); len(left) != 1 || left[0] != "s13_s0000" {
		t.Fatalf("left=%v", left)
	}
}

// List errors have their own counter: a few are retried, then the object completes.
func TestListErrorRetried(t *testing.T) {
	st := newFakeStore("s14_s0")
	st.listErrN = 3 // MaxRetry is 3, so exactly at the limit still recovers
	r := newRunner(st, fakeChain{map[uint64]string{14: "limewire"}}, &fakeMeta{rows: map[uint64]bool{}})
	r.lim = nil
	res := r.ProcessOne(context.Background(), 14)
	if res.Err != nil || res.Keys != 1 {
		t.Fatalf("res=%+v", res)
	}
	st.objs["s14_s1"] = 10
	st.listErrN = 4 // one more than MaxRetry → give up
	res = r.ProcessOne(context.Background(), 14)
	if res.Err == nil || !strings.Contains(res.Err.Error(), "list") {
		t.Fatalf("expected list give-up, got %+v", res)
	}
}

// Transient whole-request delete errors are retried (not an immediate fail) and
// the object still completes once they stop.
func TestRequestErrorRetriedThenSucceeds(t *testing.T) {
	st := newFakeStore("s11_s0", "s11_s1")
	st.deleteErrN = 3 // first 3 delete requests error out, then succeed
	r := newRunner(st, fakeChain{map[uint64]string{11: "limewire"}}, &fakeMeta{rows: map[uint64]bool{}})
	r.lim = nil
	res := r.ProcessOne(context.Background(), 11)
	if res.Err != nil || res.Keys != 2 || len(st.objs) != 0 {
		t.Fatalf("res=%+v left=%d", res, len(st.objs))
	}
}

// A persistent whole-request delete error gives up after MaxRetry, as FAILED.
func TestRequestErrorGivesUp(t *testing.T) {
	st := newFakeStore("s12_s0")
	st.deleteErrN = -1 // always error
	r := newRunner(st, fakeChain{map[uint64]string{12: "limewire"}}, &fakeMeta{rows: map[uint64]bool{}})
	r.lim = nil
	res := r.ProcessOne(context.Background(), 12)
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
	r := newRunner(st, fakeChain{map[uint64]string{5: "limewire"}}, meta)
	r.lim = nil
	first := r.ProcessOne(context.Background(), 5)
	if first.Err == nil || first.Keys != 3 {
		t.Fatalf("first attempt: %+v", first)
	}
	if !meta.rows[5] {
		t.Fatal("metadata must not be cleared while pieces remain")
	}
	st.failKeys["s5_s3"] = 0 // the cause is fixed
	second := r.ProcessOne(context.Background(), 5)
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
	r := newRunner(st, fakeChain{map[uint64]string{6: "limewire"}}, &fakeMeta{rows: map[uint64]bool{6: true}})
	r.Opt.DryRun = true
	r.lim = nil
	res := r.ProcessOne(context.Background(), 6)
	if res.Err != nil || res.Keys != 2 || len(st.objs) != 3 {
		t.Fatalf("res=%+v left=%d", res, len(st.objs))
	}
}

// Run drives Claim → workers → Done/Fail end to end with the fakes.
func TestRunEndToEnd(t *testing.T) {
	st := newFakeStore("s1_s0", "s2_s0", "e2_s0_p0", "s3_s0")
	pg := &fakeProgress{pending: []uint64{1, 2, 3}, done: map[uint64][2]uint64{}, failed: map[uint64]string{}}
	r := newRunner(st, fakeChain{map[uint64]string{1: "limewire", 2: "limewire", 3: "someone-else"}}, &fakeMeta{rows: map[uint64]bool{}})
	r.Progress = pg
	r.Opt.Concurrency = 2
	if err := r.Run(context.Background(), 3); err != nil {
		t.Fatal(err)
	}
	if len(pg.done) != 2 || pg.done[2][0] != 2 || len(pg.failed) != 1 {
		t.Fatalf("done=%v failed=%v", pg.done, pg.failed)
	}
	if len(st.sorted("s3_")) != 1 {
		t.Fatal("object of another bucket must be untouched")
	}
}

// An object that fails the main pass is re-run once automatically; a transient
// fault recovers there, a permanent one stays FAILED.
func TestRunAutoRetryPass(t *testing.T) {
	st := newFakeStore("s1_s0", "s2_s0", "s3_s0")
	st.failKeys["s2_s0"] = 2    // MaxRetry=1: fails both main-pass attempts, then deletes
	st.failKeys["s3_s0"] = 1000 // never deletes
	pg := &fakeProgress{pending: []uint64{1, 2, 3}, done: map[uint64][2]uint64{}, failed: map[uint64]string{}}
	r := newRunner(st, fakeChain{map[uint64]string{1: "limewire", 2: "limewire", 3: "limewire"}}, &fakeMeta{rows: map[uint64]bool{}})
	r.Progress = pg
	r.Opt.MaxRetry = 1
	if err := r.Run(context.Background(), 3); err != nil {
		t.Fatal(err)
	}
	if _, ok := pg.done[2]; !ok {
		t.Fatalf("oid 2 should recover in the retry pass: done=%v failed=%v", pg.done, pg.failed)
	}
	if _, ok := pg.failed[3]; !ok {
		t.Fatalf("oid 3 should stay failed: %v", pg.failed)
	}
	if st.tries["s3_s0"] != 4 {
		t.Fatalf("permanent key tried %d times, want 2 per pass × 2 passes", st.tries["s3_s0"])
	}
	if r.failed.Load() != 1 {
		t.Fatalf("failed after retry = %d, want 1", r.failed.Load())
	}
}
