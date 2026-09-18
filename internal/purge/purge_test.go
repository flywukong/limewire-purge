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
	vers       map[string][]fakeVersion // versioned view: key -> versions/markers
	failKeys   map[string]int           // key (or key@version) -> remaining failures to inject
	deleteErrN int                      // remaining whole-request delete errors to inject (-1 = forever)
	listErrN   int                      // remaining list errors to inject (-1 = forever)
	maxBatch   int                      // largest batch seen
	lists      int
}

func newFakeStore(keys ...string) *fakeStore {
	f := &fakeStore{objs: map[string]int64{}, failKeys: map[string]int{}}
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

// ---- versioned side of the fake: each key holds an ordered list of versions,
// and a delete marker is just another entry. Mirrors S3 semantics closely enough
// to exercise the version-aware path: only a delete by Key+VersionId removes an
// entry, and a prefix still holding a marker is not empty.

type fakeVersion struct {
	id     string
	size   int64
	marker bool
}

func (f *fakeStore) putVersion(key, id string, size int64, marker bool) {
	if f.vers == nil {
		f.vers = map[string][]fakeVersion{}
	}
	f.vers[key] = append(f.vers[key], fakeVersion{id: id, size: size, marker: marker})
}

func (f *fakeStore) sortedVersionKeys(prefix string) []string {
	var ks []string
	for k := range f.vers {
		if strings.HasPrefix(k, prefix) && len(f.vers[k]) > 0 {
			ks = append(ks, k)
		}
	}
	sort.Strings(ks)
	return ks
}

func (f *fakeStore) ListVersions(_ context.Context, prefix string, max int32) ([]s3store.Version, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErrN != 0 {
		if f.listErrN > 0 {
			f.listErrN--
		}
		return nil, errors.New("ListObjectVersions: injected")
	}
	var out []s3store.Version
	for _, k := range f.sortedVersionKeys(prefix) {
		for _, v := range f.vers[k] {
			if int32(len(out)) >= max {
				return out, nil
			}
			out = append(out, s3store.Version{Key: k, VersionID: v.id, Size: v.size, IsDeleteMarker: v.marker})
		}
	}
	return out, nil
}

func (f *fakeStore) ListAllVersions(ctx context.Context, prefix string, fn func([]s3store.Version) error) error {
	vs, err := f.ListVersions(ctx, prefix, 1<<30)
	if err != nil {
		return err
	}
	return fn(vs)
}

func (f *fakeStore) EmptyVersions(ctx context.Context, prefix string) (bool, error) {
	vs, err := f.ListVersions(ctx, prefix, 1)
	return len(vs) == 0, err
}

func (f *fakeStore) DeleteVersions(_ context.Context, vs []s3store.Version) ([]s3store.Version, []s3store.KeyError, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.deleteErrN != 0 {
		if f.deleteErrN > 0 {
			f.deleteErrN--
		}
		return nil, nil, errors.New("RequestTimeout: injected")
	}
	if len(vs) > 1000 {
		return nil, nil, errors.New("MalformedXML: more than 1000 keys")
	}
	if len(vs) > f.maxBatch {
		f.maxBatch = len(vs)
	}
	var deleted []s3store.Version
	var failed []s3store.KeyError
	for _, want := range vs {
		if n := f.failKeys[want.Key+"@"+want.VersionID]; n > 0 {
			f.failKeys[want.Key+"@"+want.VersionID] = n - 1
			failed = append(failed, s3store.KeyError{Key: want.Key + "@" + want.VersionID, Code: "InternalError", Message: "injected"})
			continue
		}
		kept := f.vers[want.Key][:0]
		for _, have := range f.vers[want.Key] {
			if have.id != want.VersionID {
				kept = append(kept, have)
			}
		}
		f.vers[want.Key] = kept
		deleted = append(deleted, want)
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
	done    map[uint64][2]uint64
	failed  map[uint64]string
}

func (p *fakeProgress) Claim(_ context.Context, after uint64, limit int, retryFailed bool) ([]uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []uint64
	for _, id := range p.pending {
		if id > after {
			out = append(out, id)
		}
		if len(out) == limit {
			break
		}
	}
	return out, nil
}
func (p *fakeProgress) Done(_ context.Context, oid, k, b uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.done[oid] = [2]uint64{k, b}
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

// Keys that never stop failing: the object is failed after MaxRetry stuck rounds,
// and what did get deleted is still counted.
func TestPersistentFailureGivesUp(t *testing.T) {
	st := newFakeStore("s9_s0", "s9_s1", "s9_s2")
	st.failKeys["s9_s2"] = 1000
	r := newRunner(st, fakeChain{map[uint64]string{9: "limewire"}}, &fakeMeta{rows: map[uint64]bool{}})
	r.lim = nil
	res := r.ProcessOne(context.Background(), 9)
	if res.Err == nil || !strings.Contains(res.Err.Error(), "keep failing") {
		t.Fatalf("expected give-up error, got %+v", res)
	}
	if res.Keys != 2 {
		t.Fatalf("partial progress not counted: %+v", res)
	}
	if len(st.sorted("s9_")) != 1 {
		t.Fatal("the failing key must remain for the next attempt")
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

// ---- versioned bucket (Suspended or Enabled) ----------------------------

// Everything under the prefix must go: the current null version, any historical
// version, and any leftover delete marker.
func TestVersionedDeletesAllVersionsAndMarkers(t *testing.T) {
	st := newFakeStore()
	st.putVersion("s20_s0", "null", 100, false) // written while suspended
	st.putVersion("s20_s1", "v1", 200, false)   // historical version
	st.putVersion("s20_s1", "v2", 300, false)   // current version
	st.putVersion("s20_s2", "dm1", 0, true)     // leftover delete marker
	st.putVersion("e20_s0_p1", "null", 400, false)
	st.putVersion("s200_s0", "null", 999, false) // neighbour oid — must survive
	meta := &fakeMeta{rows: map[uint64]bool{20: true}}
	r := newRunner(st, fakeChain{map[uint64]string{20: "limewire"}}, meta)
	r.Opt.Versioned = true
	r.lim = nil

	res := r.ProcessOne(context.Background(), 20)
	if res.Err != nil {
		t.Fatalf("unexpected error: %v", res.Err)
	}
	if res.Keys != 5 { // 3 versions + 1 marker under s20_, 1 under e20_
		t.Fatalf("keys=%d want 5", res.Keys)
	}
	if res.Bytes != 1000 { // 100+200+300+400, marker contributes 0
		t.Fatalf("bytes=%d want 1000", res.Bytes)
	}
	if len(st.vers["s200_s0"]) != 1 {
		t.Fatal("neighbour oid must survive")
	}
	for _, k := range []string{"s20_s0", "s20_s1", "s20_s2", "e20_s0_p1"} {
		if len(st.vers[k]) != 0 {
			t.Fatalf("%s still has %d entries", k, len(st.vers[k]))
		}
	}
	if meta.rows[20] {
		t.Fatal("metadata should be cleared")
	}
}

// A prefix left holding only a delete marker is NOT empty: the re-check must
// fail rather than declare the object done.
func TestVersionedMarkerOnlyIsResidue(t *testing.T) {
	st := newFakeStore()
	st.putVersion("s21_s0", "null", 10, false)
	st.putVersion("s21_s1", "dm", 0, true)
	st.failKeys["s21_s1@dm"] = 1000 // the marker can never be removed
	meta := &fakeMeta{rows: map[uint64]bool{21: true}}
	r := newRunner(st, fakeChain{map[uint64]string{21: "limewire"}}, meta)
	r.Opt.Versioned = true
	r.lim = nil

	res := r.ProcessOne(context.Background(), 21)
	if res.Err == nil {
		t.Fatal("expected failure while a delete marker remains")
	}
	if !meta.rows[21] {
		t.Fatal("metadata must not be cleared when residue remains")
	}
}

// Dry-run on a versioned bucket counts versions and markers without deleting.
func TestVersionedDryRun(t *testing.T) {
	st := newFakeStore()
	st.putVersion("s22_s0", "null", 50, false)
	st.putVersion("s22_s0", "old", 60, false)
	st.putVersion("s22_s1", "dm", 0, true)
	r := newRunner(st, fakeChain{map[uint64]string{22: "limewire"}}, &fakeMeta{rows: map[uint64]bool{}})
	r.Opt.Versioned = true
	r.Opt.DryRun = true
	r.lim = nil
	res := r.ProcessOne(context.Background(), 22)
	if res.Err != nil || res.Keys != 3 || res.Bytes != 110 {
		t.Fatalf("res=%+v", res)
	}
	if len(st.vers["s22_s0"]) != 2 || len(st.vers["s22_s1"]) != 1 {
		t.Fatal("dry-run must not delete")
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
