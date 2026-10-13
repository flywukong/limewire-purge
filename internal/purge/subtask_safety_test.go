package purge

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestSafetyLatchPersistedOnce(t *testing.T) {
	st := newFakeStore("s1_s0", "s2_s0")
	pg := &fakeProgress{pending: []uint64{1, 2}, done: map[uint64][2]uint64{}, failed: map[uint64]string{}}
	calls := 0
	r := &Runner{Store: st, Chain: fakeChain{bucketOf: map[uint64]string{1: "other", 2: "limewire"}}, Meta: &fakeMeta{rows: map[uint64]bool{}}, Progress: pg, Opt: Options{Bucket: "limewire", Concurrency: 1, QPS: 1e6},
		OnSafety: func(ctx context.Context, oid uint64, err error) error {
			calls++
			if ctx.Err() != nil || oid != 1 || !errors.Is(err, ErrSafety) {
				t.Errorf("invalid safety callback %d %v %v", oid, err, ctx.Err())
			}
			return errors.New("DB offline")
		}}
	err := r.Run(context.Background(), 2)
	if !errors.Is(err, ErrSafety) || !strings.Contains(err.Error(), "cannot persist SAFETY_HOLD") || calls != 1 {
		t.Fatalf("%v calls=%d", err, calls)
	}
	if len(st.objs) != 2 {
		t.Fatal("deleted after safety stop")
	}
}

func TestRequestGuardStopsBeforeDelete(t *testing.T) {
	st := newFakeStore("s1_s0")
	calls := 0
	r := &Runner{Store: st, Chain: fakeChain{bucketOf: map[uint64]string{1: "limewire"}}, Meta: &fakeMeta{rows: map[uint64]bool{1: true}}, Opt: Options{Bucket: "limewire"}, Check: func(context.Context) error {
		calls++
		if calls >= 3 {
			return errors.New("lock lost")
		}
		return nil
	}}
	res := r.ProcessOne(context.Background(), Claimed{OID: 1, Status: "SEALED"})
	if res.Err == nil || !strings.Contains(res.Err.Error(), "lock lost") || len(st.objs) != 1 {
		t.Fatalf("%+v keys=%v calls=%d", res, st.objs, calls)
	}
}
