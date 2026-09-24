package purge

import (
	"context"
	"testing"
)

// Numbers observed on the local 3+3 cluster (16 MiB segments): a secondary held
// exactly these byte counts, so the formula must reproduce them byte-for-byte.
func TestExpectedSecondaryBytesMatchesSP(t *testing.T) {
	const seg = 16 * 1024 * 1024
	cases := []struct {
		payload, want uint64
		chunks        uint32
	}{
		{5242880, 1747627, 3},   // 5 MiB, one segment
		{52428800, 17476269, 3}, // 50 MiB, 3 full segments + 2 MiB tail
		{16777216, 4194304, 4},  // mainnet 4+2, one full segment
		{0, 0, 3},               // empty object
		{1, 1, 4},               // tiny object rounds up to one byte per shard
	}
	for _, c := range cases {
		if got := ExpectedSecondaryBytes(c.payload, seg, c.chunks); got != c.want {
			t.Errorf("payload=%d chunks=%d: got %d want %d", c.payload, c.chunks, got, c.want)
		}
	}
	// the whole local secondary bucket: 50 × 5 MiB + 5 × 50 MiB
	var total uint64
	for i := 0; i < 50; i++ {
		total += ExpectedSecondaryBytes(5242880, seg, 3)
	}
	for i := 0; i < 5; i++ {
		total += ExpectedSecondaryBytes(52428800, seg, 3)
	}
	if total != 174762695 {
		t.Fatalf("bucket total %d, observed 174762695", total)
	}
}

func newCheckRunner(st *fakeStore, oid uint64) (*Runner, *fakeProgress) {
	pg := &fakeProgress{pending: []uint64{oid}, done: map[uint64][2]uint64{}, checks: map[uint64]SizeCheck{}, failed: map[uint64]string{}}
	r := newRunner(st, fakeChain{map[uint64]string{oid: "limewire"}}, &fakeMeta{rows: map[uint64]bool{}})
	r.Progress = pg
	r.Opt.Concurrency = 1
	r.Opt.DataChunks = 3
	r.Opt.SegmentSize = 16 * 1024 * 1024
	return r, pg
}

// A primary holding the full payload is inferred as primary and matches.
func TestSizeCheckPrimaryOK(t *testing.T) {
	st := newFakeStore()
	st.objs["s30_s0"] = 5242880
	r, pg := newCheckRunner(st, 30)
	pg.payload = map[uint64]uint64{30: 5242880}
	if err := r.Run(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	c := pg.checks[30]
	if c.Role != "primary" || !c.Known || c.Expected != 5242880 {
		t.Fatalf("check=%+v", c)
	}
	if r.mismatch.Load() != 0 {
		t.Fatal("unexpected mismatch")
	}
}

// A secondary holding EC shards is inferred as secondary; expected is the shard sum.
func TestSizeCheckSecondaryOK(t *testing.T) {
	st := newFakeStore()
	st.objs["e31_s0_p1"] = 5592406
	st.objs["e31_s1_p1"] = 5592406
	st.objs["e31_s2_p1"] = 5592406
	st.objs["e31_s3_p1"] = 699051
	r, pg := newCheckRunner(st, 31)
	pg.payload = map[uint64]uint64{31: 52428800}
	if err := r.Run(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	c := pg.checks[31]
	if c.Role != "secondary" || c.Expected != 17476269 {
		t.Fatalf("check=%+v", c)
	}
	if r.mismatch.Load() != 0 {
		t.Fatal("unexpected mismatch")
	}
}

// Deleted bytes that don't match the chain payload are flagged but the object is
// still recorded as done — the bytes are gone, blocking would change nothing.
func TestSizeCheckMismatchStillDone(t *testing.T) {
	st := newFakeStore()
	st.objs["s32_s0"] = 1024 // e.g. placeholder data, or the wrong bucket
	r, pg := newCheckRunner(st, 32)
	pg.payload = map[uint64]uint64{32: 5242880}
	if err := r.Run(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if _, ok := pg.done[32]; !ok {
		t.Fatal("mismatch must not prevent done")
	}
	if r.mismatch.Load() != 1 {
		t.Fatalf("mismatch=%d want 1", r.mismatch.Load())
	}
}

// Nothing on this SP (unsealed object, or already removed): nothing to compare.
func TestSizeCheckNoneSkipped(t *testing.T) {
	st := newFakeStore()
	r, pg := newCheckRunner(st, 33)
	pg.payload = map[uint64]uint64{33: 5242880}
	if err := r.Run(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	c := pg.checks[33]
	if c.Role != "none" || c.Known || r.mismatch.Load() != 0 {
		t.Fatalf("check=%+v mismatch=%d", c, r.mismatch.Load())
	}
}

// Data under both prefixes on one SP should never happen; flag it.
func TestSizeCheckMixedIsAnomaly(t *testing.T) {
	st := newFakeStore()
	st.objs["s34_s0"] = 10
	st.objs["e34_s0_p0"] = 10
	r, pg := newCheckRunner(st, 34)
	pg.payload = map[uint64]uint64{34: 10}
	if err := r.Run(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if pg.checks[34].Role != "mixed" || r.mismatch.Load() != 1 {
		t.Fatalf("check=%+v mismatch=%d", pg.checks[34], r.mismatch.Load())
	}
}
