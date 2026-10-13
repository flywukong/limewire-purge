package verify

import (
	"context"
	"errors"
	"testing"
	"time"

	"limewire-purge/internal/progress"
)

type fakeChecks struct {
	calls    map[string]int
	fail     map[string]int
	residue  map[string]bool
	metadata bool
}

func (f *fakeChecks) Empty(ctx context.Context, prefix string) (bool, error) {
	f.calls[prefix]++
	if f.fail[prefix] > 0 {
		f.fail[prefix]--
		return false, errors.New("network error")
	}
	return !f.residue[prefix], nil
}
func (f *fakeChecks) Exists(context.Context, uint64) (bool, error) { return f.metadata, nil }

type result struct {
	gen            uint64
	status, detail string
}
type fakeProgress struct {
	ids     []uint64
	results map[uint64]result
	failOID uint64
}

func (f *fakeProgress) VerifyPending(ctx context.Context, s progress.Subtask, after uint64, limit int) (out []progress.VerifyRow, err error) {
	for _, id := range f.ids {
		r := f.results[id]
		if id > after && (r.gen != s.Generation || r.status != "PASS") {
			out = append(out, progress.VerifyRow{OID: id})
		}
	}
	return
}
func (f *fakeProgress) RecordVerify(ctx context.Context, s progress.Subtask, id uint64, status, detail string) error {
	if id == f.failOID {
		return errors.New("checkpoint unavailable")
	}
	f.results[id] = result{s.Generation, status, detail}
	return nil
}
func runner(f *fakeChecks, p *fakeProgress) *Runner {
	return &Runner{Store: f, Meta: f, Progress: p, Task: progress.Subtask{ID: 1, Generation: 1}, QPS: 1e9, MaxRetry: 2, Timeout: time.Second, Backoff: func(context.Context, int) error { return nil }}
}
func checks() *fakeChecks {
	return &fakeChecks{calls: map[string]int{}, fail: map[string]int{}, residue: map[string]bool{}}
}

func TestVerifyContinuesErrorsResumesAndInvalidates(t *testing.T) {
	f := checks()
	f.fail["s1_"] = 3
	f.residue["e2_"] = true
	p := &fakeProgress{ids: []uint64{1, 2, 3}, results: map[uint64]result{}}
	r := runner(f, p)
	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p.results[1].status != "ERROR" || p.results[2].status != "RESIDUE" || p.results[3].status != "PASS" {
		t.Fatal(p.results)
	}
	if f.calls["s1_"] != 3 {
		t.Fatalf("attempts %v", f.calls)
	}
	f.residue["e2_"] = false
	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.calls["s3_"] != 1 {
		t.Fatal("PASS was not skipped on resume")
	}
	if p.results[1].status != "PASS" || p.results[2].status != "PASS" {
		t.Fatal(p.results)
	}
	r.Task.Generation++
	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.calls["s3_"] != 2 {
		t.Fatal("new generation did not recheck PASS")
	}
}

func TestVerifyCheckpointFailureStops(t *testing.T) {
	f := checks()
	p := &fakeProgress{ids: []uint64{1, 2}, results: map[uint64]result{}, failOID: 1}
	if err := runner(f, p).Run(context.Background()); err == nil {
		t.Fatal("lost checkpoint was ignored")
	}
	if len(p.results) != 0 || f.calls["s2_"] != 0 {
		t.Fatal("continued after checkpoint failure")
	}
}

func TestVerifyLockLossStops(t *testing.T) {
	f := checks()
	p := &fakeProgress{ids: []uint64{1}, results: map[uint64]result{}}
	r := runner(f, p)
	r.Check = func(context.Context) error { return errors.New("lock lost") }
	if err := r.Run(context.Background()); err == nil {
		t.Fatal("lost lock was ignored")
	}
	if len(f.calls) != 0 || len(p.results) != 0 {
		t.Fatal("request/result after lock loss")
	}
}

func TestVerifyMetadataAndPartialErrorNotPass(t *testing.T) {
	f := checks()
	f.metadata = true
	f.fail["s1_"] = 10
	p := &fakeProgress{ids: []uint64{1}, results: map[uint64]result{}}
	if err := runner(f, p).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p.results[1].status != "RESIDUE" {
		t.Fatal(p.results)
	}
}
