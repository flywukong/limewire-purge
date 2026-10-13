package progress

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"strings"
	"testing"
)

func TestSelectCurrentTaskExpectation(t *testing.T) {
	for _, expected := range []uint64{0, 7} {
		s, err := selectCurrent([]Subtask{{ID: 7}}, expected)
		if err != nil || s.ID != 7 {
			t.Fatalf("expected=%d: %v %v", expected, s, err)
		}
	}
	for _, expected := range []uint64{0, 7} {
		_, err := selectCurrent(nil, expected)
		if !errors.Is(err, ErrNoCurrentTask) || !strings.Contains(err.Error(), "task create") {
			t.Fatalf("no task: %v", err)
		}
	}
	_, err := selectCurrent([]Subtask{{ID: 8}}, 7)
	if err == nil || !strings.Contains(err.Error(), "--task-id 7") || !strings.Contains(err.Error(), "current task 8") {
		t.Fatalf("stale expectation: %v", err)
	}
	_, err = selectCurrent([]Subtask{{ID: 7}, {ID: 8}}, 7)
	if err == nil || !strings.Contains(err.Error(), "multiple unfinished") {
		t.Fatalf("inconsistent tasks: %v", err)
	}
}

func TestSelectCapacity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		sizes  []uint64
		target uint64
		max, n int
		total  uint64
	}{
		{"threshold", []uint64{40, 40, 40, 20}, 100, 10, 3, 120},
		{"tail", []uint64{20, 30}, 100, 10, 2, 50},
		{"oversize alone", []uint64{120, 1}, 100, 10, 1, 120},
		{"oversize next task", []uint64{20, 120, 1}, 100, 10, 1, 20},
		{"zero bounded", []uint64{0, 0, 0, 0}, 100, 2, 2, 0},
		{"count cap", []uint64{20, 30, 40}, 100, 2, 2, 50},
		{"empty", nil, 100, 10, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var rows []ScanRow
			for i, n := range tc.sizes {
				rows = append(rows, ScanRow{ObjectID: uint64(i + 1), PayloadSize: n})
			}
			got, total, err := SelectCapacity(rows, tc.target, tc.max)
			if err != nil || len(got) != tc.n || total != tc.total {
				t.Fatalf("got %v %d %v", got, total, err)
			}
		})
	}
	if _, _, err := SelectCapacity([]ScanRow{{PayloadSize: math.MaxUint64 - 1}, {PayloadSize: 2}}, math.MaxUint64, 10); err == nil {
		t.Fatal("overflow")
	}
}

func TestCapacityUnits(t *testing.T) {
	for s, want := range map[string]uint64{"500GiB": 500 << 30, "5TiB": 5 << 40, "1B": 1, "2KiB": 2048} {
		n, err := ParseCapacity(s)
		if err != nil || n != want {
			t.Fatalf("%s %d %v", s, n, err)
		}
	}
	for _, s := range []string{"500G", "0GiB", "-1B", "1.5GiB", "18446744073709551615TiB", ""} {
		if _, err := ParseCapacity(s); err == nil {
			t.Fatalf("accepted %q", s)
		}
	}
}

func TestTaskStates(t *testing.T) {
	for _, tc := range []struct {
		s      Subtask
		c      SubCounts
		locked bool
		want   string
	}{
		{Subtask{HoldReason: "scope mismatch"}, SubCounts{}, true, "SAFETY_HOLD"},
		{Subtask{DoneAt: sql.NullString{Valid: true}}, SubCounts{}, false, "DONE"},
		{Subtask{DoneAt: sql.NullString{Valid: true}}, SubCounts{Deferred: 1}, false, "DONE_WITH_EXCEPTIONS"},
		{Subtask{Phase: "READY"}, SubCounts{Pending: 1}, false, "READY"},
		{Subtask{Phase: "PURGE"}, SubCounts{Pending: 1}, false, "RETRY_PENDING"},
		{Subtask{Phase: "PURGE"}, SubCounts{Pending: 1}, true, "RUNNING"},
		{Subtask{Phase: "VERIFY"}, SubCounts{}, true, "VERIFYING"},
		{Subtask{Phase: "VERIFY"}, SubCounts{VerifyErrors: 1}, false, "VERIFY_FAILED"},
		{Subtask{Phase: "PURGE"}, SubCounts{Processed: 1}, false, "AWAITING_VERIFY"},
	} {
		if got := tc.s.State(tc.c, tc.locked); got != tc.want {
			t.Errorf("got %s want %s", got, tc.want)
		}
	}
}

func TestManifestBindsEverySnapshotField(t *testing.T) {
	original := ScanRow{ObjectID: 1, PayloadSize: 100, Status: "SEALED", Version: 1}
	for _, change := range []func(*ScanRow){func(r *ScanRow) { r.ObjectID++ }, func(r *ScanRow) { r.PayloadSize++ }, func(r *ScanRow) { r.Status = "CREATED" }, func(r *ScanRow) { r.Version++ }} {
		changed := original
		change(&changed)
		if digestRows([]ScanRow{original}) == digestRows([]ScanRow{changed}) {
			t.Fatal("digest did not bind changed field")
		}
	}
}

func TestGlobalHoldCannotBeDeferred(t *testing.T) {
	db := &DB{}
	err := db.DeferObject(context.Background(), Subtask{ID: 1, HoldReason: "manifest changed"}, 12, "skip")
	if err == nil || !strings.Contains(err.Error(), "cannot be bypassed") {
		t.Fatalf("%v", err)
	}
	err = db.DeferObject(context.Background(), Subtask{ID: 1, HoldReason: "wrong oid", HoldOID: sql.NullInt64{Int64: 13, Valid: true}}, 12, "skip")
	if err == nil || !strings.Contains(err.Error(), "cannot be bypassed") {
		t.Fatalf("%v", err)
	}
	if err := db.ResumeSafety(context.Background(), Subtask{ID: 1}, "retry"); err == nil {
		t.Fatal("ordinary failure should not need resume")
	}
}
