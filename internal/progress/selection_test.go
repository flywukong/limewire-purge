package progress

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

type taskRows struct{ values [][]driver.Value }

func (r *taskRows) Columns() []string { return make([]string, 12) }
func (r *taskRows) Close() error      { return nil }
func (r *taskRows) Next(v []driver.Value) error {
	if len(r.values) == 0 {
		return io.EOF
	}
	copy(v, r.values[0])
	r.values = r.values[1:]
	return nil
}

func taskValues(id int64, purged, done bool) []driver.Value {
	v := []driver.Value{id, int64(100), int64(1), int64(100), "digest", "PURGE", "", nil, int64(1), nil, nil, nil}
	if purged {
		v[10] = "2026-10-13 00:00:00"
	}
	if done {
		v[9] = "2026-10-13 00:01:00"
	}
	return v
}

// Run the actual SQL-selecting methods through a lightweight fixture driver.
// SQL syntax/transactions additionally have real-MySQL integration coverage.
func TestSeparateDeletionAndVerificationSelectors(t *testing.T) {
	ctx := context.Background()
	old := taskValues(1, true, false)
	current := taskValues(2, false, false)
	server := &lockServer{owners: map[string]int64{}}
	server.query = func(q string, args []driver.NamedValue) (driver.Rows, error) {
		switch {
		case strings.HasPrefix(q, "SELECT id FROM purge_tasks WHERE hold_reason"):
			return &oneRow{values: []driver.Value{nil}, done: true}, nil
		case strings.Contains(q, "purged_at IS NULL ORDER BY id LIMIT 2"):
			return &taskRows{[][]driver.Value{current}}, nil
		case strings.Contains(q, "purged_at IS NOT NULL ORDER BY id LIMIT 1"):
			return &taskRows{[][]driver.Value{old}}, nil
		case strings.HasSuffix(q, " WHERE id=?"):
			if args[0].Value == int64(1) {
				return &taskRows{[][]driver.Value{old}}, nil
			}
			return &taskRows{[][]driver.Value{current}}, nil
		default:
			return nil, fmt.Errorf("unexpected SQL: %s", q)
		}
	}
	p := newLockDB(server, "selection")
	defer p.Close()
	for _, tc := range []struct {
		name string
		run  func() (Subtask, error)
		want uint64
	}{
		{"default purge selects new batch", func() (Subtask, error) { return p.ResolvePurgeSubtask(ctx, 0) }, 2},
		{"default verify selects old batch", func() (Subtask, error) { return p.ResolveVerifySubtask(ctx, 0) }, 1},
		{"explicit repair selects old batch", func() (Subtask, error) { return p.ResolvePurgeSubtask(ctx, 1) }, 1},
		{"explicit current remains supported", func() (Subtask, error) { return p.ResolvePurgeSubtask(ctx, 2) }, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := tc.run()
			if err != nil || s.ID != tc.want {
				t.Fatalf("%+v %v", s, err)
			}
		})
	}
	old[9] = "2026-10-13 00:01:00"
	if _, err := p.ResolvePurgeSubtask(ctx, 1); err == nil {
		t.Fatal("verified history allowed deletion")
	}
	server.query = func(q string, args []driver.NamedValue) (driver.Rows, error) {
		if strings.HasPrefix(q, "SELECT id FROM purge_tasks WHERE hold_reason") {
			return &oneRow{values: []driver.Value{int64(1)}}, nil
		}
		return nil, fmt.Errorf("selection must stop on safety hold: %s", q)
	}
	if _, err := p.ResolvePurgeSubtask(ctx, 2); err == nil || !strings.Contains(err.Error(), "SAFETY_HOLD") {
		t.Fatal(err)
	}
}

func TestNoVerifyTaskDoesNotFallBackToDeletion(t *testing.T) {
	s := &lockServer{owners: map[string]int64{}, query: func(q string, _ []driver.NamedValue) (driver.Rows, error) {
		if !strings.Contains(q, "purged_at IS NOT NULL") {
			return nil, fmt.Errorf("wrong selector %s", q)
		}
		return &taskRows{}, nil
	}}
	p := newLockDB(s, "none")
	defer p.Close()
	if _, err := p.ResolveVerifySubtask(context.Background(), 0); !errors.Is(err, ErrNoVerifyTask) {
		t.Fatal(err)
	}
}

func TestOnlyExecutingTaskShowsRunning(t *testing.T) {
	owner := sql.NullInt64{Int64: 42, Valid: true}
	old := Subtask{Phase: "PURGE", PurgedAt: sql.NullString{Valid: true}, ExecutionConnectionID: sql.NullInt64{Int64: 41, Valid: true}}
	active := Subtask{Phase: "VERIFY", ExecutionConnectionID: owner}
	if old.LockedBy(owner) || old.State(SubCounts{Processed: 1}, old.LockedBy(owner)) != "AWAITING_VERIFY" {
		t.Fatal("old batch attributed current lock")
	}
	if active.State(SubCounts{}, active.LockedBy(owner)) != "VERIFYING" {
		t.Fatal("active verification not shown")
	}
	if active.LockedBy(sql.NullInt64{}) {
		t.Fatal("crashed process shown running")
	}
}
