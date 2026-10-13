package progress

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
)

// Explicit opt-in. Requires an EMPTY, disposable database with this prefix.
// Leaves fixture tables/data for inspection; never touches S3, BSDB or SpDB.
func TestMySQLSubtaskLifecycle(t *testing.T) {
	dsn := os.Getenv("LIMEWIRE_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set LIMEWIRE_TEST_MYSQL_DSN to an empty disposable MySQL database")
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(cfg.DBName, "limewire_purge_test_") {
		t.Fatal("test DB name must start limewire_purge_test_")
	}
	p, err := Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx := context.Background()
	var n int
	if err := p.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE()`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("requires empty disposable database: tables=%d err=%v", n, err)
	}
	l, err := p.Acquire(ctx, "integration test")
	if err != nil {
		t.Fatal(err)
	}
	ctx = l.Context()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(p.EnsureSchema(ctx))
	must(p.Bind(ctx, Binding{ChainID: "test", SPOperator: "sp0", Storage: "test/sp0", SpDB: "test/spdb", Bucket: "test", BucketID: 1}))
	must(p.BeginScan(ctx, 0))
	if err := p.FinishScan(ctx, 0); !errors.Is(err, ErrEmptyScan) {
		t.Fatalf("empty scan accepted: %v", err)
	}
	emptyTask, _, err := p.Task(ctx)
	must(err)
	if emptyTask.ScanStatus == ScanComplete || emptyTask.ScanFinishedAt.Valid {
		t.Fatal("empty scan was sealed", emptyTask)
	}
	must(p.BeginScan(ctx, 3))
	must(p.InsertScan(ctx, []ScanRow{{1, 60, "SEALED", 0}, {2, 60, "SEALED", 0}, {3, 60, "SEALED", 0}}))
	must(p.FinishScan(ctx, 3))
	if p.BeginScan(ctx, 3) == nil {
		t.Fatal("frozen scan accepted mutation")
	}
	s, err := p.CreateSubtask(ctx, 100, 10, false)
	must(err)
	if s.ObjectCount != 2 || s.LogicalBytes != 120 {
		t.Fatal(s)
	}
	selected, err := p.ResolveCurrentSubtask(ctx, s.ID)
	must(err)
	if selected.ID != s.ID {
		t.Fatal(selected)
	}
	again, err := p.CreateSubtask(ctx, 100, 10, false)
	must(err)
	if again.ID != s.ID {
		t.Fatal("repeat create allocated twice")
	}
	must(p.BeginPurge(ctx, s))
	claimed, err := p.ClaimSubtask(ctx, s.ID, 0, 10)
	must(err)
	if len(claimed) != 2 || claimed[1].ObjectID != 2 {
		t.Fatal(claimed)
	}
	must(p.DoneSubtask(ctx, s.ID, 1, 1, 60, "primary", nil))
	must(p.Fail(ctx, 2, 0, 0, "AccessDenied"))
	s, err = p.Subtask(ctx, s.ID)
	must(err)
	if _, err = p.BeginVerify(ctx, s, false); err == nil {
		t.Fatal("failed OID allowed verification")
	}
	oid := uint64(2)
	must(p.Hold(ctx, s.ID, &oid, "scope changed"))
	s, err = p.Subtask(ctx, s.ID)
	must(err)
	must(p.DeferObject(ctx, s, 2, "operator reviewed; investigate separately"))
	s, err = p.Subtask(ctx, s.ID)
	must(err)
	s, err = p.BeginVerify(ctx, s, false)
	must(err)
	must(p.RecordVerify(ctx, s, 1, "RESIDUE", "s1_"))
	if p.CompleteVerify(ctx, s) == nil {
		t.Fatal("residue passed")
	}
	must(p.BeginPurge(ctx, s))
	claimed, err = p.ClaimSubtask(ctx, s.ID, 0, 10)
	must(err)
	if len(claimed) != 1 || claimed[0].ObjectID != 1 {
		t.Fatal(claimed)
	}
	must(p.DoneSubtask(ctx, s.ID, 1, 1, 10, "primary", nil))
	s, err = p.Subtask(ctx, s.ID)
	must(err)
	s, err = p.BeginVerify(ctx, s, false)
	must(err)
	must(p.RecordVerify(ctx, s, 1, "PASS", ""))
	must(p.RecordVerify(ctx, s, 1, "PASS", ""))
	must(p.CompleteVerify(ctx, s))
	s, err = p.Subtask(ctx, s.ID)
	must(err)
	c, err := p.SubCounts(ctx, s.ID)
	must(err)
	if s.State(c, false) != "DONE_WITH_EXCEPTIONS" {
		t.Fatal(s, c)
	}
	next, err := p.CreateSubtask(ctx, 100, 10, false)
	must(err)
	if next.ObjectCount != 1 {
		t.Fatal(next)
	}
	if _, err := p.ResolveCurrentSubtask(ctx, s.ID); err == nil {
		t.Fatal("stale task ID selected a different current task")
	}
	must(p.BeginPurge(ctx, next))
	must(p.DoneSubtask(ctx, next.ID, 3, 0, 0, "none", nil))
	next, err = p.Subtask(ctx, next.ID)
	must(err)
	next, err = p.BeginVerify(ctx, next, false)
	must(err)
	must(p.RecordVerify(ctx, next, 3, "PASS", ""))
	must(p.CompleteVerify(ctx, next))
	retry, err := p.CreateSubtask(ctx, 100, 10, true)
	must(err)
	claimed, err = p.ClaimSubtask(ctx, retry.ID, 0, 10)
	must(err)
	if len(claimed) != 1 || claimed[0].ObjectID != 2 {
		t.Fatal(claimed)
	}
	// One explicit deferred retry keeps history; it does not expand the frozen list.
	must(p.BeginPurge(ctx, retry))
	must(p.DoneSubtask(ctx, retry.ID, 2, 0, 0, "none", nil))
	retry, err = p.Subtask(ctx, retry.ID)
	must(err)
	retry, err = p.BeginVerify(ctx, retry, false)
	must(err)
	must(p.RecordVerify(ctx, retry, 2, "PASS", ""))
	must(p.CompleteVerify(ctx, retry))
	unassigned, deferred, err := p.Outstanding(ctx)
	must(err)
	if unassigned != 0 || deferred != 0 {
		t.Fatal(unassigned, deferred)
	}
	if _, err := p.CreateSubtask(ctx, 100, 10, false); !errors.Is(err, ErrNoUnassigned) {
		t.Fatal(err)
	}
	if _, err := p.ResolveSubtask(ctx, 0); !errors.Is(err, ErrNoCurrentTask) {
		t.Fatal(err)
	}
	if _, err := p.ResolveCurrentSubtask(ctx, s.ID); !errors.Is(err, ErrNoCurrentTask) {
		t.Fatal("historical ID bypassed absence of current task:", err)
	}
	if _, err := p.Subtask(ctx, 999); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
}
