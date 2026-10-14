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
	again, err = p.CreateSubtask(ctx, 100, 10, false)
	must(err)
	if again.ID != s.ID {
		t.Fatal("initial deletion failure did not block allocation", again)
	}
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
	if !s.PurgedAt.Valid || s.DoneAt.Valid {
		t.Fatal("deferring last failure must finish deletion, not verification", s)
	}
	// The next batch can be allocated BEFORE the previous batch is verified.
	next, err := p.CreateSubtask(ctx, 100, 10, false)
	must(err)
	if next.ID == s.ID || next.ObjectCount != 1 {
		t.Fatal("verification still blocks allocation", next)
	}
	v, err := p.ResolveVerifySubtask(ctx, 0)
	must(err)
	if v.ID != s.ID {
		t.Fatal("verify did not choose oldest pending batch", v)
	}
	cur, err := p.ResolvePurgeSubtask(ctx, 0)
	must(err)
	if cur.ID != next.ID {
		t.Fatal("purge did not choose deletion batch", cur)
	}
	s, err = p.BeginVerify(ctx, s, false)
	must(err)
	must(p.RecordVerify(ctx, s, 1, "RESIDUE", "s1_"))
	if p.CompleteVerify(ctx, s) == nil {
		t.Fatal("residue passed")
	}
	again, err = p.CreateSubtask(ctx, 100, 10, false)
	must(err)
	if again.ID != next.ID {
		t.Fatal("old residue blocked current task", again)
	}
	repair, err := p.ResolvePurgeSubtask(ctx, s.ID)
	must(err)
	if repair.ID != s.ID {
		t.Fatal(repair)
	}
	// Even an old task's safety hold must prevent all new deletion.
	must(p.Hold(ctx, s.ID, nil, "old manifest mismatch"))
	if _, err := p.CreateSubtask(ctx, 100, 10, false); err == nil {
		t.Fatal("hold bypassed by create")
	}
	if _, err := p.ResolvePurgeSubtask(ctx, next.ID); err == nil {
		t.Fatal("hold bypassed by purge")
	}
	held, err := p.Subtask(ctx, s.ID)
	must(err)
	must(p.ResumeSafety(ctx, held, "reviewed scope"))
	must(p.BeginPurge(ctx, s))
	must(p.Fail(ctx, 1, 0, 0, "repair failed"))
	again, err = p.CreateSubtask(ctx, 100, 10, false)
	must(err)
	if again.ID != next.ID {
		t.Fatal("repair failure blocked subsequent batch", again)
	}
	claimed, err = p.ClaimSubtask(ctx, s.ID, 0, 10)
	must(err)
	if len(claimed) != 1 || claimed[0].ObjectID != 1 {
		t.Fatal(claimed)
	}
	must(p.DoneSubtask(ctx, s.ID, 1, 1, 10, "primary", nil))
	must(p.CompletePurge(ctx, s))
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
	next, err = p.CreateSubtask(ctx, 100, 10, false)
	must(err)
	if next.ObjectCount != 1 {
		t.Fatal(next)
	}
	if _, err := p.ResolveCurrentSubtask(ctx, s.ID); err == nil {
		t.Fatal("stale task ID selected a different current task")
	}
	must(p.BeginPurge(ctx, next))
	must(p.DoneSubtask(ctx, next.ID, 3, 0, 0, "none", nil))
	// Simulate old schema then crash recovery: migration must restore a deletion
	// milestone without manufacturing a verification success.
	_, err = p.db.ExecContext(ctx, `ALTER TABLE purge_tasks DROP COLUMN purged_at`)
	must(err)
	_, err = p.db.ExecContext(ctx, `ALTER TABLE purge_tasks DROP COLUMN execution_connection_id`)
	must(err)
	must(p.EnsureSchema(ctx))
	next, err = p.Subtask(ctx, next.ID)
	must(err)
	if !next.PurgedAt.Valid || next.DoneAt.Valid {
		t.Fatal("upgrade lost milestone or invented verification", next)
	}
	// Pending verification also must not block allocation from the deferred pool.
	retry, err := p.CreateSubtask(ctx, 100, 10, true)
	must(err)
	if retry.ID == next.ID {
		t.Fatal("pending verify blocked deferred retry")
	}
	// Recheck completed history even with a new deletion task allocated.
	s, err = p.BeginVerify(ctx, s, true)
	must(err)
	must(p.RecordVerify(ctx, s, 1, "PASS", ""))
	must(p.CompleteVerify(ctx, s))
	next, err = p.BeginVerify(ctx, next, false)
	must(err)
	must(p.RecordVerify(ctx, next, 3, "PASS", ""))
	must(p.CompleteVerify(ctx, next))
	retry, err = p.CreateSubtask(ctx, 100, 10, true)
	must(err)
	claimed, err = p.ClaimSubtask(ctx, retry.ID, 0, 10)
	must(err)
	if len(claimed) != 1 || claimed[0].ObjectID != 2 {
		t.Fatal(claimed)
	}
	// One explicit deferred retry keeps history; it does not expand the frozen list.
	must(p.BeginPurge(ctx, retry))
	must(p.DoneSubtask(ctx, retry.ID, 2, 0, 0, "none", nil))
	must(p.CompletePurge(ctx, retry))
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
	if _, err := p.ResolveVerifySubtask(ctx, 0); !errors.Is(err, ErrNoVerifyTask) {
		t.Fatal(err)
	}
	if _, err := p.ResolveCurrentSubtask(ctx, s.ID); !errors.Is(err, ErrNoCurrentTask) {
		t.Fatal("historical ID bypassed absence of current task:", err)
	}
	if _, err := p.Subtask(ctx, 999); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
}
