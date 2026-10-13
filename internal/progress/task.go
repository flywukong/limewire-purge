package progress

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// Binding ties a progress database to one task: one SP clearing one bucket on
// one chain. Progress rows carry only object ids, and every SP holds the same
// ids, so without this an SP pointed at another SP's database would take that
// SP's DONE rows as its own and skip them.
type Binding struct {
	ChainID    string // SP config [Chain] ChainID
	SPOperator string // SP config [SpAccount] SpOperatorAddress
	Storage    string // physical bucket: endpoint (or AWS region) + bucket name
	SpDB       string // SpDB address/database
	Bucket     string
	BucketID   uint64
}

// Diff lists every field where now differs from the stored binding b.
func (b Binding) Diff(now Binding) []string {
	var out []string
	add := func(name, stored, cur string) {
		if stored != cur {
			out = append(out, fmt.Sprintf("%s: database has %q, this run has %q", name, stored, cur))
		}
	}
	add("chain id", b.ChainID, now.ChainID)
	add("SP operator", b.SPOperator, now.SPOperator)
	add("object storage", b.Storage, now.Storage)
	add("SpDB", b.SpDB, now.SpDB)
	add("bucket", b.Bucket, now.Bucket)
	add("bucket id", fmt.Sprint(b.BucketID), fmt.Sprint(now.BucketID))
	return out
}

// Scan states recorded in task_meta.scan_status.
const (
	ScanNone     = "NONE"
	ScanRunning  = "RUNNING"
	ScanComplete = "COMPLETE"
)

// Task is the task_meta row. Timestamps are read as text so the user's progress
// DSN does not need parseTime=true.
type Task struct {
	Binding
	ScanStatus     string
	ScanBsdbCount  sql.NullInt64
	ScanRows       sql.NullInt64
	ScanFinishedAt sql.NullString
	CreatedAt      string
}

var errNoTask = errors.New("progress database has no task binding yet; run scan with this SP's config first")

// Task reads the task_meta row; ok=false when the database has never been bound.
func (p *DB) Task(ctx context.Context) (t Task, ok bool, err error) {
	err = p.db.QueryRowContext(ctx, `SELECT chain_id, sp_operator, storage, spdb, bucket_name, bucket_id,
		scan_status, scan_bsdb_count, scan_rows, CAST(scan_finished_at AS CHAR), CAST(created_at AS CHAR)
		FROM task_meta WHERE id = 1`).
		Scan(&t.ChainID, &t.SPOperator, &t.Storage, &t.SpDB, &t.Bucket, &t.BucketID,
			&t.ScanStatus, &t.ScanBsdbCount, &t.ScanRows, &t.ScanFinishedAt, &t.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return t, false, nil
	}
	return t, err == nil, err
}

// Bind records b as this database's task on first use, and from then on refuses
// any run whose binding differs. A database that already holds a list or results
// but no binding was used by a version without binding, possibly for another SP,
// so it is never bound after the fact: its DONE rows would be taken as this
// task's.
func (p *DB) Bind(ctx context.Context, b Binding) error {
	_, ok, err := p.Task(ctx)
	if err != nil {
		return err
	}
	if !ok {
		var listed, results uint64
		if err := p.db.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM scan_objects), (SELECT COUNT(*) FROM purge_progress)`).
			Scan(&listed, &results); err != nil {
			return err
		}
		if listed > 0 || results > 0 {
			return fmt.Errorf("progress database has %d listed objects and %d results but no task binding (written by an older version, "+
				"possibly for another SP); refusing to adopt it, use a new empty progress database", listed, results)
		}
	}
	if _, err := p.db.ExecContext(ctx, `INSERT IGNORE INTO task_meta
		(id, chain_id, sp_operator, storage, spdb, bucket_name, bucket_id) VALUES (1, ?, ?, ?, ?, ?, ?)`,
		b.ChainID, b.SPOperator, b.Storage, b.SpDB, b.Bucket, b.BucketID); err != nil {
		return err
	}
	return p.CheckBinding(ctx, b)
}

// CheckBinding fails unless the database is already bound to exactly b.
func (p *DB) CheckBinding(ctx context.Context, b Binding) error {
	t, ok, err := p.Task(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return errNoTask
	}
	if d := t.Binding.Diff(b); len(d) > 0 {
		return fmt.Errorf("progress database belongs to another task; refusing to run:\n  %s", strings.Join(d, "\n  "))
	}
	return nil
}

// RequireCompleteScan fails unless the last scan into this database finished and
// reconciled against bsdb. A list from an interrupted or failed scan may be short.
func (p *DB) RequireCompleteScan(ctx context.Context) error {
	t, ok, err := p.Task(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return errNoTask
	}
	if t.ScanStatus != ScanComplete {
		return fmt.Errorf("object list is not complete (scan status %s); re-run scan until it reports complete", t.ScanStatus)
	}
	return nil
}

// BeginScan marks the list incomplete until FinishScan succeeds.
func (p *DB) BeginScan(ctx context.Context, bsdbCount uint64) error {
	if err := p.CheckLock(ctx); err != nil {
		return err
	}
	t, _, err := p.Task(ctx)
	if err != nil {
		return err
	}
	if t.ScanStatus == ScanComplete {
		return errors.New("scan is already COMPLETE and frozen; use task create (do not rescan this database)")
	}
	var tasks uint64
	if err := p.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM purge_tasks`).Scan(&tasks); err != nil {
		return err
	}
	if tasks != 0 {
		return errors.New("cannot change a manifest after subtasks exist")
	}
	_, err = p.db.ExecContext(ctx, `UPDATE task_meta SET scan_status = ?, scan_bsdb_count = ?,
		scan_rows = NULL, scan_finished_at = NULL WHERE id = 1`, ScanRunning, bsdbCount)
	return err
}

var ErrEmptyScan = errors.New("scan found 0 objects; refusing to seal an empty manifest; check --bucket / --bucket-id and BSDB synchronization with the chain")

// FinishScan marks a nonempty list complete; call it only after the scan reconciled.
func (p *DB) FinishScan(ctx context.Context, rows uint64) error {
	if rows == 0 {
		return ErrEmptyScan
	}
	if err := p.CheckLock(ctx); err != nil {
		return err
	}
	var actual uint64
	if err := p.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM scan_objects`).Scan(&actual); err != nil {
		return err
	}
	if actual != rows {
		return fmt.Errorf("persisted scan count %d differs from source %d; list remains incomplete; investigate source changes", actual, rows)
	}
	_, err := p.db.ExecContext(ctx, `UPDATE task_meta SET scan_status = ?, scan_rows = ?,
		scan_finished_at = CURRENT_TIMESTAMP WHERE id = 1`, ScanComplete, rows)
	return err
}
