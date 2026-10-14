// Package progress owns the tool's manifest, subtask and progress tables:
//
//	task_meta      – one row: which task this database belongs to, and whether the list is complete
//	scan_objects   – the immutable object list scan pulled from bsdb
//	purge_progress – one row per object once it has been fully processed (or failed)
//	purge_tasks / task_objects / task_events – subtask membership, verification and operator decisions
//
// "Not yet processed" is simply scan_objects minus purge_progress.
package progress

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

const (
	StatusDone   = 1
	StatusFailed = 2
)

type DB struct {
	db   *sql.DB
	lock *Lock
}

func Open(dsn string) (*DB, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	if cfg.DBName == "" {
		return nil, fmt.Errorf("progress DSN must name a dedicated database")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 30 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 30 * time.Second
	}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(16)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("progress db: %w", err)
	}
	return &DB{db: db}, nil
}

func (p *DB) Close() error {
	var err error
	if p.lock != nil {
		err = p.lock.Close()
	}
	return errors.Join(err, p.db.Close())
}

func (p *DB) EnsureSchema(ctx context.Context) error {
	if err := p.CheckLock(ctx); err != nil {
		return err
	}
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS scan_objects (
		   object_id    BIGINT UNSIGNED NOT NULL PRIMARY KEY,
		   payload_size BIGINT UNSIGNED NOT NULL,
		   status       VARCHAR(16) NOT NULL,
		   version      INT NOT NULL DEFAULT 0
		 ) ENGINE=InnoDB`,
		`CREATE TABLE IF NOT EXISTS purge_progress (
		   object_id     BIGINT UNSIGNED NOT NULL PRIMARY KEY,
		   status        TINYINT NOT NULL,
		   deleted_keys  INT UNSIGNED NOT NULL DEFAULT 0,
		   deleted_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0,
		   fail_reason   TEXT NULL,
		   role          VARCHAR(10) NULL,
		   expected_bytes BIGINT UNSIGNED NULL,
		   updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		   KEY idx_status (status)
		 ) ENGINE=InnoDB`,
		`CREATE TABLE IF NOT EXISTS task_meta (
		   id               TINYINT UNSIGNED NOT NULL PRIMARY KEY,
		   chain_id         VARCHAR(64) NOT NULL,
		   sp_operator      VARCHAR(128) NOT NULL,
		   storage          VARCHAR(512) NOT NULL,
		   spdb             VARCHAR(512) NOT NULL,
		   bucket_name      VARCHAR(64) NOT NULL,
		   bucket_id        BIGINT UNSIGNED NOT NULL,
		   scan_status      VARCHAR(16) NOT NULL DEFAULT 'NONE',
		   scan_bsdb_count  BIGINT UNSIGNED NULL,
		   scan_rows        BIGINT UNSIGNED NULL,
		   scan_finished_at TIMESTAMP NULL,
		   created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		 ) ENGINE=InnoDB`,
		`CREATE TABLE IF NOT EXISTS purge_tasks (
		 id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
		 target_bytes BIGINT UNSIGNED NOT NULL, object_count BIGINT UNSIGNED NOT NULL,
		 logical_bytes BIGINT UNSIGNED NOT NULL, digest CHAR(64) NOT NULL,
		 phase VARCHAR(16) NOT NULL DEFAULT 'READY',
		 hold_reason VARCHAR(4000) NOT NULL DEFAULT '', hold_oid BIGINT UNSIGNED NULL,
		 verify_generation BIGINT UNSIGNED NOT NULL DEFAULT 1,
		 purged_at TIMESTAMP NULL, execution_connection_id BIGINT UNSIGNED NULL,
		 done_at TIMESTAMP NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		) ENGINE=InnoDB`,
		`CREATE TABLE IF NOT EXISTS task_objects (
		 task_id BIGINT UNSIGNED NOT NULL, object_id BIGINT UNSIGNED NOT NULL,
		 deferred BOOLEAN NOT NULL DEFAULT 0, defer_reason VARCHAR(4000) NOT NULL DEFAULT '',
		 deferred_at TIMESTAMP NULL, verify_generation BIGINT UNSIGNED NOT NULL DEFAULT 0,
		 verify_status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
		 verify_detail VARCHAR(4000) NOT NULL DEFAULT '', verified_at TIMESTAMP NULL,
		 PRIMARY KEY(task_id,object_id), KEY idx_oid(object_id)
		) ENGINE=InnoDB`,
		`CREATE TABLE IF NOT EXISTS task_events (
		 id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
		 task_id BIGINT UNSIGNED NOT NULL, object_id BIGINT UNSIGNED NULL,
		 action VARCHAR(16) NOT NULL, reason TEXT NOT NULL,
		 created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		 KEY idx_task(task_id)
		) ENGINE=InnoDB`,
	}
	for _, s := range stmts {
		if _, err := p.db.ExecContext(ctx, s); err != nil {
			return err
		}
	}
	// columns added after the first release; progress databases created by an
	// older binary get them added in place
	for _, col := range []struct{ table, name, ddl string }{
		{"purge_progress", "role", "ALTER TABLE purge_progress ADD COLUMN role VARCHAR(10) NULL"},
		{"purge_progress", "expected_bytes", "ALTER TABLE purge_progress ADD COLUMN expected_bytes BIGINT UNSIGNED NULL"},
		{"purge_tasks", "purged_at", "ALTER TABLE purge_tasks ADD COLUMN purged_at TIMESTAMP NULL"},
		{"purge_tasks", "execution_connection_id", "ALTER TABLE purge_tasks ADD COLUMN execution_connection_id BIGINT UNSIGNED NULL"},
	} {
		var n int
		if err := p.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.columns
			WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?`, col.table, col.name).Scan(&n); err != nil {
			return err
		}
		if n == 0 {
			if _, err := p.db.ExecContext(ctx, col.ddl); err != nil {
				return err
			}
		}
	}
	// Task allocation and progress rely on transactions, including upgraded DBs.
	var nonTransactional int
	if err := p.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.tables
	 WHERE table_schema=DATABASE() AND table_name IN ('task_meta','scan_objects','purge_progress','purge_tasks','task_objects','task_events')
	 AND engine<>'InnoDB'`).Scan(&nonTransactional); err != nil {
		return err
	}
	if nonTransactional != 0 {
		return fmt.Errorf("progress tables must use InnoDB; review/migrate storage engines before running")
	}
	// A new command owns the lock: do not attribute it to a previous task.
	if _, err := p.db.ExecContext(ctx, `UPDATE purge_tasks SET execution_connection_id=NULL WHERE execution_connection_id IS NOT NULL`); err != nil {
		return err
	}
	// Upgrade old databases, and recover a crash after the last per-object write.
	// This never marks verification DONE or erases an existing safety hold.
	_, err := p.db.ExecContext(ctx, `UPDATE purge_tasks t SET purged_at=COALESCE(done_at,CURRENT_TIMESTAMP)
 WHERE purged_at IS NULL AND hold_reason='' AND object_count>0
 AND (phase<>'READY' OR NOT EXISTS (SELECT 1 FROM task_objects active WHERE active.task_id=t.id AND active.deferred=0))
 AND object_count=(SELECT COUNT(*) FROM task_objects o WHERE o.task_id=t.id)
 AND NOT EXISTS (SELECT 1 FROM task_objects o LEFT JOIN purge_progress p ON p.object_id=o.object_id
 WHERE o.task_id=t.id AND o.deferred=0 AND (p.object_id IS NULL OR p.status<>1))`)
	return err
}

// ScanRow is what scan pulls out of bsdb for one object.
type ScanRow struct {
	ObjectID    uint64
	PayloadSize uint64
	Status      string
	Version     int64
}

// InsertScan writes a batch of scan rows; re-running scan is harmless (INSERT IGNORE).
func (p *DB) InsertScan(ctx context.Context, rows []ScanRow) error {
	if err := p.CheckLock(ctx); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	var b strings.Builder
	b.WriteString("INSERT IGNORE INTO scan_objects (object_id, payload_size, status, version) VALUES ")
	args := make([]any, 0, len(rows)*4)
	for i, r := range rows {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString("(?,?,?,?)")
		args = append(args, r.ObjectID, r.PayloadSize, r.Status, r.Version)
	}
	_, err := p.db.ExecContext(ctx, b.String(), args...)
	return err
}

// ClaimRow is one claimed object with the snapshot scan recorded from bsdb.
type ClaimRow struct {
	ObjectID    uint64
	PayloadSize uint64
	Status      string
	Version     int64
}

// Claim returns up to limit objects with id greater than after that still need work:
// never processed (retryFailed=false) or previously failed (retryFailed=true).
// Callers page with the last id they received, so in-flight ids are never handed out twice.
func (p *DB) Claim(ctx context.Context, after uint64, limit int, retryFailed bool) ([]ClaimRow, error) {
	q := `SELECT s.object_id, s.payload_size, s.status, s.version FROM scan_objects s
	      LEFT JOIN purge_progress p ON p.object_id = s.object_id
	      WHERE s.object_id > ? AND p.object_id IS NULL
	      ORDER BY s.object_id LIMIT ?`
	if retryFailed {
		q = `SELECT s.object_id, s.payload_size, s.status, s.version FROM scan_objects s
		     JOIN purge_progress p ON p.object_id = s.object_id
		     WHERE s.object_id > ? AND p.status = 2
		     ORDER BY s.object_id LIMIT ?`
	}
	rows, err := p.db.QueryContext(ctx, q, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ClaimRow
	for rows.Next() {
		var c ClaimRow
		if err := rows.Scan(&c.ObjectID, &c.PayloadSize, &c.Status, &c.Version); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// Done records a fully processed object. Counters accumulate so a fail-then-succeed
// object keeps what its first attempt already removed. role/expected are kept from
// an earlier attempt when this one found nothing left to infer them from, so the
// size comparison is always cumulative deleted_bytes vs expected_bytes.
func (p *DB) Done(ctx context.Context, oid uint64, keys, bytes uint64, role string, expected *uint64) error {
	_, err := p.db.ExecContext(ctx, `INSERT INTO purge_progress (object_id, status, deleted_keys, deleted_bytes, fail_reason, role, expected_bytes)
		VALUES (?, 1, ?, ?, NULL, ?, ?)
		ON DUPLICATE KEY UPDATE status = 1, fail_reason = NULL,
		  deleted_keys = deleted_keys + VALUES(deleted_keys), deleted_bytes = deleted_bytes + VALUES(deleted_bytes),
		  role = IF(VALUES(role) = 'none', role, VALUES(role)),
		  expected_bytes = COALESCE(VALUES(expected_bytes), expected_bytes)`,
		oid, keys, bytes, role, expected)
	return err
}

// Fail records a failure, still crediting whatever this attempt did delete.
func (p *DB) Fail(ctx context.Context, oid uint64, keys, bytes uint64, reason string) error {
	if len(reason) > 4000 {
		reason = reason[:4000]
	}
	_, err := p.db.ExecContext(ctx, `INSERT INTO purge_progress (object_id, status, deleted_keys, deleted_bytes, fail_reason)
		VALUES (?, 2, ?, ?, ?)
		ON DUPLICATE KEY UPDATE status = 2, fail_reason = VALUES(fail_reason),
		  deleted_keys = deleted_keys + VALUES(deleted_keys), deleted_bytes = deleted_bytes + VALUES(deleted_bytes)`,
		oid, keys, bytes, reason)
	return err
}

type Counts struct {
	Total, Done, Failed       uint64
	DeletedKeys, DeletedBytes uint64
	ScanBytes                 uint64
	// size check over done objects: compared (expected known), of which mismatched
	SizeChecked, SizeMismatch uint64
}

func (c Counts) Remaining() uint64 { return c.Total - c.Done - c.Failed }

func (p *DB) Counts(ctx context.Context) (Counts, error) {
	var c Counts
	err := p.db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(payload_size),0) FROM scan_objects`).Scan(&c.Total, &c.ScanBytes)
	if err != nil {
		return c, err
	}
	err = p.db.QueryRowContext(ctx, `SELECT
		COALESCE(SUM(status = 1),0), COALESCE(SUM(status = 2),0),
		COALESCE(SUM(deleted_keys),0), COALESCE(SUM(deleted_bytes),0),
		COALESCE(SUM(status = 1 AND expected_bytes IS NOT NULL),0),
		COALESCE(SUM(status = 1 AND expected_bytes IS NOT NULL AND deleted_bytes <> expected_bytes),0)
		FROM purge_progress`).
		Scan(&c.Done, &c.Failed, &c.DeletedKeys, &c.DeletedBytes, &c.SizeChecked, &c.SizeMismatch)
	return c, err
}

// Mismatches lists done objects whose cumulative deleted_bytes differ from the
// expected size derived from the chain payload.
func (p *DB) Mismatches(ctx context.Context, limit int) ([]string, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT object_id, role, deleted_bytes, expected_bytes FROM purge_progress
		WHERE status = 1 AND expected_bytes IS NOT NULL AND deleted_bytes <> expected_bytes
		ORDER BY object_id LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id, del, exp uint64
		var role sql.NullString
		if err := rows.Scan(&id, &role, &del, &exp); err != nil {
			return nil, err
		}
		out = append(out, fmt.Sprintf("%d\t%s\tdeleted=%d\texpected=%d", id, role.String, del, exp))
	}
	return out, rows.Err()
}

// Failures lists recent failure reasons for the status command.
func (p *DB) Failures(ctx context.Context, limit int) ([]string, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT object_id, fail_reason FROM purge_progress WHERE status = 2 ORDER BY updated_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id uint64
		var reason sql.NullString
		if err := rows.Scan(&id, &reason); err != nil {
			return nil, err
		}
		out = append(out, fmt.Sprintf("%d\t%s", id, reason.String))
	}
	return out, rows.Err()
}

// ScanIDs streams every object id in scan_objects (used by verify, which ignores progress).
func (p *DB) ScanIDs(ctx context.Context, after uint64, limit int) ([]ScanRow, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT object_id, payload_size, status, version FROM scan_objects WHERE object_id > ? ORDER BY object_id LIMIT ?`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ScanRow
	for rows.Next() {
		var r ScanRow
		if err := rows.Scan(&r.ObjectID, &r.PayloadSize, &r.Status, &r.Version); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
