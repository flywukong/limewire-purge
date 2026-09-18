// Package progress owns the tool's two tables:
//
//	scan_objects   – the immutable object list scan pulled from bsdb
//	purge_progress – one row per object once it has been fully processed (or failed)
//
// "Not yet processed" is simply scan_objects minus purge_progress.
package progress

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

const (
	StatusDone   = 1
	StatusFailed = 2
)

type DB struct{ db *sql.DB }

func Open(dsn string) (*DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(16)
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("progress db: %w", err)
	}
	return &DB{db: db}, nil
}

func (p *DB) Close() error { return p.db.Close() }

func (p *DB) EnsureSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS scan_objects (
		   object_id    BIGINT UNSIGNED NOT NULL PRIMARY KEY,
		   payload_size BIGINT UNSIGNED NOT NULL,
		   status       VARCHAR(16) NOT NULL,
		   version      INT NOT NULL DEFAULT 0
		 )`,
		`CREATE TABLE IF NOT EXISTS purge_progress (
		   object_id     BIGINT UNSIGNED NOT NULL PRIMARY KEY,
		   status        TINYINT NOT NULL,
		   deleted_keys  INT UNSIGNED NOT NULL DEFAULT 0,
		   deleted_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0,
		   fail_reason   TEXT NULL,
		   updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		   KEY idx_status (status)
		 )`,
	}
	for _, s := range stmts {
		if _, err := p.db.ExecContext(ctx, s); err != nil {
			return err
		}
	}
	return nil
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

// Claim returns up to limit object ids greater than after that still need work:
// never processed (retryFailed=false) or previously failed (retryFailed=true).
// Callers page with the last id they received, so in-flight ids are never handed out twice.
func (p *DB) Claim(ctx context.Context, after uint64, limit int, retryFailed bool) ([]uint64, error) {
	q := `SELECT s.object_id FROM scan_objects s
	      LEFT JOIN purge_progress p ON p.object_id = s.object_id
	      WHERE s.object_id > ? AND p.object_id IS NULL
	      ORDER BY s.object_id LIMIT ?`
	if retryFailed {
		q = `SELECT s.object_id FROM scan_objects s
		     JOIN purge_progress p ON p.object_id = s.object_id
		     WHERE s.object_id > ? AND p.status = 2
		     ORDER BY s.object_id LIMIT ?`
	}
	rows, err := p.db.QueryContext(ctx, q, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []uint64
	for rows.Next() {
		var id uint64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// Done records a fully processed object. Counters accumulate so a fail-then-succeed
// object keeps what its first attempt already removed.
func (p *DB) Done(ctx context.Context, oid uint64, keys, bytes uint64) error {
	_, err := p.db.ExecContext(ctx, `INSERT INTO purge_progress (object_id, status, deleted_keys, deleted_bytes, fail_reason)
		VALUES (?, 1, ?, ?, NULL)
		ON DUPLICATE KEY UPDATE status = 1, fail_reason = NULL,
		  deleted_keys = deleted_keys + VALUES(deleted_keys), deleted_bytes = deleted_bytes + VALUES(deleted_bytes)`,
		oid, keys, bytes)
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
		COALESCE(SUM(deleted_keys),0), COALESCE(SUM(deleted_bytes),0) FROM purge_progress`).
		Scan(&c.Done, &c.Failed, &c.DeletedKeys, &c.DeletedBytes)
	return c, err
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
