package progress

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

var ErrNoCurrentTask = errors.New("no unfinished subtask; run task create first (or supply --task-id for history)")
var ErrNoUnassigned = errors.New("no unassigned objects; inspect status for deferred objects and final completion")

type Subtask struct {
	ID, TargetBytes, ObjectCount, LogicalBytes uint64
	Digest, Phase, HoldReason                  string
	HoldOID                                    sql.NullInt64
	Generation                                 uint64
	DoneAt                                     sql.NullString
}

type SubCounts struct {
	Total, Deferred, Pending, Failed, Processed, Verified, Residue, VerifyErrors uint64
	DeletedKeys, DeletedBytes                                                    uint64
}

func (s Subtask) State(c SubCounts, locked bool) string {
	if s.HoldReason != "" {
		return "SAFETY_HOLD"
	}
	if s.DoneAt.Valid {
		if c.Deferred > 0 {
			return "DONE_WITH_EXCEPTIONS"
		}
		return "DONE"
	}
	if s.Phase == "READY" {
		return "READY"
	}
	if locked {
		if s.Phase == "VERIFY" {
			return "VERIFYING"
		}
		return "RUNNING"
	}
	if c.Residue > 0 || c.VerifyErrors > 0 {
		return "VERIFY_FAILED"
	}
	if c.Pending > 0 || c.Failed > 0 {
		return "RETRY_PENDING"
	}
	return "AWAITING_VERIFY"
}

func ParseCapacity(value string) (uint64, error) {
	for _, unit := range []struct {
		name   string
		factor uint64
	}{{"TiB", 1 << 40}, {"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}, {"B", 1}} {
		if strings.HasSuffix(value, unit.name) {
			n, err := strconv.ParseUint(strings.TrimSuffix(value, unit.name), 10, 64)
			if err != nil || n == 0 || n > math.MaxUint64/unit.factor {
				break
			}
			return n * unit.factor, nil
		}
	}
	return 0, fmt.Errorf("invalid capacity %q: use a positive integer with B/KiB/MiB/GiB/TiB", value)
}

// SelectCapacity returns a contiguous prefix. Oversized objects stand alone;
// zero-size objects still consume a slot. Candidates must already be sorted.
func SelectCapacity(rows []ScanRow, target uint64, max int) ([]ScanRow, uint64, error) {
	if target == 0 || max <= 0 {
		return nil, 0, errors.New("positive target size and max-objects required")
	}
	var out []ScanRow
	var total uint64
	for _, r := range rows {
		if len(out) >= max || total >= target {
			break
		}
		if r.PayloadSize > target && len(out) > 0 {
			break
		}
		if math.MaxUint64-total < r.PayloadSize {
			return nil, 0, errors.New("logical size overflow")
		}
		out = append(out, r)
		total += r.PayloadSize
	}
	return out, total, nil
}

func digestRows(rows []ScanRow) string {
	h := sha256.New()
	for _, r := range rows {
		fmt.Fprintf(h, "%d\t%d\t%s\t%d\n", r.ObjectID, r.PayloadSize, r.Status, r.Version)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

const taskSelect = `SELECT id,target_bytes,object_count,logical_bytes,digest,phase,hold_reason,hold_oid,verify_generation,CAST(done_at AS CHAR) FROM purge_tasks`

type rowScanner interface{ Scan(...any) error }

func readSubtask(row rowScanner) (s Subtask, err error) {
	err = row.Scan(&s.ID, &s.TargetBytes, &s.ObjectCount, &s.LogicalBytes, &s.Digest, &s.Phase, &s.HoldReason, &s.HoldOID, &s.Generation, &s.DoneAt)
	return
}

func (p *DB) Subtask(ctx context.Context, id uint64) (Subtask, error) {
	return readSubtask(p.db.QueryRowContext(ctx, taskSelect+` WHERE id=?`, id))
}

func (p *DB) ResolveSubtask(ctx context.Context, id uint64) (Subtask, error) {
	if id > 0 {
		return p.Subtask(ctx, id)
	}
	rows, err := p.db.QueryContext(ctx, taskSelect+` WHERE done_at IS NULL ORDER BY id LIMIT 2`)
	if err != nil {
		return Subtask{}, err
	}
	defer rows.Close()
	var found []Subtask
	for rows.Next() {
		s, e := readSubtask(rows)
		if e != nil {
			return Subtask{}, e
		}
		found = append(found, s)
	}
	if err := rows.Err(); err != nil {
		return Subtask{}, err
	}
	return selectCurrent(found, 0)
}

// ResolveCurrentSubtask treats an explicit ID as an expectation, never as a
// way to choose another task. Call while holding the execution lock.
func (p *DB) ResolveCurrentSubtask(ctx context.Context, expected uint64) (Subtask, error) {
	s, err := p.ResolveSubtask(ctx, 0)
	if err != nil {
		return Subtask{}, err
	}
	return selectCurrent([]Subtask{s}, expected)
}

func selectCurrent(found []Subtask, expected uint64) (Subtask, error) {
	if len(found) == 0 {
		return Subtask{}, ErrNoCurrentTask
	}
	if len(found) > 1 {
		return Subtask{}, errors.New("multiple unfinished subtasks: investigate database consistency before operating")
	}
	if expected != 0 && expected != found[0].ID {
		return Subtask{}, fmt.Errorf("--task-id %d does not match current task %d; refusing to run; review the current task summary first", expected, found[0].ID)
	}
	return found[0], nil
}

func (p *DB) Subtasks(ctx context.Context) ([]Subtask, error) {
	rows, err := p.db.QueryContext(ctx, taskSelect+` ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Subtask
	for rows.Next() {
		s, e := readSubtask(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func (p *DB) SubCounts(ctx context.Context, id uint64) (c SubCounts, err error) {
	err = p.db.QueryRowContext(ctx, `SELECT COUNT(*),
 COALESCE(SUM(o.deferred),0),
 COALESCE(SUM(NOT o.deferred AND (p.object_id IS NULL OR p.status<>1) AND (p.object_id IS NULL OR p.status<>2)),0),
 COALESCE(SUM(NOT o.deferred AND p.status=2),0),
 COALESCE(SUM(NOT o.deferred AND p.status=1),0),
 COALESCE(SUM(NOT o.deferred AND o.verify_generation=t.verify_generation AND o.verify_status='PASS'),0),
 COALESCE(SUM(NOT o.deferred AND o.verify_generation=t.verify_generation AND o.verify_status='RESIDUE'),0),
 COALESCE(SUM(NOT o.deferred AND o.verify_generation=t.verify_generation AND o.verify_status='ERROR'),0),
 COALESCE(SUM(p.deleted_keys),0),COALESCE(SUM(p.deleted_bytes),0)
 FROM task_objects o JOIN purge_tasks t ON t.id=o.task_id
 LEFT JOIN purge_progress p ON p.object_id=o.object_id WHERE o.task_id=?`, id).
		Scan(&c.Total, &c.Deferred, &c.Pending, &c.Failed, &c.Processed, &c.Verified, &c.Residue, &c.VerifyErrors, &c.DeletedKeys, &c.DeletedBytes)
	return
}

// CreateSubtask allocates at most max objects atomically. Deferred retries are
// explicit and preserve historical task membership. All writers hold one lock.
func (p *DB) CreateSubtask(ctx context.Context, target uint64, max int, retryDeferred bool) (Subtask, error) {
	if err := p.CheckLock(ctx); err != nil {
		return Subtask{}, err
	}
	if target == 0 || max <= 0 || max > 100000 {
		return Subtask{}, errors.New("target must be positive; max-objects must be 1..100000")
	}
	if err := p.RequireCompleteScan(ctx); err != nil {
		return Subtask{}, err
	}
	if s, err := p.ResolveSubtask(ctx, 0); err == nil {
		return s, nil
	} else if !errors.Is(err, ErrNoCurrentTask) {
		return Subtask{}, err
	}
	var legacy uint64
	if err := p.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM purge_progress p WHERE NOT EXISTS (SELECT 1 FROM task_objects o WHERE o.object_id=p.object_id)`).Scan(&legacy); err != nil {
		return Subtask{}, err
	}
	if legacy > 0 {
		return Subtask{}, errors.New("unassigned legacy purge results: use a fresh progress DB or an explicitly reviewed migration")
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return Subtask{}, err
	}
	defer tx.Rollback()
	var bindingID int
	if err := tx.QueryRowContext(ctx, `SELECT id FROM task_meta WHERE id=1 FOR UPDATE`).Scan(&bindingID); err != nil {
		return Subtask{}, err
	}
	condition := `NOT EXISTS (SELECT 1 FROM task_objects o WHERE o.object_id=s.object_id)`
	if retryDeferred {
		condition = `EXISTS (SELECT 1 FROM task_objects o JOIN purge_tasks t ON t.id=o.task_id
 WHERE o.object_id=s.object_id AND o.deferred=1 AND t.done_at IS NOT NULL
 AND NOT EXISTS (SELECT 1 FROM task_objects newer WHERE newer.object_id=o.object_id AND newer.task_id>o.task_id))`
	}
	rows, err := tx.QueryContext(ctx, `SELECT s.object_id,s.payload_size,s.status,s.version FROM scan_objects s WHERE `+condition+` ORDER BY s.object_id LIMIT ?`, max)
	if err != nil {
		return Subtask{}, err
	}
	var candidates []ScanRow
	for rows.Next() {
		var r ScanRow
		if err := rows.Scan(&r.ObjectID, &r.PayloadSize, &r.Status, &r.Version); err != nil {
			rows.Close()
			return Subtask{}, err
		}
		candidates = append(candidates, r)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return Subtask{}, err
	}
	selected, total, err := SelectCapacity(candidates, target, max)
	if err != nil {
		return Subtask{}, err
	}
	if len(selected) == 0 {
		return Subtask{}, ErrNoUnassigned
	}
	digest := digestRows(selected)
	res, err := tx.ExecContext(ctx, `INSERT INTO purge_tasks (target_bytes,object_count,logical_bytes,digest) VALUES (?,?,?,?)`, target, len(selected), total, digest)
	if err != nil {
		return Subtask{}, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return Subtask{}, err
	}
	for _, r := range selected {
		if _, err := tx.ExecContext(ctx, `INSERT INTO task_objects(task_id,object_id) VALUES (?,?)`, id, r.ObjectID); err != nil {
			return Subtask{}, err
		}
		if retryDeferred {
			// Force a fresh identity check and deletion attempt, even when an earlier
			// attempt was DONE but verify later found residue. Preserve counters.
			if _, err := tx.ExecContext(ctx, `UPDATE purge_progress SET status=2,fail_reason='explicit deferred retry' WHERE object_id=?`, r.ObjectID); err != nil {
				return Subtask{}, err
			}
		}
	}
	if err := p.CheckLock(ctx); err != nil {
		return Subtask{}, err
	}
	if err := tx.Commit(); err != nil {
		return Subtask{}, err
	}
	return p.Subtask(ctx, uint64(id))
}

func (p *DB) CheckManifest(ctx context.Context, s Subtask) error {
	rows, err := p.db.QueryContext(ctx, `SELECT s.object_id,s.payload_size,s.status,s.version FROM task_objects o JOIN scan_objects s ON s.object_id=o.object_id WHERE o.task_id=? ORDER BY s.object_id`, s.ID)
	if err != nil {
		return err
	}
	defer rows.Close()
	var list []ScanRow
	for rows.Next() {
		var r ScanRow
		if err := rows.Scan(&r.ObjectID, &r.PayloadSize, &r.Status, &r.Version); err != nil {
			return err
		}
		list = append(list, r)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if uint64(len(list)) != s.ObjectCount || digestRows(list) != s.Digest {
		return errors.New("subtask manifest changed; refusing to operate")
	}
	return nil
}

func (p *DB) ClaimSubtask(ctx context.Context, id, after uint64, limit int) ([]ClaimRow, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT s.object_id,s.payload_size,s.status,s.version
 FROM task_objects o JOIN scan_objects s ON s.object_id=o.object_id
 LEFT JOIN purge_progress p ON p.object_id=o.object_id
 WHERE o.task_id=? AND o.deferred=0 AND s.object_id>?
 AND (p.object_id IS NULL OR p.status<>1 OR o.verify_status='RESIDUE') ORDER BY s.object_id LIMIT ?`, id, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ClaimRow
	for rows.Next() {
		var r ClaimRow
		if err := rows.Scan(&r.ObjectID, &r.PayloadSize, &r.Status, &r.Version); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (p *DB) BeginPurge(ctx context.Context, s Subtask) error {
	if err := p.CheckLock(ctx); err != nil {
		return err
	}
	if s.DoneAt.Valid || s.HoldReason != "" {
		return errors.New("task is completed or on SAFETY_HOLD")
	}
	if err := p.CheckManifest(ctx, s); err != nil {
		return err
	}
	// Residue markers survive generation reset until the object is reprocessed.
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `UPDATE purge_progress p JOIN task_objects o ON o.object_id=p.object_id
 SET p.status=2,p.fail_reason='verify found residue'
 WHERE o.task_id=? AND o.deferred=0 AND o.verify_status='RESIDUE'`, s.ID)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE purge_tasks SET phase='PURGE',verify_generation=verify_generation+1 WHERE id=? AND done_at IS NULL AND hold_reason=''`, s.ID)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (p *DB) Hold(ctx context.Context, id uint64, oid *uint64, reason string) error {
	if err := p.CheckLock(ctx); err != nil {
		return err
	}
	// Preserve the latch even if an upstream error contains many long keys.
	if len([]rune(reason)) > 3900 {
		reason = string([]rune(reason)[:3900])
	}
	_, err := p.db.ExecContext(ctx, `UPDATE purge_tasks SET hold_reason=?,hold_oid=? WHERE id=? AND done_at IS NULL`, reason, oid, id)
	return err
}

func (p *DB) BeginVerify(ctx context.Context, s Subtask, restart bool) (Subtask, error) {
	if err := p.CheckLock(ctx); err != nil {
		return s, err
	}
	if s.HoldReason != "" {
		return s, errors.New("SAFETY_HOLD: investigate and task resume first")
	}
	if s.DoneAt.Valid && !restart {
		return s, nil
	}
	if err := p.CheckManifest(ctx, s); err != nil {
		return s, err
	}
	c, err := p.SubCounts(ctx, s.ID)
	if err != nil {
		return s, err
	}
	if c.Pending+c.Failed > 0 {
		return s, errors.New("task has unprocessed/failed objects; purge or explicitly defer them before verify")
	}
	// Reopening historical tasks must not manufacture a second active task.
	if s.DoneAt.Valid {
		var n int
		if err := p.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM purge_tasks WHERE done_at IS NULL`).Scan(&n); err != nil {
			return s, err
		}
		if n > 0 {
			return s, errors.New("another unfinished task exists; cannot reopen historical verification")
		}
	}
	if restart {
		s.Generation++
	}
	_, err = p.db.ExecContext(ctx, `UPDATE purge_tasks SET phase='VERIFY',verify_generation=?,done_at=NULL WHERE id=?`, s.Generation, s.ID)
	if err != nil {
		return s, err
	}
	return p.Subtask(ctx, s.ID)
}

type VerifyRow struct{ OID uint64 }

func (p *DB) VerifyPending(ctx context.Context, s Subtask, after uint64, limit int) ([]VerifyRow, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT object_id FROM task_objects WHERE task_id=? AND deferred=0 AND object_id>?
 AND (verify_generation<>? OR verify_status<>'PASS') ORDER BY object_id LIMIT ?`, s.ID, after, s.Generation, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []VerifyRow
	for rows.Next() {
		var r VerifyRow
		if err := rows.Scan(&r.OID); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (p *DB) RecordVerify(ctx context.Context, s Subtask, oid uint64, status, detail string) error {
	if status != "PASS" && status != "ERROR" && status != "RESIDUE" {
		return errors.New("invalid verification result")
	}
	if err := p.CheckLock(ctx); err != nil {
		return err
	}
	_, err := p.db.ExecContext(ctx, `UPDATE task_objects SET verify_generation=?,verify_status=?,verify_detail=?,verified_at=CURRENT_TIMESTAMP
 WHERE task_id=? AND object_id=? AND deferred=0`, s.Generation, status, detail, s.ID, oid)
	return err
}

func (p *DB) CompleteVerify(ctx context.Context, s Subtask) error {
	if err := p.CheckLock(ctx); err != nil {
		return err
	}
	c, err := p.SubCounts(ctx, s.ID)
	if err != nil {
		return err
	}
	if c.Total != s.ObjectCount || c.Pending+c.Failed > 0 || c.Verified+c.Deferred != c.Total {
		return fmt.Errorf("verification incomplete: pass=%d deferred=%d total=%d residue=%d errors=%d", c.Verified, c.Deferred, c.Total, c.Residue, c.VerifyErrors)
	}
	_, err = p.db.ExecContext(ctx, `UPDATE purge_tasks SET done_at=CURRENT_TIMESTAMP WHERE id=? AND hold_reason='' AND phase='VERIFY' AND verify_generation=?`, s.ID, s.Generation)
	if err != nil {
		return err
	}
	now, err := p.Subtask(ctx, s.ID)
	if err != nil {
		return err
	}
	if !now.DoneAt.Valid {
		return errors.New("task could not be finalized")
	}
	return nil
}

// DeferObject preserves the frozen manifest and result. It is never PASS.
func (p *DB) DeferObject(ctx context.Context, s Subtask, oid uint64, reason string) error {
	if strings.TrimSpace(reason) == "" {
		return errors.New("--reason is required")
	}
	if len([]rune(reason)) > 3900 {
		return errors.New("--reason must be at most 3900 characters")
	}
	if s.DoneAt.Valid {
		return errors.New("cannot defer from a completed task")
	}
	if s.HoldReason != "" && (!s.HoldOID.Valid || uint64(s.HoldOID.Int64) != oid) {
		return errors.New("global/unrelated safety hold cannot be bypassed by deferring this object")
	}
	if err := p.CheckLock(ctx); err != nil {
		return err
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE task_objects SET deferred=1,defer_reason=?,deferred_at=CURRENT_TIMESTAMP WHERE task_id=? AND object_id=? AND deferred=0`, reason, s.ID, oid)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("OID is not an active member of this task")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO task_events(task_id,object_id,action,reason) VALUES (?,?,'DEFER',?)`, s.ID, oid, reason); err != nil {
		return err
	}
	if s.HoldReason != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE purge_tasks SET hold_reason='',hold_oid=NULL WHERE id=?`, s.ID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (p *DB) ResumeSafety(ctx context.Context, s Subtask, reason string) error {
	if strings.TrimSpace(reason) == "" {
		return errors.New("--reason is required")
	}
	if s.HoldReason == "" {
		return errors.New("task is not on SAFETY_HOLD; rerun purge/verify directly")
	}
	if err := p.CheckLock(ctx); err != nil {
		return err
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO task_events(task_id,action,reason) VALUES (?,'RESUME',?)`, s.ID, reason+"; previous hold: "+s.HoldReason); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE purge_tasks SET hold_reason='',hold_oid=NULL WHERE id=?`, s.ID); err != nil {
		return err
	}
	return tx.Commit()
}

type ReportRow struct {
	OID          uint64
	Kind, Detail string
}

func (p *DB) VerifyReport(ctx context.Context, s Subtask) ([]ReportRow, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT object_id,IF(deferred,'DEFERRED',verify_status),IF(deferred,defer_reason,verify_detail)
 FROM task_objects WHERE task_id=? AND (deferred=1 OR (verify_generation=? AND verify_status IN ('ERROR','RESIDUE'))) ORDER BY object_id`, s.ID, s.Generation)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ReportRow
	for rows.Next() {
		var r ReportRow
		if err := rows.Scan(&r.OID, &r.Kind, &r.Detail); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (p *DB) Outstanding(ctx context.Context) (unassigned, deferred uint64, err error) {
	err = p.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM scan_objects s WHERE NOT EXISTS (SELECT 1 FROM task_objects o WHERE o.object_id=s.object_id)`).Scan(&unassigned)
	if err != nil {
		return
	}
	err = p.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM task_objects o WHERE o.deferred=1 AND NOT EXISTS
 (SELECT 1 FROM task_objects newer WHERE newer.object_id=o.object_id AND newer.task_id>o.task_id)`).Scan(&deferred)
	return
}

// DoneSubtask makes the deletion result and invalidation of stale residue
// markers atomic; a restart must not requeue an already-reprocessed OID.
func (p *DB) DoneSubtask(ctx context.Context, id, oid, keys, bytes uint64, role string, expected *uint64) error {
	if err := p.CheckLock(ctx); err != nil {
		return err
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var member int
	if err := tx.QueryRowContext(ctx, `SELECT 1 FROM task_objects WHERE task_id=? AND object_id=? AND deferred=0`, id, oid).Scan(&member); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO purge_progress(object_id,status,deleted_keys,deleted_bytes,fail_reason,role,expected_bytes)
 VALUES (?,1,?,?,NULL,?,?) ON DUPLICATE KEY UPDATE status=1,fail_reason=NULL,
 deleted_keys=deleted_keys+VALUES(deleted_keys),deleted_bytes=deleted_bytes+VALUES(deleted_bytes),
 role=IF(VALUES(role)='none',role,VALUES(role)),expected_bytes=COALESCE(VALUES(expected_bytes),expected_bytes)`, oid, keys, bytes, role, expected)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE task_objects SET verify_generation=0,verify_status='PENDING',verify_detail='',verified_at=NULL WHERE task_id=? AND object_id=?`, id, oid); err != nil {
		return err
	}
	return tx.Commit()
}

func (p *DB) CheckMember(ctx context.Context, id, oid uint64) error {
	if err := p.CheckLock(ctx); err != nil {
		return err
	}
	var member int
	return p.db.QueryRowContext(ctx, `SELECT 1 FROM task_objects WHERE task_id=? AND object_id=? AND deferred=0`, id, oid).Scan(&member)
}
