// Package spdb clears an object's rows from the SP's own metadata database:
// integrity_meta_NN (sharded by object id) and piece_hash.
package spdb

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

const (
	integrityShards     = 64
	reasonableTableSize = 5_000_000 // store/sqldb/object_integrity_schema.go
)

// IntegrityTable mirrors GetIntegrityMetasTableName(objectID).
func IntegrityTable(oid uint64) string {
	return fmt.Sprintf("integrity_meta_%02d", oid/reasonableTableSize%integrityShards)
}

type DB struct{ db *sql.DB }

func Open(dsn string) (*DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("spdb: %w", err)
	}
	return &DB{db: db}, nil
}

func (d *DB) Close() error { return d.db.Close() }

// Delete removes every integrity_meta row (all redundancy indexes) and every
// piece_hash row for the object. Explicit object_id predicate, nothing else.
func (d *DB) Delete(ctx context.Context, oid uint64) error {
	if _, err := d.db.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE object_id = ?`, IntegrityTable(oid)), oid); err != nil {
		return fmt.Errorf("%s: %w", IntegrityTable(oid), err)
	}
	if _, err := d.db.ExecContext(ctx, `DELETE FROM piece_hash WHERE object_id = ?`, oid); err != nil {
		return fmt.Errorf("piece_hash: %w", err)
	}
	return nil
}

// Exists reports whether any metadata row is still present for the object.
func (d *DB) Exists(ctx context.Context, oid uint64) (bool, error) {
	var n int
	q := fmt.Sprintf(`SELECT (SELECT COUNT(*) FROM %s WHERE object_id = ?) + (SELECT COUNT(*) FROM piece_hash WHERE object_id = ?)`, IntegrityTable(oid))
	if err := d.db.QueryRowContext(ctx, q, oid, oid).Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}
