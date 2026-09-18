// Package bsdb reads the object list for one bucket out of the SP's block-syncer
// index database. Objects are sharded into objects_00..objects_63 by
// murmur3_32(bucketName) % 64 (store/bsdb/object.go), so one bucket lives in one table.
package bsdb

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spaolacci/murmur3"

	"limewire-purge/internal/progress"
)

const shards = 64

// TableFor returns the objects_NN shard that holds bucketName.
func TableFor(bucketName string) string {
	return fmt.Sprintf("objects_%02d", murmur3.Sum32([]byte(bucketName))%shards)
}

type Scanner struct {
	db    *sql.DB
	table string
}

func Open(dsn, bucketName string) (*Scanner, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("bsdb: %w", err)
	}
	return &Scanner{db: db, table: TableFor(bucketName)}, nil
}

func (s *Scanner) Close() error { return s.db.Close() }

func (s *Scanner) Table() string { return s.table }

// Count is the cheap sanity check before a full scan.
func (s *Scanner) Count(ctx context.Context, bucketID uint64, bucketName string) (uint64, error) {
	var n uint64
	err := s.db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT COUNT(*) FROM %s WHERE bucket_id = UNHEX(LPAD(?, 64, '0')) AND bucket_name = ? AND removed = 0`, s.table),
		fmt.Sprintf("%x", bucketID), bucketName).Scan(&n)
	return n, err
}

// Scan walks the shard by primary key in batches and hands each batch to fn.
// The table has no bucket index, so this is a full scan of the shard; keyset paging
// keeps every statement short.
func (s *Scanner) Scan(ctx context.Context, bucketID uint64, bucketName string, batch int, fn func([]progress.ScanRow) error) (uint64, error) {
	q := fmt.Sprintf(`SELECT id, CONV(RIGHT(HEX(object_id), 16), 16, 10), payload_size, status, version
	                   FROM %s
	                   WHERE bucket_id = UNHEX(LPAD(?, 64, '0')) AND bucket_name = ? AND removed = 0 AND id > ?
	                   ORDER BY id LIMIT ?`, s.table)
	hexID := fmt.Sprintf("%x", bucketID)
	var lastID, total uint64
	for {
		rows, err := s.db.QueryContext(ctx, q, hexID, bucketName, lastID, batch)
		if err != nil {
			return total, err
		}
		out := make([]progress.ScanRow, 0, batch)
		for rows.Next() {
			var id uint64
			var oidStr string
			var r progress.ScanRow
			if err := rows.Scan(&id, &oidStr, &r.PayloadSize, &r.Status, &r.Version); err != nil {
				rows.Close()
				return total, err
			}
			r.ObjectID, err = strconv.ParseUint(oidStr, 10, 64)
			if err != nil {
				rows.Close()
				return total, fmt.Errorf("row id %d: bad object_id %q", id, oidStr)
			}
			r.Status = strings.TrimPrefix(r.Status, "OBJECT_STATUS_")
			lastID = id
			out = append(out, r)
		}
		rows.Close()
		if len(out) == 0 {
			return total, nil
		}
		if err := fn(out); err != nil {
			return total, err
		}
		total += uint64(len(out))
		if len(out) < batch {
			return total, nil
		}
	}
}
