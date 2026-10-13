package spdb

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
)

// Retry is for purge's idempotent OID deletes and read-only metadata checks.
// No retry layer is added to verify. Each DB attempt honors its own deadline.
func Retry(ctx context.Context, what string, fn func(context.Context) error) error {
	for attempt := 1; attempt <= 3; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := fn(cctx)
		cancel()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt == 3 || !transient(err) {
			return fmt.Errorf("%s failed after %d attempt(s): %w", what, attempt, err)
		}
		log.Printf("%s: attempt %d/3 failed: %v; retrying", what, attempt, err)
		timer := time.NewTimer(time.Duration(attempt) * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	panic("unreachable")
}

func transient(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	for _, cause := range []error{context.DeadlineExceeded, driver.ErrBadConn, mysql.ErrInvalidConn, io.EOF, io.ErrUnexpectedEOF, syscall.ECONNRESET, syscall.ECONNREFUSED, syscall.EPIPE} {
		if errors.Is(err, cause) {
			return true
		}
	}
	var ne net.Error
	if errors.As(err, &ne) && (ne.Timeout() || ne.Temporary()) {
		return true
	}
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		switch me.Number {
		case 1040, 1053, 1205, 1213, 2006, 2013:
			return true // overload/shutdown/locks/lost connection
		}
	}
	return false // permission, missing table, invalid SQL: don't retry
}
