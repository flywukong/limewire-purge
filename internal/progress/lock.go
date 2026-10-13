package progress

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Lock owns a MySQL session, not a pooled DB operation. All cooperating commands
// use the same progress database on a single MySQL writer. This is NOT fencing
// for S3: already submitted requests cannot be recalled on lock loss.
type Lock struct {
	conn   *sql.Conn
	name   string
	id     int64
	ctx    context.Context
	cancel context.CancelCauseFunc
	stop   chan struct{}
	done   chan struct{}
	mu     sync.Mutex
	once   sync.Once
	lost   error // protected by mu; distinct from an operator cancelling ctx
}

func lockName(database string) string {
	return fmt.Sprintf("limewire-purge:%x", sha256.Sum256([]byte(database)))[:63]
}

func (p *DB) Acquire(ctx context.Context, operation string) (*Lock, error) {
	if p.lock != nil {
		return nil, errors.New("database already has an execution lock")
	}
	conn, err := p.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	var database string
	var id int64
	if err := conn.QueryRowContext(ctx, "SELECT DATABASE(), CONNECTION_ID()").Scan(&database, &id); err != nil {
		conn.Close()
		return nil, err
	}
	name := lockName(database)
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", name).Scan(&got); err != nil || !got.Valid || got.Int64 != 1 {
		conn.Close()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("another command holds the execution lock for %s; refusing %s", database, operation)
	}
	lctx, cancel := context.WithCancelCause(ctx)
	l := &Lock{conn: conn, name: name, id: id, ctx: lctx, cancel: cancel, stop: make(chan struct{}), done: make(chan struct{})}
	p.lock = l
	go func() {
		defer close(l.done)
		tick := time.NewTicker(2 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-l.stop:
				return
			case <-lctx.Done():
				return
			case <-tick.C:
				if l.Check(lctx) != nil {
					return
				}
			}
		}
	}()
	return l, nil
}

func (l *Lock) Context() context.Context { return l.ctx }

// Check verifies ownership, not merely connectivity. A different connection
// must never be substituted for the original owner after a disconnect.
func (l *Lock) Check(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lost != nil {
		return l.lost
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var owner sql.NullInt64
	err := l.conn.QueryRowContext(cctx, "SELECT IS_USED_LOCK(?)", l.name).Scan(&owner)
	if err == nil && (!owner.Valid || owner.Int64 != l.id) {
		err = errors.New("execution lock is no longer owned by this session")
	}
	if err != nil {
		l.lost = fmt.Errorf("execution lock lost: %w", err)
		l.cancel(l.lost)
	}
	return err
}

func (l *Lock) Close() error {
	var result error
	l.once.Do(func() {
		close(l.stop)
		<-l.done
		l.mu.Lock()
		defer l.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		var released sql.NullInt64
		result = l.conn.QueryRowContext(ctx, "SELECT RELEASE_LOCK(?)", l.name).Scan(&released)
		if result != nil {
			// Do not return a possibly still-lock-owning session to the pool.
			_ = l.conn.Raw(func(any) error { return driver.ErrBadConn })
		}
		l.lost = errors.New("execution lock released")
		l.cancel(l.lost)
		// Conn.Close returns to the pool. Explicit RELEASE_LOCK above is essential.
		result = errors.Join(result, l.conn.Close())
	})
	return result
}

func (p *DB) CheckLock(ctx context.Context) error {
	if p.lock == nil {
		return errors.New("mutation requires execution lock")
	}
	return p.lock.Check(ctx)
}

// LockOwner is read-only and does not acquire the execution lock.
func (p *DB) LockOwner(ctx context.Context) (sql.NullInt64, error) {
	var db string
	var owner sql.NullInt64
	if err := p.db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&db); err != nil {
		return owner, err
	}
	err := p.db.QueryRowContext(ctx, "SELECT IS_USED_LOCK(?)", lockName(db)).Scan(&owner)
	return owner, err
}
