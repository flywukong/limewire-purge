package progress

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
)

// Minimal session-aware driver: tests the dedicated-connection protocol, not
// MySQL's SQL parser. Real transaction/DDL coverage is opt-in integration below.
type lockServer struct {
	mu     sync.Mutex
	next   int64
	owners map[string]int64
}
type lockConnector struct {
	s        *lockServer
	database string
}

func (c lockConnector) Connect(context.Context) (driver.Conn, error) {
	c.s.mu.Lock()
	defer c.s.mu.Unlock()
	c.s.next++
	return &lockConn{c.s, c.database, c.s.next}, nil
}
func (c lockConnector) Driver() driver.Driver { return lockDriver{} }

type lockDriver struct{}

func (lockDriver) Open(string) (driver.Conn, error) { return nil, errors.New("use connector") }

type lockConn struct {
	s        *lockServer
	database string
	id       int64
}

func (c *lockConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not implemented") }
func (c *lockConn) Begin() (driver.Tx, error)           { return nil, errors.New("not implemented") }
func (c *lockConn) Close() error {
	c.s.mu.Lock()
	defer c.s.mu.Unlock()
	for k, id := range c.s.owners {
		if id == c.id {
			delete(c.s.owners, k)
		}
	}
	return nil
}
func (c *lockConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.s.mu.Lock()
	defer c.s.mu.Unlock()
	var values []driver.Value
	switch {
	case q == "SELECT DATABASE(), CONNECTION_ID()":
		values = []driver.Value{c.database, c.id}
	case q == "SELECT DATABASE()":
		values = []driver.Value{c.database}
	default:
		name := args[0].Value.(string)
		owner := c.s.owners[name]
		switch {
		case strings.Contains(q, "GET_LOCK"):
			var got int64
			if owner == 0 || owner == c.id {
				got = 1
				c.s.owners[name] = c.id
			}
			values = []driver.Value{got}
		case strings.Contains(q, "IS_USED_LOCK"):
			values = []driver.Value{nil}
			if owner != 0 {
				values[0] = owner
			}
		case strings.Contains(q, "RELEASE_LOCK"):
			var ok int64
			if owner == c.id {
				delete(c.s.owners, name)
				ok = 1
			}
			values = []driver.Value{ok}
		default:
			return nil, fmt.Errorf("unexpected query %s", q)
		}
	}
	return &oneRow{values: values}, nil
}

type oneRow struct {
	values []driver.Value
	done   bool
}

func (r *oneRow) Columns() []string {
	out := make([]string, len(r.values))
	for i := range out {
		out[i] = fmt.Sprint(i)
	}
	return out
}
func (r *oneRow) Close() error { return nil }
func (r *oneRow) Next(out []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	copy(out, r.values)
	return nil
}
func newLockDB(s *lockServer, name string) *DB { return &DB{db: sql.OpenDB(lockConnector{s, name})} }

func TestExecutionLockIsolationReleaseAndLoss(t *testing.T) {
	ctx := context.Background()
	server := &lockServer{owners: map[string]int64{}}
	a := newLockDB(server, "sp0")
	b := newLockDB(server, "sp0")
	other := newLockDB(server, "sp1")
	defer a.Close()
	defer b.Close()
	defer other.Close()
	l, err := a.Acquire(ctx, "purge")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Acquire(ctx, "verify"); err == nil {
		t.Fatal("concurrent command acquired same DB")
	}
	if _, err := other.Acquire(ctx, "purge"); err != nil {
		t.Fatal("different SP databases block one another:", err)
	}
	if err := a.CheckLock(ctx); err != nil {
		t.Fatal(err)
	}
	server.mu.Lock()
	delete(server.owners, lockName("sp0"))
	server.mu.Unlock()
	if err := a.CheckLock(ctx); err == nil {
		t.Fatal("ownership loss missed")
	}
	if context.Cause(l.Context()) == nil {
		t.Fatal("work context not cancelled")
	}
	if err := a.CheckLock(ctx); err == nil {
		t.Fatal("lock auto-reacquired")
	}
	l2, err := b.Acquire(ctx, "verify")
	if err != nil {
		t.Fatal(err)
	}
	if err := l2.Close(); err != nil {
		t.Fatal(err)
	}
	observer := newLockDB(server, "sp0")
	defer observer.Close()
	owner, err := observer.LockOwner(ctx)
	if err != nil || owner.Valid {
		t.Fatalf("lock not explicitly released: %v %v", owner, err)
	}
}

func TestOperatorCancellationStillAllowsBoundedFinalWriteCheck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	server := &lockServer{owners: map[string]int64{}}
	p := newLockDB(server, "sp0")
	defer p.Close()
	if _, err := p.Acquire(ctx, "purge"); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := p.CheckLock(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := p.CheckLock(context.Background()); err != nil {
		t.Fatalf("still-held lock must permit detached result recording: %v", err)
	}
}
