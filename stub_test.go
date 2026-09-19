package kvlite

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestStubContract pins the parts of the contract that hold before the workstreams land: option
// validation happens in Open, ErrClosed/ErrReadOnly/ErrInvalidTTL are checked before any I/O, and every
// unfinished body reports itself as a stub instead of panicking.
func TestStubContract(t *testing.T) {
	t.Parallel()

	t.Run("open validates options", func(t *testing.T) {
		t.Parallel()
		if _, err := Open(t.TempDir(), WithShards(3)); err == nil {
			t.Fatal("Open(WithShards(3)) = nil error, want validation error")
		}
		if _, err := Open(""); err == nil {
			t.Fatal("Open(\"\") = nil error")
		}
	})

	t.Run("closed db", func(t *testing.T) {
		t.Parallel()
		db := &DB{dir: "x", opts: defaultOptions()}
		if err := db.Close(); err == nil || !strings.HasPrefix(err.Error(), "stub:") {
			t.Fatalf("first Close() = %v, want stub error", err)
		}
		if err := db.Close(); !errors.Is(err, ErrClosed) {
			t.Fatalf("second Close() = %v, want ErrClosed", err)
		}
		if _, err := db.Get([]byte("k")); !errors.Is(err, ErrClosed) {
			t.Errorf("Get on closed = %v, want ErrClosed", err)
		}
		if err := db.Put([]byte("k"), nil); !errors.Is(err, ErrClosed) {
			t.Errorf("Put on closed = %v, want ErrClosed", err)
		}
		if err := db.Batch().Put([]byte("k"), nil).Commit(); !errors.Is(err, ErrClosed) {
			t.Errorf("Commit on closed = %v, want ErrClosed", err)
		}
		if _, err := db.Snapshot(); !errors.Is(err, ErrClosed) {
			t.Errorf("Snapshot on closed = %v, want ErrClosed", err)
		}
	})

	t.Run("read only", func(t *testing.T) {
		t.Parallel()
		db := &DB{dir: "x", opts: defaultOptions()}
		db.opts.readOnly = true
		if err := db.Delete([]byte("k")); !errors.Is(err, ErrReadOnly) {
			t.Errorf("Delete read-only = %v, want ErrReadOnly", err)
		}
		if err := db.Compact(context.Background()); !errors.Is(err, ErrReadOnly) {
			t.Errorf("Compact read-only = %v, want ErrReadOnly", err)
		}
	})

	t.Run("invalid ttl", func(t *testing.T) {
		t.Parallel()
		db := &DB{dir: "x", opts: defaultOptions()}
		if err := db.PutTTL([]byte("k"), nil, 0); !errors.Is(err, ErrInvalidTTL) {
			t.Errorf("PutTTL(0) = %v, want ErrInvalidTTL", err)
		}
	})

	t.Run("batch accumulates", func(t *testing.T) {
		t.Parallel()
		db := &DB{dir: "x", opts: defaultOptions()}
		b := db.Batch().Put([]byte("a"), []byte("1")).PutTTL([]byte("b"), nil, time.Second).Delete([]byte("c"))
		if b.Len() != 3 {
			t.Errorf("Len() = %d, want 3", b.Len())
		}
		if err := db.Batch().Commit(); err != nil {
			t.Errorf("empty Commit() = %v, want nil", err)
		}
	})

	t.Run("compact honours ctx", func(t *testing.T) {
		t.Parallel()
		db := &DB{dir: "x", opts: defaultOptions()}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := db.Compact(ctx); !errors.Is(err, context.Canceled) {
			t.Errorf("Compact(cancelled) = %v, want context.Canceled", err)
		}
	})

	t.Run("iter on nil snapshot", func(t *testing.T) {
		t.Parallel()
		it := (&Snapshot{}).Iter(nil)
		if it.Next() {
			t.Error("Next() = true on stub")
		}
		if it.Err() == nil {
			t.Error("Err() = nil, want stub error")
		}
	})
}
