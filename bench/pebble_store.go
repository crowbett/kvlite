package bench

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
)

type pebbleStore struct {
	db     *pebble.DB
	closed bool
}

// OpenPebble opens (or creates) a pebble store in dir with default options and pebble.NoSync writes,
// which matches kvlite's SyncNever: the WAL is still appended, only the fsync is skipped.
func OpenPebble(dir string) (Store, error) {
	db, err := pebble.Open(dir, &pebble.Options{
		// Quiet: the default logger prints to stderr during the benchmark.
		Logger: quietLogger{},
	})
	if err != nil {
		return nil, fmt.Errorf("pebble: open: %w", err)
	}
	return &pebbleStore{db: db}, nil
}

// quietLogger drops pebble's info logs; Fatalf still aborts, as pebble requires.
type quietLogger struct{}

func (quietLogger) Infof(string, ...any) {}
func (quietLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf("pebble: "+format, args...))
}

func (s *pebbleStore) Get(key []byte) ([]byte, error) {
	v, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	out := bytes.Clone(v) // v is only valid until closer.Close
	return out, closer.Close()
}

func (s *pebbleStore) Put(key, value []byte) error { return s.db.Set(key, value, pebble.NoSync) }
func (s *pebbleStore) Delete(key []byte) error     { return s.db.Delete(key, pebble.NoSync) }

// Close is idempotent: pebble routes a second Close through Logger.Fatalf, which the other engines
// (and Store's users) do not expect.
func (s *pebbleStore) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.db.Close()
}

// Compact flushes the memtable and compacts the whole key space so the on-disk size reflects live
// data, the same thing kvlite's Compact does.
func (s *pebbleStore) Compact(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.db.Flush(); err != nil {
		return err
	}
	return s.db.Compact([]byte{0}, bytes.Repeat([]byte{0xff}, 32), true)
}

func (s *pebbleStore) Scan(prefix []byte, fn func(k, v []byte) bool) error {
	opts := &pebble.IterOptions{}
	if len(prefix) > 0 {
		opts.LowerBound = prefix
		opts.UpperBound = PrefixUpperBound(prefix)
	}
	it, err := s.db.NewIter(opts)
	if err != nil {
		return err
	}
	for ok := it.First(); ok; ok = it.Next() {
		if !fn(it.Key(), it.Value()) {
			break
		}
	}
	if err := it.Error(); err != nil {
		_ = it.Close()
		return err
	}
	return it.Close()
}
