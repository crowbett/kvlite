package bench

import (
	"context"
	"errors"

	"github.com/crowbett/kvlite"
)

type kvliteStore struct {
	db *kvlite.DB
}

// OpenKVLite opens a kvlite store in dir with default options (SyncNever).
func OpenKVLite(dir string) (Store, error) {
	db, err := kvlite.Open(dir)
	if err != nil {
		return nil, err
	}
	return &kvliteStore{db: db}, nil
}

func (s *kvliteStore) Get(key []byte) ([]byte, error) {
	v, err := s.db.Get(key)
	if errors.Is(err, kvlite.ErrNotFound) {
		return nil, ErrNotFound
	}
	return v, err
}

func (s *kvliteStore) Put(key, value []byte) error       { return s.db.Put(key, value) }
func (s *kvliteStore) Delete(key []byte) error           { return s.db.Delete(key) }
func (s *kvliteStore) Close() error                      { return s.db.Close() }
func (s *kvliteStore) Compact(ctx context.Context) error { return s.db.Compact(ctx) }

func (s *kvliteStore) Scan(prefix []byte, fn func(k, v []byte) bool) error {
	snap, err := s.db.Snapshot()
	if err != nil {
		return err
	}
	defer func() { _ = snap.Close() }()
	it := snap.Iter(prefix)
	for it.Next() {
		v, err := it.Value()
		if err != nil {
			return err
		}
		if !fn(it.Key(), v) {
			break
		}
	}
	return it.Err()
}
