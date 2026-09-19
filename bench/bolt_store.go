package bench

import (
	"bytes"
	"fmt"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

var boltBucket = []byte("kv")

type boltStore struct {
	db *bolt.DB
}

// OpenBolt opens (or creates) dir/bolt.db with one bucket. NoSync matches kvlite's default SyncNever
// (bbolt still writes through its B+tree and freelist on every commit; only the fsync is skipped).
func OpenBolt(dir string) (Store, error) {
	db, err := bolt.Open(filepath.Join(dir, "bolt.db"), 0o600, &bolt.Options{
		Timeout:        time.Second,
		NoSync:         true,
		NoFreelistSync: true,
		FreelistType:   bolt.FreelistMapType,
	})
	if err != nil {
		return nil, fmt.Errorf("bbolt: open: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(boltBucket)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("bbolt: create bucket: %w", err)
	}
	return &boltStore{db: db}, nil
}

func (s *boltStore) Get(key []byte) ([]byte, error) {
	var out []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(boltBucket).Get(key)
		if v == nil {
			return ErrNotFound
		}
		out = bytes.Clone(v) // v aliases the mmap and dies with the tx
		return nil
	})
	return out, err
}

func (s *boltStore) Put(key, value []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(boltBucket).Put(key, value)
	})
}

func (s *boltStore) Delete(key []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(boltBucket).Delete(key)
	})
}

func (s *boltStore) Scan(prefix []byte, fn func(k, v []byte) bool) error {
	return s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(boltBucket).Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			if !fn(k, v) {
				return nil
			}
		}
		return nil
	})
}

func (s *boltStore) Close() error { return s.db.Close() }
