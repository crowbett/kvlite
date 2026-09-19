// STUB written by the integrator to freeze the contract; workstream D (cli + bench + fuzz) owns this file
// and replaces the bodies, keeping every exported signature.

package bench

import (
	"errors"

	bolt "go.etcd.io/bbolt"
)

type boltStore struct {
	db *bolt.DB
}

// OpenBolt opens (or creates) dir/bolt.db with one bucket, NoSync to match kvlite's SyncNever.
func OpenBolt(dir string) (Store, error) {
	_ = dir
	return nil, errors.New("stub: bench.OpenBolt")
}

func (s *boltStore) Get(key []byte) ([]byte, error) {
	_, _ = s.db, key
	return nil, errors.New("stub: bench.boltStore.Get")
}

func (s *boltStore) Put(key, value []byte) error {
	_, _ = key, value
	return errors.New("stub: bench.boltStore.Put")
}

func (s *boltStore) Delete(key []byte) error {
	_ = key
	return errors.New("stub: bench.boltStore.Delete")
}

func (s *boltStore) Scan(prefix []byte, fn func(k, v []byte) bool) error {
	_, _ = prefix, fn
	return errors.New("stub: bench.boltStore.Scan")
}

func (s *boltStore) Close() error { return errors.New("stub: bench.boltStore.Close") }
