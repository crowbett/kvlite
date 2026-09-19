// STUB written by the integrator to freeze the contract; workstream D (cli + bench + fuzz) owns this file
// and replaces the bodies, keeping every exported signature.

package bench

import (
	"errors"

	"github.com/cockroachdb/pebble"
)

type pebbleStore struct {
	db *pebble.DB
}

// OpenPebble opens (or creates) a pebble store in dir with pebble.NoSync writes to match SyncNever.
func OpenPebble(dir string) (Store, error) {
	_ = dir
	return nil, errors.New("stub: bench.OpenPebble")
}

func (s *pebbleStore) Get(key []byte) ([]byte, error) {
	_, _ = s.db, key
	return nil, errors.New("stub: bench.pebbleStore.Get")
}

func (s *pebbleStore) Put(key, value []byte) error {
	_, _ = key, value
	return errors.New("stub: bench.pebbleStore.Put")
}

func (s *pebbleStore) Delete(key []byte) error {
	_ = key
	return errors.New("stub: bench.pebbleStore.Delete")
}

func (s *pebbleStore) Scan(prefix []byte, fn func(k, v []byte) bool) error {
	_, _ = prefix, fn
	return errors.New("stub: bench.pebbleStore.Scan")
}

func (s *pebbleStore) Close() error { return errors.New("stub: bench.pebbleStore.Close") }
