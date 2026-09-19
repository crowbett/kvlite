// STUB written by the integrator to freeze the contract; workstream D (cli + bench + fuzz) owns this file
// and replaces the bodies, keeping every exported signature. The kvlite adapter is pure delegation and
// works as soon as the root package does.

package bench

import (
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

func (s *kvliteStore) Get(key []byte) ([]byte, error) { return s.db.Get(key) }
func (s *kvliteStore) Put(key, value []byte) error    { return s.db.Put(key, value) }
func (s *kvliteStore) Delete(key []byte) error        { return s.db.Delete(key) }
func (s *kvliteStore) Close() error                   { return s.db.Close() }

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
