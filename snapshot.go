// STUB written by the integrator to freeze the contract; workstream C (durability) owns this file and
// replaces the bodies, keeping every exported signature.

package kvlite

import "errors"

// Snapshot is a consistent, sorted view of the keys live at the moment DB.Snapshot was called. It pins
// the segments it references by refcount, so a compaction that finishes later cannot delete them;
// leaking a Snapshot blocks space reclamation until Close.
type Snapshot struct {
	db *DB
}

// Iter walks a Snapshot in key order. Key is valid until the next call to Next; Value reads lazily from
// the pinned segment.
type Iter struct {
	s   *Snapshot
	err error
}

// Snapshot materialises a sorted copy of the keydir (skipping expired keys) and pins the referenced
// segments. Scans pay this O(keys) copy; point reads do not.
func (db *DB) Snapshot() (*Snapshot, error) {
	if err := db.guard(false); err != nil {
		return nil, err
	}
	return nil, errors.New("stub: kvlite.DB.Snapshot")
}

// Get returns a fresh copy of the value as of the snapshot, or ErrNotFound.
func (s *Snapshot) Get(key []byte) ([]byte, error) {
	if s.db == nil {
		return nil, ErrClosed
	}
	_ = key
	return nil, errors.New("stub: kvlite.Snapshot.Get")
}

// Len returns the number of keys in the snapshot.
func (s *Snapshot) Len() int { return 0 }

// Iter returns an iterator over every key starting with prefix, in key order. A nil or empty prefix is
// a full scan.
func (s *Snapshot) Iter(prefix []byte) *Iter {
	_ = prefix
	return &Iter{s: s, err: errors.New("stub: kvlite.Snapshot.Iter")}
}

// Range returns an iterator over keys in [lo, hi), in key order. A nil hi is unbounded.
func (s *Snapshot) Range(lo, hi []byte) *Iter {
	_, _ = lo, hi
	return &Iter{s: s, err: errors.New("stub: kvlite.Snapshot.Range")}
}

// Close releases the segment pins. It is idempotent.
func (s *Snapshot) Close() error {
	if s.db == nil {
		return nil
	}
	return errors.New("stub: kvlite.Snapshot.Close")
}

// Next advances the iterator and reports whether a key is available. After it returns false, check Err.
func (it *Iter) Next() bool {
	if it.s == nil {
		it.err = ErrClosed
	}
	return false
}

// Err returns the first error hit by Next or Value, if any.
func (it *Iter) Err() error { return it.err }

// Key returns the current key. The slice is valid until the next call to Next.
func (it *Iter) Key() []byte { return nil }

// Value reads the current value from the pinned segment and returns a fresh copy.
func (it *Iter) Value() ([]byte, error) {
	if it.err != nil {
		return nil, it.err
	}
	return nil, errors.New("stub: kvlite.Iter.Value")
}
