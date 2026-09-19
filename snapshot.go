package kvlite

import (
	"bytes"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/crowbett/kvlite/internal/keydir"
	"github.com/crowbett/kvlite/internal/record"
	"github.com/crowbett/kvlite/internal/segment"
)

// Snapshot is a consistent, sorted view of the keys live at the moment DB.Snapshot was called. It pins
// the segments it references by refcount, so a compaction that finishes later cannot delete them;
// leaking a Snapshot blocks space reclamation until Close.
//
// Pinning: every reader in the table is Ref'd and the keydir is copied while the writer mutex is held,
// so no compaction can re-point an entry at a segment the snapshot did not pin (compaction publishes
// its outputs under that same mutex). A segment retired by a later compaction stays open, unrenamed
// and undeleted until the last pin drops (see DB.retireSegment); Close drops this snapshot's pins and
// reaps whatever became unpinned.
type Snapshot struct {
	db      *DB
	sorted  keydir.Sorted
	readers map[uint32]segReader
	closed  atomic.Bool
}

// Iter walks a Snapshot in key order. Key is valid until the next call to Next; Value reads lazily from
// the pinned segment.
type Iter struct {
	s   *Snapshot
	err error
	// i is the current index into s.sorted (-1 before the first Next), end one past the last.
	i, end int
}

var errNoCurrentKey = errors.New("kvlite: Iter.Value: no current key")

// Snapshot materialises a sorted copy of the keydir (skipping expired keys) and pins the referenced
// segments. Scans pay this O(keys) copy; point reads do not. The copy is taken under the writer mutex,
// so writers stall for its duration (the stated v1 trade-off in docs/PLAN.md).
func (db *DB) Snapshot() (*Snapshot, error) {
	if err := db.guard(false); err != nil {
		return nil, err
	}
	db.wmu.Lock()
	if db.closed.Load() {
		db.wmu.Unlock()
		return nil, ErrClosed
	}
	db.rmu.RLock()
	pinned := make(map[uint32]segReader, len(db.readers))
	for id, r := range db.readers {
		r.Ref()
		pinned[id] = r
	}
	db.rmu.RUnlock()
	sorted := db.kd.Snapshot(nil, db.now())
	db.snapshots.Add(1)
	db.wmu.Unlock()
	return &Snapshot{db: db, sorted: sorted, readers: pinned}, nil
}

// Get returns a fresh copy of the value as of the snapshot, or ErrNotFound.
func (s *Snapshot) Get(key []byte) ([]byte, error) {
	if s.db == nil || s.closed.Load() {
		return nil, ErrClosed
	}
	i := s.sorted.Search(key)
	if i >= s.sorted.Len() || !bytes.Equal(s.sorted.Key(i), key) {
		return nil, ErrNotFound
	}
	return s.read(s.sorted.Entry(i))
}

// read fetches the value behind e from the pinned segment. ReadAt allocates a fresh buffer per call,
// so the returned slice aliases nothing inside the store.
func (s *Snapshot) read(e keydir.Entry) ([]byte, error) {
	r := s.readers[e.Seg]
	if r == nil {
		return nil, &CorruptError{Seg: e.Seg, Off: int64(e.Off), Reason: "index points at a segment the snapshot did not pin"}
	}
	rec, _, err := r.ReadAt(e.Off, e.Size)
	if err != nil {
		return nil, readError(e, err)
	}
	return rec.Value, nil
}

// readError translates a failed ReadAt into the public error vocabulary: format findings become a
// CorruptError located at the entry (errors.Is ErrCorrupt), anything else (an I/O error) is wrapped as is.
// DB.Get and Snapshot reads share it.
func readError(e keydir.Entry, err error) error {
	switch {
	case errors.Is(err, record.ErrChecksum), errors.Is(err, record.ErrMalformed),
		errors.Is(err, record.ErrTooLarge), errors.Is(err, record.ErrTruncated), errors.Is(err, segment.ErrOutOfRange):
		return &CorruptError{Seg: e.Seg, Off: int64(e.Off), Reason: corruptReason(err), Err: err}
	default:
		return fmt.Errorf("kvlite: read segment %d at %d: %w", e.Seg, e.Off, err)
	}
}

// Len returns the number of keys in the snapshot.
func (s *Snapshot) Len() int { return s.sorted.Len() }

// Iter returns an iterator over every key starting with prefix, in key order. A nil or empty prefix is
// a full scan.
func (s *Snapshot) Iter(prefix []byte) *Iter {
	if s.db == nil || s.closed.Load() {
		return &Iter{s: s, err: ErrClosed}
	}
	lo := s.sorted.Search(prefix)
	hi := s.sorted.Len()
	if succ, ok := prefixSuccessor(prefix); ok {
		hi = s.sorted.Search(succ)
	}
	return &Iter{s: s, i: lo - 1, end: max(hi, lo)}
}

// Range returns an iterator over keys in [lo, hi), in key order. A nil hi is unbounded.
func (s *Snapshot) Range(lo, hi []byte) *Iter {
	if s.db == nil || s.closed.Load() {
		return &Iter{s: s, err: ErrClosed}
	}
	start := s.sorted.Search(lo)
	end := s.sorted.Len()
	if hi != nil {
		end = s.sorted.Search(hi)
	}
	return &Iter{s: s, i: start - 1, end: max(end, start)}
}

// prefixSuccessor returns the smallest key greater than every key with the given prefix: the prefix
// with its last non-0xFF byte incremented and the rest dropped. ok is false when no such key exists
// (empty prefix or all 0xFF), meaning the range is unbounded above.
func prefixSuccessor(prefix []byte) ([]byte, bool) {
	for i := len(prefix) - 1; i >= 0; i-- {
		if prefix[i] != 0xFF {
			succ := make([]byte, i+1)
			copy(succ, prefix[:i+1])
			succ[i]++
			return succ, true
		}
	}
	return nil, false
}

// Close releases the segment pins. It is idempotent.
func (s *Snapshot) Close() error {
	if s.db == nil || !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	for _, r := range s.readers {
		r.Unref()
	}
	s.db.snapshots.Add(-1)
	s.db.reapRetired()
	return nil
}

// Next advances the iterator and reports whether a key is available. After it returns false, check Err.
func (it *Iter) Next() bool {
	if it.err != nil {
		return false
	}
	if it.s == nil || it.s.db == nil || it.s.closed.Load() {
		it.err = ErrClosed
		return false
	}
	if it.i+1 >= it.end {
		it.i = it.end
		return false
	}
	it.i++
	return true
}

// Err returns the first error hit by Next or Value, if any.
func (it *Iter) Err() error { return it.err }

// Key returns the current key. The slice is valid until the next call to Next.
func (it *Iter) Key() []byte {
	if !it.valid() {
		return nil
	}
	return it.s.sorted.Key(it.i)
}

// Value reads the current value from the pinned segment and returns a fresh copy. A read failure is
// also recorded for Err and stops the iteration.
func (it *Iter) Value() ([]byte, error) {
	if it.err != nil {
		return nil, it.err
	}
	if it.s == nil || it.s.db == nil || it.s.closed.Load() {
		it.err = ErrClosed
		return nil, it.err
	}
	if !it.valid() {
		return nil, errNoCurrentKey
	}
	v, err := it.s.read(it.s.sorted.Entry(it.i))
	if err != nil {
		it.err = err
	}
	return v, err
}

func (it *Iter) valid() bool {
	return it.s != nil && it.err == nil && it.i >= 0 && it.i < it.end
}
