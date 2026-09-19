// STUB written by the integrator to freeze the contract; workstream B (engine) owns this file and replaces
// the bodies, keeping every exported signature.

// Package kvlite is an embedded bitcask-style key-value store: writes append to one active segment,
// every live key sits in an in-memory keydir, and a read is one map lookup plus one ReadAt.
//
// The public surface is DB (constructed by Open), Batch, Snapshot/Iter, Stats, the Option set and the
// sentinel errors. See docs/PLAN.md for the design (ADR-0001) and the crash-safety rules.
package kvlite

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// DB is an open store. It is safe for concurrent use: reads run on the caller's goroutine under a shard
// read lock, writes are serialised by a single writer mutex.
type DB struct {
	dir  string
	opts options

	// wmu serialises appends so offsets are assigned without CAS games.
	wmu    sync.Mutex
	closed atomic.Bool
}

// guard returns ErrClosed once Close has been called, ErrReadOnly for a mutation on a read-only DB.
func (db *DB) guard(mutating bool) error {
	if db.closed.Load() {
		return ErrClosed
	}
	if mutating && db.opts.readOnly {
		return ErrReadOnly
	}
	return nil
}

// Get returns a fresh copy of the value for key (owner decision: copy, not a pooled view), or ErrNotFound
// when the key is absent or its TTL has passed.
func (db *DB) Get(key []byte) ([]byte, error) {
	if err := db.guard(false); err != nil {
		return nil, err
	}
	_ = key
	return nil, errors.New("stub: kvlite.DB.Get")
}

// Has reports whether key is present and not expired, without reading the value.
func (db *DB) Has(key []byte) (bool, error) {
	if err := db.guard(false); err != nil {
		return false, err
	}
	_ = key
	return false, errors.New("stub: kvlite.DB.Has")
}

// Put stores value under key with no expiry. It validates ErrEmptyKey/ErrKeyTooLarge/ErrValueTooLarge
// before touching the log.
func (db *DB) Put(key, value []byte) error {
	if err := db.guard(true); err != nil {
		return err
	}
	_, _ = key, value
	return errors.New("stub: kvlite.DB.Put")
}

// PutTTL stores value under key, expiring ttl after the clock's now. ttl <= 0 returns ErrInvalidTTL.
// Expiry is best-effort wall clock, rounded to whole seconds.
func (db *DB) PutTTL(key, value []byte, ttl time.Duration) error {
	if err := db.guard(true); err != nil {
		return err
	}
	if ttl <= 0 {
		return ErrInvalidTTL
	}
	_, _ = key, value
	return errors.New("stub: kvlite.DB.PutTTL")
}

// Delete removes key. It always appends a tombstone so the delete survives a crash; deleting an absent
// key is not an error.
func (db *DB) Delete(key []byte) error {
	if err := db.guard(true); err != nil {
		return err
	}
	_ = key
	return errors.New("stub: kvlite.DB.Delete")
}

// Batch starts an atomic write group. Nothing is written until Commit.
func (db *DB) Batch() *Batch {
	return &Batch{db: db}
}

// Stats returns a snapshot of the counters.
func (db *DB) Stats() Stats {
	db.wmu.Lock()
	defer db.wmu.Unlock()
	return Stats{}
}

// Sync fsyncs the active segment regardless of the sync policy.
func (db *DB) Sync() error {
	if err := db.guard(true); err != nil {
		return err
	}
	return errors.New("stub: kvlite.DB.Sync")
}

// Close stops the background goroutines, syncs, waits up to 5 s for open snapshots, closes every segment
// and releases the LOCK. Every later call returns ErrClosed.
func (db *DB) Close() error {
	if !db.closed.CompareAndSwap(false, true) {
		return ErrClosed
	}
	return errors.New("stub: kvlite.DB.Close")
}
