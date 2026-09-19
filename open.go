package kvlite

import (
	"errors"
	"sync"
)

// testHooks are seams for the crash and recovery tests in this package. They are nil in production;
// every call site reads them under the lock so tests can install and remove them under -race.
var testHooks struct {
	mu sync.RWMutex
	// compact is called at every compaction stage; a non-nil error makes Compact return at once with
	// no cleanup, which is how the tests simulate a crash at that point.
	compact func(db *DB, stage string) error
	// recovery is called once per segment loaded by Open with whether the hint fast path was used.
	recovery func(db *DB, seg uint32, viaHint bool)
}

// Open opens or creates the store in dir, applying opts in order. It acquires dir/LOCK (ErrLocked on
// contention, skipped WithReadOnly), sweeps trash/, reads the MANIFEST (rebuilt WithRepair, else ErrCorrupt
// when bad), moves unlisted segments to trash/, loads the keydir hint-first, truncates a torn tail on the
// active segment, and starts the flusher and auto-compactor. See docs/PLAN.md "Open / recovery procedure".
func Open(dir string, opts ...Option) (*DB, error) {
	if dir == "" {
		return nil, errors.New("kvlite: Open: empty dir")
	}
	o := defaultOptions()
	if err := o.apply(opts); err != nil {
		return nil, err
	}
	db := newDB(dir, o)
	if err := db.recover(); err != nil {
		db.abortOpen()
		return nil, err
	}
	db.startBackground()
	return db, nil
}

// abortOpen releases whatever recover acquired before it failed. The DB is never handed out.
func (db *DB) abortOpen() {
	db.closed.Store(true)
	db.cancel()
	if db.active != nil {
		_ = db.active.Close()
	}
	db.rmu.Lock()
	for id, r := range db.readers {
		_ = r.Close()
		delete(db.readers, id)
	}
	db.rmu.Unlock()
	if db.lock != nil {
		_ = db.lock.Release()
		db.lock = nil
	}
}

// startBackground is step 6 of Open: the interval flusher under SyncInterval (db.start) and the
// auto-compactor when WithAutoCompact is set. Neither runs on a read-only store. Both stop when db.ctx
// is cancelled by Close and signal db.wg when done.
func (db *DB) startBackground() {
	db.start()
	if db.opts.autoCompact > 0 && !db.opts.readOnly {
		db.wg.Add(1)
		go db.autoCompactLoop()
	}
}
