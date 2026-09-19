// STUB written by the integrator to freeze the contract; workstream C (durability) owns this file and
// replaces the bodies, keeping every exported signature.

package kvlite

import "errors"

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
	db := &DB{dir: dir, opts: o}
	if err := db.recover(); err != nil {
		return nil, err
	}
	return db, nil
}
