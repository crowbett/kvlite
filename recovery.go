// STUB written by the integrator to freeze the contract; workstream C (durability) owns this file and
// replaces the bodies, keeping every exported signature. Everything here is unexported: WS-C is free to
// reshape it as long as Open's documented behaviour holds.

package kvlite

import "errors"

// recover runs steps 1-6 of the Open procedure against db.dir: lock, manifest, orphan sweep, hint-first
// keydir load with tail truncation and WithRepair skipping, then opens the active segment for append.
func (db *DB) recover() error {
	if db.dir == "" {
		return errors.New("kvlite: recover: empty dir")
	}
	return errors.New("stub: kvlite.recover")
}
