// STUB written by the integrator to freeze the contract; workstream C (durability) owns this file and
// replaces the bodies, keeping every exported signature.

package kvlite

import (
	"context"
	"errors"
)

// Compact merges every non-active segment into fresh ones, dropping tombstones, expired and superseded
// records, writes a .hint per output segment, swaps the MANIFEST atomically and re-points the keydir by
// CompareAndSwap on (segment, offset) so a key overwritten mid-merge is never rolled back. Cancelling
// ctx aborts before the MANIFEST swap and deletes the partial output; the old segment set stays
// authoritative. At most one compaction runs at a time. See docs/PLAN.md "Compaction algorithm".
func (db *DB) Compact(ctx context.Context) error {
	if err := db.guard(true); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.New("stub: kvlite.DB.Compact")
}
