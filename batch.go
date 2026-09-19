// STUB written by the integrator to freeze the contract; workstream B (engine) owns this file and replaces
// the bodies, keeping every exported signature.

package kvlite

import (
	"errors"
	"time"
)

// Batch accumulates puts and deletes that Commit applies atomically: one contiguous record group tagged
// batchMember plus a trailing batchCommit marker, never spanning two segments. A crash before the marker
// lands leaves members that recovery discards. A Batch is not safe for concurrent use.
type Batch struct {
	db  *DB
	ops []batchOp
}

type batchOp struct {
	key, value []byte
	ttl        time.Duration
	delete     bool
}

// Put queues a write. It returns b for chaining.
func (b *Batch) Put(key, value []byte) *Batch {
	b.ops = append(b.ops, batchOp{key: key, value: value})
	return b
}

// PutTTL queues a write with a TTL; ttl <= 0 makes Commit return ErrInvalidTTL.
func (b *Batch) PutTTL(key, value []byte, ttl time.Duration) *Batch {
	b.ops = append(b.ops, batchOp{key: key, value: value, ttl: ttl})
	return b
}

// Delete queues a tombstone.
func (b *Batch) Delete(key []byte) *Batch {
	b.ops = append(b.ops, batchOp{key: key, delete: true})
	return b
}

// Len returns the number of queued operations.
func (b *Batch) Len() int { return len(b.ops) }

// Commit validates every queued operation, appends the group under the writer lock, syncs per policy and
// applies the keys to the keydir. An empty batch commits nothing and returns nil. After Commit the batch
// is empty and may be reused.
func (b *Batch) Commit() error {
	if err := b.db.guard(true); err != nil {
		return err
	}
	if len(b.ops) == 0 {
		return nil
	}
	return errors.New("stub: kvlite.Batch.Commit")
}
