// Workstream B (engine) owns this file: Batch, the atomic write group.

package kvlite

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/crowbett/kvlite/internal/keydir"
	"github.com/crowbett/kvlite/internal/record"
)

// Batch accumulates puts and deletes that Commit applies atomically: one contiguous record group tagged
// batchMember plus a trailing batchCommit marker, never spanning two segments. A crash before the marker
// lands leaves members that recovery discards. A Batch is not safe for concurrent use. Key and value
// slices are retained (not copied) until Commit or Discard.
type Batch struct {
	db  *DB
	ops []batchOp
}

type batchOp struct {
	key, value []byte
	ttl        time.Duration
	hasTTL     bool // PutTTL was used; ttl <= 0 is then ErrInvalidTTL at Commit
	delete     bool
}

// Put queues a write. It returns b for chaining.
func (b *Batch) Put(key, value []byte) *Batch {
	b.ops = append(b.ops, batchOp{key: key, value: value})
	return b
}

// PutTTL queues a write with a TTL; ttl <= 0 makes Commit return ErrInvalidTTL.
func (b *Batch) PutTTL(key, value []byte, ttl time.Duration) *Batch {
	b.ops = append(b.ops, batchOp{key: key, value: value, ttl: ttl, hasTTL: true})
	return b
}

// Delete queues a tombstone.
func (b *Batch) Delete(key []byte) *Batch {
	b.ops = append(b.ops, batchOp{key: key, delete: true})
	return b
}

// Len returns the number of queued operations.
func (b *Batch) Len() int { return len(b.ops) }

// Discard drops every queued operation without writing anything. The batch may be reused.
func (b *Batch) Discard() {
	clear(b.ops)
	b.ops = b.ops[:0]
}

// Commit validates every queued operation, appends the group under the writer lock, syncs per policy and
// applies the keys to the keydir. An empty batch commits nothing and returns nil. After Commit the batch
// is empty and may be reused.
//
// On disk the group is members (FlagBatchMember, plus FlagTombstone/FlagExpiry as needed) followed by
// one FlagBatchCommit marker whose value is uvarint(count) uvarint(batchID). The whole group must fit in
// one segment: the active segment is rotated first if the group would not fit in it, and a group larger
// than a whole segment fails with an error wrapping ErrValueTooLarge. The keydir is updated in a single
// keydir.Apply, so readers see all of the batch or none of it; a failure before that point (including a
// SyncAlways fsync failure) leaves the keydir untouched even though bytes may already be in the log,
// exactly like a crash at that moment.
func (b *Batch) Commit() error {
	db := b.db
	if err := db.guard(true); err != nil {
		return err
	}
	if len(b.ops) == 0 {
		return nil
	}
	for i := range b.ops {
		op := &b.ops[i]
		if err := db.checkKey(op.key); err != nil {
			return err
		}
		if op.delete {
			continue
		}
		if err := db.checkValue(op.value); err != nil {
			return err
		}
		if op.hasTTL && op.ttl <= 0 {
			return ErrInvalidTTL
		}
	}

	db.wmu.Lock()
	defer db.wmu.Unlock()
	if err := db.writableLocked(); err != nil {
		return err
	}
	now := db.now()

	// Encode the whole group into the scratch buffer, remembering each record's size.
	buf := db.buf[:0]
	sizes := make([]int, len(b.ops))
	ups := make([]keydir.Update, len(b.ops))
	var err error
	for i := range b.ops {
		op := &b.ops[i]
		rec := record.Record{Key: op.key, TS: now, Flags: record.FlagBatchMember}
		u := &ups[i]
		u.Key = op.key
		if op.delete {
			rec.Flags |= record.FlagTombstone
			u.Delete = true
		} else {
			rec.Value = op.value
			if op.hasTTL {
				u.Entry.ExpSec = expSec(now.Add(op.ttl))
				rec.Expiry = time.Unix(int64(u.Entry.ExpSec), 0)
				rec.Flags |= record.FlagExpiry
			}
		}
		start := len(buf)
		if buf, err = db.encode(buf, rec); err != nil {
			db.buf = buf
			return fmt.Errorf("kvlite: encode batch member %d: %w", i, err)
		}
		sizes[i] = len(buf) - start
	}
	db.batchSeq++
	marker := record.Record{TS: now, Flags: record.FlagBatchCommit}
	marker.Value = binary.AppendUvarint(nil, uint64(len(b.ops)))
	marker.Value = binary.AppendUvarint(marker.Value, db.batchSeq)
	if buf, err = db.encode(buf, marker); err != nil {
		db.buf = buf
		return fmt.Errorf("kvlite: encode batch commit marker: %w", err)
	}
	db.buf = buf
	defer db.releaseScratch()

	// Group-fits-in-segment rule.
	total := int64(len(buf))
	if total > db.opts.maxSegmentBytes-record.SegmentHeaderSize {
		return fmt.Errorf("kvlite: batch of %d bytes cannot fit in one segment of %d bytes: %w",
			total, db.opts.maxSegmentBytes, ErrValueTooLarge)
	}
	if db.active.Size()+total > db.opts.maxSegmentBytes && db.active.Size() > record.SegmentHeaderSize {
		if err := db.rotateLocked(); err != nil {
			return err
		}
	}

	// Append the members and the marker back to back. wmu is held, so nothing interleaves.
	seg := db.active.ID()
	pos := 0
	for i := range ups {
		off, err := db.active.Append(buf[pos : pos+sizes[i]])
		if err != nil {
			db.writeErr = fmt.Errorf("kvlite: append batch member to segment %d: %w", seg, err)
			return db.writeErr
		}
		u := &ups[i]
		u.Entry.Seg, u.Entry.Off, u.Entry.Size = seg, off, uint32(sizes[i]) //nolint:gosec // bounded by MaxRecord
		pos += sizes[i]
	}
	if _, err := db.active.Append(buf[pos:]); err != nil {
		db.writeErr = fmt.Errorf("kvlite: append batch commit marker to segment %d: %w", seg, err)
		return db.writeErr
	}
	db.dirty.Store(true)
	if err := db.afterWriteLocked(); err != nil {
		return err
	}

	// Publish: one atomic keydir step, then the accounting.
	db.kd.Apply(ups)
	var puts, deletes uint64
	for i := range ups {
		u := &ups[i]
		if u.Existed {
			db.liveBytes -= int64(u.Old.Size)
			db.deadBytes += int64(u.Old.Size)
		}
		if u.Delete {
			db.deadBytes += int64(sizes[i])
			deletes++
		} else {
			db.liveBytes += int64(u.Entry.Size)
			puts++
		}
	}
	db.deadBytes += total - int64(pos) // the commit marker
	db.puts.Add(puts)
	db.deletes.Add(deletes)
	b.Discard()
	return nil
}
