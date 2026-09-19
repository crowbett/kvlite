package kvlite

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/crowbett/kvlite/internal/keydir"
	"github.com/crowbett/kvlite/internal/manifest"
	"github.com/crowbett/kvlite/internal/record"
	"github.com/crowbett/kvlite/internal/segment"
)

// Compaction stages, in order, as reported to the test hook. A crash (simulated by the hook returning
// an error, which skips every cleanup below) at any of them leaves a consistent store; see Compact.
const (
	stageRotated  = "rotated"  // output ids reserved, active rotated, inputs pinned; nothing written yet
	stageMerged   = "merged"   // every output written, fsynced and hinted; MANIFEST still names the inputs
	stageAdopted  = "adopted"  // keydir re-pointed at the outputs (in memory only)
	stageSwapped  = "swapped"  // MANIFEST names the outputs; the inputs are still on disk
	stageReleased = "released" // inputs retired: deleted, or awaiting their last pin
)

// Auto-compactor knobs. Production uses the exported constants; tests lower them through the atomics
// so a loop iteration can be observed without waiting minutes or writing 64 MiB.
var (
	autoCompactTick    atomic.Int64
	autoCompactMinDead atomic.Int64
)

func init() {
	autoCompactTick.Store(int64(AutoCompactInterval))
	autoCompactMinDead.Store(AutoCompactMinDead)
}

// compactLocks serialises compactions per DB (one merge at a time, docs/PLAN.md). The DB struct has no
// slot for it (workstream B owns db.go); entries are dropped once the store is closed.
var compactLocks sync.Map // *DB -> *sync.Mutex

func (db *DB) compactMutex() *sync.Mutex {
	mu, _ := compactLocks.LoadOrStore(db, &sync.Mutex{})
	return mu.(*sync.Mutex) //nolint:errcheck // only *sync.Mutex is ever stored
}

// compaction is the state of one Compact call.
type compaction struct {
	db      *DB
	ctx     context.Context
	now     time.Time
	started time.Time

	inputs  []uint32    // merged segments, ascending
	pinned  []segReader // Ref'd for the duration of the merge
	nextID  uint32      // next reserved output id
	limitID uint32      // one past the last reserved id
	// outputs completed so far, their sizes, and the keydir entry each merged record came from, in
	// output order (adopt re-reads the keys from the outputs themselves, so only 16 bytes per move
	// are kept in memory).
	outputs []uint32
	sizes   []int64
	froms   []keydir.Entry
	// expired are current keys whose TTL has passed: dropped from the outputs, deleted from the keydir
	// at swap time if still unchanged.
	expired []expiredKey
	cur     segWriter
	buf     []byte
	inBytes int64
	skipped int
}

type expiredKey struct {
	key []byte
	e   keydir.Entry
}

// Compact merges every non-active segment into fresh ones, dropping tombstones, expired and superseded
// records, writes a .hint per output segment, swaps the MANIFEST atomically and re-points the keydir by
// CompareAndSwap on (segment, offset) so a key overwritten mid-merge is never rolled back. Cancelling
// ctx aborts before the MANIFEST swap and deletes the partial output; the old segment set stays
// authoritative. At most one compaction runs at a time. See docs/PLAN.md "Compaction algorithm".
//
// Crash safety, stage by stage (the MANIFEST is the only source of truth on disk):
//   - before "swapped": the MANIFEST still names the inputs, which are untouched; the outputs and their
//     hints are unlisted files that the next Open moves to trash/. The in-memory CAS (stage "adopted")
//     is never persisted, so it cannot be observed after a restart.
//   - "swapped" is one atomic rename of MANIFEST.tmp, fsynced beforehand together with every output
//     and hint; a restart sees either the old set or the new one, both complete.
//   - after "swapped": the inputs are unlisted and become the orphans the next Open trashes. Deleting
//     them is deferred while a snapshot pins them; a crash just defers it to the next Open.
//
// Ordering: output ids are reserved *before* the rotation, so they sort below the new active segment
// and every record they hold is older than everything the active segment receives; recovery's
// "ascending id = ascending write time" rule holds with or without hints.
//
// Concurrency: the merge runs without the writer lock. A key rewritten or deleted meanwhile makes its
// CompareAndSwap fail and the merged copy is dead bytes in the output; the newer record wins. Readers
// pin segments by refcount, so an input is closed and deleted only once its last pin drops.
func (db *DB) Compact(ctx context.Context) error {
	if err := db.guard(true); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	mu := db.compactMutex()
	mu.Lock()
	defer mu.Unlock()
	if db.closed.Load() {
		compactLocks.Delete(db)
		return ErrClosed
	}
	// Close cancels db.ctx; fold it into the caller's ctx so a compaction never outlives the store.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(db.ctx, cancel)
	defer stop()

	db.reapRetired()
	c := &compaction{db: db, ctx: ctx, now: db.now(), started: time.Now()}
	ok, err := c.begin()
	if err != nil || !ok {
		return err
	}
	if err := db.hook(stageRotated); err != nil {
		c.unpin()
		return err
	}

	if err := c.merge(); err != nil {
		c.discardOutputs()
		c.unpin()
		return err
	}
	if err := db.hook(stageMerged); err != nil {
		c.unpin()
		return err
	}

	if err := c.adopt(); err != nil {
		c.rollback()
		c.discardOutputs()
		c.unpin()
		return err
	}
	if err := db.hook(stageAdopted); err != nil {
		c.unpin()
		return err
	}

	if err := c.swap(); err != nil {
		c.rollback()
		c.discardOutputs()
		c.unpin()
		return err
	}
	if err := db.hook(stageSwapped); err != nil {
		c.unpin()
		return err
	}

	c.unpin()
	for _, id := range c.inputs {
		db.retireSegment(id)
	}
	db.sweepTrash()
	if err := db.hook(stageReleased); err != nil {
		return err
	}
	db.logger().Info("kvlite: compaction finished", "inputs", len(c.inputs), "outputs", len(c.outputs),
		"moved", len(c.froms), "expired", len(c.expired), "skipped", c.skipped, "dur", time.Since(c.started))
	return nil
}

// begin reserves the output ids, rotates the active segment and pins the inputs, all under the writer
// lock. ok is false when there is nothing to merge.
func (c *compaction) begin() (ok bool, err error) {
	db := c.db
	db.wmu.Lock()
	defer db.wmu.Unlock()
	if db.closed.Load() {
		return false, ErrClosed
	}
	if db.writeErr != nil {
		return false, db.writeErr
	}
	m := db.man
	if len(m.Segments) == 1 && db.active.Size() <= record.SegmentHeaderSize {
		return false, nil // an empty store: rotating would only add a segment
	}
	// Bound the number of outputs to reserve ids for: every completed output holds at least
	// maxSegmentBytes, so at most inputBytes/maxSegmentBytes + 1 are needed; one more is slack.
	var inputBytes int64
	for _, id := range m.Segments {
		if id == m.Active {
			inputBytes += db.active.Size()
			continue
		}
		inputBytes += db.segmentSize(id)
	}
	n := uint32(inputBytes/db.opts.maxSegmentBytes) + 2 //nolint:gosec // inputBytes/maxSegmentBytes is tiny
	base := db.allocIDLocked()
	if base > segment.MaxSize-n-1 {
		db.man.NextID = base
		return false, errors.New("kvlite: compact: segment ids exhausted")
	}
	db.man.NextID = base + n
	if err := db.rotateLocked(); err != nil {
		db.man.NextID = base
		return false, fmt.Errorf("kvlite: compact: %w", err)
	}
	c.nextID, c.limitID = base, base+n
	m = db.man
	c.inputs = slices.DeleteFunc(slices.Clone(m.Segments), func(id uint32) bool { return id == m.Active })
	c.inBytes = inputBytes
	for _, id := range c.inputs {
		r := db.readers[id] // wmu holder may read the table
		if r == nil {
			c.unpin()
			return false, &CorruptError{Seg: id, Reason: "segment not open"}
		}
		r.Ref()
		c.pinned = append(c.pinned, r)
	}
	return true, nil
}

// segmentSize stats a segment file; 0 when it cannot be read (the merge will report the real error).
func (db *DB) segmentSize(id uint32) int64 {
	st, err := os.Stat(filepath.Join(db.dir, segment.LogName(id)))
	if err != nil {
		return 0
	}
	return st.Size()
}

func (c *compaction) unpin() {
	for _, r := range c.pinned {
		r.Unref()
	}
	c.pinned = nil
}

// merge scans every input and copies each record that is still current into the outputs.
func (c *compaction) merge() error {
	for i, id := range c.inputs {
		if err := c.mergeSegment(id, c.pinned[i]); err != nil {
			return err
		}
	}
	return c.finishOutput()
}

func (c *compaction) mergeSegment(id uint32, r segReader) error {
	db := c.db
	n := 0
	fn := func(off uint32, rec record.Record) error {
		n++
		if n&127 == 0 {
			if err := c.ctx.Err(); err != nil {
				return err
			}
		}
		return c.mergeRecord(id, off, rec)
	}
	err := r.Scan(fn)
	for err != nil {
		var se *segment.ScanError
		if !errors.As(err, &se) {
			return fmt.Errorf("kvlite: compact segment %d: %w", id, err)
		}
		reason := corruptReason(se.Err)
		rp, ok := r.(repairer)
		if !db.opts.repair || !ok {
			return &CorruptError{Seg: id, Off: int64(se.Off), Reason: reason, Err: se.Err}
		}
		c.skipped++
		next, found, rerr := rp.Resync(se.Off + 1)
		if rerr != nil {
			return fmt.Errorf("kvlite: compact segment %d: %w", id, rerr)
		}
		db.logger().Warn("kvlite: compact: skipped corrupt bytes", "seg", id, "off", se.Off, "reason", reason, "found", found)
		if !found {
			return nil
		}
		err = rp.ScanFrom(next, fn)
	}
	return nil
}

// mergeRecord copies one input record when it is exactly what the keydir points at, is not a tombstone
// and has not expired. Batch flags are cleared: the commit already happened, so the copy stands alone.
func (c *compaction) mergeRecord(seg, off uint32, rec record.Record) error {
	db := c.db
	size := uint32(record.EncodedSize(rec)) //nolint:gosec // bounded by record.MaxRecord
	cur, ok := db.kd.Get(rec.Key)
	if !ok || cur.Seg != seg || cur.Off != off || cur.Size != size {
		return nil // superseded or deleted
	}
	if rec.Flags.Tombstone() {
		return nil // never in the keydir; defensive
	}
	if cur.Expired(c.now) {
		c.expired = append(c.expired, expiredKey{key: slices.Clone(rec.Key), e: cur})
		return nil
	}
	rec.Flags &^= record.FlagBatchMember | record.FlagBatchCommit
	enc, err := db.encode(c.buf[:0], rec)
	c.buf = enc
	if err != nil {
		return fmt.Errorf("kvlite: compact: encode: %w", err)
	}
	if c.cur != nil && c.cur.Size()+int64(len(enc)) > db.opts.maxSegmentBytes && c.cur.Size() > record.SegmentHeaderSize {
		if err := c.finishOutput(); err != nil {
			return err
		}
	}
	if c.cur == nil {
		if err := c.openOutput(); err != nil {
			return err
		}
	}
	if _, err := c.cur.Append(enc); err != nil {
		return fmt.Errorf("kvlite: compact: append to segment %d: %w", c.cur.ID(), err)
	}
	c.froms = append(c.froms, cur)
	return nil
}

func (c *compaction) openOutput() error {
	if c.nextID >= c.limitID {
		return errors.New("kvlite: compact: reserved segment ids exhausted")
	}
	w, err := c.db.createSegment(c.db.dir, c.nextID)
	if err != nil {
		return fmt.Errorf("kvlite: compact: %w", err)
	}
	c.nextID++
	c.cur = w
	return nil
}

// finishOutput fsyncs and closes the current output, then writes its hint by re-reading it (no per-key
// memory). An output that received no record is removed instead of published.
func (c *compaction) finishOutput() error {
	w := c.cur
	if w == nil {
		return nil
	}
	c.cur = nil
	id, size := w.ID(), w.Size()
	if err := w.Close(); err != nil {
		return fmt.Errorf("kvlite: compact: close segment %d: %w", id, err)
	}
	if size <= record.SegmentHeaderSize {
		_ = os.Remove(filepath.Join(c.db.dir, segment.LogName(id)))
		return nil
	}
	r, err := c.db.openSegment(c.db.dir, id)
	if err != nil {
		return fmt.Errorf("kvlite: compact: %w", err)
	}
	var scanErr error
	err = segment.WriteHint(c.db.dir, id, func(yield func(key []byte, e segment.HintEntry) bool) {
		scanErr = r.Scan(func(off uint32, rec record.Record) error {
			h := segment.HintEntry{Off: off, Size: uint32(record.EncodedSize(rec)), ExpSec: rec.ExpSec(), Flags: rec.Flags} //nolint:gosec // bounded
			if !yield(rec.Key, h) {
				return errHintStopped
			}
			return nil
		})
	})
	_ = r.Close()
	if err != nil {
		return fmt.Errorf("kvlite: compact: hint for segment %d: %w", id, err)
	}
	if scanErr != nil && !errors.Is(scanErr, errHintStopped) {
		return fmt.Errorf("kvlite: compact: verify segment %d: %w", id, scanErr)
	}
	c.outputs = append(c.outputs, id)
	c.sizes = append(c.sizes, size)
	return nil
}

var errHintStopped = errors.New("hint writer stopped")

// adopt registers a reader for every output and re-points the keydir at the merged copies, one
// CompareAndSwap per record. It re-reads the outputs for the keys; froms supplies what each record
// replaced, in the same order. A CAS that fails means the key was rewritten or deleted mid-merge.
func (c *compaction) adopt() error {
	db := c.db
	i := 0
	for _, id := range c.outputs {
		r, err := db.openSegment(db.dir, id)
		if err != nil {
			return fmt.Errorf("kvlite: compact: %w", err)
		}
		db.putReader(r)
		err = r.Scan(func(off uint32, rec record.Record) error {
			if i >= len(c.froms) {
				return errors.New("more records than moves")
			}
			from := c.froms[i]
			i++
			to := keydir.Entry{Seg: id, Off: off, Size: uint32(record.EncodedSize(rec)), ExpSec: from.ExpSec} //nolint:gosec // bounded
			db.kd.CompareAndSwap(rec.Key, from, to)
			return nil
		})
		if err != nil {
			return fmt.Errorf("kvlite: compact: adopt segment %d: %w", id, err)
		}
	}
	if i != len(c.froms) {
		return fmt.Errorf("kvlite: compact: adopted %d records, expected %d", i, len(c.froms))
	}
	return nil
}

// rollback undoes adopt after a failure: every entry still pointing at an output is pointed back at
// its input record. It is best effort; the inputs are intact and still named by the MANIFEST, so a
// restart would reach the same state anyway.
func (c *compaction) rollback() {
	db := c.db
	i := 0
	for _, id := range c.outputs {
		r := db.pinReader(id)
		if r == nil {
			continue
		}
		_ = r.Scan(func(off uint32, rec record.Record) error {
			if i >= len(c.froms) {
				return errHintStopped
			}
			from := c.froms[i]
			i++
			to := keydir.Entry{Seg: id, Off: off, Size: uint32(record.EncodedSize(rec)), ExpSec: from.ExpSec} //nolint:gosec // bounded
			db.kd.CompareAndSwap(rec.Key, to, from)
			return nil
		})
		r.Unref()
	}
}

// discardOutputs removes every output file (and hint) after an abort. Outputs already registered as
// readers are retired through the usual path so a snapshot that pinned one keeps working.
func (c *compaction) discardOutputs() {
	db := c.db
	if c.cur != nil {
		id := c.cur.ID()
		_ = c.cur.Close()
		c.cur = nil
		_ = os.Remove(filepath.Join(db.dir, segment.LogName(id)))
	}
	for _, id := range c.outputs {
		db.retireSegment(id)
	}
	c.outputs, c.sizes = nil, nil
	db.sweepTrash()
}

// swap publishes the outputs: under the writer lock the MANIFEST (gen+1) names the outputs plus every
// segment that is not an input (the active one and anything rotated during the merge), expired keys
// are dropped from the keydir, and the byte accounting is recomputed from the new set.
func (c *compaction) swap() error {
	db := c.db
	db.wmu.Lock()
	defer db.wmu.Unlock()
	if db.closed.Load() {
		return ErrClosed
	}
	old := db.man
	segs := slices.DeleteFunc(slices.Clone(old.Segments), func(id uint32) bool { return slices.Contains(c.inputs, id) })
	segs = append(segs, c.outputs...)
	slices.Sort(segs)
	m := &manifest.Manifest{Gen: old.Gen + 1, Active: old.Active, NextID: old.NextID, Segments: segs}
	if err := db.saveManifest(db.dir, m); err != nil {
		return fmt.Errorf("kvlite: compact: save MANIFEST: %w", err)
	}
	db.rmu.Lock()
	db.man = m
	db.rmu.Unlock()

	for _, ek := range c.expired {
		if e, ok := db.kd.Get(ek.key); ok && e == ek.e {
			db.kd.Delete(ek.key)
			db.liveBytes -= int64(e.Size)
		}
	}
	before := db.liveBytes + db.deadBytes
	var total int64
	for _, id := range segs {
		if id == m.Active {
			total += db.active.Size()
		} else {
			total += db.segmentSize(id)
		}
	}
	db.deadBytes = max(total-db.liveBytes, 0)
	db.recordCompaction(max(before-total, 0), c.started)
	return nil
}

// retireSegment removes id from the reader table's live set: closed, moved to trash/ and deleted at
// once when nothing pins it, otherwise left open (still reachable by the snapshots that pinned it)
// until reapRetired finds it unpinned. A reader whose id is not in the MANIFEST is retired by
// definition, so no extra bookkeeping is needed.
func (db *DB) retireSegment(id uint32) {
	db.rmu.Lock()
	r := db.readers[id]
	if r != nil && r.Refs() > 0 {
		db.rmu.Unlock()
		return
	}
	delete(db.readers, id)
	db.rmu.Unlock()
	if r != nil {
		_ = r.Close()
	}
	db.trash(segment.LogName(id))
	db.trash(segment.HintName(id))
}

// reapRetired closes and deletes every retired reader (in the table but not in the MANIFEST) whose last
// pin has dropped. Snapshot.Close, Compact and the auto-compactor tick call it.
func (db *DB) reapRetired() {
	var done []uint32
	db.rmu.Lock()
	m := db.man
	for id, r := range db.readers {
		if m != nil && slices.Contains(m.Segments, id) {
			continue
		}
		if r.Refs() > 0 {
			continue
		}
		delete(db.readers, id)
		_ = r.Close()
		done = append(done, id)
	}
	db.rmu.Unlock()
	for _, id := range done {
		db.trash(segment.LogName(id))
		db.trash(segment.HintName(id))
	}
}

// hook reports a compaction stage to the test hook. A non-nil error aborts Compact without cleanup.
func (db *DB) hook(stage string) error {
	testHooks.mu.RLock()
	fn := testHooks.compact
	testHooks.mu.RUnlock()
	if fn == nil {
		return nil
	}
	return fn(db, stage)
}

// shouldAutoCompact evaluates the WithAutoCompact trigger against the current Stats.
func (db *DB) shouldAutoCompact() bool {
	s := db.Stats()
	return s.DeadRatio >= db.opts.autoCompact && s.DeadBytes >= autoCompactMinDead.Load()
}

// autoCompactLoop is the background compactor: every tick it sweeps trash/, reaps retired readers and
// runs Compact when the dead-bytes trigger is met. A failed merge is logged and retried next tick. It
// exits when db.ctx is cancelled by Close.
func (db *DB) autoCompactLoop() {
	defer db.wg.Done()
	defer compactLocks.Delete(db)
	t := time.NewTicker(time.Duration(autoCompactTick.Load()))
	defer t.Stop()
	for {
		select {
		case <-db.ctx.Done():
			return
		case <-t.C:
			db.sweepTrash()
			db.reapRetired()
			if !db.shouldAutoCompact() {
				continue
			}
			if err := db.Compact(db.ctx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, ErrClosed) {
				db.logger().Warn("kvlite: auto-compaction failed", "err", err)
			}
		}
	}
}
