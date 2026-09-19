// Package kvlite is an embedded bitcask-style key-value store: writes append to one active segment,
// every live key sits in an in-memory keydir, and a read is one map lookup plus one ReadAt.
//
// The public surface is DB (constructed by Open), Batch, Snapshot/Iter, Stats, the Option set and the
// sentinel errors. See docs/PLAN.md for the design (ADR-0001) and the crash-safety rules.
//
// Workstream B (engine) owns this file: the DB struct, the writer path (Put/PutTTL/Delete, rotation,
// sync policies, the interval flusher), the read path (Get/Has), Stats, Sync and Close. Open/recovery,
// Snapshot and Compact live in open.go, recovery.go, snapshot.go and compact.go (workstream C) and build
// on the fields and seams declared here:
//
//   - newDB(dir, opts) allocates a DB (keydir, reader table, seams, background context); Open calls it
//     first, then either recover() on an existing directory or initEmpty() on a fresh one, then start().
//   - db.kd (index), db.readers/db.active/db.man under db.rmu, db.lock, db.liveBytes/db.deadBytes/
//     db.repairs under db.wmu are what recovery fills in.
//   - rotateLocked() is the only place the write path creates a segment and publishes a MANIFEST;
//     allocIDLocked() hands out segment ids to compaction. Both require db.wmu.
//   - pinReader(seg) is the read-side pin discipline: a reader whose Refs() > 0 must not be closed;
//     retire a reader only under db.rmu.Lock after checking Refs() == 0.
//   - db.ctx/db.wg are the lifetime of every background goroutine; Close cancels and waits. Open's
//     startBackground launches flushLoop and the auto-compactor with db.wg.Add(1) each.
//   - recordCompaction(reclaimed, started) updates the compaction counters for Stats.
//   - createSegment/openSegment/saveManifest/encode are seams over internal/segment, internal/manifest
//     and internal/record so tests (and fault injection) can substitute them.
package kvlite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/crowbett/kvlite/internal/fslock"
	"github.com/crowbett/kvlite/internal/keydir"
	"github.com/crowbett/kvlite/internal/manifest"
	"github.com/crowbett/kvlite/internal/record"
	"github.com/crowbett/kvlite/internal/segment"
)

const (
	// closeGrace is how long Close waits for open snapshots (and in-flight reads) to release their
	// segment pins before force-closing the files.
	closeGrace = 5 * time.Second
	// maxScratch caps the encode buffer kept between writes; a bigger one (a large value or batch) is
	// dropped after use so a single big write does not pin its size forever.
	maxScratch = 1 << 20
	// readRetries is how many times Get re-resolves a key whose segment was retired between the keydir
	// lookup and the pin (compaction re-pointed it in between).
	readRetries = 3
)

// index is the seam in front of the keydir so an ordered structure can replace it (P16) without
// touching DB. *keydir.Keydir is the only implementation in v1.
type index interface {
	Get(key []byte) (keydir.Entry, bool)
	Put(key []byte, e keydir.Entry) (old keydir.Entry, existed bool)
	Delete(key []byte) (old keydir.Entry, existed bool)
	CompareAndSwap(key []byte, from, to keydir.Entry) bool
	Apply(ups []keydir.Update)
	Snapshot(prefix []byte, now time.Time) keydir.Sorted
	Len() int
	Bytes() int64
}

// segWriter is the narrow view of *segment.Writer the write path needs.
type segWriter interface {
	ID() uint32
	Append(rec []byte) (off uint32, err error)
	Size() int64
	Sync() error
	Close() error
}

// segReader is the narrow view of *segment.Reader the read path, snapshots and compaction need.
type segReader interface {
	ID() uint32
	ReadAt(off, size uint32) (record.Record, []byte, error)
	Scan(fn func(off uint32, rec record.Record) error) error
	Ref()
	Unref()
	Refs() int
	Close() error
}

// DB is an open store. It is safe for concurrent use: reads run on the caller's goroutine under a shard
// read lock, writes are serialised by a single writer mutex.
type DB struct {
	dir  string
	opts options

	// wmu serialises appends so offsets are assigned without CAS games. It also guards the byte
	// accounting, the sticky errors, the encode scratch and the compaction stamps.
	wmu    sync.Mutex
	closed atomic.Bool

	// kd is the keydir. Reads take a shard read lock; every mutation happens under wmu except
	// recovery (single-threaded) and the compaction CAS (also under wmu).
	kd index

	// rmu guards the segment set for readers. Every mutation of readers/active/man happens under wmu
	// as well, so a wmu holder may read them without rmu; anyone else takes rmu.RLock. The table holds
	// the segReader interface (a *segment.Reader in production, a fake or fault injector in tests), so
	// code that copies entries out of it declares them as segReader too.
	rmu     sync.RWMutex
	readers map[uint32]segReader
	active  segWriter
	man     *manifest.Manifest
	// snapshots counts open Snapshots (snapshot.go maintains it).
	snapshots atomic.Int32

	// lock is the directory LOCK; nil when opened WithReadOnly.
	lock *fslock.Lock

	// ctx/cancel/wg are the lifetime of the background goroutines (flusher, auto-compactor). The
	// context lives in the struct on purpose: it is Close's signal, not a request scope. Each
	// goroutine is wg.Add(1)'d by its launcher and calls wg.Done() itself.
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Write-path state under wmu.
	buf      []byte // encode scratch
	syncErr  error  // sticky background-sync failure, cleared by a successful Sync
	writeErr error  // a failed Append poisons the store until it is reopened
	batchSeq uint64 // commit-marker id, unique within one open
	dirty    atomic.Bool

	// Byte accounting under wmu. TotalBytes = liveBytes + deadBytes; DeadBytes covers superseded
	// values, tombstones, commit markers, expired-at-load records and segment prologues.
	liveBytes int64
	deadBytes int64
	// repairs counts records WithRepair skipped at Open (set by recovery).
	repairs int

	// Compaction stamps, under wmu.
	reclaimedBytes    int64
	compactions       uint64
	lastCompaction    time.Time
	lastCompactionDur time.Duration

	// Read-path counters, lock-free.
	puts, deletes, gets, hits, misses, expiredHits, syncs atomic.Uint64

	// Seams over the format packages; newDB installs the real ones.
	createSegment func(dir string, id uint32) (segWriter, error)
	openSegment   func(dir string, id uint32) (segReader, error)
	saveManifest  func(dir string, m *manifest.Manifest) error
	encode        func(dst []byte, r record.Record) ([]byte, error)
}

// errNotOpen is returned by methods on a DB that was never opened (no active segment); it cannot
// happen through Open.
var errNotOpen = errors.New("kvlite: database not opened")

// newDB allocates a DB for dir with the real format packages behind the seams. Nothing is touched on
// disk: the caller runs recover() or initEmpty(), then start().
func newDB(dir string, o options) *DB {
	db := &DB{
		dir:           dir,
		opts:          o,
		kd:            keydir.New(o.shards, 0),
		readers:       make(map[uint32]segReader),
		createSegment: createSegment,
		openSegment:   openSegment,
		saveManifest:  manifest.Save,
		encode:        record.Append,
	}
	db.ctx, db.cancel = context.WithCancel(context.Background())
	return db
}

func createSegment(dir string, id uint32) (segWriter, error) {
	w, err := segment.Create(dir, id)
	if err != nil {
		return nil, err
	}
	return w, nil
}

func openSegment(dir string, id uint32) (segReader, error) {
	r, err := segment.OpenReader(dir, id)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// newDBForTest is Open's fresh-directory path without recovery: apply opts, create dir, take the LOCK,
// write generation 0 (segment 0 active) and start the background goroutines. configure, when non-nil,
// runs after newDB and before anything touches disk so tests can swap the seams. Open on a directory
// that holds no MANIFEST does the same through newDB + initEmpty + start.
func newDBForTest(dir string, configure func(*DB), opts ...Option) (*DB, error) {
	o := defaultOptions()
	if err := o.apply(opts); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("kvlite: open %s: %w", dir, err)
	}
	db := newDB(dir, o)
	if configure != nil {
		configure(db)
	}
	if !o.readOnly {
		l, err := fslock.Acquire(filepath.Join(dir, fslock.FileName))
		if err != nil {
			if errors.Is(err, fslock.ErrLocked) {
				return nil, fmt.Errorf("%w: %s", ErrLocked, dir)
			}
			return nil, fmt.Errorf("kvlite: open %s: %w", dir, err)
		}
		db.lock = l
	}
	if err := db.initEmpty(); err != nil {
		db.cancel()
		if db.lock != nil {
			_ = db.lock.Release()
		}
		return nil, err
	}
	db.start()
	return db, nil
}

// initEmpty writes generation 0 of a fresh store: creates segment 0 and a MANIFEST naming it active.
func (db *DB) initEmpty() error {
	db.wmu.Lock()
	defer db.wmu.Unlock()
	db.man = &manifest.Manifest{}
	return db.rotateLocked()
}

// start launches the interval flusher under SyncInterval (never on a read-only store). It is the
// newDBForTest half of Open's startBackground, which also launches the auto-compactor.
func (db *DB) start() {
	if db.opts.sync == SyncInterval && !db.opts.readOnly {
		db.wg.Add(1)
		go db.flushLoop()
	}
}

// expSec converts an absolute expiry to keydir.Entry.ExpSec: unix seconds rounded up (a key never
// expires early), clamped to [1, MaxUint32]; 0 is reserved for "never" and returned for the zero
// time. The write path stores the rounded second back into the record so recovery derives the same
// ExpSec from the log.
func expSec(t time.Time) uint32 {
	if t.IsZero() {
		return 0
	}
	ms := t.UnixMilli()
	sec := ms / 1000
	if ms%1000 != 0 {
		sec++
	}
	switch {
	case sec < 1:
		return 1
	case sec > math.MaxUint32:
		return math.MaxUint32
	default:
		return uint32(sec)
	}
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

func (db *DB) now() time.Time {
	if db.opts.clock == nil {
		return time.Now()
	}
	return db.opts.clock()
}

func (db *DB) logger() *slog.Logger {
	if db.opts.logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	return db.opts.logger
}

// checkKey validates a key for every operation: a key that can never be stored is rejected up front.
func (db *DB) checkKey(key []byte) error {
	switch {
	case len(key) == 0:
		return ErrEmptyKey
	case len(key) > db.opts.maxKey:
		return ErrKeyTooLarge
	}
	return nil
}

func (db *DB) checkValue(value []byte) error {
	if len(value) > db.opts.maxValue {
		return ErrValueTooLarge
	}
	return nil
}

// Get returns a fresh copy of the value for key (owner decision: copy, not a pooled view), or ErrNotFound
// when the key is absent or its TTL has passed. Expiry is lazy: the record dies at the next compaction,
// never on the read path.
func (db *DB) Get(key []byte) ([]byte, error) {
	if err := db.guard(false); err != nil {
		return nil, err
	}
	if err := db.checkKey(key); err != nil {
		return nil, err
	}
	if db.kd == nil {
		return nil, errNotOpen
	}
	db.gets.Add(1)
	for attempt := 0; ; attempt++ {
		e, ok := db.kd.Get(key)
		if !ok {
			db.misses.Add(1)
			return nil, ErrNotFound
		}
		if e.Expired(db.now()) {
			db.expiredHits.Add(1)
			return nil, ErrNotFound
		}
		r := db.pinReader(e.Seg)
		if r == nil {
			// The segment was retired between the lookup and the pin; the keydir now points at the
			// merged copy. Re-resolve.
			if attempt < readRetries {
				continue
			}
			return nil, &CorruptError{Seg: e.Seg, Off: int64(e.Off), Reason: "segment not open"}
		}
		rec, _, err := r.ReadAt(e.Off, e.Size)
		r.Unref()
		if err != nil {
			return nil, readError(e, err)
		}
		if !bytes.Equal(rec.Key, key) || rec.Flags.Tombstone() {
			return nil, &CorruptError{Seg: e.Seg, Off: int64(e.Off), Reason: "keydir points at a different record"}
		}
		db.hits.Add(1)
		// ReadAt hands back a buffer it allocated for this call; Value aliases it, so this is already
		// the caller's private copy.
		if rec.Value == nil {
			return []byte{}, nil
		}
		return rec.Value, nil
	}
}

// pinReader returns the reader for seg with one reference taken, or nil when seg is not open. The
// caller must Unref it. Retiring a reader (compaction, Close) happens under rmu.Lock after checking
// Refs() == 0, so a pinned reader is never closed underneath its user.
func (db *DB) pinReader(seg uint32) segReader {
	db.rmu.RLock()
	r := db.readers[seg]
	if r != nil {
		r.Ref()
	}
	db.rmu.RUnlock()
	return r
}

// Has reports whether key is present and not expired, without reading the value.
func (db *DB) Has(key []byte) (bool, error) {
	if err := db.guard(false); err != nil {
		return false, err
	}
	if err := db.checkKey(key); err != nil {
		return false, err
	}
	if db.kd == nil {
		return false, errNotOpen
	}
	db.gets.Add(1)
	e, ok := db.kd.Get(key)
	switch {
	case !ok:
		db.misses.Add(1)
		return false, nil
	case e.Expired(db.now()):
		db.expiredHits.Add(1)
		return false, nil
	}
	db.hits.Add(1)
	return true, nil
}

// Put stores value under key with no expiry. It validates ErrEmptyKey/ErrKeyTooLarge/ErrValueTooLarge
// before touching the log. A nil value is stored as an empty one.
func (db *DB) Put(key, value []byte) error {
	if err := db.guard(true); err != nil {
		return err
	}
	return db.put(key, value, 0)
}

// PutTTL stores value under key, expiring ttl after the clock's now. ttl <= 0 returns ErrInvalidTTL.
// Expiry is best-effort wall clock, rounded up to whole seconds so a key never expires early.
func (db *DB) PutTTL(key, value []byte, ttl time.Duration) error {
	if err := db.guard(true); err != nil {
		return err
	}
	if ttl <= 0 {
		return ErrInvalidTTL
	}
	return db.put(key, value, ttl)
}

func (db *DB) put(key, value []byte, ttl time.Duration) error {
	if err := db.checkKey(key); err != nil {
		return err
	}
	if err := db.checkValue(value); err != nil {
		return err
	}
	db.wmu.Lock()
	defer db.wmu.Unlock()
	if err := db.writableLocked(); err != nil {
		return err
	}
	now := db.now()
	rec := record.Record{Key: key, Value: value, TS: now}
	var exp uint32
	if ttl > 0 {
		exp = expSec(now.Add(ttl))
		rec.Expiry = time.Unix(int64(exp), 0)
		rec.Flags |= record.FlagExpiry
	}
	buf, err := db.encode(db.buf[:0], rec)
	db.buf = buf
	if err != nil {
		return fmt.Errorf("kvlite: encode: %w", err)
	}
	defer db.releaseScratch()
	seg, off, err := db.appendLocked(buf)
	if err != nil {
		return err
	}
	e := keydir.Entry{Seg: seg, Off: off, Size: uint32(len(buf)), ExpSec: exp} //nolint:gosec // bounded by MaxRecord
	old, existed := db.kd.Put(key, e)
	db.liveBytes += int64(e.Size)
	if existed {
		db.liveBytes -= int64(old.Size)
		db.deadBytes += int64(old.Size)
	}
	db.puts.Add(1)
	return db.afterWriteLocked()
}

// Delete removes key. It always appends a tombstone so the delete survives a crash; deleting an absent
// key is not an error.
func (db *DB) Delete(key []byte) error {
	if err := db.guard(true); err != nil {
		return err
	}
	if err := db.checkKey(key); err != nil {
		return err
	}
	db.wmu.Lock()
	defer db.wmu.Unlock()
	if err := db.writableLocked(); err != nil {
		return err
	}
	rec := record.Record{Key: key, TS: db.now(), Flags: record.FlagTombstone}
	buf, err := db.encode(db.buf[:0], rec)
	db.buf = buf
	if err != nil {
		return fmt.Errorf("kvlite: encode: %w", err)
	}
	if _, _, err := db.appendLocked(buf); err != nil {
		return err
	}
	db.deadBytes += int64(len(buf)) // the tombstone itself is never live
	if old, existed := db.kd.Delete(key); existed {
		db.liveBytes -= int64(old.Size)
		db.deadBytes += int64(old.Size)
	}
	db.deletes.Add(1)
	return db.afterWriteLocked()
}

// writableLocked is the per-write precondition under wmu: not closed, no poisoned writer, an active
// segment, no unreported background-sync failure.
func (db *DB) writableLocked() error {
	switch {
	case db.closed.Load():
		return ErrClosed
	case db.writeErr != nil:
		return db.writeErr
	case db.active == nil || db.kd == nil:
		return errNotOpen
	case db.syncErr != nil:
		return db.syncErr
	}
	return nil
}

// appendLocked writes one encoded record to the active segment, rotating first when it would push the
// segment past MaxSegmentBytes, and returns where it landed. A failed Append poisons the store: the
// file may now hold a partial record that only a reopen (tail truncation) can clean up.
func (db *DB) appendLocked(rec []byte) (seg, off uint32, err error) {
	if db.active.Size()+int64(len(rec)) > db.opts.maxSegmentBytes && db.active.Size() > record.SegmentHeaderSize {
		if err := db.rotateLocked(); err != nil {
			return 0, 0, err
		}
	}
	seg = db.active.ID()
	off, err = db.active.Append(rec)
	if err != nil {
		db.writeErr = fmt.Errorf("kvlite: append to segment %d: %w", seg, err)
		return 0, 0, db.writeErr
	}
	db.dirty.Store(true)
	return seg, off, nil
}

// afterWriteLocked applies the sync policy after a successful append.
func (db *DB) afterWriteLocked() error {
	if db.opts.sync == SyncAlways {
		return db.syncLocked()
	}
	return nil
}

// syncLocked fsyncs the active segment. Caller holds wmu.
func (db *DB) syncLocked() error {
	if db.active == nil {
		return nil
	}
	if err := db.active.Sync(); err != nil {
		return fmt.Errorf("kvlite: sync segment %d: %w", db.active.ID(), err)
	}
	db.syncs.Add(1)
	db.dirty.Store(false)
	return nil
}

// releaseScratch drops an oversized encode buffer after use. Caller holds wmu.
func (db *DB) releaseScratch() {
	if cap(db.buf) > maxScratch {
		db.buf = nil
	}
}

// allocIDLocked reserves the next segment id. Caller holds wmu. Compaction reserves its output ids
// this way before it rotates, so merged segments sort below the segment that was active meanwhile
// (recovery relies on ascending id = ascending write time).
func (db *DB) allocIDLocked() uint32 {
	id := db.man.NextID
	db.man.NextID++
	return id
}

// rotateLocked makes the active segment immutable and opens a fresh one: create the file, open its
// reader, sync the old writer, publish a MANIFEST (gen+1) that names the new segment, then swap the
// in-memory set under rmu and close the old writer. It is the single place the write path creates a
// segment; initEmpty (first segment) and Compact (fresh active before a merge) call it too. Caller
// holds wmu. On failure the old active segment stays in service and the new file is removed.
func (db *DB) rotateLocked() error {
	id := db.allocIDLocked()
	w, err := db.createSegment(db.dir, id)
	if err != nil {
		return fmt.Errorf("kvlite: create segment %d: %w", id, err)
	}
	undo := func() {
		_ = w.Close()
		_ = os.Remove(filepath.Join(db.dir, segment.LogName(id)))
	}
	r, err := db.openSegment(db.dir, id)
	if err != nil {
		undo()
		return fmt.Errorf("kvlite: open segment %d: %w", id, err)
	}
	old := db.active
	if old != nil {
		if err := old.Sync(); err != nil {
			_ = r.Close()
			undo()
			return fmt.Errorf("kvlite: sync segment %d before rotation: %w", old.ID(), err)
		}
		db.syncs.Add(1)
	}
	m := &manifest.Manifest{
		Gen:      db.man.Gen + 1,
		Active:   id,
		NextID:   db.man.NextID,
		Segments: append(slices.Clone(db.man.Segments), id),
	}
	slices.Sort(m.Segments)
	if err := db.saveManifest(db.dir, m); err != nil {
		_ = r.Close()
		undo()
		return fmt.Errorf("kvlite: save MANIFEST: %w", err)
	}
	db.rmu.Lock()
	db.readers[id] = r
	db.active = w
	db.man = m
	db.rmu.Unlock()
	db.deadBytes += w.Size() // the prologue is never live
	db.dirty.Store(false)
	if old != nil {
		if err := old.Close(); err != nil {
			db.logger().Warn("kvlite: closing rotated segment writer", "seg", old.ID(), "err", err)
		}
	}
	return nil
}

// flushLoop is the SyncInterval flusher goroutine: every opts.syncInterval it fsyncs the active
// segment if anything was appended since the last sync. A failure is logged and made sticky (syncErr)
// so the next Put/Delete/Commit/Sync reports it; a later successful Sync clears it. The launcher does
// db.wg.Add(1); the loop returns, and signals wg, when db.ctx is cancelled by Close.
func (db *DB) flushLoop() {
	defer db.wg.Done()
	t := time.NewTicker(db.opts.syncInterval)
	defer t.Stop()
	for {
		select {
		case <-db.ctx.Done():
			return
		case <-t.C:
			if !db.dirty.Load() {
				continue
			}
			db.wmu.Lock()
			err := db.syncLocked()
			if err != nil {
				db.syncErr = fmt.Errorf("kvlite: background sync failed: %w", err)
			}
			db.wmu.Unlock()
			if err != nil {
				db.logger().Warn("kvlite: background sync failed", "err", err)
			}
		}
	}
}

// Batch starts an atomic write group. Nothing is written until Commit.
func (db *DB) Batch() *Batch {
	return &Batch{db: db}
}

// recordCompaction stamps a finished compaction into the counters. Compact calls it under wmu (the
// same critical section as the MANIFEST swap) so Stats never sees a half-updated set.
func (db *DB) recordCompaction(reclaimed int64, started time.Time) {
	db.reclaimedBytes += reclaimed
	db.compactions++
	db.lastCompaction = time.Now()
	db.lastCompactionDur = time.Since(started)
}

// Stats returns a snapshot of the counters. Keys counts keydir entries; a key whose TTL has passed but
// that no compaction has swept yet is still counted (Get reports it as ExpiredHits).
func (db *DB) Stats() Stats {
	db.wmu.Lock()
	s := Stats{
		LiveBytes:         db.liveBytes,
		DeadBytes:         db.deadBytes,
		TotalBytes:        db.liveBytes + db.deadBytes,
		ReclaimedBytes:    db.reclaimedBytes,
		Compactions:       db.compactions,
		LastCompaction:    db.lastCompaction,
		LastCompactionDur: db.lastCompactionDur,
		Repairs:           db.repairs,
	}
	if db.man != nil {
		s.Segments = len(db.man.Segments)
		s.ActiveSegment = db.man.Active
	}
	db.wmu.Unlock()
	if db.kd != nil {
		s.Keys = db.kd.Len()
		s.KeydirBytes = db.kd.Bytes()
	}
	if s.TotalBytes > 0 {
		s.DeadRatio = float64(s.DeadBytes) / float64(s.TotalBytes)
	}
	s.Puts = db.puts.Load()
	s.Deletes = db.deletes.Load()
	s.Gets = db.gets.Load()
	s.Hits = db.hits.Load()
	s.Misses = db.misses.Load()
	s.ExpiredHits = db.expiredHits.Load()
	s.Syncs = db.syncs.Load()
	return s
}

// Sync fsyncs the active segment regardless of the sync policy and clears a sticky background-sync
// error once the flush succeeds.
func (db *DB) Sync() error {
	if err := db.guard(true); err != nil {
		return err
	}
	db.wmu.Lock()
	defer db.wmu.Unlock()
	if db.closed.Load() {
		return ErrClosed
	}
	if db.writeErr != nil {
		return db.writeErr
	}
	if err := db.syncLocked(); err != nil {
		return err
	}
	db.syncErr = nil
	return nil
}

// Close stops the background goroutines, syncs, waits up to 5 s for open snapshots, closes every segment
// and releases the LOCK. Every later call returns ErrClosed.
func (db *DB) Close() error {
	if !db.closed.CompareAndSwap(false, true) {
		return ErrClosed
	}
	if db.cancel != nil {
		db.cancel()
	}
	db.wg.Wait()

	var errs []error
	db.wmu.Lock()
	if !db.opts.readOnly && db.writeErr == nil {
		if err := db.syncLocked(); err != nil {
			errs = append(errs, fmt.Errorf("kvlite: close: %w", err))
		}
	}
	db.wmu.Unlock()

	db.waitUnpinned(closeGrace)

	db.wmu.Lock()
	db.rmu.Lock()
	if db.active != nil {
		if err := db.active.Close(); err != nil {
			errs = append(errs, fmt.Errorf("kvlite: close segment %d: %w", db.active.ID(), err))
		}
		db.active = nil
	}
	for id, r := range db.readers {
		if err := r.Close(); err != nil {
			errs = append(errs, fmt.Errorf("kvlite: close segment %d reader: %w", id, err))
		}
		delete(db.readers, id)
	}
	db.rmu.Unlock()
	db.wmu.Unlock()

	if db.lock != nil {
		if err := db.lock.Release(); err != nil {
			errs = append(errs, err)
		}
		db.lock = nil
	}
	return errors.Join(errs...)
}

// waitUnpinned polls the reader pins until none is held or d elapses (real time, not the injected
// clock). Snapshots still open after that are force-closed by the caller and their reads will fail.
func (db *DB) waitUnpinned(d time.Duration) {
	deadline := time.Now().Add(d)
	for {
		pinned := false
		db.rmu.RLock()
		for _, r := range db.readers {
			if r.Refs() > 0 {
				pinned = true
				break
			}
		}
		db.rmu.RUnlock()
		if !pinned {
			return
		}
		if time.Now().After(deadline) {
			db.logger().Warn("kvlite: close: snapshots still open after grace period, force-closing segments",
				"grace", d)
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}
