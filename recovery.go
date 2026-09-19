package kvlite

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/crowbett/kvlite/internal/fslock"
	"github.com/crowbett/kvlite/internal/keydir"
	"github.com/crowbett/kvlite/internal/manifest"
	"github.com/crowbett/kvlite/internal/record"
	"github.com/crowbett/kvlite/internal/segment"
)

// repairer is the optional part of a segment reader that WithRepair needs: *segment.Reader implements
// it; a test double that does not simply cannot be repaired around.
type repairer interface {
	ScanFrom(off uint32, fn func(off uint32, rec record.Record) error) error
	Resync(from uint32) (next uint32, found bool, err error)
}

// errFreshStore is loadManifest's signal that the directory holds nothing yet.
var errFreshStore = errors.New("kvlite: fresh store")

// recover runs steps 1-5 of the Open procedure (docs/PLAN.md "Open / recovery procedure") against db.dir:
// lock, MANIFEST, orphan sweep, hint-first keydir load with tail truncation and WithRepair skipping, then
// the active segment. Step 6 (background goroutines) is Open's.
//
// Ordering invariant relied on here: segment ids ascend with write time. Data segments are created by
// rotation with a fresh id, and compaction reserves the ids of its outputs *before* rotating (see
// Compact), so merged records always sort below the segment that was active while they were merged.
// Applying segments in ascending id order and records in ascending offset order is therefore "last write
// wins", with no per-record timestamp comparison and no dependence on hints being present.
func (db *DB) recover() error {
	if db.dir == "" {
		return errors.New("kvlite: recover: empty dir")
	}
	if !db.opts.readOnly {
		if err := os.MkdirAll(db.dir, 0o750); err != nil {
			return fmt.Errorf("kvlite: open %s: %w", db.dir, err)
		}
		l, err := fslock.Acquire(filepath.Join(db.dir, fslock.FileName))
		if err != nil {
			if errors.Is(err, fslock.ErrLocked) {
				return fmt.Errorf("%w: %s", ErrLocked, db.dir)
			}
			return fmt.Errorf("kvlite: open %s: %w", db.dir, err)
		}
		db.lock = l
		db.sweepTrash()
	} else if _, err := os.Stat(db.dir); err != nil {
		return fmt.Errorf("kvlite: open %s: %w", db.dir, err)
	}

	m, err := db.loadManifest()
	switch {
	case errors.Is(err, errFreshStore):
		if db.opts.readOnly {
			db.man = &manifest.Manifest{}
			return nil
		}
		return db.initEmpty()
	case err != nil:
		return err
	}
	if err := db.sweepOrphans(m); err != nil {
		return err
	}

	ld := &loader{db: db, now: db.now()}
	var total int64
	keep := make([]uint32, 0, len(m.Segments))
	for _, id := range m.Segments {
		size, err := db.loadSegment(ld, m, id)
		if err != nil {
			return err
		}
		if size < 0 {
			continue // dropped under WithRepair, or the never-created active segment
		}
		total += size
		keep = append(keep, id)
	}
	m.Segments = keep

	added, err := db.openActive(m)
	if err != nil {
		return err
	}
	total += added
	db.liveBytes = ld.live
	db.deadBytes = max(total-ld.live, 0)
	db.repairs = ld.repairs
	return nil
}

// loadManifest reads dir/MANIFEST or, when it is missing or corrupt, either reports a fresh store (no
// segment files at all) or rebuilds the manifest from the *.log files on disk under WithRepair.
func (db *DB) loadManifest() (*manifest.Manifest, error) {
	m, err := manifest.Load(db.dir)
	switch {
	case err == nil:
		return m, nil
	case errors.Is(err, manifest.ErrNotExist):
		ids, lerr := logIDs(db.dir)
		if lerr != nil {
			return nil, lerr
		}
		if len(ids) == 0 {
			return nil, errFreshStore
		}
		if !db.opts.readOnly && !db.opts.repair {
			return nil, fmt.Errorf("kvlite: MANIFEST missing but %d segment file(s) present (open WithRepair to rebuild): %w",
				len(ids), ErrCorrupt)
		}
		return db.rebuildManifest(ids)
	case errors.Is(err, manifest.ErrCorrupt):
		if !db.opts.repair {
			return nil, fmt.Errorf("kvlite: MANIFEST: %w", ErrCorrupt)
		}
		ids, lerr := logIDs(db.dir)
		if lerr != nil {
			return nil, lerr
		}
		if len(ids) == 0 {
			return nil, fmt.Errorf("kvlite: MANIFEST corrupt and no segment files to rebuild from: %w", ErrCorrupt)
		}
		return db.rebuildManifest(ids)
	default:
		return nil, fmt.Errorf("kvlite: MANIFEST: %w", err)
	}
}

// rebuildManifest names every *.log file as live, the highest id as active. Segment ids ascend with
// write time, so this is the only order that could be right; a merged output orphaned by a crashed
// compaction that never swapped is adopted too, which is harmless: its records duplicate live inputs.
func (db *DB) rebuildManifest(ids []uint32) (*manifest.Manifest, error) {
	m := &manifest.Manifest{Active: ids[len(ids)-1], NextID: ids[len(ids)-1] + 1, Segments: ids}
	db.logger().Warn("kvlite: MANIFEST rebuilt from segment files", "dir", db.dir, "segments", len(ids), "active", m.Active)
	if db.opts.readOnly {
		return m, nil
	}
	if err := db.saveManifest(db.dir, m); err != nil {
		return nil, fmt.Errorf("kvlite: rebuild MANIFEST: %w", err)
	}
	return m, nil
}

// logIDs lists the ids of every NNNNNNNN.log in dir, ascending.
func logIDs(dir string) ([]uint32, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("kvlite: read %s: %w", dir, err)
	}
	var ids []uint32
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		if id, hint, ok := segment.ParseName(e.Name()); ok && !hint {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids, nil
}

// sweepOrphans moves every *.log/*.hint not named by m (and every *.tmp scratch file) into trash/ and
// deletes it. Read-only opens skip it (the files are simply ignored). A hint for the active segment is
// an orphan too: hints describe immutable segments only.
func (db *DB) sweepOrphans(m *manifest.Manifest) error {
	names, err := manifest.Orphans(db.dir, m)
	if err != nil {
		return fmt.Errorf("kvlite: list orphans: %w", err)
	}
	if _, err := os.Stat(filepath.Join(db.dir, segment.HintName(m.Active))); err == nil {
		names = append(names, segment.HintName(m.Active))
	}
	if db.opts.readOnly || len(names) == 0 {
		return nil
	}
	db.logger().Warn("kvlite: removing orphan files", "dir", db.dir, "count", len(names))
	for _, n := range names {
		db.trash(n)
	}
	return nil
}

// loadSegment feeds one segment into the loader, hint-first when it is immutable, registers its reader
// and returns its file size after any tail truncation. A negative size means the segment was dropped
// (missing or unusable under WithRepair) or is the active segment that a crash left uncreated; the
// caller leaves it out of the manifest and openActive rotates to a fresh one.
func (db *DB) loadSegment(ld *loader, m *manifest.Manifest, id uint32) (int64, error) {
	path := filepath.Join(db.dir, segment.LogName(id))
	st, err := os.Stat(path)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return 0, fmt.Errorf("kvlite: stat %s: %w", path, err)
		}
		if id == m.Active {
			return -1, nil // MANIFEST was saved before the file was created and the process died in between
		}
		if db.opts.repair {
			db.logger().Warn("kvlite: repair: dropping missing segment", "seg", id)
			return -1, nil
		}
		return 0, &CorruptError{Seg: id, Off: 0, Reason: "segment file missing", Err: err}
	}
	size := st.Size()
	active := id == m.Active

	if size < record.SegmentHeaderSize && active && !db.opts.readOnly {
		// A crash between creating the file and its prologue reaching disk; nothing can be in it.
		db.logger().Warn("kvlite: recreating short active segment", "seg", id, "size", size)
		if err := os.Remove(path); err != nil {
			return 0, fmt.Errorf("kvlite: remove %s: %w", path, err)
		}
		return -1, nil
	}

	r, err := db.openSegment(db.dir, id)
	if err != nil {
		if errors.Is(err, segment.ErrBadHeader) {
			return db.dropOrCorrupt(id, 0, "bad segment header", err)
		}
		return 0, fmt.Errorf("kvlite: open segment %d: %w", id, err)
	}
	if !active && db.loadHint(ld, id, size) {
		db.observeRecovery(id, true)
		db.putReader(r)
		return size, nil
	}
	db.observeRecovery(id, false)
	end, err := db.scanSegment(ld, r, size, active)
	if err != nil {
		_ = r.Close()
		return 0, err
	}
	db.putReader(r)
	return end, nil
}

// dropOrCorrupt returns a CorruptError for seg, or under WithRepair moves the segment to trash/ and
// reports it dropped.
func (db *DB) dropOrCorrupt(id uint32, off int64, reason string, err error) (int64, error) {
	if !db.opts.repair {
		return 0, &CorruptError{Seg: id, Off: off, Reason: reason, Err: err}
	}
	db.logger().Warn("kvlite: repair: dropping unusable segment", "seg", id, "off", off, "reason", reason)
	if !db.opts.readOnly {
		db.trash(segment.LogName(id))
		db.trash(segment.HintName(id))
	}
	return -1, nil
}

// loadHint applies dir/<id>.hint and reports whether it was usable. A missing or invalid hint (bad
// footer crc, an entry past the end of the log) means "scan the log", which is always safe. Nothing is
// applied before the whole hint has been validated.
func (db *DB) loadHint(ld *loader, id uint32, size int64) bool {
	ents, err := segment.ReadHint(db.dir, id)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			db.logger().Warn("kvlite: ignoring bad hint file", "seg", id, "err", err)
		}
		return false
	}
	for _, h := range ents {
		if int64(h.Off)+int64(h.Size) > size {
			db.logger().Warn("kvlite: ignoring hint file: entry outside the log", "seg", id, "off", h.Off, "size", h.Size)
			return false
		}
	}
	for _, h := range ents {
		ld.apply(h.Key, keydir.Entry{Seg: id, Off: h.Off, Size: h.Size, ExpSec: h.ExpSec}, h.Flags.Tombstone())
	}
	return true
}

// scanSegment walks the log record by record. A torn tail (ScanError.Tail) in the active segment is
// truncated away (logged, skipped read-only). Any other bad record is ErrCorrupt, or under WithRepair
// the scan resyncs to the next decodable record and counts the skip. It returns the size the caller
// should account for.
func (db *DB) scanSegment(ld *loader, r segReader, size int64, active bool) (int64, error) {
	id := r.ID()
	fn := func(off uint32, rec record.Record) error {
		ld.record(id, off, uint32(record.EncodedSize(rec)), rec) //nolint:gosec // bounded by record.MaxRecord
		return nil
	}
	err := r.Scan(fn)
	for err != nil {
		var se *segment.ScanError
		if !errors.As(err, &se) {
			return 0, fmt.Errorf("kvlite: scan segment %d: %w", id, err)
		}
		ld.dropGroup("bad record")
		bad := int64(se.Off)
		reason := corruptReason(se.Err)
		if active && se.Tail {
			if db.opts.readOnly {
				db.logger().Warn("kvlite: torn tail ignored (read-only)", "seg", id, "off", bad, "reason", reason)
				return bad, nil
			}
			if err := db.truncateActive(id, bad); err != nil {
				return 0, err
			}
			db.logger().Warn("kvlite: truncated torn tail of active segment", "seg", id, "off", bad,
				"dropped", size-bad, "reason", reason)
			return bad, nil
		}
		if !db.opts.repair {
			return 0, &CorruptError{Seg: id, Off: bad, Reason: reason, Err: se.Err}
		}
		ld.repairs++
		rp, ok := r.(repairer)
		if !ok {
			return 0, &CorruptError{Seg: id, Off: bad, Reason: reason, Err: se.Err}
		}
		next, found, rerr := rp.Resync(se.Off + 1)
		if rerr != nil {
			return 0, fmt.Errorf("kvlite: repair segment %d: %w", id, rerr)
		}
		if !found {
			db.logger().Warn("kvlite: repair: dropped unreadable tail", "seg", id, "off", bad, "reason", reason)
			return size, nil
		}
		db.logger().Warn("kvlite: repair: skipped corrupt bytes", "seg", id, "off", bad, "next", next, "reason", reason)
		err = rp.ScanFrom(next, fn)
	}
	ld.dropGroup("end of segment")
	return size, nil
}

// truncateActive opens the active segment for append, cuts it at off and keeps the writer as db.active.
func (db *DB) truncateActive(id uint32, off int64) error {
	w, err := segment.Open(db.dir, id)
	if err != nil {
		return fmt.Errorf("kvlite: reopen active segment %d: %w", id, err)
	}
	if err := w.Truncate(off); err != nil {
		_ = w.Close()
		return fmt.Errorf("kvlite: truncate torn tail of segment %d at %d: %w", id, off, err)
	}
	db.active = w
	return nil
}

// corruptReason maps a decoder error to the short CorruptError.Reason vocabulary.
func corruptReason(err error) string {
	switch {
	case errors.Is(err, record.ErrChecksum):
		return "checksum"
	case errors.Is(err, record.ErrTruncated):
		return "truncated"
	case errors.Is(err, record.ErrTooLarge):
		return "bad length"
	case errors.Is(err, record.ErrMalformed):
		return "malformed"
	default:
		return "decode"
	}
}

// openActive publishes m as the live set and makes m.Active writable: reopened for append when the
// recovery left it in place (or already reopened by truncateActive), otherwise rotated to a fresh id
// through rotateLocked, which also publishes the MANIFEST. It returns the bytes of any segment it
// created. Read-only opens have no writer.
func (db *DB) openActive(m *manifest.Manifest) (int64, error) {
	for _, id := range m.Segments {
		m.NextID = max(m.NextID, id+1)
	}
	db.man = m
	if db.opts.readOnly {
		return 0, nil
	}
	if slices.Contains(m.Segments, m.Active) {
		if db.active != nil {
			return 0, nil
		}
		w, err := segment.Open(db.dir, m.Active)
		if err != nil {
			return 0, fmt.Errorf("kvlite: open active segment %d: %w", m.Active, err)
		}
		db.active = w
		return 0, nil
	}
	db.wmu.Lock()
	defer db.wmu.Unlock()
	if err := db.rotateLocked(); err != nil {
		return 0, err
	}
	return db.active.Size(), nil
}

// putReader registers r in the reader table.
func (db *DB) putReader(r segReader) {
	db.rmu.Lock()
	db.readers[r.ID()] = r
	db.rmu.Unlock()
}

// loader applies scanned records to the keydir with the recovery rules: later wins, a tombstone or an
// expired record removes the key (an expired write must not resurrect an older non-expiring value from
// an earlier segment, so it is applied as a delete, not skipped), and batch members are buffered until
// their commit marker; a group without one is discarded.
type loader struct {
	db      *DB
	now     time.Time
	live    int64
	repairs int
	group   []pendingRec
}

type pendingRec struct {
	key  []byte
	e    keydir.Entry
	tomb bool
}

func (l *loader) record(seg, off, size uint32, rec record.Record) {
	e := keydir.Entry{Seg: seg, Off: off, Size: size, ExpSec: rec.ExpSec()}
	switch {
	case rec.Flags.BatchCommit():
		count, ok := decodeCommit(rec.Value)
		if ok && count == len(l.group) && count > 0 {
			for _, p := range l.group {
				l.apply(p.key, p.e, p.tomb)
			}
		} else {
			l.db.logger().Warn("kvlite: discarding batch group with mismatched commit marker",
				"seg", seg, "off", off, "members", len(l.group), "marker", count)
		}
		l.group = l.group[:0]
	case rec.Flags.BatchMember():
		l.group = append(l.group, pendingRec{key: slices.Clone(rec.Key), e: e, tomb: rec.Flags.Tombstone()})
	default:
		l.dropGroup("plain record")
		l.apply(rec.Key, e, rec.Flags.Tombstone())
	}
}

// dropGroup discards buffered batch members that never saw their commit marker.
func (l *loader) dropGroup(why string) {
	if len(l.group) == 0 {
		return
	}
	l.db.logger().Warn("kvlite: discarding uncommitted batch group", "members", len(l.group), "seg", l.group[0].e.Seg,
		"off", l.group[0].e.Off, "reason", why)
	l.group = l.group[:0]
}

func (l *loader) apply(key []byte, e keydir.Entry, tomb bool) {
	if tomb || e.Expired(l.now) {
		if old, ok := l.db.kd.Delete(key); ok {
			l.live -= int64(old.Size)
		}
		return
	}
	if old, ok := l.db.kd.Put(key, e); ok {
		l.live -= int64(old.Size)
	}
	l.live += int64(e.Size)
}

// decodeCommit parses a commit marker's value: uvarint count + uvarint batchID.
func decodeCommit(v []byte) (int, bool) {
	count, n := binary.Uvarint(v)
	if n <= 0 || count > math.MaxInt32 {
		return 0, false
	}
	if _, m := binary.Uvarint(v[n:]); m <= 0 {
		return 0, false
	}
	return int(count), true
}

// trash renames dir/name into dir/trash/ and then tries to delete it. On Windows a file that is still
// open elsewhere cannot be renamed or deleted; it stays where it is and the next sweep retries.
func (db *DB) trash(name string) {
	src := filepath.Join(db.dir, name)
	if _, err := os.Stat(src); err != nil {
		return
	}
	tdir := filepath.Join(db.dir, manifest.TrashDir)
	if err := os.MkdirAll(tdir, 0o750); err != nil {
		db.logger().Debug("kvlite: trash dir", "err", err)
		return
	}
	dst := filepath.Join(tdir, name)
	if err := os.Rename(src, dst); err != nil {
		if rerr := os.Remove(src); rerr != nil {
			db.logger().Debug("kvlite: trash: cannot move or delete, retrying later", "file", name, "err", err)
		}
		return
	}
	if err := os.Remove(dst); err != nil {
		db.logger().Debug("kvlite: trash: delete deferred", "file", name, "err", err)
	}
}

// sweepTrash deletes whatever an earlier run (or a still-pinned reader) left in dir/trash/. Best effort.
func (db *DB) sweepTrash() {
	tdir := filepath.Join(db.dir, manifest.TrashDir)
	ents, err := os.ReadDir(tdir)
	if err != nil {
		return
	}
	for _, e := range ents {
		if err := os.Remove(filepath.Join(tdir, e.Name())); err != nil {
			db.logger().Debug("kvlite: trash sweep: delete deferred", "file", e.Name(), "err", err)
		}
	}
}

// observeRecovery reports how a segment was loaded to the recovery test hook, if one is installed.
func (db *DB) observeRecovery(seg uint32, viaHint bool) {
	testHooks.mu.RLock()
	fn := testHooks.recovery
	testHooks.mu.RUnlock()
	if fn != nil {
		fn(db, seg, viaHint)
	}
}
