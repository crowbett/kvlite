package kvlite

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/crowbett/kvlite/internal/record"
	"github.com/crowbett/kvlite/internal/segment"
)

// TestTruncatedTail cuts the active segment inside the last record at several points and checks that
// Open drops exactly that record, truncates the file there, and keeps everything before it.
func TestTruncatedTail(t *testing.T) {
	t.Parallel()
	const n = 25
	lastKey, lastVal := key(n-1), bytes.Repeat([]byte("v"), 100)
	tests := []struct {
		name string
		// cut returns the byte offset to truncate at given the last record's offset, size and the
		// length of its metadata (everything between the header and the key).
		cut func(off, size, meta int64) int64
	}{
		{"inside crc", func(off, _, _ int64) int64 { return off + 2 }},
		{"inside plen", func(off, _, _ int64) int64 { return off + record.HeaderSize - 2 }},
		{"after header", func(off, _, _ int64) int64 { return off + record.HeaderSize }},
		{"inside key", func(off, _, meta int64) int64 { return off + record.HeaderSize + meta + 2 }},
		{"inside value", func(off, size, _ int64) int64 { return off + size - 10 }},
		{"last byte missing", func(off, size, _ int64) int64 { return off + size - 1 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			db := openTest(t, dir)
			m := fill(t, db, n-1)
			mustPut(t, db, lastKey, lastVal)
			active := db.Stats().ActiveSegment
			closeTest(t, db)

			recs := segmentRecords(t, dir, active)
			last := recs[len(recs)-1]
			off, size := last[0], last[1]
			meta := size - record.HeaderSize - int64(len(lastKey)) - int64(len(lastVal))
			cut := tt.cut(off, size, meta)
			if cut <= off || cut >= off+size {
				t.Fatalf("bad cut %d for record [%d, %d)", cut, off, off+size)
			}
			if err := os.Truncate(logPath(dir, active), cut); err != nil {
				t.Fatal(err)
			}

			db = openTest(t, dir)
			m.check(t, db, nil)
			wantNotFound(t, db, lastKey)
			if got := fileSize(t, logPath(dir, active)); got != off {
				t.Errorf("active segment size after recovery = %d, want %d (truncated at the torn record)", got, off)
			}
			s := db.Stats()
			if s.TotalBytes != off {
				t.Errorf("TotalBytes = %d, want %d", s.TotalBytes, off)
			}
			// New writes go right where the torn record was and survive another reopen.
			mustPut(t, db, []byte("fresh"), []byte("write"))
			m.put([]byte("fresh"), []byte("write"))
			closeTest(t, db)
			db = openTest(t, dir)
			m.check(t, db, nil)
			wantNotFound(t, db, lastKey)
			closeTest(t, db)
			noOrphans(t, dir)
		})
	}
}

// TestTruncatedTailZeroFill simulates a file-system extension that landed before the data: zeros after
// the last record are a torn tail, not corruption.
func TestTruncatedTailZeroFill(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openTest(t, dir)
	m := fill(t, db, 10)
	active := db.Stats().ActiveSegment
	closeTest(t, db)
	path := logPath(dir, active)
	end := fileSize(t, path)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	db = openTest(t, dir)
	m.check(t, db, nil)
	if got := fileSize(t, path); got != end {
		t.Errorf("size after recovery = %d, want %d", got, end)
	}
	closeTest(t, db)
}

// TestTruncatedTailReadOnly: a read-only open ignores the torn tail without touching the file.
func TestTruncatedTailReadOnly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openTest(t, dir)
	m := fill(t, db, 10)
	mustPut(t, db, []byte("torn"), bytes.Repeat([]byte("x"), 50))
	active := db.Stats().ActiveSegment
	closeTest(t, db)
	recs := segmentRecords(t, dir, active)
	last := recs[len(recs)-1]
	if err := os.Truncate(logPath(dir, active), last[0]+last[1]-5); err != nil {
		t.Fatal(err)
	}
	size := fileSize(t, logPath(dir, active))
	ro := openTest(t, dir, WithReadOnly())
	m.check(t, ro, nil)
	wantNotFound(t, ro, []byte("torn"))
	closeTest(t, ro)
	if got := fileSize(t, logPath(dir, active)); got != size {
		t.Errorf("read-only open changed the file: %d -> %d", size, got)
	}
}

// TestShortActiveSegment: the active file exists but holds less than a prologue (a crash right after
// create). Open recreates it.
func TestShortActiveSegment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openTest(t, dir)
	m := fill(t, db, 10)
	if err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	active := db.Stats().ActiveSegment
	closeTest(t, db)
	if err := os.Truncate(logPath(dir, active), 5); err != nil {
		t.Fatal(err)
	}
	db = openTest(t, dir)
	m.check(t, db, nil)
	mustPut(t, db, []byte("k"), []byte("v"))
	m.put([]byte("k"), []byte("v"))
	closeTest(t, db)
	db = openTest(t, dir)
	m.check(t, db, nil)
	closeTest(t, db)
	noOrphans(t, dir)
}

// twoSegments builds a store with one immutable segment (a compaction output, hint removed so that Open
// must scan it) and an active segment, and returns the immutable segment's id and the model.
func twoSegments(t *testing.T, dir string, n int) (uint32, model) {
	t.Helper()
	db := openTest(t, dir)
	m := fill(t, db, n)
	if err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	s := db.Stats()
	if s.Segments != 2 {
		t.Fatalf("Segments after Compact = %d, want 2", s.Segments)
	}
	var immutable uint32
	for _, id := range db.man.Segments {
		if id != s.ActiveSegment {
			immutable = id
		}
	}
	for i := n; i < n+10; i++ {
		mustPut(t, db, key(i), val(i))
		m.put(key(i), val(i))
	}
	closeTest(t, db)
	if err := os.Remove(hintPath(dir, immutable)); err != nil {
		t.Fatal(err)
	}
	return immutable, m
}

func TestCorruptMiddle(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	seg, m := twoSegments(t, dir, 40)
	recs := segmentRecords(t, dir, seg)
	victim := recs[len(recs)/2]
	// Flip a byte inside the value of the middle record: its crc no longer matches.
	flipByte(t, logPath(dir, seg), victim[0]+victim[1]-1)

	_, err := Open(dir)
	var ce *CorruptError
	if !errors.As(err, &ce) || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open = %v, want *CorruptError", err)
	}
	if ce.Seg != seg || ce.Off != victim[0] {
		t.Fatalf("CorruptError = seg %d off %d, want seg %d off %d", ce.Seg, ce.Off, seg, victim[0])
	}
	if ce.Reason != "checksum" {
		t.Errorf("Reason = %q, want checksum", ce.Reason)
	}
	if _, err := Open(dir); !errors.Is(err, ErrLocked) {
		// The failed Open released the LOCK: the error must not be ErrLocked.
		if errors.Is(err, ErrLocked) {
			t.Fatalf("LOCK leaked by the failed Open")
		}
	}

	db := openTest(t, dir, WithRepair())
	// Exactly the victim record is gone; the key it held was written once, so it is absent.
	var lost string
	for k := range m {
		if _, err := db.Get([]byte(k)); errors.Is(err, ErrNotFound) {
			lost = k
			m.del([]byte(k))
			break
		}
	}
	if lost == "" {
		t.Fatal("no key was lost to the corrupt record")
	}
	m.check(t, db, []string{lost})
	if s := db.Stats(); s.Repairs != 1 {
		t.Errorf("Stats().Repairs = %d, want 1", s.Repairs)
	}
	// Compaction rewrites the segment without the corrupt bytes, so a plain Open works afterwards.
	if err := db.Compact(t.Context()); err != nil {
		t.Fatalf("Compact after repair = %v", err)
	}
	closeTest(t, db)
	db = openTest(t, dir)
	m.check(t, db, []string{lost})
	if s := db.Stats(); s.Repairs != 0 {
		t.Errorf("Repairs after compaction = %d, want 0", s.Repairs)
	}
	closeTest(t, db)
	noOrphans(t, dir)
}

// TestCorruptActiveMiddle: a bad crc in the active segment with good records after it is corruption,
// not a torn tail.
func TestCorruptActiveMiddle(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openTest(t, dir)
	m := fill(t, db, 30)
	active := db.Stats().ActiveSegment
	closeTest(t, db)
	recs := segmentRecords(t, dir, active)
	victim := recs[10]
	flipByte(t, logPath(dir, active), victim[0]+victim[1]-1)
	_, err := Open(dir)
	var ce *CorruptError
	if !errors.As(err, &ce) || ce.Seg != active || ce.Off != victim[0] {
		t.Fatalf("Open = %v, want CorruptError at seg %d off %d", err, active, victim[0])
	}
	if got := fileSize(t, logPath(dir, active)); got != recs[len(recs)-1][0]+recs[len(recs)-1][1] {
		t.Errorf("failed Open truncated the file to %d", got)
	}
	db = openTest(t, dir, WithRepair())
	m.del(key(10))
	m.check(t, db, []string{string(key(10))})
	if s := db.Stats(); s.Repairs != 1 {
		t.Errorf("Repairs = %d, want 1", s.Repairs)
	}
	closeTest(t, db)
}

// TestBadSegmentHeader: a segment whose prologue is damaged is corrupt; WithRepair drops it.
func TestBadSegmentHeader(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	seg, m := twoSegments(t, dir, 10)
	flipByte(t, logPath(dir, seg), 1)
	var ce *CorruptError
	if _, err := Open(dir); !errors.As(err, &ce) || ce.Seg != seg {
		t.Fatalf("Open = %v, want CorruptError for seg %d", err, seg)
	}
	db := openTest(t, dir, WithRepair())
	for i := range 10 {
		wantNotFound(t, db, key(i))
		m.del(key(i))
	}
	m.check(t, db, nil)
	closeTest(t, db)
	noOrphans(t, dir)
}

// TestIncompleteBatchDiscarded: a batch whose commit marker never reached the disk is dropped whole on
// recovery, while an earlier complete batch and plain writes survive.
func TestIncompleteBatchDiscarded(t *testing.T) { //nolint:tparallel // the cases truncate one file in sequence
	t.Parallel()
	dir := t.TempDir()
	db := openTest(t, dir)
	m := fill(t, db, 5)
	if err := db.Batch().Put([]byte("ok-1"), []byte("1")).Put([]byte("ok-2"), []byte("2")).Delete(key(0)).Commit(); err != nil {
		t.Fatal(err)
	}
	m.put([]byte("ok-1"), []byte("1"))
	m.put([]byte("ok-2"), []byte("2"))
	m.del(key(0))
	before := len(segmentRecords(t, dir, db.Stats().ActiveSegment))
	if err := db.Batch().Put([]byte("lost-1"), []byte("1")).Put([]byte("lost-2"), []byte("2")).Put(key(1), []byte("no")).Commit(); err != nil {
		t.Fatal(err)
	}
	active := db.Stats().ActiveSegment
	closeTest(t, db)

	recs := segmentRecords(t, dir, active)
	if len(recs) != before+4 {
		t.Fatalf("batch of 3 wrote %d records, want 4 (members + marker)", len(recs)-before)
	}
	tests := []struct {
		name string
		cut  int64
	}{
		{"marker missing", recs[len(recs)-1][0]},
		{"marker torn", recs[len(recs)-1][0] + 3},
		{"second member torn", recs[len(recs)-2][0] + 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Sequential on purpose: every case truncates the same file further.
			if err := os.Truncate(logPath(dir, active), tt.cut); err != nil {
				t.Fatal(err)
			}
			db := openTest(t, dir)
			m.check(t, db, []string{string(key(0))})
			wantNotFound(t, db, []byte("lost-1"))
			wantNotFound(t, db, []byte("lost-2"))
			wantGet(t, db, key(1), val(1))
			closeTest(t, db)
		})
	}
}

// TestRecoveryExpiredIsTombstone: an expired write must not resurrect an older value of the same key.
func TestRecoveryExpiredIsTombstone(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	clk := newFakeClock()
	db := openTest(t, dir, clk.Option())
	mustPut(t, db, []byte("k"), []byte("forever"))
	if err := db.PutTTL([]byte("k"), []byte("brief"), time.Second); err != nil {
		t.Fatal(err)
	}
	if err := db.PutTTL([]byte("still"), []byte("here"), time.Hour); err != nil {
		t.Fatal(err)
	}
	closeTest(t, db)
	clk.Advance(5 * time.Second)
	db = openTest(t, dir, clk.Option())
	wantNotFound(t, db, []byte("k"))
	wantGet(t, db, []byte("still"), []byte("here"))
	if s := db.Stats(); s.Keys != 1 {
		t.Errorf("Keys = %d, want 1", s.Keys)
	}
	closeTest(t, db)
}

// TestReopenUsesHints: after a compaction every immutable segment loads through its hint; without the
// hint the log is scanned and the data is the same.
func TestReopenUsesHints(t *testing.T) {
	dir := t.TempDir()
	obs := observeRecovery(t, dir)
	db := openTest(t, dir)
	m := fill(t, db, 100)
	for i := 0; i < 100; i += 3 {
		mustPut(t, db, key(i), []byte("v2"))
		m.put(key(i), []byte("v2"))
	}
	if err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	active := db.Stats().ActiveSegment
	mustPut(t, db, []byte("late"), []byte("write"))
	m.put([]byte("late"), []byte("write"))
	closeTest(t, db)

	db = openTest(t, dir)
	m.check(t, db, nil)
	hinted, scanned := obs.hinted()
	if len(hinted) != 1 || len(scanned) != 1 || scanned[0] != active {
		t.Fatalf("hinted %v scanned %v, want one hinted immutable segment and the active %d scanned", hinted, scanned, active)
	}
	immutable := hinted[0]
	closeTest(t, db)

	// A damaged hint is ignored, not trusted.
	flipByte(t, hintPath(dir, immutable), fileSize(t, hintPath(dir, immutable))-6)
	obs.reset()
	db = openTest(t, dir)
	m.check(t, db, nil)
	if hinted, _ := obs.hinted(); len(hinted) != 0 {
		t.Errorf("corrupt hint was used for %v", hinted)
	}
	closeTest(t, db)

	// A missing hint falls back to the scan as well.
	if err := os.Remove(hintPath(dir, immutable)); err != nil {
		t.Fatal(err)
	}
	obs.reset()
	db = openTest(t, dir)
	m.check(t, db, nil)
	if hinted, scanned := obs.hinted(); len(hinted) != 0 || len(scanned) != 2 {
		t.Errorf("hinted %v scanned %v after removing the hint", hinted, scanned)
	}
	closeTest(t, db)
}

// TestRecoveryLastWriteWins: the same key rewritten in several segments (plain and merged) resolves to
// the newest value after a reopen without hints.
func TestRecoveryLastWriteWins(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openTest(t, dir)
	mustPut(t, db, []byte("k"), []byte("v1"))
	if err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, []byte("k"), []byte("v2"))
	if err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, []byte("k"), []byte("v3"))
	closeTest(t, db)
	// Strip every hint so ordering alone decides.
	ents, _ := os.ReadDir(dir)
	for _, e := range ents {
		if _, hint, ok := segment.ParseName(e.Name()); ok && hint {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
	db = openTest(t, dir)
	wantGet(t, db, []byte("k"), []byte("v3"))
	closeTest(t, db)
}
