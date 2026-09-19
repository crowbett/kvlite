package segment

import (
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/crowbett/kvlite/internal/record"
)

// update regenerates testdata/ from the fixture below: go test ./internal/segment/ -run TestGolden -update
var update = flag.Bool("update", false, "rewrite the golden segments under testdata/")

// Golden fixture. testdata/{valid,torn,corrupt}/00000001.log are built from these six records, so
// every offset below is a stable fact lanes B and C can rely on:
//
//	off  size  record
//	 16    25  put   "alpha" = "one"                     ts 1700000000000
//	 41    30  put   "beta"  = "two"  expiry 1700000060000 (ExpSec 1700000060)
//	 71    22  tomb  "gamma"
//	 93   279  put   "delta" = 256 x 'd'
//	372    21  put   "eps"   = "5"    batch member
//	393    19  batch commit (count 1, batchID 42)         end of file at 412
//
// valid/   the six records + 00000001.hint with entries alpha, beta, delta, eps
// torn/    the same file cut at byte 403 (inside the commit marker): Scan -> ScanError{Off: 393, Tail: true}
// corrupt/ byte 70 (last byte of "two") flipped: Scan -> ScanError{Off: 41, Tail: false}, Resync -> 71
const (
	goldenID       = 1
	goldenTS       = int64(1_700_000_000_000)
	goldenExpiry   = int64(1_700_000_060_000)
	goldenEnd      = 412
	goldenTornCut  = 403
	goldenTornOff  = 393
	goldenFlipByte = 70
	goldenFlipOff  = 41
	goldenResync   = 71
)

var (
	goldenOffsets = []uint32{16, 41, 71, 93, 372, 393}
	goldenSizes   = []uint32{25, 30, 22, 279, 21, 19}
)

func ms(v int64) time.Time { return time.UnixMilli(v) }

func fixtureRecords() []record.Record {
	commit := binary.AppendUvarint(binary.AppendUvarint(nil, 1), 42)
	return []record.Record{
		{Key: []byte("alpha"), Value: []byte("one"), TS: ms(goldenTS)},
		{Key: []byte("beta"), Value: []byte("two"), TS: ms(goldenTS + 1), Expiry: ms(goldenExpiry), Flags: record.FlagExpiry},
		{Key: []byte("gamma"), TS: ms(goldenTS + 2), Flags: record.FlagTombstone},
		{Key: []byte("delta"), Value: bytes.Repeat([]byte("d"), 256), TS: ms(goldenTS + 3)},
		{Key: []byte("eps"), Value: []byte("5"), TS: ms(goldenTS + 4), Flags: record.FlagBatchMember},
		{Value: commit, TS: ms(goldenTS + 4), Flags: record.FlagBatchCommit},
	}
}

// fixtureHint is the hint lane C would write for the fixture: live puts only, batch member included
// because its commit landed.
func fixtureHint() []HintEntry {
	recs := fixtureRecords()
	var out []HintEntry
	for i, r := range recs {
		if r.Flags.Tombstone() || r.Flags.BatchCommit() {
			continue
		}
		out = append(out, HintEntry{Key: r.Key, Off: goldenOffsets[i], Size: goldenSizes[i], ExpSec: r.ExpSec(), Flags: r.Flags})
	}
	return out
}

// build writes recs into dir/<id>.log and returns each record's offset and size.
func build(t *testing.T, dir string, id uint32, recs []record.Record) (offs, sizes []uint32) {
	t.Helper()
	w, err := Create(dir, id)
	if err != nil {
		t.Fatalf("Create() = %v", err)
	}
	var buf []byte
	for i, r := range recs {
		buf, err = record.Append(buf[:0], r)
		if err != nil {
			t.Fatalf("record %d: Append() = %v", i, err)
		}
		off, err := w.Append(buf)
		if err != nil {
			t.Fatalf("record %d: Writer.Append() = %v", i, err)
		}
		offs = append(offs, off)
		sizes = append(sizes, uint32(len(buf)))
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
	return offs, sizes
}

func readLog(t *testing.T, dir string, id uint32) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, LogName(id)))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func writeLog(t *testing.T, dir string, id uint32, b []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, LogName(id)), b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func assertEqualRecord(t *testing.T, got, want record.Record) {
	t.Helper()
	if !bytes.Equal(got.Key, want.Key) || !bytes.Equal(got.Value, want.Value) || got.Flags != want.Flags ||
		!got.TS.Equal(want.TS) || (want.Flags.HasExpiry() && !got.Expiry.Equal(want.Expiry)) {
		t.Fatalf("record mismatch:\n got %+v\nwant %+v", got, want)
	}
}

// scanAll collects every record Scan yields.
func scanAll(t *testing.T, r *Reader) (offs []uint32, recs []record.Record, err error) {
	t.Helper()
	err = r.Scan(func(off uint32, rec record.Record) error {
		offs = append(offs, off)
		recs = append(recs, record.Record{
			Key: bytes.Clone(rec.Key), Value: bytes.Clone(rec.Value),
			TS: rec.TS, Expiry: rec.Expiry, Flags: rec.Flags,
		})
		return nil
	})
	return offs, recs, err
}

func TestNames(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		in       string
		wantID   uint32
		wantHint bool
		wantOK   bool
	}{
		{"log", "00000007.log", 7, false, true},
		{"hint", "00000007.hint", 7, true, true},
		{"max", "04294967295.log", 0, false, false},
		{"big id", "99999999.log", 99999999, false, true},
		{"short", "7.log", 0, false, false},
		{"other", "MANIFEST", 0, false, false},
		{"lock", "LOCK", 0, false, false},
		{"tmp", "00000007.log.tmp", 0, false, false},
		{"alpha", "0000000a.log", 0, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			id, hint, ok := ParseName(tt.in)
			if ok != tt.wantOK || id != tt.wantID || hint != tt.wantHint {
				t.Errorf("ParseName(%q) = (%d, %v, %v), want (%d, %v, %v)",
					tt.in, id, hint, ok, tt.wantID, tt.wantHint, tt.wantOK)
			}
		})
	}
	if LogName(7) != "00000007.log" || HintName(7) != "00000007.hint" || LogName(123456789) != "123456789.log" {
		t.Errorf("LogName/HintName: %q %q %q", LogName(7), HintName(7), LogName(123456789))
	}
}

func TestReaderRefs(t *testing.T) {
	t.Parallel()
	r := &Reader{id: 3}
	if r.ID() != 3 || r.Refs() != 0 {
		t.Fatalf("fresh reader: id %d refs %d", r.ID(), r.Refs())
	}
	r.Ref()
	r.Ref()
	r.Unref()
	if r.Refs() != 1 {
		t.Errorf("Refs() = %d, want 1", r.Refs())
	}
	r.Unref()
	r.Unref()
	if r.Refs() != 0 {
		t.Errorf("Refs() after over-unref = %d, want 0", r.Refs())
	}
	if !(VerifyReport{}).OK() || (VerifyReport{Findings: []Finding{{Reason: "x"}}}).OK() {
		t.Error("VerifyReport.OK wrong")
	}
	if err := r.Close(); err != nil {
		t.Errorf("Close() of a fileless reader = %v", err)
	}
}

func TestWriter(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := Create(dir, 7)
	if err != nil {
		t.Fatalf("Create() = %v", err)
	}
	if w.ID() != 7 || w.Size() != record.SegmentHeaderSize {
		t.Fatalf("fresh writer: id %d size %d", w.ID(), w.Size())
	}
	if _, err := Create(dir, 7); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("Create() twice = %v, want ErrExist", err)
	}
	h := Header(7)
	if got := readLog(t, dir, 7); !bytes.Equal(got, h[:]) || !bytes.Equal(got[:5], []byte("KVLT\x01")) ||
		binary.LittleEndian.Uint32(got[5:9]) != 7 || !bytes.Equal(got[9:], make([]byte, 7)) {
		t.Fatalf("prologue = % x", got)
	}

	recs := fixtureRecords()
	var buf []byte
	want := int64(record.SegmentHeaderSize)
	for i, r := range recs {
		buf, _ = record.Append(buf[:0], r)
		off, err := w.Append(buf)
		if err != nil {
			t.Fatalf("Append(%d) = %v", i, err)
		}
		if int64(off) != want || off != goldenOffsets[i] || len(buf) != int(goldenSizes[i]) {
			t.Fatalf("record %d: off %d size %d, want %d / %d", i, off, len(buf), goldenOffsets[i], goldenSizes[i])
		}
		want += int64(len(buf))
		if w.Size() != want {
			t.Fatalf("Size() = %d, want %d", w.Size(), want)
		}
	}
	if want != goldenEnd {
		t.Fatalf("fixture ends at %d, want %d", want, goldenEnd)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync() = %v", err)
	}

	// Malformed input is refused before anything hits the disk.
	bad := bytes.Clone(buf)
	binary.LittleEndian.PutUint32(bad[4:8], uint32(len(bad))) // header claims more than it has
	if _, err := w.Append(bad); !errors.Is(err, record.ErrMalformed) {
		t.Errorf("Append(bad plen) = %v, want ErrMalformed", err)
	}
	if _, err := w.Append(buf[:3]); !errors.Is(err, record.ErrTruncated) {
		t.Errorf("Append(short) = %v, want ErrTruncated", err)
	}
	if w.Size() != goldenEnd {
		t.Errorf("refused appends changed Size() to %d", w.Size())
	}
	// ErrFull guards the uint32 offset space without touching the file.
	w.size = MaxSize - int64(len(buf)) + 1
	if _, err := w.Append(buf); !errors.Is(err, ErrFull) {
		t.Errorf("Append near MaxSize = %v, want ErrFull", err)
	}
	w.size = goldenEnd

	if err := w.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
	if err := w.Close(); err != nil {
		t.Errorf("second Close() = %v, want nil", err)
	}
	if _, err := w.Append(buf); !errors.Is(err, os.ErrClosed) {
		t.Errorf("Append after Close = %v, want ErrClosed", err)
	}
	if err := w.Sync(); !errors.Is(err, os.ErrClosed) {
		t.Errorf("Sync after Close = %v, want ErrClosed", err)
	}
	if err := w.Truncate(16); !errors.Is(err, os.ErrClosed) {
		t.Errorf("Truncate after Close = %v, want ErrClosed", err)
	}
	if got := readLog(t, dir, 7); len(got) != goldenEnd {
		t.Errorf("file is %d bytes, want %d", len(got), goldenEnd)
	}
}

func TestOpenAndTruncate(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	recs := fixtureRecords()
	build(t, dir, 3, recs[:3])

	if _, err := Open(dir, 4); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Open(missing) = %v, want ErrNotExist", err)
	}
	if _, err := Open(dir, 3); err != nil {
		// A second Open of the same file must work too (the reader path never holds it exclusively).
		t.Fatalf("Open() = %v", err)
	}
	w, err := Open(dir, 3)
	if err != nil {
		t.Fatalf("Open() = %v", err)
	}
	if w.Size() != int64(goldenOffsets[3]) {
		t.Fatalf("Open() Size() = %d, want %d", w.Size(), goldenOffsets[3])
	}
	enc, _ := record.Append(nil, recs[3])
	off, err := w.Append(enc)
	if err != nil || off != goldenOffsets[3] {
		t.Fatalf("Append after Open = %d, %v", off, err)
	}
	// Truncate below the prologue or above the size is refused; a legal cut drops the tail.
	if err := w.Truncate(15); !errors.Is(err, ErrOutOfRange) {
		t.Errorf("Truncate(15) = %v, want ErrOutOfRange", err)
	}
	if err := w.Truncate(w.Size() + 1); !errors.Is(err, ErrOutOfRange) {
		t.Errorf("Truncate(past end) = %v, want ErrOutOfRange", err)
	}
	if err := w.Truncate(int64(goldenOffsets[2])); err != nil {
		t.Fatalf("Truncate() = %v", err)
	}
	if w.Size() != int64(goldenOffsets[2]) {
		t.Errorf("Size() after Truncate = %d", w.Size())
	}
	// Appending after the cut lands at the cut.
	off, err = w.Append(enc)
	if err != nil || off != goldenOffsets[2] {
		t.Fatalf("Append after Truncate = %d, %v", off, err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(dir, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	offs, got, err := scanAll(t, r)
	if err != nil {
		t.Fatalf("Scan() = %v", err)
	}
	if !slices.Equal(offs, []uint32{16, 41, 71}) || len(got) != 3 {
		t.Fatalf("Scan offsets = %v", offs)
	}
	assertEqualRecord(t, got[2], recs[3])
	if _, err := Open(dir, 3); err != nil {
		t.Fatalf("Open() while a Reader is open = %v", err)
	}
}

func TestOpenReaderBadHeader(t *testing.T) {
	t.Parallel()
	good := Header(5)
	tests := []struct {
		name      string
		content   []byte
		id        uint32
		wantTrunc bool
	}{
		{"empty", nil, 5, true},
		{"short", good[:10], 5, true},
		{"bad magic", append([]byte("KVLH"), good[4:]...), 5, false},
		{"bad version", append([]byte("KVLT\x02"), good[5:]...), 5, false},
		{"wrong id", good[:], 6, false},
		{"hint magic", append([]byte("XXXX"), good[4:]...), 5, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			writeLog(t, dir, tt.id, tt.content)
			_, err := OpenReader(dir, tt.id)
			if !errors.Is(err, ErrBadHeader) {
				t.Fatalf("OpenReader() = %v, want ErrBadHeader", err)
			}
			if errors.Is(err, record.ErrTruncated) != tt.wantTrunc {
				t.Errorf("OpenReader() = %v, truncated match = %v, want %v", err, !tt.wantTrunc, tt.wantTrunc)
			}
			if _, err := Open(dir, tt.id); !errors.Is(err, ErrBadHeader) {
				t.Errorf("Open() = %v, want ErrBadHeader", err)
			}
			rep, err := VerifySegment(dir, tt.id)
			if err != nil || rep.OK() || rep.Findings[0].Reason != "bad header" || rep.Findings[0].Tail != tt.wantTrunc {
				t.Errorf("VerifySegment() = %+v, %v", rep, err)
			}
		})
	}
	if _, err := OpenReader(t.TempDir(), 1); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("OpenReader(missing) = %v, want ErrNotExist", err)
	}
}

func TestReadAt(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	recs := fixtureRecords()
	offs, sizes := build(t, dir, 1, recs)
	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatalf("OpenReader() = %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	if size, err := r.Size(); err != nil || size != goldenEnd {
		t.Fatalf("Size() = %d, %v", size, err)
	}
	for i := range recs {
		rec, buf, err := r.ReadAt(offs[i], sizes[i])
		if err != nil {
			t.Fatalf("ReadAt(%d) = %v", i, err)
		}
		assertEqualRecord(t, rec, recs[i])
		if len(buf) != int(sizes[i]) {
			t.Errorf("ReadAt buffer is %d bytes, want %d", len(buf), sizes[i])
		}
	}
	// A pooled buffer is reused when big enough and the record aliases it.
	buf := make([]byte, 0, 1024)
	rec, out, err := r.ReadAtBuf(offs[0], sizes[0], buf)
	if err != nil || &out[:1][0] != &buf[:1][0] || string(rec.Key) != "alpha" {
		t.Fatalf("ReadAtBuf did not reuse the buffer: %v", err)
	}

	tests := []struct {
		name    string
		off     uint32
		size    uint32
		wantErr error
	}{
		{"size below header", offs[0], record.HeaderSize - 1, ErrOutOfRange},
		{"size above max", offs[0], record.MaxRecord + 1, ErrOutOfRange},
		{"inside prologue", 8, sizes[0], ErrOutOfRange},
		{"past end", goldenEnd - 4, 20, ErrOutOfRange},
		{"beyond end", goldenEnd, 20, ErrOutOfRange},
		{"size too big for record", offs[0], sizes[0] + 1, record.ErrMalformed},
		{"size too small for record", offs[0], sizes[0] - 1, record.ErrMalformed},
		{"misaligned", offs[0] + 1, sizes[0], record.ErrMalformed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, _, err := r.ReadAt(tt.off, tt.size); !errors.Is(err, tt.wantErr) {
				t.Errorf("ReadAt(%d, %d) = %v, want %v", tt.off, tt.size, err, tt.wantErr)
			}
		})
	}

	// A flipped byte in the value is a checksum error on the random-read path too.
	t.Run("checksum", func(t *testing.T) {
		t.Parallel()
		d := t.TempDir()
		b := readLog(t, dir, 1)
		b[goldenFlipByte] ^= 0xff
		writeLog(t, d, 1, b)
		r2, err := OpenReader(d, 1)
		if err != nil {
			t.Fatal(err)
		}
		defer r2.Close()
		if _, _, err := r2.ReadAt(offs[1], sizes[1]); !errors.Is(err, record.ErrChecksum) {
			t.Errorf("ReadAt(flipped) = %v, want ErrChecksum", err)
		}
		if _, _, err := r2.ReadAt(offs[0], sizes[0]); err != nil {
			t.Errorf("ReadAt(intact neighbour) = %v", err)
		}
	})
}

func TestScanClean(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	recs := fixtureRecords()
	build(t, dir, 1, recs)
	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	offs, got, err := scanAll(t, r)
	if err != nil {
		t.Fatalf("Scan() = %v", err)
	}
	if !slices.Equal(offs, goldenOffsets) {
		t.Fatalf("Scan offsets = %v, want %v", offs, goldenOffsets)
	}
	for i := range recs {
		assertEqualRecord(t, got[i], recs[i])
	}
	// fn's error comes back unchanged and stops the walk.
	boom := errors.New("boom")
	n := 0
	err = r.Scan(func(uint32, record.Record) error {
		n++
		return boom
	})
	if !errors.Is(err, boom) || n != 1 {
		t.Errorf("Scan with failing fn = %v after %d calls", err, n)
	}
	// ScanFrom at the end is a clean empty walk; out of range is refused.
	if err := r.ScanFrom(goldenEnd, func(uint32, record.Record) error { return errors.New("called") }); err != nil {
		t.Errorf("ScanFrom(end) = %v", err)
	}
	if err := r.ScanFrom(goldenEnd+1, nil); !errors.Is(err, ErrOutOfRange) {
		t.Errorf("ScanFrom(past end) = %v, want ErrOutOfRange", err)
	}
	if err := r.ScanFrom(3, nil); !errors.Is(err, ErrOutOfRange) {
		t.Errorf("ScanFrom(prologue) = %v, want ErrOutOfRange", err)
	}
	// An empty segment (prologue only) scans cleanly.
	empty := t.TempDir()
	w, err := Create(empty, 2)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	r2, err := OpenReader(empty, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	if err := r2.Scan(func(uint32, record.Record) error { return errors.New("called") }); err != nil {
		t.Errorf("Scan(empty) = %v", err)
	}
	if err := r2.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r2.Close(); err != nil {
		t.Errorf("second Close() = %v", err)
	}
	if _, _, err := r2.ReadAt(16, 20); !errors.Is(err, os.ErrClosed) {
		t.Errorf("ReadAt after Close = %v, want ErrClosed", err)
	}
	if err := r2.Scan(nil); !errors.Is(err, os.ErrClosed) {
		t.Errorf("Scan after Close = %v, want ErrClosed", err)
	}
}

func TestScanTornTail(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	build(t, base, 1, fixtureRecords())
	whole := readLog(t, base, 1)
	last := goldenOffsets[5]
	pad := func(b []byte, n int) []byte { return append(bytes.Clone(b), make([]byte, n)...) }
	tests := []struct {
		name     string
		content  []byte
		wantOff  uint32 // offset of the first bad record
		wantN    int    // records before it
		wantTail bool
		wantErr  error
	}{
		{"cut in crc", whole[:last+2], last, 5, true, record.ErrTruncated},
		{"cut in plen", whole[:last+6], last, 5, true, record.ErrTruncated},
		{"header only", whole[:last+8], last, 5, true, record.ErrTruncated},
		{"cut in flags", whole[:last+9], last, 5, true, record.ErrTruncated},
		{"cut in key", whole[:goldenOffsets[3]+20], goldenOffsets[3], 3, true, record.ErrTruncated},
		{"cut in value", whole[:goldenTornCut], last, 5, true, record.ErrTruncated},
		{"cut at boundary", whole[:last], 0, 5, false, nil},
		{"zero-filled tail", pad(whole, 4096), goldenEnd, 6, true, record.ErrMalformed},
		{"zero-filled tail short", pad(whole, 5), goldenEnd, 6, true, record.ErrTruncated},
		{"cut then zero-filled", pad(whole[:goldenTornCut], 4096), last, 5, true, record.ErrChecksum},
		{"last record zeroed", pad(whole[:last], goldenEnd-int(last)), last, 5, true, record.ErrMalformed},
		{"zeroed payload of last", append(bytes.Clone(whole[:last+8]), make([]byte, 11)...), last, 5, true, record.ErrChecksum},
		{"garbage tail", append(bytes.Clone(whole), 0xde, 0xad, 0xbe, 0xef, 0xff, 0xff, 0xff, 0xff, 1, 2, 3), goldenEnd, 6, true, record.ErrTooLarge},
		{"garbage then record", append(append(bytes.Clone(whole[:last]), 0xde, 0xad, 0xbe, 0xef, 0xff, 0xff, 0xff, 0xff, 1, 2, 3), whole[last:]...), last, 5, false, record.ErrTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			writeLog(t, dir, 1, tt.content)
			r, err := OpenReader(dir, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			offs, _, err := scanAll(t, r)
			if len(offs) != tt.wantN {
				t.Fatalf("Scan yielded %d records (%v), want %d", len(offs), offs, tt.wantN)
			}
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("Scan() = %v, want nil", err)
				}
				return
			}
			var se *ScanError
			if !errors.As(err, &se) {
				t.Fatalf("Scan() = %v (%T), want *ScanError", err, err)
			}
			if se.Off != tt.wantOff || se.Tail != tt.wantTail || se.ID != 1 || !errors.Is(err, tt.wantErr) {
				t.Fatalf("ScanError = {Off %d Tail %v Err %v}, want {Off %d Tail %v Err %v}",
					se.Off, se.Tail, se.Err, tt.wantOff, tt.wantTail, tt.wantErr)
			}
			if se.Error() == "" {
				t.Error("empty Error()")
			}
			// Recovery's move: reopen for append and cut at the reported offset.
			if tt.wantTail {
				w, err := Open(dir, 1)
				if err != nil {
					t.Fatal(err)
				}
				if err := w.Truncate(int64(se.Off)); err != nil {
					t.Fatalf("Truncate(%d) = %v", se.Off, err)
				}
				_ = w.Close()
				if offs, _, err := scanAll(t, r); err != nil || len(offs) != tt.wantN {
					t.Fatalf("Scan after truncation = %d records, %v", len(offs), err)
				}
			}
			rep, err := VerifySegment(dir, 1)
			if err != nil {
				t.Fatal(err)
			}
			if tt.wantTail {
				// The file was truncated above; verify now sees a clean segment.
				if !rep.OK() || rep.Records != int64(tt.wantN) {
					t.Errorf("VerifySegment after truncation = %+v", rep)
				}
			} else if rep.OK() || rep.Findings[0].Off != int64(tt.wantOff) || rep.Findings[0].Tail {
				t.Errorf("VerifySegment = %+v", rep)
			}
		})
	}
}

func TestScanCorruptMiddle(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	build(t, base, 1, fixtureRecords())
	whole := readLog(t, base, 1)
	flip := func(i int) []byte {
		b := bytes.Clone(whole)
		b[i] ^= 0xff
		return b
	}
	badPlen := bytes.Clone(whole)
	binary.LittleEndian.PutUint32(badPlen[goldenFlipOff+4:], 0xffffffff)
	hugePlen := bytes.Clone(whole)
	binary.LittleEndian.PutUint32(hugePlen[goldenFlipOff+4:], record.MaxPayload) // fits the limit, not the file
	tests := []struct {
		name       string
		content    []byte
		wantOff    uint32
		wantErr    error
		wantResync uint32
		wantAfter  int // records from the resync point on
	}{
		{"value byte", flip(goldenFlipByte), goldenFlipOff, record.ErrChecksum, goldenResync, 4},
		{"crc byte", flip(goldenFlipOff), goldenFlipOff, record.ErrChecksum, goldenResync, 4},
		{"flags byte", flip(goldenFlipOff + 8), goldenFlipOff, record.ErrChecksum, goldenResync, 4},
		{"plen garbage", badPlen, goldenFlipOff, record.ErrTooLarge, goldenResync, 4},
		{"plen runs past eof", hugePlen, goldenFlipOff, record.ErrTruncated, goldenResync, 4},
		{"first record", flip(20), 16, record.ErrChecksum, goldenOffsets[1], 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			writeLog(t, dir, 1, tt.content)
			r, err := OpenReader(dir, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			offs, _, err := scanAll(t, r)
			var se *ScanError
			if !errors.As(err, &se) || se.Off != tt.wantOff || !errors.Is(err, tt.wantErr) {
				t.Fatalf("Scan() = %v after %v, want ScanError at %d with %v", err, offs, tt.wantOff, tt.wantErr)
			}
			// Decodable records follow in every case, so none of these is a torn tail; in particular a
			// bogus plen that runs past EOF must not be mistaken for one (it would truncate live data).
			if se.Tail {
				t.Fatal("Tail = true for mid-file corruption")
			}
			next, found, err := r.Resync(se.Off + 1)
			if err != nil || !found || next != tt.wantResync {
				t.Fatalf("Resync(%d) = %d, %v, %v; want %d", se.Off+1, next, found, err, tt.wantResync)
			}
			n := 0
			if err := r.ScanFrom(next, func(uint32, record.Record) error { n++; return nil }); err != nil {
				t.Fatalf("ScanFrom(%d) = %v", next, err)
			}
			if n != tt.wantAfter {
				t.Errorf("ScanFrom yielded %d records, want %d", n, tt.wantAfter)
			}
			// Resync past the last record finds nothing.
			if _, found, err := r.Resync(goldenOffsets[5] + 1); found || err != nil {
				t.Errorf("Resync(after last) = found %v, %v", found, err)
			}
			rep, err := VerifySegment(dir, 1)
			if err != nil {
				t.Fatal(err)
			}
			if rep.OK() || len(rep.Findings) != 1 || rep.Findings[0].Off != int64(tt.wantOff) ||
				rep.Records != int64(len(offs)+tt.wantAfter) || rep.Bytes != int64(len(tt.content)) {
				t.Errorf("VerifySegment = %+v", rep)
			}
		})
	}

	// Resync from the first byte lands on the first record; from inside the prologue too.
	r, err := OpenReader(base, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	for _, from := range []uint32{0, 1, 16} {
		if next, found, err := r.Resync(from); err != nil || !found || next != 16 {
			t.Errorf("Resync(%d) = %d, %v, %v; want 16", from, next, found, err)
		}
	}
	if next, found, err := r.Resync(17); err != nil || !found || next != goldenOffsets[1] {
		t.Errorf("Resync(17) = %d, %v, %v; want %d", next, found, err, goldenOffsets[1])
	}
}

func TestHint(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	entries := fixtureHint()
	keys := make([][]byte, len(entries))
	for i := range entries {
		keys[i] = bytes.Clone(entries[i].Key)
	}
	iter := func(yield func(key []byte, e HintEntry) bool) {
		for i, e := range entries {
			if !yield(keys[i], e) {
				return
			}
		}
	}
	if err := WriteHint(dir, 1, iter); err != nil {
		t.Fatalf("WriteHint() = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, HintName(1)+TempSuffix)); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("temp file left behind: %v", err)
	}
	// Mutating the caller's keys after the write must not matter: ReadHint returns copies.
	got, err := ReadHint(dir, 1)
	if err != nil {
		t.Fatalf("ReadHint() = %v", err)
	}
	for i := range keys {
		keys[i][0] = 'X'
	}
	if len(got) != len(entries) {
		t.Fatalf("ReadHint() returned %d entries, want %d", len(got), len(entries))
	}
	for i, e := range got {
		w := entries[i]
		if !bytes.Equal(e.Key, w.Key) || e.Off != w.Off || e.Size != w.Size || e.ExpSec != w.ExpSec || e.Flags != w.Flags {
			t.Errorf("entry %d = %+v, want %+v", i, e, w)
		}
		if cap(e.Key) != len(e.Key) {
			t.Errorf("entry %d key has spare capacity", i)
		}
	}
	if got[1].ExpSec != 1_700_000_060 {
		t.Errorf("beta ExpSec = %d, want 1700000060", got[1].ExpSec)
	}
	b, err := os.ReadFile(filepath.Join(dir, HintName(1)))
	if err != nil {
		t.Fatal(err)
	}
	if string(b[:5]) != "KVLH\x01" || binary.LittleEndian.Uint32(b[5:9]) != 1 || string(b[len(b)-4:]) != "KVLH" ||
		binary.LittleEndian.Uint32(b[len(b)-8:len(b)-4]) != uint32(len(entries)) {
		t.Fatalf("hint framing wrong: head % x tail % x", b[:16], b[len(b)-12:])
	}

	// An empty hint is legal and round-trips; an overwrite replaces atomically.
	if err := WriteHint(dir, 1, func(func([]byte, HintEntry) bool) {}); err != nil {
		t.Fatalf("WriteHint(empty) = %v", err)
	}
	if got, err := ReadHint(dir, 1); err != nil || len(got) != 0 {
		t.Fatalf("ReadHint(empty) = %v, %v", got, err)
	}
	if _, err := ReadHint(dir, 2); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("ReadHint(missing) = %v, want ErrNotExist", err)
	}

	// Refused entries leave no file behind.
	bad := []struct {
		name string
		key  []byte
		e    HintEntry
		want error
	}{
		{"key too large", make([]byte, record.MaxKey+1), HintEntry{Off: 16, Size: 8}, record.ErrTooLarge},
		{"unknown flags", []byte("k"), HintEntry{Off: 16, Size: 8, Flags: 0x80}, record.ErrMalformed},
		{"offset in prologue", []byte("k"), HintEntry{Off: 8, Size: 8}, record.ErrMalformed},
		{"size below header", []byte("k"), HintEntry{Off: 16, Size: 7}, record.ErrMalformed},
	}
	for _, tt := range bad {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			d := t.TempDir()
			err := WriteHint(d, 9, func(yield func([]byte, HintEntry) bool) { yield(tt.key, tt.e) })
			if !errors.Is(err, tt.want) {
				t.Fatalf("WriteHint() = %v, want %v", err, tt.want)
			}
			if ents, err := os.ReadDir(d); err != nil || len(ents) != 0 {
				t.Errorf("dir not clean after a refused hint: %v %v", ents, err)
			}
		})
	}
	// A bad entry after good ones also leaves nothing behind.
	d := t.TempDir()
	err = WriteHint(d, 9, func(yield func([]byte, HintEntry) bool) {
		if !yield([]byte("ok"), HintEntry{Off: 16, Size: 8}) {
			t.Error("first yield refused")
		}
		if yield([]byte("bad"), HintEntry{Off: 1, Size: 8}) {
			t.Error("bad entry accepted")
		}
	})
	if !errors.Is(err, record.ErrMalformed) {
		t.Fatalf("WriteHint() = %v", err)
	}
	if ents, _ := os.ReadDir(d); len(ents) != 0 {
		t.Errorf("dir not clean: %v", ents)
	}
	if err := WriteHint(filepath.Join(d, "missing"), 1, iter); err == nil {
		t.Error("WriteHint into a missing dir = nil")
	}
}

func TestHintCorrupt(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	entries := fixtureHint()
	iter := func(yield func(key []byte, e HintEntry) bool) {
		for _, e := range entries {
			if !yield(e.Key, e) {
				return
			}
		}
	}
	if err := WriteHint(dir, 1, iter); err != nil {
		t.Fatal(err)
	}
	good, err := os.ReadFile(filepath.Join(dir, HintName(1)))
	if err != nil {
		t.Fatal(err)
	}
	flip := func(i int) []byte {
		b := bytes.Clone(good)
		b[i] ^= 0xff
		return b
	}
	fixCRC := func(b []byte) {
		body := b[:len(b)-HintFooterSize]
		binary.LittleEndian.PutUint32(b[len(b)-HintFooterSize:], record.Checksum(body))
	}
	badCount := bytes.Clone(good)
	binary.LittleEndian.PutUint32(badCount[len(badCount)-8:], uint32(len(entries)+1))
	fixCRC(badCount)
	badFlags := bytes.Clone(good)
	badFlags[HintHeaderSize] = 0x80
	fixCRC(badFlags)
	badKeyLen := bytes.Clone(good)
	badKeyLen[HintHeaderSize+4] = 0x7f // alpha's keyLen byte (flags, expSec 0, off 16, size 25, keyLen)
	fixCRC(badKeyLen)
	wrongID := bytes.Clone(good)
	binary.LittleEndian.PutUint32(wrongID[5:9], 2)
	fixCRC(wrongID)
	tests := []struct {
		name string
		in   []byte
	}{
		{"empty", nil},
		{"header only", good[:HintHeaderSize]},
		{"truncated", good[:len(good)-1]},
		{"bad head magic", flip(0)},
		{"bad version", flip(4)},
		{"wrong segment id", wrongID},
		{"bad tail magic", flip(len(good) - 1)},
		{"bad crc", flip(len(good) - HintFooterSize)},
		{"flipped entry byte", flip(HintHeaderSize + 6)},
		{"bad count", badCount},
		{"bad entry flags", badFlags},
		{"bad key length", badKeyLen},
		{"trailing byte", append(bytes.Clone(good), 0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			d := t.TempDir()
			if err := os.WriteFile(filepath.Join(d, HintName(1)), tt.in, 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := ReadHint(d, 1); !errors.Is(err, ErrBadHint) {
				t.Errorf("ReadHint() = %v, want ErrBadHint", err)
			}
		})
	}
	// The unmodified file still reads.
	if _, err := ReadHint(dir, 1); err != nil {
		t.Errorf("ReadHint(good) = %v", err)
	}
}

func TestVerifySegment(t *testing.T) {
	t.Parallel()
	entries := fixtureHint()
	hintIter := func(entries []HintEntry) func(yield func([]byte, HintEntry) bool) {
		return func(yield func([]byte, HintEntry) bool) {
			for _, e := range entries {
				if !yield(e.Key, e) {
					return
				}
			}
		}
	}
	t.Run("missing log", func(t *testing.T) {
		t.Parallel()
		if _, err := VerifySegment(t.TempDir(), 1); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("VerifySegment(missing) = %v, want ErrNotExist", err)
		}
	})
	t.Run("clean with hint", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		build(t, dir, 1, fixtureRecords())
		if err := WriteHint(dir, 1, hintIter(entries)); err != nil {
			t.Fatal(err)
		}
		rep, err := VerifySegment(dir, 1)
		if err != nil {
			t.Fatal(err)
		}
		if !rep.OK() || rep.ID != 1 || rep.Records != 6 || rep.Bytes != goldenEnd || !rep.HintPresent || !rep.HintValid ||
			rep.HintEntries != int64(len(entries)) {
			t.Errorf("VerifySegment = %+v", rep)
		}
	})
	t.Run("clean without hint", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		build(t, dir, 1, fixtureRecords())
		rep, err := VerifySegment(dir, 1)
		if err != nil {
			t.Fatal(err)
		}
		if !rep.OK() || rep.Records != 6 || rep.HintPresent || rep.HintValid {
			t.Errorf("VerifySegment = %+v", rep)
		}
	})
	t.Run("hint mismatch", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		build(t, dir, 1, fixtureRecords())
		stale := slices.Clone(entries)
		stale[0].Off, stale[0].Size = goldenOffsets[1], goldenSizes[1] // alpha points at beta's record
		stale[1].ExpSec++                                              // beta's expiry drifted
		stale[2].Flags |= record.FlagTombstone                         // delta's flags differ
		if err := WriteHint(dir, 1, hintIter(stale)); err != nil {
			t.Fatal(err)
		}
		rep, err := VerifySegment(dir, 1)
		if err != nil {
			t.Fatal(err)
		}
		if rep.OK() || rep.Records != 6 || !rep.HintPresent || rep.HintValid || len(rep.Findings) != 3 {
			t.Fatalf("VerifySegment = %+v", rep)
		}
		for i, f := range rep.Findings {
			if !f.Hint || f.Reason != "hint mismatch" || f.Off < HintHeaderSize {
				t.Errorf("finding %d = %+v", i, f)
			}
		}
		if rep.Findings[0].Off != HintHeaderSize {
			t.Errorf("first mismatch at hint offset %d, want %d", rep.Findings[0].Off, HintHeaderSize)
		}
	})
	t.Run("bad hint", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		build(t, dir, 1, fixtureRecords())
		if err := os.WriteFile(filepath.Join(dir, HintName(1)), []byte("KVLH junk"), 0o644); err != nil {
			t.Fatal(err)
		}
		rep, err := VerifySegment(dir, 1)
		if err != nil {
			t.Fatal(err)
		}
		if rep.OK() || rep.Records != 6 || !rep.HintPresent || rep.HintValid ||
			len(rep.Findings) != 1 || !rep.Findings[0].Hint || rep.Findings[0].Reason != "bad hint" {
			t.Errorf("VerifySegment = %+v", rep)
		}
	})
	t.Run("two corrupt stretches", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		build(t, dir, 1, fixtureRecords())
		b := readLog(t, dir, 1)
		b[goldenFlipByte] ^= 0xff           // record 2
		b[goldenOffsets[4]+10] ^= 0xff      // record 5
		b = append(b, make([]byte, 100)...) // and a zero-filled tail
		writeLog(t, dir, 1, b)
		rep, err := VerifySegment(dir, 1)
		if err != nil {
			t.Fatal(err)
		}
		want := []Finding{
			{Off: int64(goldenOffsets[1]), Reason: "checksum"},
			{Off: int64(goldenOffsets[4]), Reason: "checksum"},
			{Off: goldenEnd, Reason: "malformed", Tail: true},
		}
		if rep.Records != 4 || !slices.Equal(rep.Findings, want) {
			t.Errorf("VerifySegment = %+v, want findings %+v", rep, want)
		}
	})
}

func TestConcurrentReaders(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	recs := fixtureRecords()
	offs, sizes := build(t, dir, 1, recs)
	r, err := OpenReader(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.Ref()
			defer r.Unref()
			var buf []byte
			for i := range 50 {
				j := (g + i) % len(recs)
				var (
					rec record.Record
					err error
				)
				rec, buf, err = r.ReadAtBuf(offs[j], sizes[j], buf)
				if err != nil {
					t.Errorf("goroutine %d: ReadAt(%d) = %v", g, j, err)
					return
				}
				if !bytes.Equal(rec.Key, recs[j].Key) {
					t.Errorf("goroutine %d: key %q, want %q", g, rec.Key, recs[j].Key)
					return
				}
				if i%10 == 0 {
					n := 0
					if err := r.Scan(func(uint32, record.Record) error { n++; return nil }); err != nil || n != len(recs) {
						t.Errorf("goroutine %d: Scan = %d, %v", g, n, err)
						return
					}
					if next, found, err := r.Resync(17); err != nil || !found || next != offs[1] {
						t.Errorf("goroutine %d: Resync = %d %v %v", g, next, found, err)
						return
					}
				}
				if r.Refs() < 1 {
					t.Errorf("goroutine %d: Refs() = %d while pinned", g, r.Refs())
					return
				}
			}
		}()
	}
	wg.Wait()
	if r.Refs() != 0 {
		t.Errorf("Refs() = %d after all pins dropped", r.Refs())
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestReaderHandlePool: OpenReaderN clamps the handle count, every handle in the pool serves correct
// reads under concurrency (more goroutines than handles, so the round-robin wraps), the default is
// platform-specific, and Close closes every handle (later reads fail with os.ErrClosed).
func TestReaderHandlePool(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	recs := fixtureRecords()
	offs, sizes := build(t, dir, 1, recs)
	tests := []struct {
		name string
		n    int
		want int
	}{
		{"zero clamps to one", 0, 1},
		{"negative clamps to one", -3, 1},
		{"one", 1, 1},
		{"four", 4, 4},
		{"above the cap", MaxReadHandles + 5, MaxReadHandles},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r, err := OpenReaderN(dir, 1, tt.n)
			if err != nil {
				t.Fatal(err)
			}
			if got := r.Handles(); got != tt.want {
				t.Fatalf("Handles() = %d, want %d", got, tt.want)
			}
			var wg sync.WaitGroup
			for g := range 3 * tt.want {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := range 3 * MaxReadHandles {
						j := (g + i) % len(recs)
						rec, _, err := r.ReadAt(offs[j], sizes[j])
						if err != nil || !bytes.Equal(rec.Key, recs[j].Key) || !bytes.Equal(rec.Value, recs[j].Value) {
							t.Errorf("goroutine %d: ReadAt(%d) = %+v, %v", g, j, rec, err)
							return
						}
					}
					n := 0
					if err := r.Scan(func(uint32, record.Record) error { n++; return nil }); err != nil || n != len(recs) {
						t.Errorf("goroutine %d: Scan = %d, %v", g, n, err)
					}
				}()
			}
			wg.Wait()
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			if _, _, err := r.ReadAt(offs[0], sizes[0]); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("ReadAt after Close = %v, want os.ErrClosed", err)
			}
			if err := r.Close(); err != nil {
				t.Fatalf("second Close = %v", err)
			}
		})
	}
	t.Run("default", func(t *testing.T) {
		t.Parallel()
		want := DefaultReadHandles()
		if runtime.GOOS == "windows" {
			if want < 1 || want > 8 {
				t.Fatalf("DefaultReadHandles() = %d on windows, want 1..8", want)
			}
		} else if want != 1 {
			t.Fatalf("DefaultReadHandles() = %d, want 1 off windows (pread is lock-free)", want)
		}
		r, err := OpenReader(dir, 1)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		if got := r.Handles(); got != want {
			t.Fatalf("OpenReader opened %d handles, want %d", got, want)
		}
	})
	t.Run("missing file fails cleanly", func(t *testing.T) {
		t.Parallel()
		if _, err := OpenReaderN(dir, 99, 4); err == nil {
			t.Fatal("OpenReaderN on a missing segment succeeded")
		}
	})
}

// TestGolden pins testdata/ (see the fixture comment): with -update it rewrites the files, otherwise
// it checks they are byte-identical to what the current code produces and behave as documented.
func TestGolden(t *testing.T) {
	t.Parallel()
	scratch := t.TempDir()
	build(t, scratch, goldenID, fixtureRecords())
	if err := WriteHint(scratch, goldenID, func(yield func([]byte, HintEntry) bool) {
		for _, e := range fixtureHint() {
			if !yield(e.Key, e) {
				return
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	whole := readLog(t, scratch, goldenID)
	hint, err := os.ReadFile(filepath.Join(scratch, HintName(goldenID)))
	if err != nil {
		t.Fatal(err)
	}
	corrupt := bytes.Clone(whole)
	corrupt[goldenFlipByte] ^= 0xff
	files := map[string][]byte{
		filepath.Join("valid", LogName(goldenID)):   whole,
		filepath.Join("valid", HintName(goldenID)):  hint,
		filepath.Join("torn", LogName(goldenID)):    whole[:goldenTornCut],
		filepath.Join("corrupt", LogName(goldenID)): corrupt,
	}
	if *update {
		for name, b := range files {
			p := filepath.Join("testdata", name)
			if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(p, b, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	for name, want := range files {
		got, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			t.Fatalf("%s: %v (run with -update)", name, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("testdata/%s differs from the current encoding (format change? run with -update)", name)
		}
	}

	// valid: six records at the documented offsets, hint valid.
	r, err := OpenReader(filepath.Join("testdata", "valid"), goldenID)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	offs, recs, err := scanAll(t, r)
	if err != nil || !slices.Equal(offs, goldenOffsets) {
		t.Fatalf("valid: Scan = %v, %v", offs, err)
	}
	for i, want := range fixtureRecords() {
		assertEqualRecord(t, recs[i], want)
	}
	rep, err := VerifySegment(filepath.Join("testdata", "valid"), goldenID)
	if err != nil || !rep.OK() || rep.Records != 6 || !rep.HintValid || rep.HintEntries != 4 {
		t.Fatalf("valid: VerifySegment = %+v, %v", rep, err)
	}
	ents, err := ReadHint(filepath.Join("testdata", "valid"), goldenID)
	if err != nil || len(ents) != 4 || string(ents[0].Key) != "alpha" || string(ents[3].Key) != "eps" {
		t.Fatalf("valid: ReadHint = %v, %v", ents, err)
	}

	// torn: five records, then a torn tail at 393.
	r2, err := OpenReader(filepath.Join("testdata", "torn"), goldenID)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	offs, _, err = scanAll(t, r2)
	var se *ScanError
	if !errors.As(err, &se) || se.Off != goldenTornOff || !se.Tail || len(offs) != 5 || !errors.Is(err, record.ErrTruncated) {
		t.Fatalf("torn: Scan = %v after %v", err, offs)
	}

	// corrupt: one record, then mid-file corruption at 41; resync lands on 71.
	r3, err := OpenReader(filepath.Join("testdata", "corrupt"), goldenID)
	if err != nil {
		t.Fatal(err)
	}
	defer r3.Close()
	offs, _, err = scanAll(t, r3)
	if !errors.As(err, &se) || se.Off != goldenFlipOff || se.Tail || len(offs) != 1 || !errors.Is(err, record.ErrChecksum) {
		t.Fatalf("corrupt: Scan = %v after %v", err, offs)
	}
	if next, found, err := r3.Resync(se.Off + 1); err != nil || !found || next != goldenResync {
		t.Fatalf("corrupt: Resync = %d, %v, %v", next, found, err)
	}
}

func BenchmarkWriterAppend(b *testing.B) {
	dir := b.TempDir()
	w, err := Create(dir, 1)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	enc, _ := record.Append(nil, record.Record{Key: []byte("user:0000012345"), Value: bytes.Repeat([]byte("x"), 1024), TS: ms(goldenTS)})
	b.SetBytes(int64(len(enc)))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := w.Append(enc); err != nil {
			b.Fatal(err)
		}
	}
}

func benchSegment(b *testing.B, n int) (dir string, offs, sizes []uint32) {
	b.Helper()
	dir = b.TempDir()
	w, err := Create(dir, 1)
	if err != nil {
		b.Fatal(err)
	}
	var buf []byte
	for i := range n {
		buf, _ = record.Append(buf[:0], record.Record{Key: []byte("user:" + pad(uint32(i))), Value: bytes.Repeat([]byte("x"), 1024), TS: ms(goldenTS)})
		off, err := w.Append(buf)
		if err != nil {
			b.Fatal(err)
		}
		offs = append(offs, off)
		sizes = append(sizes, uint32(len(buf)))
	}
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}
	return dir, offs, sizes
}

func BenchmarkReadAt(b *testing.B) {
	dir, offs, sizes := benchSegment(b, 1024)
	r, err := OpenReader(dir, 1)
	if err != nil {
		b.Fatal(err)
	}
	defer r.Close()
	var buf []byte
	b.SetBytes(int64(sizes[0]))
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		j := i % len(offs)
		i++
		_, buf, err = r.ReadAtBuf(offs[j], sizes[j], buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkScan(b *testing.B) {
	dir, _, sizes := benchSegment(b, 8192)
	r, err := OpenReader(dir, 1)
	if err != nil {
		b.Fatal(err)
	}
	defer r.Close()
	b.SetBytes(int64(sizes[0]) * int64(len(sizes)))
	b.ReportAllocs()
	for b.Loop() {
		if err := r.Scan(func(uint32, record.Record) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
}
