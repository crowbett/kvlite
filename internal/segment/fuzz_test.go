package segment

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/crowbett/kvlite/internal/record"
)

// fuzzID is the segment id every fuzzed file is opened as; seeds carry it in their prologue so the
// header check passes and the mutator can still flip it.
const fuzzID = 1

// segmentSeeds builds the seed corpus for FuzzSegmentScan: every branch of Reader.Scan, scanError,
// Resync and candidate that can be named, from a clean file through torn tails, mid-file corruption,
// bogus lengths and garbage stretches with real records behind them.
func segmentSeeds(tb testing.TB) [][]byte {
	tb.Helper()
	enc := func(recs ...record.Record) []byte {
		var out []byte
		for _, r := range recs {
			b, err := record.Append(nil, r)
			if err != nil {
				tb.Fatal(err)
			}
			out = append(out, b...)
		}
		return out
	}
	hdr := Header(fuzzID)
	fix := fixtureRecords()
	body := enc(fix...)
	whole := append(append([]byte{}, hdr[:]...), body...)
	first := enc(fix[0])
	second := enc(fix[1])

	seeds := [][]byte{
		nil,                  // empty file: bad header (truncated)
		hdr[:8],              // half a prologue
		hdr[:],               // prologue only: clean empty segment
		whole,                // the golden six records
		whole[:len(whole)-9], // torn inside the last record
		whole[:len(whole)-1], // torn one byte short
		append(whole[:0:0], whole[:record.SegmentHeaderSize+3]...), // torn inside the first header
	}
	// Zero fill after a clean record: Tail (nothing decodable follows).
	zeroFill := append([]byte{}, hdr[:]...)
	zeroFill = append(zeroFill, first...)
	seeds = append(seeds, append(zeroFill, make([]byte, 40)...))
	// Garbage then a real record: mid-file corruption, Resync must find the record.
	garbage := append([]byte{}, hdr[:]...)
	garbage = append(garbage, first...)
	garbage = append(garbage, "garbagegarbage"...)
	seeds = append(seeds, append(garbage, second...))
	// Flipped byte in the middle (crc failure) with intact records after it.
	corrupt := bytes.Clone(whole)
	corrupt[goldenFlipByte] ^= 0xff
	seeds = append(seeds, corrupt)
	// Bogus plen: the header declares MaxPayload+1 (ErrTooLarge) with a good record behind it.
	big := append([]byte{}, hdr[:]...)
	big = append(big, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.LittleEndian.PutUint32(big[record.SegmentHeaderSize+4:], record.MaxPayload+1)
	seeds = append(seeds, append(big, second...))
	// plen that runs past EOF while intact records sit inside its claimed extent: corruption, not tail.
	runs := append([]byte{}, hdr[:]...)
	runs = append(runs, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.LittleEndian.PutUint32(runs[record.SegmentHeaderSize+4:], 1<<20)
	seeds = append(seeds, append(runs, second...))
	// Wrong magic, wrong version, wrong id.
	badMagic := bytes.Clone(whole)
	copy(badMagic, "NOPE")
	badVer := bytes.Clone(whole)
	badVer[4] = 9
	badID := bytes.Clone(whole)
	binary.LittleEndian.PutUint32(badID[5:9], 7)
	seeds = append(seeds, badMagic, badVer, badID)
	// Every flag combination in one segment, including an expiry and a max-size key.
	var recs []record.Record
	for f := record.Flags(0); f <= record.FlagsMask; f++ {
		r := record.Record{Key: []byte{'k', byte(f)}, Value: []byte{byte(f)}, TS: time.UnixMilli(1_700_000_000_000 + int64(f)), Flags: f}
		if f.HasExpiry() {
			r.Expiry = time.UnixMilli(1_700_000_060_000)
		}
		recs = append(recs, r)
	}
	recs = append(recs, record.Record{Key: bytes.Repeat([]byte("K"), 4<<10), Value: []byte("v"), TS: time.UnixMilli(1)}) // a long key; MaxKey itself is FuzzRecord territory and would make every mutation 64 KiB
	seeds = append(seeds, append(append([]byte{}, hdr[:]...), enc(recs...)...))
	// A record whose value is itself a valid record: Resync's byte-granular search meets a decoy.
	decoy := record.Record{Key: []byte("outer"), Value: first, TS: time.UnixMilli(2)}
	decoyFile := append([]byte{}, hdr[:]...)
	decoyFile = append(decoyFile, enc(decoy)...)
	seeds = append(seeds, append(decoyFile, 0xff, 0xff))
	// The committed golden files, if present.
	for _, name := range []string{"valid", "torn", "corrupt"} {
		if b, err := os.ReadFile(filepath.Join("testdata", name, LogName(goldenID))); err == nil {
			seeds = append(seeds, b)
		}
	}
	return seeds
}

// FuzzSegmentScan writes arbitrary bytes as 00000001.log and drives OpenReader, Scan, ScanFrom,
// Resync, ReadAt and VerifySegment over it. Invariants: nothing panics; every record Scan yields
// sits inside the file, re-parses identically from its offset and reads back through ReadAt;
// a ScanError names an offset inside the file, Tail == false always has a record Resync can find and
// ScanFrom(next) then yields it; VerifySegment counts exactly what a repairing walk (resync after a
// mid-file error, stop at a tail error, like recovery under WithRepair) counts, and its findings sit
// inside the file.
func FuzzSegmentScan(f *testing.F) {
	for _, s := range segmentSeeds(f) {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 4<<20 {
			t.Skip("cap the file size; the format is exercised well below it")
		}
		dir := fuzzScratch(t)
		path := filepath.Join(dir, LogName(fuzzID))
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		r, err := OpenReaderN(dir, fuzzID, 1)
		if err != nil {
			if !errors.Is(err, ErrBadHeader) {
				t.Fatalf("OpenReader: unexpected error class: %v", err)
			}
			if len(data) >= record.SegmentHeaderSize && bytes.Equal(data[:record.SegmentHeaderSize], hdrBytes()) {
				t.Fatalf("OpenReader rejected a valid prologue: %v", err)
			}
			rep, verr := VerifySegment(dir, fuzzID)
			if verr != nil || len(rep.Findings) != 1 || rep.Findings[0].Reason != "bad header" {
				t.Fatalf("VerifySegment on a bad header = %+v, %v", rep, verr)
			}
			return
		}
		defer r.Close()

		type got struct {
			off  uint32
			size int
			rec  record.Record
		}
		var yielded []got
		walk := func(from uint32) error {
			return r.ScanFrom(from, func(off uint32, rec record.Record) error {
				size := record.EncodedSize(rec)
				if int(off) < record.SegmentHeaderSize || int(off)+size > len(data) {
					t.Fatalf("Scan yielded off=%d size=%d outside %d bytes", off, size, len(data))
				}
				want, n, err := record.Parse(data[off:])
				if err != nil || n != size {
					t.Fatalf("record at %d does not re-parse: n=%d err=%v", off, n, err)
				}
				if !bytes.Equal(want.Key, rec.Key) || !bytes.Equal(want.Value, rec.Value) || want.Flags != rec.Flags {
					t.Fatalf("Scan and Parse disagree at %d", off)
				}
				yielded = append(yielded, got{off, size, record.Record{Key: bytes.Clone(rec.Key), Value: bytes.Clone(rec.Value), Flags: rec.Flags}})
				return nil
			})
		}

		// A repairing walk: Scan, and on a mid-file error Resync and continue, exactly like WithRepair.
		// Resync only reports an offset where a whole record parsed, so ScanFrom there must yield it.
		pos := uint32(record.SegmentHeaderSize)
		for {
			before := len(yielded)
			err := walk(pos)
			if pos != record.SegmentHeaderSize && len(yielded) == before {
				t.Fatalf("Resync said a record starts at %d but ScanFrom yielded none: %v", pos, err)
			}
			if err == nil {
				break
			}
			var se *ScanError
			if !errors.As(err, &se) {
				t.Fatalf("Scan: not a ScanError: %v", err)
			}
			if se.ID != fuzzID || int(se.Off) < record.SegmentHeaderSize || int64(se.Off) > int64(len(data)) {
				t.Fatalf("ScanError out of range: %+v (file %d bytes)", se, len(data))
			}
			if !errors.Is(err, record.ErrTruncated) && !errors.Is(err, record.ErrChecksum) &&
				!errors.Is(err, record.ErrMalformed) && !errors.Is(err, record.ErrTooLarge) {
				t.Fatalf("ScanError wraps an unknown cause: %v", err)
			}
			if se.Tail {
				// Nothing decodable follows as a record. Resync may still find a decoy inside the bad
				// record's own bytes (a value that contains a record), which is exactly why recovery
				// and VerifySegment stop here instead of resyncing.
				break
			}
			next, found, rerr := r.Resync(se.Off + 1)
			if rerr != nil {
				t.Fatalf("Resync: %v", rerr)
			}
			if !found {
				t.Fatalf("Tail=false at %d but Resync finds nothing after it", se.Off)
			}
			if next <= se.Off || int(next)+record.HeaderSize > len(data) {
				t.Fatalf("Resync(%d) = %d, out of range for %d bytes", se.Off+1, next, len(data))
			}
			pos = next
		}

		// Everything Scan yielded must read back through ReadAt with the index it would get.
		var buf []byte
		for _, g := range yielded {
			rec, b, err := r.ReadAtBuf(g.off, uint32(g.size), buf)
			buf = b
			if err != nil {
				t.Fatalf("ReadAt(%d, %d): %v", g.off, g.size, err)
			}
			if !bytes.Equal(rec.Key, g.rec.Key) || !bytes.Equal(rec.Value, g.rec.Value) || rec.Flags != g.rec.Flags {
				t.Fatalf("ReadAt(%d) disagrees with Scan", g.off)
			}
		}
		// ReadAt with an index that lies must fail, never panic or return a different record silently.
		if len(yielded) > 0 {
			g := yielded[0]
			if _, _, err := r.ReadAt(g.off, uint32(g.size)+1); err == nil {
				t.Fatalf("ReadAt(%d, %d+1) succeeded", g.off, g.size)
			}
			if _, _, err := r.ReadAt(g.off+1, uint32(g.size)); err == nil {
				t.Fatalf("ReadAt(%d+1, %d) succeeded", g.off, g.size)
			}
		}

		rep, err := VerifySegment(dir, fuzzID)
		if err != nil {
			t.Fatalf("VerifySegment: %v", err)
		}
		if rep.Records != int64(len(yielded)) {
			t.Fatalf("VerifySegment counted %d records, the repairing walk %d", rep.Records, len(yielded))
		}
		for _, fd := range rep.Findings {
			if fd.Off < 0 || fd.Off > int64(len(data)) {
				t.Fatalf("finding outside the file: %+v", fd)
			}
		}
	})
}

// fuzzScratch is one directory per fuzz worker process, reused across iterations: a fresh TempDir per
// input costs more than the parse it hosts. The segment file is overwritten each time.
func fuzzScratch(t *testing.T) string {
	t.Helper()
	fuzzScratchOnce.Do(func() {
		d, err := os.MkdirTemp("", "kvlite-fuzz-*")
		if err != nil {
			t.Fatal(err)
		}
		fuzzScratchDir = d
	})
	return fuzzScratchDir
}

var (
	fuzzScratchOnce sync.Once
	fuzzScratchDir  string
)

func hdrBytes() []byte {
	h := Header(fuzzID)
	return h[:]
}

// hintSeeds is the seed corpus for FuzzHint: a real hint, an empty one, every structural failure.
func hintSeeds(tb testing.TB) [][]byte {
	tb.Helper()
	dir := tb.TempDir()
	write := func(entries []HintEntry) []byte {
		if err := WriteHint(dir, fuzzID, func(yield func([]byte, HintEntry) bool) {
			for _, e := range entries {
				if !yield(e.Key, e) {
					return
				}
			}
		}); err != nil {
			tb.Fatal(err)
		}
		b, err := os.ReadFile(filepath.Join(dir, HintName(fuzzID)))
		if err != nil {
			tb.Fatal(err)
		}
		return b
	}
	full := write(fixtureHint())
	empty := write(nil)
	maxKey := write([]HintEntry{{Key: bytes.Repeat([]byte("K"), record.MaxKey), Off: 16, Size: 8 + 5 + record.MaxKey, ExpSec: 1}})
	badCRC := bytes.Clone(full)
	badCRC[HintHeaderSize] ^= 0xff
	badCount := bytes.Clone(full)
	badCount[len(full)-8]++
	badMagic := bytes.Clone(full)
	copy(badMagic[len(full)-4:], "NOPE")
	seeds := [][]byte{nil, full[:HintHeaderSize], full, empty, maxKey, badCRC, badCount, badMagic}
	if b, err := os.ReadFile(filepath.Join("testdata", "valid", HintName(goldenID))); err == nil {
		seeds = append(seeds, b)
	}
	return seeds
}

// FuzzHint feeds arbitrary bytes to parseHint. Invariants: no panic; a parse that succeeds yields
// exactly the footer's count of entries, every key aliases the input and stays within its own bounds,
// every entry satisfies the offset/size limits WriteHint enforces, and writing the entries back with
// WriteHint then re-reading them yields the same entries (round trip through the canonical encoder).
func FuzzHint(f *testing.F) {
	for _, s := range hintSeeds(f) {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		entries, offs, err := parseHint(data, fuzzID)
		if err != nil {
			if !errors.Is(err, ErrBadHint) {
				t.Fatalf("parseHint: not ErrBadHint: %v", err)
			}
			if entries != nil || offs != nil {
				t.Fatal("parseHint returned entries with an error")
			}
			return
		}
		count := binary.LittleEndian.Uint32(data[len(data)-8 : len(data)-4])
		if uint64(len(entries)) != uint64(count) || len(offs) != len(entries) {
			t.Fatalf("parseHint: %d entries, %d offsets, footer count %d", len(entries), len(offs), count)
		}
		for i, e := range entries {
			if len(e.Key) > record.MaxKey || e.Off < record.SegmentHeaderSize || e.Size < record.HeaderSize ||
				e.Size > record.MaxRecord || e.Flags&^record.FlagsMask != 0 {
				t.Fatalf("entry %d violates the limits: %+v", i, e)
			}
			if len(e.Key) > 0 && !aliases(data, e.Key) {
				t.Fatalf("entry %d key does not alias the input", i)
			}
			if cap(e.Key) != len(e.Key) {
				t.Fatalf("entry %d key capacity %d leaks past its length %d", i, cap(e.Key), len(e.Key))
			}
			if offs[i] < HintHeaderSize || offs[i] >= int64(len(data)-HintFooterSize) {
				t.Fatalf("entry %d offset %d outside the body", i, offs[i])
			}
		}
		dir := t.TempDir()
		err = WriteHint(dir, fuzzID, func(yield func([]byte, HintEntry) bool) {
			for _, e := range entries {
				if !yield(e.Key, e) {
					return
				}
			}
		})
		if err != nil {
			t.Fatalf("WriteHint of parsed entries: %v", err)
		}
		back, err := ReadHint(dir, fuzzID)
		if err != nil {
			t.Fatalf("ReadHint after WriteHint: %v", err)
		}
		if len(back) != len(entries) {
			t.Fatalf("round trip: %d entries, want %d", len(back), len(entries))
		}
		for i := range entries {
			a, b := entries[i], back[i]
			if !bytes.Equal(a.Key, b.Key) || a.Off != b.Off || a.Size != b.Size || a.ExpSec != b.ExpSec || a.Flags != b.Flags {
				t.Fatalf("round trip: entry %d %+v != %+v", i, a, b)
			}
		}
	})
}

// aliases reports whether sub lies inside the backing array of b.
func aliases(b, sub []byte) bool {
	if len(b) == 0 || len(sub) == 0 {
		return true
	}
	start := uintptr(unsafe.Pointer(unsafe.SliceData(b)))
	p := uintptr(unsafe.Pointer(unsafe.SliceData(sub)))
	return p >= start && p+uintptr(len(sub)) <= start+uintptr(len(b))
}

// FuzzParseName pins the file-name parser against its printers: any name that parses prints back to
// itself (the padding is canonical) and every printed name parses.
func FuzzParseName(f *testing.F) {
	f.Add("00000001.log")
	f.Add("00000001.hint")
	f.Add("00000001.log.tmp")
	f.Add("4294967295.log")
	f.Add("1.log")
	f.Add("MANIFEST")
	f.Add("")
	f.Add("0000000a.log")
	f.Fuzz(func(t *testing.T, name string) {
		id, hint, ok := ParseName(name)
		if !ok {
			return
		}
		want := LogName(id)
		if hint {
			want = HintName(id)
		}
		if want != name {
			t.Fatalf("ParseName(%q) = %d, hint=%v; prints back as %q", name, id, hint, want)
		}
	})
}
