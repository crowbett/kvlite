// Package segment reads and writes kvlite segment files (NNNNNNNN.log) and their hint files
// (NNNNNNNN.hint). A segment is the 16-byte prologue from record.SegmentHeaderSize followed by records
// encoded by package record. Concrete types only: any narrow interface lives in package kvlite.
//
// Recovery contract (package kvlite's open/recovery code relies on this):
//   - Reader.Scan stops at the first bad record and returns a *ScanError carrying the record's
//     absolute offset. ScanError.Tail is true exactly when no decodable record starts after that
//     offset (the file ends inside the record, the record's extent reaches end of file, or only
//     zero-fill/garbage follows), so truncating the log at ScanError.Off loses nothing decodable.
//     Tail == false means corruption in the middle of the file: decodable records follow.
//   - Reader.Resync locates the next decodable record after a corruption so a repairing open can
//     continue with Reader.ScanFrom.
//   - Offsets passed to fn, stored in hints and accepted by ReadAt are absolute file offsets of the
//     record's 8-byte header; sizes include the header (keydir.Entry.Size).
//   - A Reader is shared: Ref before using it from a snapshot, Unref after; the owner closes it and
//     moves the file to trash only once Refs() == 0. Close never waits.
package segment

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/crowbett/kvlite/internal/record"
)

// File-name suffixes. Segment ids are zero-padded to 8 decimal digits so lexical order is id order.
const (
	// LogSuffix is the segment file extension.
	LogSuffix = ".log"
	// HintSuffix is the hint file extension.
	HintSuffix = ".hint"
	// TempSuffix is appended to a file name while it is being written (hints); a leftover is garbage.
	TempSuffix = ".tmp"
	// ScanBufferSize is the read buffer used by Reader.Scan.
	ScanBufferSize = 1 << 20
	// MaxSize is the largest a segment file can grow: offsets are uint32.
	MaxSize = math.MaxUint32
)

// Sentinel errors. Record-level errors (record.ErrChecksum, ...) pass through unchanged.
var (
	// ErrBadHeader means the 16-byte segment prologue is missing, has the wrong magic/version or names
	// a different segment id. A file shorter than the prologue also matches record.ErrTruncated.
	ErrBadHeader = errors.New("segment: bad header")
	// ErrBadHint means a hint file failed its footer crc, count or magic check; callers fall back to
	// scanning the log, which is always safe.
	ErrBadHint = errors.New("segment: bad hint")
	// ErrOutOfRange means a ReadAt offset/size does not fit inside the segment.
	ErrOutOfRange = errors.New("segment: read out of range")
	// ErrFull means an Append would push the segment past MaxSize.
	ErrFull = errors.New("segment: full")
)

// ScanError is returned by Reader.Scan/ScanFrom for the first bad record. It wraps the record-level
// cause (record.ErrTruncated, ErrChecksum, ErrMalformed, ErrTooLarge) for errors.Is.
type ScanError struct {
	// ID is the segment id, Off the absolute offset of the bad record's header.
	ID, Off uint32
	// Tail reports a torn tail: no decodable record starts after Off, so truncating the file there
	// loses only undecodable bytes. False means corruption in the middle of the segment.
	Tail bool
	// Err is the underlying record error.
	Err error
}

// Error implements error.
func (e *ScanError) Error() string {
	kind := "corrupt record"
	if e.Tail {
		kind = "torn tail"
	}
	return "segment " + strconv.FormatUint(uint64(e.ID), 10) + ": " + kind + " at offset " +
		strconv.FormatUint(uint64(e.Off), 10) + ": " + e.Err.Error()
}

// Unwrap returns the record-level cause.
func (e *ScanError) Unwrap() error { return e.Err }

// LogName returns the file name of segment id ("00000007.log").
func LogName(id uint32) string { return pad(id) + LogSuffix }

// HintName returns the hint file name of segment id ("00000007.hint").
func HintName(id uint32) string { return pad(id) + HintSuffix }

// ParseName parses a segment or hint file name. ok is false for any other file.
func ParseName(name string) (id uint32, hint, ok bool) {
	var base string
	switch {
	case strings.HasSuffix(name, LogSuffix):
		base = strings.TrimSuffix(name, LogSuffix)
	case strings.HasSuffix(name, HintSuffix):
		base, hint = strings.TrimSuffix(name, HintSuffix), true
	default:
		return 0, false, false
	}
	if len(base) != 8 {
		return 0, false, false
	}
	n, err := strconv.ParseUint(base, 10, 32)
	if err != nil {
		return 0, false, false
	}
	return uint32(n), hint, true
}

func pad(id uint32) string {
	s := strconv.FormatUint(uint64(id), 10)
	if len(s) < 8 {
		s = strings.Repeat("0", 8-len(s)) + s
	}
	return s
}

// Header returns the 16-byte segment prologue for id.
func Header(id uint32) [record.SegmentHeaderSize]byte {
	var h [record.SegmentHeaderSize]byte
	copy(h[:4], record.SegmentMagic)
	h[4] = record.FormatVersion
	binary.LittleEndian.PutUint32(h[5:9], id)
	return h
}

// checkHeader validates a prologue read from the file of segment id.
func checkHeader(b []byte, id uint32) error {
	if len(b) < record.SegmentHeaderSize {
		return fmt.Errorf("%w: %d bytes: %w", ErrBadHeader, len(b), record.ErrTruncated)
	}
	if string(b[:4]) != record.SegmentMagic {
		return fmt.Errorf("%w: magic %q", ErrBadHeader, b[:4])
	}
	if b[4] != record.FormatVersion {
		return fmt.Errorf("%w: version %d", ErrBadHeader, b[4])
	}
	if got := binary.LittleEndian.Uint32(b[5:9]); got != id {
		return fmt.Errorf("%w: file names segment %d, expected %d", ErrBadHeader, got, id)
	}
	return nil
}

// Writer appends records to the active segment. The caller (package kvlite) holds the DB writer mutex
// around every Append; Writer does no locking of its own.
type Writer struct {
	id   uint32
	size int64
	f    *os.File
	// closeOnce makes Close idempotent; closeErr is what the first Close returned.
	closeOnce sync.Once
	closeErr  error
	closed    bool
}

// Create creates dir/<id>.log, writes and fsyncs the segment prologue and returns a Writer positioned
// after it. It fails if the file already exists.
func Create(dir string, id uint32) (*Writer, error) {
	path := filepath.Join(dir, LogName(id))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644) //nolint:gosec // segment file
	if err != nil {
		return nil, fmt.Errorf("segment: create %s: %w", path, err)
	}
	h := Header(id)
	if _, err := f.Write(h[:]); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("segment: create %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("segment: create %s: %w", path, err)
	}
	return &Writer{id: id, size: record.SegmentHeaderSize, f: f}, nil
}

// Open opens an existing dir/<id>.log for append at its current size, validating the prologue
// (ErrBadHeader). Recovery uses it to reopen the active segment after Scan, typically followed by
// Truncate at the ScanError offset.
func Open(dir string, id uint32) (*Writer, error) {
	path := filepath.Join(dir, LogName(id))
	f, err := os.OpenFile(path, os.O_RDWR, 0o644) //nolint:gosec // segment file
	if err != nil {
		return nil, fmt.Errorf("segment: open %s: %w", path, err)
	}
	var h [record.SegmentHeaderSize]byte
	n, err := f.ReadAt(h[:], 0)
	if err != nil && !errors.Is(err, io.EOF) {
		_ = f.Close()
		return nil, fmt.Errorf("segment: open %s: %w", path, err)
	}
	if err := checkHeader(h[:n], id); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("segment: open %s: %w", path, err)
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("segment: open %s: %w", path, err)
	}
	if st.Size() > MaxSize {
		_ = f.Close()
		return nil, fmt.Errorf("segment: open %s: %d bytes: %w", path, st.Size(), ErrFull)
	}
	return &Writer{id: id, size: st.Size(), f: f}, nil
}

// ID returns the segment id.
func (w *Writer) ID() uint32 { return w.id }

// Append writes one complete encoded record (header + payload, as produced by record.Append) and
// returns the absolute file offset of its first byte. Offsets are uint32 because a segment never
// exceeds 4 GiB (kvlite caps MaxSegmentBytes well below that); ErrFull guards the limit. A record
// whose header does not describe its length is refused with record.ErrMalformed before anything is
// written. If the write fails part-way the file is truncated back to the previous size (best effort)
// so the segment never carries a known-torn record.
func (w *Writer) Append(rec []byte) (off uint32, err error) {
	if w.closed {
		return 0, os.ErrClosed
	}
	_, plen, err := record.ParseHeader(rec)
	if err != nil {
		return 0, fmt.Errorf("segment: append: %w", err)
	}
	if int(plen)+record.HeaderSize != len(rec) {
		return 0, fmt.Errorf("segment: append: header says %d bytes, got %d: %w",
			int(plen)+record.HeaderSize, len(rec), record.ErrMalformed)
	}
	if w.size+int64(len(rec)) > MaxSize {
		return 0, ErrFull
	}
	off = uint32(w.size) //nolint:gosec // w.size < MaxSize, checked above
	n, err := w.f.WriteAt(rec, w.size)
	if err != nil {
		if terr := w.f.Truncate(w.size); terr != nil {
			// Could not roll back: account for the partial bytes so Size() stays truthful.
			w.size += int64(n)
		}
		return 0, fmt.Errorf("segment %d: append at %d: %w", w.id, off, err)
	}
	w.size += int64(n)
	return off, nil
}

// Size returns the current file size in bytes, prologue included.
func (w *Writer) Size() int64 { return w.size }

// Truncate cuts the file to size bytes (never below the prologue) and fsyncs it. Recovery calls it
// with ScanError.Off to drop a torn tail.
func (w *Writer) Truncate(size int64) error {
	if w.closed {
		return os.ErrClosed
	}
	if size < record.SegmentHeaderSize || size > w.size {
		return fmt.Errorf("segment %d: truncate to %d of %d: %w", w.id, size, w.size, ErrOutOfRange)
	}
	if err := w.f.Truncate(size); err != nil {
		return fmt.Errorf("segment %d: truncate: %w", w.id, err)
	}
	w.size = size
	return w.Sync()
}

// Sync fsyncs the file.
func (w *Writer) Sync() error {
	if w.closed {
		return os.ErrClosed
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("segment %d: sync: %w", w.id, err)
	}
	return nil
}

// Close syncs and closes the file. It is idempotent and returns the first error.
func (w *Writer) Close() error {
	w.closeOnce.Do(func() {
		w.closed = true
		err := w.f.Sync()
		if cerr := w.f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			w.closeErr = fmt.Errorf("segment %d: close: %w", w.id, err)
		}
	})
	return w.closeErr
}

// Reader serves random and sequential reads of one immutable (or append-only) segment. It is safe for
// concurrent use; readers share it and pin it with Ref/Unref.
type Reader struct {
	id     uint32
	refs   atomic.Int32
	f      *os.File
	closed atomic.Bool
	// closeOnce makes Close idempotent; closeErr is what the first Close returned.
	closeOnce sync.Once
	closeErr  error
}

// OpenReader opens dir/<id>.log for reading and validates the prologue (ErrBadHeader).
func OpenReader(dir string, id uint32) (*Reader, error) {
	path := filepath.Join(dir, LogName(id))
	f, err := os.Open(path) //nolint:gosec // segment file
	if err != nil {
		return nil, fmt.Errorf("segment: open %s: %w", path, err)
	}
	var h [record.SegmentHeaderSize]byte
	n, err := f.ReadAt(h[:], 0)
	if err != nil && !errors.Is(err, io.EOF) {
		_ = f.Close()
		return nil, fmt.Errorf("segment: open %s: %w", path, err)
	}
	if err := checkHeader(h[:n], id); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("segment: open %s: %w", path, err)
	}
	return &Reader{id: id, f: f}, nil
}

// ID returns the segment id.
func (r *Reader) ID() uint32 { return r.id }

// Size returns the current file size (a live value for the active segment).
func (r *Reader) Size() (int64, error) {
	if r.closed.Load() {
		return 0, os.ErrClosed
	}
	st, err := r.f.Stat()
	if err != nil {
		return 0, fmt.Errorf("segment %d: stat: %w", r.id, err)
	}
	return st.Size(), nil
}

// ReadAt reads the record whose header starts at off and spans size bytes (header included, as
// recorded in keydir.Entry.Size), verifies its crc and decodes it. The returned buffer backs the
// record's Key and Value and is owned by the caller.
func (r *Reader) ReadAt(off, size uint32) (record.Record, []byte, error) {
	return r.ReadAtBuf(off, size, nil)
}

// ReadAtBuf is ReadAt into a caller-supplied buffer (grown when too small), for callers that pool
// buffers. The returned buffer is buf or its replacement; the record aliases it. Errors: ErrOutOfRange
// when off/size do not fit in the file, record.ErrMalformed when the header disagrees with size (the
// index and the log are out of step), record.ErrChecksum, or a Decode error.
func (r *Reader) ReadAtBuf(off, size uint32, buf []byte) (record.Record, []byte, error) {
	if r.closed.Load() {
		return record.Record{}, buf, os.ErrClosed
	}
	if size < record.HeaderSize || size > record.MaxRecord || off < record.SegmentHeaderSize ||
		uint64(off)+uint64(size) > MaxSize {
		return record.Record{}, buf, fmt.Errorf("segment %d: read %d+%d: %w", r.id, off, size, ErrOutOfRange)
	}
	if cap(buf) < int(size) {
		buf = make([]byte, size)
	}
	buf = buf[:size]
	n, err := r.f.ReadAt(buf, int64(off))
	if err != nil {
		if errors.Is(err, io.EOF) {
			return record.Record{}, buf[:n], fmt.Errorf("segment %d: read %d+%d past end: %w", r.id, off, size, ErrOutOfRange)
		}
		return record.Record{}, buf[:n], fmt.Errorf("segment %d: read %d+%d: %w", r.id, off, size, err)
	}
	crc, plen, err := record.ParseHeader(buf)
	if err != nil {
		return record.Record{}, buf, fmt.Errorf("segment %d: record at %d: %w", r.id, off, err)
	}
	if int(plen)+record.HeaderSize != int(size) {
		return record.Record{}, buf, fmt.Errorf("segment %d: record at %d: header says %d bytes, index says %d: %w",
			r.id, off, int(plen)+record.HeaderSize, size, record.ErrMalformed)
	}
	payload := buf[record.HeaderSize:]
	if !record.Verify(payload, crc) {
		return record.Record{}, buf, fmt.Errorf("segment %d: record at %d: %w", r.id, off, record.ErrChecksum)
	}
	rec, err := record.Decode(payload)
	if err != nil {
		return record.Record{}, buf, fmt.Errorf("segment %d: record at %d: %w", r.id, off, err)
	}
	return rec, buf, nil
}

// Scan walks every record in file order with a ScanBufferSize buffer, calling fn with the record's
// absolute offset. fn returning an error stops the scan and returns that error unchanged. Record and
// Key/Value passed to fn are only valid during the call. The first bad record stops the scan with a
// *ScanError (see the package comment for its Tail semantics); a clean end of file returns nil.
func (r *Reader) Scan(fn func(off uint32, rec record.Record) error) error {
	return r.ScanFrom(record.SegmentHeaderSize, fn)
}

// ScanFrom is Scan starting at the record whose header is at off (typically a Resync result).
func (r *Reader) ScanFrom(off uint32, fn func(off uint32, rec record.Record) error) error {
	if r.closed.Load() {
		return os.ErrClosed
	}
	size, err := r.Size()
	if err != nil {
		return err
	}
	if int64(off) > size || off < record.SegmentHeaderSize {
		return fmt.Errorf("segment %d: scan from %d of %d: %w", r.id, off, size, ErrOutOfRange)
	}
	br := bufio.NewReaderSize(io.NewSectionReader(r.f, int64(off), size-int64(off)), ScanBufferSize)
	buf := make([]byte, 0, 64<<10)
	pos := off
	for {
		rec, n, b, err := record.Read(br, buf)
		buf = b
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return r.scanError(pos, n, size, err)
		}
		if err := fn(pos, rec); err != nil {
			return err
		}
		pos += uint32(n) //nolint:gosec // n <= MaxRecord and pos+n <= size <= MaxSize
	}
}

// scanError classifies a record.Read failure at pos into a *ScanError. n is how many bytes of the bad
// record were consumed; for a checksum/decode failure that is the whole declared record.
func (r *Reader) scanError(pos uint32, n int, size int64, err error) error {
	var rerr error
	switch {
	case errors.Is(err, record.ErrTruncated), errors.Is(err, record.ErrChecksum),
		errors.Is(err, record.ErrMalformed), errors.Is(err, record.ErrTooLarge):
		rerr = err
	default:
		// An I/O error is not a format finding.
		return fmt.Errorf("segment %d: scan at %d: %w", r.id, pos, err)
	}
	se := &ScanError{ID: r.id, Off: pos, Err: rerr}
	// Fast path: a whole record was read (crc/decode failure) and it ends exactly at end of file, so
	// nothing can follow. Otherwise (torn read, bogus plen, or bytes after the record) look for the
	// next decodable record; a bogus plen that "runs past EOF" with intact records behind it is
	// corruption, not a torn tail, and must never be truncated away.
	wholeAtEOF := !errors.Is(err, record.ErrTruncated) && !errors.Is(err, record.ErrTooLarge) &&
		int64(pos)+int64(n) >= size
	if wholeAtEOF {
		se.Tail = true
		return se
	}
	_, found, rerr := r.Resync(pos + 1)
	if rerr != nil {
		return fmt.Errorf("segment %d: scan at %d: %w", r.id, pos, rerr)
	}
	se.Tail = !found
	return se
}

// Resync scans forward from byte offset from for the next position where a complete, crc-valid,
// decodable record starts, so a repairing open can skip a corrupt stretch and continue with ScanFrom.
// found is false when no record starts at or after from. It is byte-granular and may, with
// probability about 2^-32 per candidate, accept a false record inside garbage; callers treat it as a
// repair heuristic, never as a correctness guarantee.
func (r *Reader) Resync(from uint32) (next uint32, found bool, err error) {
	if r.closed.Load() {
		return 0, false, os.ErrClosed
	}
	size, err := r.Size()
	if err != nil {
		return 0, false, err
	}
	if from < record.SegmentHeaderSize {
		from = record.SegmentHeaderSize
	}
	if int64(from)+record.HeaderSize > size {
		return 0, false, nil
	}
	const peek = record.HeaderSize + record.MaxMeta
	br := bufio.NewReaderSize(io.NewSectionReader(r.f, int64(from), size-int64(from)), ScanBufferSize)
	var buf []byte
	for pos := int64(from); pos+record.HeaderSize <= size; pos++ {
		b, perr := br.Peek(peek)
		if perr != nil && !errors.Is(perr, io.EOF) {
			return 0, false, fmt.Errorf("segment %d: resync at %d: %w", r.id, pos, perr)
		}
		if len(b) < record.HeaderSize {
			break
		}
		if plen, ok := candidate(b, pos, size); ok {
			total := record.HeaderSize + int(plen)
			if cap(buf) < total {
				buf = make([]byte, total)
			}
			if _, rerr := r.f.ReadAt(buf[:total], pos); rerr != nil && !errors.Is(rerr, io.EOF) {
				return 0, false, fmt.Errorf("segment %d: resync at %d: %w", r.id, pos, rerr)
			}
			if _, n, perr := record.Parse(buf[:total]); perr == nil && n == total {
				return uint32(pos), true, nil
			}
		}
		// Discard only fails at end of input, which the loop condition already covers.
		_, _ = br.Discard(1)
	}
	return 0, false, nil
}

// candidate is Resync's cheap pre-filter: does the header at pos declare a record that fits in the
// file and whose peeked metadata is self-consistent with plen? It only rules candidates out.
func candidate(b []byte, pos, size int64) (plen uint32, ok bool) {
	_, plen, err := record.ParseHeader(b)
	if err != nil || plen == 0 || pos+record.HeaderSize+int64(plen) > size {
		return 0, false
	}
	meta := b[record.HeaderSize:]
	if len(meta) > int(plen) {
		meta = meta[:plen]
	}
	if len(meta) == 0 || record.Flags(meta[0])&^record.FlagsMask != 0 {
		return 0, false
	}
	n := 1
	varints := 3
	if record.Flags(meta[0]).HasExpiry() {
		varints = 4
	}
	var lens [2]uint64
	for i := range varints {
		v, m := binary.Uvarint(meta[n:])
		if m < 0 {
			return 0, false
		}
		if m == 0 {
			// The peek window ended inside the metadata (only possible right before EOF, where plen
			// is tiny): let the full parse decide.
			return plen, true
		}
		n += m
		if i >= varints-2 {
			lens[i-(varints-2)] = v
		}
	}
	if lens[0] > record.MaxKey || lens[1] > record.MaxValue {
		return 0, false
	}
	return plen, uint64(n)+lens[0]+lens[1] == uint64(plen)
}

// Ref pins the reader: while the count is above zero the segment must not be closed or deleted.
func (r *Reader) Ref() { r.refs.Add(1) }

// Unref releases a pin. The count never goes below zero.
func (r *Reader) Unref() {
	if r.refs.Add(-1) < 0 {
		r.refs.Store(0)
	}
}

// Refs returns the current pin count.
func (r *Reader) Refs() int { return int(r.refs.Load()) }

// Close closes the underlying file. Callers check Refs() == 0 first; Close does not wait. It is
// idempotent; every method afterwards returns os.ErrClosed.
func (r *Reader) Close() error {
	r.closeOnce.Do(func() {
		r.closed.Store(true)
		if r.f != nil {
			if err := r.f.Close(); err != nil {
				r.closeErr = fmt.Errorf("segment %d: close: %w", r.id, err)
			}
		}
	})
	return r.closeErr
}
