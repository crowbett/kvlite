// STUB written by the integrator to freeze the contract; workstream A (format) owns this file and replaces
// the bodies, keeping every exported signature. The file-name helpers are real and tested.

// Package segment reads and writes kvlite segment files (NNNNNNNN.log) and their hint files
// (NNNNNNNN.hint). A segment is the 16-byte prologue from record.SegmentHeaderSize followed by records
// encoded by package record. Concrete types only: any narrow interface lives in package kvlite.
package segment

import (
	"errors"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/crowbett/kvlite/internal/record"
)

// File-name suffixes. Segment ids are zero-padded to 8 decimal digits so lexical order is id order.
const (
	// LogSuffix is the segment file extension.
	LogSuffix = ".log"
	// HintSuffix is the hint file extension.
	HintSuffix = ".hint"
	// ScanBufferSize is the read buffer used by Reader.Scan.
	ScanBufferSize = 1 << 20
)

// Sentinel errors. Record-level errors (record.ErrChecksum, ...) pass through unchanged.
var (
	// ErrBadHeader means the 16-byte segment prologue is missing, has the wrong magic/version or names
	// a different segment id.
	ErrBadHeader = errors.New("segment: bad header")
	// ErrBadHint means a hint file failed its footer crc, count or magic check; callers fall back to
	// scanning the log, which is always safe.
	ErrBadHint = errors.New("segment: bad hint")
	// ErrOutOfRange means a ReadAt offset/size does not fit inside the segment.
	ErrOutOfRange = errors.New("segment: read out of range")
)

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

// Writer appends records to the active segment. The caller (package kvlite) holds the DB writer mutex
// around every Append; Writer does no locking of its own.
type Writer struct {
	id   uint32
	size int64
}

// Create creates dir/<id>.log, writes the segment prologue and returns a Writer positioned after it.
// It fails if the file already exists.
func Create(dir string, id uint32) (*Writer, error) {
	_ = dir
	_ = id
	return nil, errors.New("stub: segment.Create")
}

// ID returns the segment id.
func (w *Writer) ID() uint32 { return w.id }

// Append writes one complete encoded record (header + payload, as produced by record.Append) and
// returns the absolute file offset of its first byte. Offsets are uint32 because a segment never
// exceeds 4 GiB (kvlite caps MaxSegmentBytes well below that).
func (w *Writer) Append(rec []byte) (off uint32, err error) {
	_ = rec
	return 0, errors.New("stub: segment.Writer.Append")
}

// Size returns the current file size in bytes, prologue included.
func (w *Writer) Size() int64 { return w.size }

// Sync fsyncs the file.
func (w *Writer) Sync() error { return errors.New("stub: segment.Writer.Sync") }

// Close syncs and closes the file. It is idempotent.
func (w *Writer) Close() error { return errors.New("stub: segment.Writer.Close") }

// Reader serves random and sequential reads of one immutable (or append-only) segment. It is safe for
// concurrent use; readers share it and pin it with Ref/Unref.
type Reader struct {
	id   uint32
	refs atomic.Int32
}

// OpenReader opens dir/<id>.log for reading and validates the prologue (ErrBadHeader).
func OpenReader(dir string, id uint32) (*Reader, error) {
	_ = dir
	_ = id
	return nil, errors.New("stub: segment.OpenReader")
}

// ID returns the segment id.
func (r *Reader) ID() uint32 { return r.id }

// ReadAt reads the record whose header starts at off and spans size bytes (header included, as
// recorded in keydir.Entry.Size), verifies its crc and decodes it. The returned buffer backs the
// record's Key and Value and is owned by the caller.
func (r *Reader) ReadAt(off, size uint32) (record.Record, []byte, error) {
	_, _ = off, size
	return record.Record{}, nil, errors.New("stub: segment.Reader.ReadAt")
}

// Scan walks every record in file order with a ScanBufferSize buffer, calling fn with the record's
// absolute offset. fn returning an error stops the scan and returns that error. Record and Key/Value
// passed to fn are only valid during the call. A torn tail surfaces as record.ErrTruncated, a bad
// crc as record.ErrChecksum, both wrapped with the offset.
func (r *Reader) Scan(fn func(off uint32, rec record.Record) error) error {
	_ = fn
	return errors.New("stub: segment.Reader.Scan")
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

// Close closes the underlying file. Callers check Refs() == 0 first; Close does not wait.
func (r *Reader) Close() error { return errors.New("stub: segment.Reader.Close") }
