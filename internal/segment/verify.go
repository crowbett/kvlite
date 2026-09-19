package segment

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/crowbett/kvlite/internal/record"
)

// Finding is one problem located by VerifySegment.
type Finding struct {
	// Off is the file offset of the problem (in the log, or in the hint when Hint is true).
	Off int64
	// Reason is a short cause: "checksum", "truncated", "malformed", "too large", "bad header",
	// "bad hint", "hint mismatch".
	Reason string
	// Hint is true when the finding concerns the hint file rather than the log.
	Hint bool
	// Tail is true for a log finding at a torn tail (see ScanError.Tail): recovery would truncate
	// there rather than report corruption.
	Tail bool
}

// VerifyReport is the result of VerifySegment; it powers `kvlite verify`.
type VerifyReport struct {
	// ID is the segment id.
	ID uint32
	// Records is the number of records decoded from the log; Bytes the log size.
	Records, Bytes int64
	// HintPresent says a hint file exists; HintValid says its footer checked out and every entry
	// matched the record at its offset in the log.
	HintPresent, HintValid bool
	// HintEntries is the number of entries in a hint whose footer checked out.
	HintEntries int64
	// Findings lists every problem found; empty means the segment is clean.
	Findings []Finding
}

// OK reports whether the segment (and its hint, if present) has no findings.
func (r VerifyReport) OK() bool { return len(r.Findings) == 0 }

// reason maps a record-level error to a Finding.Reason.
func reason(err error) string {
	switch {
	case errors.Is(err, record.ErrTruncated):
		return "truncated"
	case errors.Is(err, record.ErrChecksum):
		return "checksum"
	case errors.Is(err, record.ErrTooLarge):
		return "too large"
	case errors.Is(err, record.ErrMalformed):
		return "malformed"
	default:
		return err.Error()
	}
}

// VerifySegment walks dir/<id>.log record by record checking every crc, then cross-checks the hint
// (if any) against the log. It is read-only and never mutates a file. A missing log is an error;
// findings are reported in the VerifyReport, not as an error. After a bad record the walk resyncs to
// the next decodable record (Reader.Resync) so every corrupt stretch is reported once.
func VerifySegment(dir string, id uint32) (VerifyReport, error) {
	rep := VerifyReport{ID: id}
	logPath := filepath.Join(dir, LogName(id))
	st, err := os.Stat(logPath)
	if err != nil {
		return rep, fmt.Errorf("segment: verify %s: %w", logPath, err)
	}
	rep.Bytes = st.Size()
	if _, err := os.Stat(filepath.Join(dir, HintName(id))); err == nil {
		rep.HintPresent = true
	}

	r, err := OpenReader(dir, id)
	if err != nil {
		if !errors.Is(err, ErrBadHeader) {
			return rep, err
		}
		rep.Findings = append(rep.Findings, Finding{Off: 0, Reason: "bad header", Tail: errors.Is(err, record.ErrTruncated)})
		return rep, nil
	}
	defer func() { _ = r.Close() }()

	pos := uint32(record.SegmentHeaderSize)
	for {
		err := r.ScanFrom(pos, func(_ uint32, _ record.Record) error {
			rep.Records++
			return nil
		})
		if err == nil {
			break
		}
		var se *ScanError
		if !errors.As(err, &se) {
			return rep, err
		}
		rep.Findings = append(rep.Findings, Finding{Off: int64(se.Off), Reason: reason(se.Err), Tail: se.Tail})
		if se.Tail {
			break
		}
		next, found, err := r.Resync(se.Off + 1)
		if err != nil {
			return rep, err
		}
		if !found {
			break
		}
		pos = next
	}

	if !rep.HintPresent {
		return rep, nil
	}
	b, err := os.ReadFile(filepath.Join(dir, HintName(id))) //nolint:gosec // hint file
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			rep.HintPresent = false
			return rep, nil
		}
		return rep, fmt.Errorf("segment: verify hint %d: %w", id, err)
	}
	entries, offs, err := parseHint(b, id)
	if err != nil {
		rep.Findings = append(rep.Findings, Finding{Off: 0, Reason: "bad hint", Hint: true})
		return rep, nil
	}
	rep.HintEntries = int64(len(entries))
	rep.HintValid = true
	var buf []byte
	for i, e := range entries {
		var rec record.Record
		rec, buf, err = r.ReadAtBuf(e.Off, e.Size, buf)
		if err != nil || !bytes.Equal(rec.Key, e.Key) || rec.Flags != e.Flags || rec.ExpSec() != e.ExpSec {
			rep.HintValid = false
			rep.Findings = append(rep.Findings, Finding{Off: offs[i], Reason: "hint mismatch", Hint: true})
		}
	}
	return rep, nil
}
