// STUB written by the integrator to freeze the contract; workstream A (format) owns this file and replaces
// the bodies, keeping every exported signature.

package segment

import "errors"

// Finding is one problem located by VerifySegment.
type Finding struct {
	// Off is the file offset of the problem (in the log, or in the hint when Hint is true).
	Off int64
	// Reason is a short cause: "checksum", "truncated", "bad header", "hint mismatch", ...
	Reason string
	// Hint is true when the finding concerns the hint file rather than the log.
	Hint bool
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
	// Findings lists every problem found; empty means the segment is clean.
	Findings []Finding
}

// OK reports whether the segment (and its hint, if present) has no findings.
func (r VerifyReport) OK() bool { return len(r.Findings) == 0 }

// VerifySegment walks dir/<id>.log record by record checking every crc, then cross-checks the hint
// (if any) against the log. It is read-only and never mutates a file. A missing log is an error;
// findings are reported in the VerifyReport, not as an error.
func VerifySegment(dir string, id uint32) (VerifyReport, error) {
	_ = dir
	return VerifyReport{ID: id}, errors.New("stub: segment.VerifySegment")
}
