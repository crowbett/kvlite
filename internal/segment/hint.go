// STUB written by the integrator to freeze the contract; workstream A (format) owns this file and replaces
// the bodies, keeping every exported signature.

package segment

import (
	"errors"

	"github.com/crowbett/kvlite/internal/record"
)

// HintEntry is one line of a hint file: enough to rebuild a keydir.Entry for Key without reading the log.
// Layout on disk: [flags:1][expSec:uvarint][off:uvarint][size:uvarint][keyLen:uvarint][key].
type HintEntry struct {
	// Key is the record key (owned by the caller of WriteHint; a fresh copy after ReadHint).
	Key []byte
	// Off is the record's absolute offset in the log, Size its full length (header included).
	Off, Size uint32
	// ExpSec is the expiry as unix seconds, 0 for never.
	ExpSec uint32
	// Flags are the record flags (tombstones are not written to hints; the field is kept for verify).
	Flags record.Flags
}

// WriteHint writes dir/<id>.hint atomically (tmp -> sync -> rename) from the entries yielded by it:
// header "KVLH"|1|segID|pad, one HintEntry per key, footer [crc32c(body):4][count:uint32]["KVLH"].
// A hint is only ever written for a segment that is immutable at that point.
func WriteHint(dir string, id uint32, it func(yield func(key []byte, e HintEntry) bool)) error {
	_, _, _ = dir, id, it
	return errors.New("stub: segment.WriteHint")
}

// ReadHint reads dir/<id>.hint. A missing file is os.ErrNotExist; a bad footer crc, count or magic is
// ErrBadHint. Either way the caller falls back to Reader.Scan.
func ReadHint(dir string, id uint32) ([]HintEntry, error) {
	_, _ = dir, id
	return nil, errors.New("stub: segment.ReadHint")
}
