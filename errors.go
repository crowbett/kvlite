// Workstream B (engine) owns this file. The sentinels and CorruptError are a frozen contract
// (docs/PLAN.md): additions only, together with the integrator.

package kvlite

import (
	"errors"
	"strconv"
)

// Sentinel errors. Every error returned by the public API either is one of these or wraps one, so callers
// match with errors.Is. CorruptError wraps ErrCorrupt and carries the location.
var (
	// ErrNotFound is returned by Get/Snapshot.Get when the key is absent or expired.
	ErrNotFound = errors.New("kvlite: key not found")
	// ErrClosed is returned by every method once Close has been called.
	ErrClosed = errors.New("kvlite: database closed")
	// ErrCorrupt is returned by Open when a segment or the MANIFEST fails validation outside the torn tail.
	ErrCorrupt = errors.New("kvlite: corrupt data")
	// ErrKeyTooLarge is returned when a key exceeds WithMaxKeyBytes.
	ErrKeyTooLarge = errors.New("kvlite: key too large")
	// ErrValueTooLarge is returned when a value exceeds WithMaxValueBytes.
	ErrValueTooLarge = errors.New("kvlite: value too large")
	// ErrEmptyKey is returned for a zero-length key.
	ErrEmptyKey = errors.New("kvlite: empty key")
	// ErrLocked is returned by Open when another process holds the directory LOCK.
	ErrLocked = errors.New("kvlite: directory locked by another process")
	// ErrReadOnly is returned by every mutating method on a DB opened WithReadOnly.
	ErrReadOnly = errors.New("kvlite: read-only")
	// ErrInvalidTTL is returned by PutTTL when ttl <= 0.
	ErrInvalidTTL = errors.New("kvlite: invalid ttl")
)

// CorruptError locates a corruption inside a segment. errors.Is(err, ErrCorrupt) is true for it, and Err
// (when set) is the underlying record/segment error reachable through errors.Unwrap.
type CorruptError struct {
	// Seg is the segment id of the bad record.
	Seg uint32
	// Off is the file offset of the bad record.
	Off int64
	// Reason is a short human-readable cause ("checksum", "truncated", "bad magic", ...).
	Reason string
	// Err is the underlying error, if any.
	Err error
}

// Error implements error.
func (e *CorruptError) Error() string {
	s := "kvlite: corrupt segment " + strconv.FormatUint(uint64(e.Seg), 10) +
		" at offset " + strconv.FormatInt(e.Off, 10)
	if e.Reason != "" {
		s += ": " + e.Reason
	}
	if e.Err != nil {
		s += ": " + e.Err.Error()
	}
	return s
}

// Unwrap returns the underlying error.
func (e *CorruptError) Unwrap() error { return e.Err }

// Is reports that a CorruptError matches ErrCorrupt.
func (e *CorruptError) Is(target error) bool { return target == ErrCorrupt }
