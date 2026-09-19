// STUB written by the integrator to freeze the contract; workstream A (format) owns this file and replaces
// the bodies, keeping every exported signature. Constants, flags, Checksum/Verify and the header helpers
// are real and tested; Append/Decode/Read are stubs.

// Package record encodes and decodes the on-disk log record of a kvlite segment.
//
// Record = [crc32c:4][plen:uint32][payload:plen], little-endian. The crc is Castagnoli over the payload
// only. Payload = [flags:1][tsMillis:uvarint][expiryMillis:uvarint, only when FlagExpiry][keyLen:uvarint]
// [valLen:uvarint][key][value]. Flags precede the variable fields so the decoder knows whether the expiry
// is present and can skip a tombstone body in one pass. See docs/PLAN.md "On-disk format".
package record

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"time"
)

// Size limits and layout constants. These are frozen; changing them changes the file format.
const (
	// MaxKey is the largest key the format accepts (64 KiB).
	MaxKey = 64 << 10
	// MaxValue is the largest value the format accepts (16 MiB, owner decision: v1 refuses anything bigger).
	MaxValue = 16 << 20
	// HeaderSize is the fixed prefix of every record: crc32c (4) + plen (4).
	HeaderSize = 8
	// MaxMeta bounds the non-key/value part of a payload: flags (1) + two uvarint64 timestamps + two uvarint32
	// lengths. The ADR's "16" was a loose figure; this is the exact worst case.
	MaxMeta = 1 + 2*binary.MaxVarintLen64 + 2*binary.MaxVarintLen32
	// MaxPayload is the largest legal plen; a larger plen means the header is corrupt.
	MaxPayload = MaxMeta + MaxKey + MaxValue
	// MaxRecord is the largest on-disk record including the header.
	MaxRecord = HeaderSize + MaxPayload

	// SegmentMagic opens every segment file; HintMagic opens every hint file.
	SegmentMagic = "KVLT"
	// HintMagic opens every hint file (header and footer).
	HintMagic = "KVLH"
	// FormatVersion is the on-disk format version written after the magic.
	FormatVersion = 1
	// SegmentHeaderSize is the fixed segment prologue: magic (4) | version (1) | segID (4) | 7 zero bytes.
	SegmentHeaderSize = 16
)

// Sentinel errors returned by the decoder. Callers match them with errors.Is.
var (
	// ErrChecksum means the crc32c in the header does not match the payload.
	ErrChecksum = errors.New("record: checksum mismatch")
	// ErrTooLarge means a key, value or plen exceeds its limit.
	ErrTooLarge = errors.New("record: too large")
	// ErrTruncated means the input ended inside a record (a torn write at the tail of a segment).
	ErrTruncated = errors.New("record: truncated")
	// ErrMalformed means the payload is self-inconsistent (bad varint, lengths past the end, ...).
	ErrMalformed = errors.New("record: malformed")
)

// Flags is the one-byte flag field that leads every payload.
type Flags uint8

// Flag bits. Bit order is part of the on-disk format.
const (
	// FlagTombstone marks a delete; the value is empty.
	FlagTombstone Flags = 1 << iota
	// FlagExpiry says expiryMillis follows tsMillis.
	FlagExpiry
	// FlagBatchMember marks a record that only becomes visible once its batch commit marker lands.
	FlagBatchMember
	// FlagBatchCommit marks the commit marker; its value is uvarint count + uvarint batchID.
	FlagBatchCommit

	// FlagsMask is every defined bit; a payload with other bits set is ErrMalformed.
	FlagsMask = FlagTombstone | FlagExpiry | FlagBatchMember | FlagBatchCommit
)

// Tombstone reports whether FlagTombstone is set.
func (f Flags) Tombstone() bool { return f&FlagTombstone != 0 }

// HasExpiry reports whether FlagExpiry is set.
func (f Flags) HasExpiry() bool { return f&FlagExpiry != 0 }

// BatchMember reports whether FlagBatchMember is set.
func (f Flags) BatchMember() bool { return f&FlagBatchMember != 0 }

// BatchCommit reports whether FlagBatchCommit is set.
func (f Flags) BatchCommit() bool { return f&FlagBatchCommit != 0 }

// Record is one decoded log entry. After Decode, Key and Value alias the input payload.
type Record struct {
	Key   []byte
	Value []byte
	// TS is the write time (millisecond precision); it breaks ties during a merge.
	TS time.Time
	// Expiry is the zero time unless Flags.HasExpiry().
	Expiry time.Time
	Flags  Flags
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Checksum returns the crc32c (Castagnoli) of payload, the value stored in the record header.
func Checksum(payload []byte) uint32 { return crc32.Checksum(payload, castagnoli) }

// Verify reports whether crc is the checksum of payload.
func Verify(payload []byte, crc uint32) bool { return Checksum(payload) == crc }

// PutHeader writes the 8-byte record header (crc, plen) into dst, which must have room for HeaderSize.
func PutHeader(dst []byte, crc, plen uint32) {
	binary.LittleEndian.PutUint32(dst[0:4], crc)
	binary.LittleEndian.PutUint32(dst[4:8], plen)
}

// ParseHeader reads a record header. It returns ErrTruncated when b is shorter than HeaderSize and
// ErrTooLarge when plen exceeds MaxPayload (a corrupt header, never a legal record).
func ParseHeader(b []byte) (crc, plen uint32, err error) {
	if len(b) < HeaderSize {
		return 0, 0, ErrTruncated
	}
	crc = binary.LittleEndian.Uint32(b[0:4])
	plen = binary.LittleEndian.Uint32(b[4:8])
	if plen > MaxPayload {
		return crc, plen, ErrTooLarge
	}
	return crc, plen, nil
}

// MaxEncodedSize is an upper bound on the on-disk size (header included) of a record with the given
// key and value lengths; writers size their reusable buffer with it.
func MaxEncodedSize(keyLen, valLen int) int { return HeaderSize + MaxMeta + keyLen + valLen }

// Append encodes r as a complete on-disk record (header + payload) onto dst and returns the grown slice.
// It validates the size limits and returns ErrTooLarge instead of writing an oversized record.
func Append(dst []byte, r Record) ([]byte, error) {
	_, _ = dst, r
	return nil, errors.New("stub: record.Append")
}

// Decode parses one payload (the bytes after the 8-byte header) without copying: the returned Key and
// Value alias payload. It never panics on arbitrary input and returns ErrMalformed or ErrTooLarge.
func Decode(payload []byte) (Record, error) {
	_ = payload
	return Record{}, errors.New("stub: record.Decode")
}

// Read reads exactly one record from r using buf as scratch (grown as needed) and returns the record, the
// number of bytes consumed and the possibly-grown buffer. io.EOF at a record boundary is returned as is;
// EOF inside a record is ErrTruncated; a crc mismatch is ErrChecksum.
func Read(r io.Reader, buf []byte) (Record, int, []byte, error) {
	_ = r
	return Record{}, 0, buf, errors.New("stub: record.Read")
}
