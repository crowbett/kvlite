// Package record encodes and decodes the on-disk log record of a kvlite segment.
//
// Record = [crc32c:4][plen:uint32][payload:plen], little-endian. The crc is Castagnoli over the payload
// only. Payload = [flags:1][tsMillis:uvarint][expiryMillis:uvarint, only when FlagExpiry][keyLen:uvarint]
// [valLen:uvarint][key][value]. Flags precede the variable fields so the decoder knows whether the expiry
// is present and can skip a tombstone body in one pass. See docs/PLAN.md "On-disk format".
//
// Timestamps are encoded as the uvarint of the two's-complement int64 millisecond value, so every
// time.Time round-trips exactly (a zero time.Time costs 10 bytes; the engine always sets TS).
//
// Error taxonomy, matched with errors.Is:
//   - ErrTruncated: the input ends inside the record (a torn write at the tail of a segment).
//   - ErrTooLarge: a key, value or plen exceeds its limit (a corrupt header, or a caller bug in Append).
//   - ErrChecksum: the header crc does not match the payload.
//   - ErrMalformed: the payload is self-inconsistent (unknown flag bit, bad or non-minimal varint,
//     lengths that do not add up to plen). The encoding is canonical: a payload that decodes re-encodes
//     to the identical bytes.
//
// Only Read and Parse can return ErrTruncated or ErrChecksum; Decode sees a payload of known length and
// reports ErrMalformed for a short one.
package record

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"
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

// ExpSec returns the record's expiry as keydir/hint seconds: 0 when the record has no expiry, else
// ExpirySeconds(r.Expiry). Every hint entry and keydir.Entry derives ExpSec through this one function.
func (r Record) ExpSec() uint32 {
	if !r.Flags.HasExpiry() {
		return 0
	}
	return ExpirySeconds(r.Expiry)
}

// ExpirySeconds is the canonical conversion from an expiry instant to the uint32 unix seconds stored in
// keydir.Entry.ExpSec and in hint files. It rounds up, so a key never expires before its instant; the
// result is clamped to [1, MaxUint32] (0 is reserved for "never", and 2106 is the end of the format).
func ExpirySeconds(t time.Time) uint32 {
	ms := t.UnixMilli()
	if ms <= 0 {
		return 1
	}
	sec := (ms + 999) / 1000
	if sec > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(sec)
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

// EncodedSize is the exact on-disk size (header included) Append will produce for r. It does not
// validate r; pair it with the limits when sizing a segment.
func EncodedSize(r Record) int {
	n := HeaderSize + 1 + uvarintLen(uint64(r.TS.UnixMilli()))
	if r.Flags.HasExpiry() {
		n += uvarintLen(uint64(r.Expiry.UnixMilli()))
	}
	return n + uvarintLen(uint64(len(r.Key))) + uvarintLen(uint64(len(r.Value))) + len(r.Key) + len(r.Value)
}

func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

// uvarint is binary.Uvarint restricted to the canonical (minimal) encoding, so every payload has
// exactly one byte representation. n <= 0 means malformed.
func uvarint(b []byte) (v uint64, n int) {
	v, n = binary.Uvarint(b)
	if n <= 0 || n != uvarintLen(v) {
		return 0, -1
	}
	return v, n
}

// Append encodes r as a complete on-disk record (header + payload) onto dst and returns the grown slice.
// It validates the size limits and returns ErrTooLarge instead of writing an oversized record, and
// ErrMalformed for a flag bit outside FlagsMask. On error dst is returned unchanged.
func Append(dst []byte, r Record) ([]byte, error) {
	if len(r.Key) > MaxKey || len(r.Value) > MaxValue {
		return dst, ErrTooLarge
	}
	if r.Flags&^FlagsMask != 0 {
		return dst, ErrMalformed
	}
	start := len(dst)
	need := MaxEncodedSize(len(r.Key), len(r.Value))
	if cap(dst)-start < need {
		grown := make([]byte, start, start+need)
		copy(grown, dst)
		dst = grown
	}
	// Reserve the header, then write the payload directly after it.
	dst = dst[:start+HeaderSize]
	dst = append(dst, byte(r.Flags))
	dst = binary.AppendUvarint(dst, uint64(r.TS.UnixMilli()))
	if r.Flags.HasExpiry() {
		dst = binary.AppendUvarint(dst, uint64(r.Expiry.UnixMilli()))
	}
	dst = binary.AppendUvarint(dst, uint64(len(r.Key)))
	dst = binary.AppendUvarint(dst, uint64(len(r.Value)))
	dst = append(dst, r.Key...)
	dst = append(dst, r.Value...)
	payload := dst[start+HeaderSize:]
	PutHeader(dst[start:], Checksum(payload), uint32(len(payload))) //nolint:gosec // <= MaxPayload by the limits above
	return dst, nil
}

// Decode parses one payload (the bytes after the 8-byte header) without copying: the returned Key and
// Value alias payload (with their capacity clipped, so appending to them never clobbers it). It never
// panics on arbitrary input and returns ErrMalformed or ErrTooLarge.
func Decode(payload []byte) (Record, error) {
	var r Record
	if len(payload) < 1 {
		return r, ErrMalformed
	}
	r.Flags = Flags(payload[0])
	if r.Flags&^FlagsMask != 0 {
		return r, ErrMalformed
	}
	pos := 1
	ts, n := uvarint(payload[pos:])
	if n <= 0 {
		return r, ErrMalformed
	}
	pos += n
	// Timestamps are the two's-complement int64 stored as a uvarint (see the package comment), so the
	// wrapping conversion is the decoding step, not an overflow.
	r.TS = time.UnixMilli(int64(ts)) //nolint:gosec // intentional wrap
	if r.Flags.HasExpiry() {
		exp, n := uvarint(payload[pos:])
		if n <= 0 {
			return r, ErrMalformed
		}
		pos += n
		r.Expiry = time.UnixMilli(int64(exp)) //nolint:gosec // intentional wrap
	}
	keyLen, n := uvarint(payload[pos:])
	if n <= 0 {
		return r, ErrMalformed
	}
	pos += n
	valLen, n := uvarint(payload[pos:])
	if n <= 0 {
		return r, ErrMalformed
	}
	pos += n
	if keyLen > MaxKey || valLen > MaxValue {
		return r, ErrTooLarge
	}
	// Both lengths are small now, so the sums cannot overflow.
	kl, vl := int(keyLen), int(valLen)
	if len(payload)-pos != kl+vl {
		return r, ErrMalformed
	}
	r.Key = payload[pos : pos+kl : pos+kl]
	pos += kl
	r.Value = payload[pos : pos+vl : pos+vl]
	return r, nil
}

// Parse decodes one complete record (header + payload) from the front of b and returns it with its
// on-disk length. It distinguishes the failure modes a segment reader cares about: ErrTruncated when b
// ends inside the record (n is then len(b) or HeaderSize+plen, whichever is smaller), ErrTooLarge for a
// corrupt plen, ErrChecksum for a crc mismatch (n is the full record length), and Decode's errors. Key
// and Value alias b.
func Parse(b []byte) (Record, int, error) {
	crc, plen, err := ParseHeader(b)
	if err != nil {
		return Record{}, min(len(b), HeaderSize), err
	}
	n := HeaderSize + int(plen)
	if len(b) < n {
		return Record{}, len(b), ErrTruncated
	}
	payload := b[HeaderSize:n]
	if !Verify(payload, crc) {
		return Record{}, n, ErrChecksum
	}
	r, err := Decode(payload)
	return r, n, err
}

// Read reads exactly one record from r using buf as scratch (grown as needed) and returns the record, the
// number of bytes consumed and the possibly-grown buffer. io.EOF at a record boundary is returned as is;
// EOF inside a record is ErrTruncated; a crc mismatch is ErrChecksum. The record's Key and Value alias
// the returned buffer, which the caller may reuse for the next call. On error the count is the number of
// bytes actually consumed from r, so a sequential caller can still locate the failure.
func Read(r io.Reader, buf []byte) (Record, int, []byte, error) {
	if cap(buf) < HeaderSize {
		buf = make([]byte, 0, 4096)
	}
	buf = buf[:HeaderSize]
	n, err := io.ReadFull(r, buf)
	if err != nil {
		if errors.Is(err, io.EOF) && n == 0 {
			return Record{}, 0, buf[:0], io.EOF
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Record{}, n, buf[:n], ErrTruncated
		}
		return Record{}, n, buf[:n], err
	}
	crc, plen, err := ParseHeader(buf)
	if err != nil {
		return Record{}, n, buf, err
	}
	total := HeaderSize + int(plen)
	if cap(buf) < total {
		grown := make([]byte, total)
		copy(grown, buf[:HeaderSize])
		buf = grown
	}
	buf = buf[:total]
	m, err := io.ReadFull(r, buf[HeaderSize:])
	n += m
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Record{}, n, buf[:n], ErrTruncated
		}
		return Record{}, n, buf[:n], err
	}
	payload := buf[HeaderSize:total]
	if !Verify(payload, crc) {
		return Record{}, n, buf, ErrChecksum
	}
	rec, err := Decode(payload)
	return rec, n, buf, err
}
