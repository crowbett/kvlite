package segment

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/crowbett/kvlite/internal/record"
)

// Hint layout constants.
const (
	// HintHeaderSize is the hint prologue: "KVLH" (4) | version (1) | segID (4) | 7 zero bytes.
	HintHeaderSize = 16
	// HintFooterSize is the hint trailer: crc32c (4) | count uint32 (4) | "KVLH" (4). The crc covers
	// every byte before the footer, header included.
	HintFooterSize = 12
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

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

// hintHeader returns the 16-byte hint prologue for segment id.
func hintHeader(id uint32) [HintHeaderSize]byte {
	var h [HintHeaderSize]byte
	copy(h[:4], record.HintMagic)
	h[4] = record.FormatVersion
	binary.LittleEndian.PutUint32(h[5:9], id)
	return h
}

// appendHintEntry encodes one entry onto dst.
func appendHintEntry(dst, key []byte, e HintEntry) []byte {
	dst = append(dst, byte(e.Flags))
	dst = binary.AppendUvarint(dst, uint64(e.ExpSec))
	dst = binary.AppendUvarint(dst, uint64(e.Off))
	dst = binary.AppendUvarint(dst, uint64(e.Size))
	dst = binary.AppendUvarint(dst, uint64(len(key)))
	return append(dst, key...)
}

// WriteHint writes dir/<id>.hint atomically (tmp -> sync -> rename) from the entries yielded by it:
// header "KVLH"|1|segID|pad, one HintEntry per key, footer [crc32c(body):4][count:uint32]["KVLH"].
// A hint is only ever written for a segment that is immutable at that point. The key passed to yield
// is used for the entry (e.Key is ignored) and is copied out before yield returns. An entry with an
// oversized key, unknown flag bits or an offset inside the prologue is refused with record.ErrMalformed
// and no hint is left behind.
func WriteHint(dir string, id uint32, it func(yield func(key []byte, e HintEntry) bool)) error {
	path := filepath.Join(dir, HintName(id))
	tmp := path + TempSuffix
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644) //nolint:gosec // hint file
	if err != nil {
		return fmt.Errorf("segment: write hint %s: %w", path, err)
	}
	fail := func(err error) error {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("segment: write hint %s: %w", path, err)
	}

	bw := bufio.NewWriterSize(f, 256<<10)
	crc := uint32(0)
	write := func(b []byte) error {
		crc = crc32.Update(crc, castagnoli, b)
		_, err := bw.Write(b)
		return err
	}
	h := hintHeader(id)
	if err := write(h[:]); err != nil {
		return fail(err)
	}
	var (
		count uint32
		werr  error
		buf   = make([]byte, 0, 4096)
	)
	it(func(key []byte, e HintEntry) bool {
		switch {
		case len(key) > record.MaxKey:
			werr = fmt.Errorf("entry %d: key of %d bytes: %w", count, len(key), record.ErrTooLarge)
		case e.Flags&^record.FlagsMask != 0:
			werr = fmt.Errorf("entry %d: flags %#x: %w", count, e.Flags, record.ErrMalformed)
		case e.Off < record.SegmentHeaderSize || e.Size < record.HeaderSize:
			werr = fmt.Errorf("entry %d: off %d size %d: %w", count, e.Off, e.Size, record.ErrMalformed)
		case count == MaxSize:
			werr = fmt.Errorf("entry %d: %w", count, ErrFull)
		}
		if werr != nil {
			return false
		}
		buf = appendHintEntry(buf[:0], key, e)
		if werr = write(buf); werr != nil {
			return false
		}
		count++
		return true
	})
	if werr != nil {
		return fail(werr)
	}
	var footer [HintFooterSize]byte
	binary.LittleEndian.PutUint32(footer[0:4], crc)
	binary.LittleEndian.PutUint32(footer[4:8], count)
	copy(footer[8:12], record.HintMagic)
	if _, err := bw.Write(footer[:]); err != nil {
		return fail(err)
	}
	if err := bw.Flush(); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("segment: write hint %s: %w", path, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("segment: write hint %s: %w", path, err)
	}
	return nil
}

// ReadHint reads dir/<id>.hint. A missing file is os.ErrNotExist; a bad footer crc, count or magic is
// ErrBadHint. Either way the caller falls back to Reader.Scan. Every returned Key is a fresh copy
// (they share one backing array, each clipped to its own length).
func ReadHint(dir string, id uint32) ([]HintEntry, error) {
	path := filepath.Join(dir, HintName(id))
	b, err := os.ReadFile(path) //nolint:gosec // hint file
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("segment: read hint %s: %w", path, os.ErrNotExist)
		}
		return nil, fmt.Errorf("segment: read hint %s: %w", path, err)
	}
	entries, _, err := parseHint(b, id)
	if err != nil {
		return nil, fmt.Errorf("segment: read hint %s: %w", path, err)
	}
	return entries, nil
}

// parseHint validates a whole hint file and decodes its entries; offs holds each entry's byte offset
// in the file (for verify findings). Any structural problem is ErrBadHint.
func parseHint(b []byte, id uint32) (entries []HintEntry, offs []int64, err error) {
	if len(b) < HintHeaderSize+HintFooterSize {
		return nil, nil, fmt.Errorf("%w: %d bytes", ErrBadHint, len(b))
	}
	if string(b[:4]) != record.HintMagic || string(b[len(b)-4:]) != record.HintMagic {
		return nil, nil, fmt.Errorf("%w: magic", ErrBadHint)
	}
	if b[4] != record.FormatVersion {
		return nil, nil, fmt.Errorf("%w: version %d", ErrBadHint, b[4])
	}
	if got := binary.LittleEndian.Uint32(b[5:9]); got != id {
		return nil, nil, fmt.Errorf("%w: names segment %d, expected %d", ErrBadHint, got, id)
	}
	body := b[:len(b)-HintFooterSize]
	footer := b[len(b)-HintFooterSize:]
	if want := binary.LittleEndian.Uint32(footer[0:4]); crc32.Checksum(body, castagnoli) != want {
		return nil, nil, fmt.Errorf("%w: crc", ErrBadHint)
	}
	count := binary.LittleEndian.Uint32(footer[4:8])
	// Each entry is at least 5 bytes; refuse a count that cannot fit before allocating for it.
	if uint64(count)*5 > uint64(len(body)-HintHeaderSize) { //nolint:gosec // len(b) >= header+footer checked above
		return nil, nil, fmt.Errorf("%w: count %d", ErrBadHint, count)
	}
	entries = make([]HintEntry, 0, count)
	offs = make([]int64, 0, count)
	pos := HintHeaderSize
	for pos < len(body) {
		start := pos
		e := HintEntry{Flags: record.Flags(body[pos])}
		if e.Flags&^record.FlagsMask != 0 {
			return nil, nil, fmt.Errorf("%w: entry %d flags %#x", ErrBadHint, len(entries), e.Flags)
		}
		pos++
		var fields [4]uint64
		for i := range fields {
			v, n := binary.Uvarint(body[pos:])
			if n <= 0 || v > MaxSize {
				return nil, nil, fmt.Errorf("%w: entry %d field %d", ErrBadHint, len(entries), i)
			}
			fields[i] = v
			pos += n
		}
		// Every field was bounded by MaxSize (uint32) in the loop above.
		e.ExpSec, e.Off, e.Size = uint32(fields[0]), uint32(fields[1]), uint32(fields[2]) //nolint:gosec // <= MaxSize
		keyLen := int(fields[3])                                                          //nolint:gosec // <= MaxSize
		if keyLen > record.MaxKey || pos+keyLen > len(body) {
			return nil, nil, fmt.Errorf("%w: entry %d key length %d", ErrBadHint, len(entries), keyLen)
		}
		if e.Off < record.SegmentHeaderSize || e.Size < record.HeaderSize || e.Size > record.MaxRecord {
			return nil, nil, fmt.Errorf("%w: entry %d off %d size %d", ErrBadHint, len(entries), e.Off, e.Size)
		}
		e.Key = body[pos : pos+keyLen : pos+keyLen]
		pos += keyLen
		entries = append(entries, e)
		offs = append(offs, int64(start))
	}
	if uint64(len(entries)) != uint64(count) {
		return nil, nil, fmt.Errorf("%w: footer says %d entries, found %d", ErrBadHint, count, len(entries))
	}
	return entries, offs, nil
}
