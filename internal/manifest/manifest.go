// STUB written by the integrator to freeze the contract; workstream A (format) owns this file and replaces
// the bodies, keeping every exported signature. Encode/Decode implement the frozen byte layout and are
// real and tested; Load/Save/Orphans are stubs.

// Package manifest reads and writes kvlite's MANIFEST: the small file that names the live segment set.
// Layout (little-endian): "KVLM" | version u8 | gen u64 | active u32 | nextID u32 | count u32 |
// segID u32 x count | crc32c u32 over everything before it. It is rewritten whole as MANIFEST.tmp,
// synced, then renamed over MANIFEST so a crash never leaves a half-visible segment set. It imports
// nothing from the rest of the module.
package manifest

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
)

// File names and format constants.
const (
	// FileName is the manifest file name inside the store directory.
	FileName = "MANIFEST"
	// TempName is the scratch file Save writes before renaming over FileName.
	TempName = "MANIFEST.tmp"
	// TrashDir is the sub-directory segments are renamed into before deletion.
	TrashDir = "trash"
	// Magic opens the file.
	Magic = "KVLM"
	// Version is the format version.
	Version = 1
	// fixedSize is the encoded size without segment ids: magic 4 + ver 1 + gen 8 + active 4 + next 4 + count 4 + crc 4.
	fixedSize = 4 + 1 + 8 + 4 + 4 + 4 + 4
)

// Sentinel errors.
var (
	// ErrNotExist means no MANIFEST is present (a fresh directory, or a lost rename).
	ErrNotExist = errors.New("manifest: not found")
	// ErrCorrupt means the file failed its magic, version, length or crc check.
	ErrCorrupt = errors.New("manifest: corrupt")
)

// Manifest is the decoded live segment set.
type Manifest struct {
	// Gen increments on every Save; compaction publishes gen+1.
	Gen uint64
	// Active is the id of the segment receiving appends.
	Active uint32
	// NextID is the next unused segment id.
	NextID uint32
	// Segments lists every live segment id in ascending order, Active included.
	Segments []uint32
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Encode serialises m in the frozen layout, crc included.
func Encode(m *Manifest) []byte {
	b := make([]byte, 0, fixedSize+4*len(m.Segments))
	b = append(b, Magic...)
	b = append(b, Version)
	b = binary.LittleEndian.AppendUint64(b, m.Gen)
	b = binary.LittleEndian.AppendUint32(b, m.Active)
	b = binary.LittleEndian.AppendUint32(b, m.NextID)
	b = binary.LittleEndian.AppendUint32(b, uint32(min(len(m.Segments), math.MaxUint32)))
	for _, id := range m.Segments {
		b = binary.LittleEndian.AppendUint32(b, id)
	}
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

// Decode parses b. Any magic, version, length or crc problem is ErrCorrupt.
func Decode(b []byte) (*Manifest, error) {
	if len(b) < fixedSize || string(b[:4]) != Magic || b[4] != Version {
		return nil, ErrCorrupt
	}
	count := binary.LittleEndian.Uint32(b[21:25])
	if uint64(len(b)) != uint64(fixedSize)+4*uint64(count) {
		return nil, ErrCorrupt
	}
	body, crc := b[:len(b)-4], binary.LittleEndian.Uint32(b[len(b)-4:])
	if crc32.Checksum(body, castagnoli) != crc {
		return nil, ErrCorrupt
	}
	m := &Manifest{
		Gen:      binary.LittleEndian.Uint64(b[5:13]),
		Active:   binary.LittleEndian.Uint32(b[13:17]),
		NextID:   binary.LittleEndian.Uint32(b[17:21]),
		Segments: make([]uint32, count),
	}
	for i := range m.Segments {
		m.Segments[i] = binary.LittleEndian.Uint32(b[25+4*i:])
	}
	return m, nil
}

// Load reads dir/MANIFEST: ErrNotExist when absent, ErrCorrupt when it fails validation.
func Load(dir string) (*Manifest, error) {
	_ = dir
	return nil, errors.New("stub: manifest.Load")
}

// Save writes m as dir/MANIFEST.tmp, fsyncs it and renames it over dir/MANIFEST (REPLACE_EXISTING on
// Windows). Directory fsync is skipped on Windows; the crc plus the *.log rescan in Open cover a lost
// rename.
func Save(dir string, m *Manifest) error {
	_, _ = dir, m
	return errors.New("stub: manifest.Save")
}

// Orphans lists every *.log and *.hint file in dir whose segment id is not in m.Segments: the leftovers
// of a compaction that crashed before or after the MANIFEST swap. Names are relative to dir.
func Orphans(dir string, m *Manifest) ([]string, error) {
	_, _ = dir, m
	return nil, errors.New("stub: manifest.Orphans")
}
