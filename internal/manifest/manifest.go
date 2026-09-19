// Package manifest reads and writes kvlite's MANIFEST: the small file that names the live segment set.
// Layout (little-endian): "KVLM" | version u8 | gen u64 | active u32 | nextID u32 | count u32 |
// segID u32 x count | crc32c u32 over everything before it. It is rewritten whole as MANIFEST.tmp,
// synced, then renamed over MANIFEST so a crash never leaves a half-visible segment set. It imports
// nothing from the rest of the module.
package manifest

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
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
	// ErrNotExist means no MANIFEST is present (a fresh directory, or a lost rename). Errors from Load
	// that wrap it also match fs.ErrNotExist.
	ErrNotExist = errors.New("manifest: not found")
	// ErrCorrupt means the file failed its magic, version, length or crc check.
	ErrCorrupt = errors.New("manifest: corrupt")
	// ErrInvalid means Save was handed a Manifest that breaks an invariant (unsorted or duplicate
	// segment ids, Active not in Segments, NextID not above every id).
	ErrInvalid = errors.New("manifest: invalid")
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

// Validate checks the invariants Save enforces: Segments strictly ascending, Active listed in Segments
// when there are any, NextID above every listed id.
func (m *Manifest) Validate() error {
	for i, id := range m.Segments {
		if i > 0 && id <= m.Segments[i-1] {
			return fmt.Errorf("%w: segments not strictly ascending at index %d", ErrInvalid, i)
		}
		if id >= m.NextID {
			return fmt.Errorf("%w: segment %d not below NextID %d", ErrInvalid, id, m.NextID)
		}
	}
	if len(m.Segments) > 0 && !slices.Contains(m.Segments, m.Active) {
		return fmt.Errorf("%w: active segment %d not listed", ErrInvalid, m.Active)
	}
	return nil
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

// Load reads dir/MANIFEST: ErrNotExist when absent, ErrCorrupt when it fails validation. A leftover
// MANIFEST.tmp from a crashed Save is ignored.
func Load(dir string) (*Manifest, error) {
	path := filepath.Join(dir, FileName)
	b, err := os.ReadFile(path) //nolint:gosec // the store's MANIFEST
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("manifest: load %s: %w: %w", path, ErrNotExist, err)
		}
		return nil, fmt.Errorf("manifest: load %s: %w", path, err)
	}
	m, err := Decode(b)
	if err != nil {
		return nil, fmt.Errorf("manifest: load %s (%d bytes): %w", path, len(b), err)
	}
	return m, nil
}

// Save writes m as dir/MANIFEST.tmp, fsyncs it and renames it over dir/MANIFEST (REPLACE_EXISTING on
// Windows). Directory fsync is skipped on Windows; the crc plus the *.log rescan in Open cover a lost
// rename. m is written verbatim (Save does not bump Gen; the caller publishes gen+1) after Validate.
// A crash at any point leaves either the old MANIFEST or the new one, never a mix; a stray
// MANIFEST.tmp is overwritten by the next Save and ignored by Load.
func Save(dir string, m *Manifest) error {
	if err := m.Validate(); err != nil {
		return err
	}
	path := filepath.Join(dir, FileName)
	tmp := filepath.Join(dir, TempName)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644) //nolint:gosec // the store's MANIFEST
	if err != nil {
		return fmt.Errorf("manifest: save %s: %w", path, err)
	}
	fail := func(err error) error {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("manifest: save %s: %w", path, err)
	}
	if _, err := f.Write(Encode(m)); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("manifest: save %s: %w", path, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("manifest: save %s: %w", path, err)
	}
	syncDir(dir)
	return nil
}

// syncDir makes the rename durable where the platform supports fsync on a directory. Best effort:
// Windows has no equivalent, and a failure here is covered by the crc + rescan fallback.
func syncDir(dir string) {
	if runtime.GOOS == "windows" {
		return
	}
	d, err := os.Open(dir) //nolint:gosec // the store directory
	if err != nil {
		return
	}
	_ = d.Sync()
	_ = d.Close()
}

// Orphans lists every *.log and *.hint file in dir whose segment id is not in m.Segments: the leftovers
// of a compaction that crashed before or after the MANIFEST swap. A scratch file of either kind
// (NNNNNNNN.log.tmp / NNNNNNNN.hint.tmp) is always an orphan. Names are relative to dir and sorted;
// sub-directories (trash/) and unrelated files are ignored.
func Orphans(dir string, m *Manifest) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("manifest: orphans in %s: %w", dir, err)
	}
	live := make(map[uint32]struct{}, len(m.Segments))
	for _, id := range m.Segments {
		live[id] = struct{}{}
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		id, tmp, ok := parseSegmentName(e.Name())
		if !ok {
			continue
		}
		if _, isLive := live[id]; tmp || !isLive {
			out = append(out, e.Name())
		}
	}
	slices.Sort(out)
	return out, nil
}

// parseSegmentName recognises NNNNNNNN.log, NNNNNNNN.hint and their .tmp scratch forms (mirrors
// segment.ParseName without importing it).
func parseSegmentName(name string) (id uint32, tmp, ok bool) {
	base, isTmp := strings.CutSuffix(name, ".tmp")
	switch {
	case strings.HasSuffix(base, ".log"):
		base = strings.TrimSuffix(base, ".log")
	case strings.HasSuffix(base, ".hint"):
		base = strings.TrimSuffix(base, ".hint")
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
	return uint32(n), isTmp, true
}
