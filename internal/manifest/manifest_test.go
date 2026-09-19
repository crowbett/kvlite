package manifest

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestEncodeDecode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		m    Manifest
	}{
		{"empty", Manifest{}},
		{"one", Manifest{Gen: 1, Active: 0, NextID: 1, Segments: []uint32{0}}},
		{"many", Manifest{Gen: 1 << 40, Active: 9, NextID: 10, Segments: []uint32{3, 5, 9}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := Encode(&tt.m)
			if len(b) != fixedSize+4*len(tt.m.Segments) {
				t.Fatalf("Encode len = %d, want %d", len(b), fixedSize+4*len(tt.m.Segments))
			}
			if string(b[:4]) != Magic || b[4] != Version {
				t.Fatalf("bad prologue % x", b[:5])
			}
			got, err := Decode(b)
			if err != nil {
				t.Fatalf("Decode() = %v", err)
			}
			if got.Gen != tt.m.Gen || got.Active != tt.m.Active || got.NextID != tt.m.NextID ||
				!slices.Equal(got.Segments, tt.m.Segments) {
				t.Errorf("round trip = %+v, want %+v", got, tt.m)
			}
		})
	}
}

func TestDecodeCorrupt(t *testing.T) {
	t.Parallel()
	good := Encode(&Manifest{Gen: 2, Active: 1, NextID: 2, Segments: []uint32{0, 1}})
	flip := func(i int) []byte {
		b := slices.Clone(good)
		b[i] ^= 0xff
		return b
	}
	tests := []struct {
		name string
		in   []byte
	}{
		{"nil", nil},
		{"short", good[:fixedSize-1]},
		{"bad magic", flip(0)},
		{"bad version", flip(4)},
		{"bad gen", flip(5)},
		{"bad count", flip(21)},
		{"bad segment", flip(25)},
		{"bad crc", flip(len(good) - 1)},
		{"trailing byte", append(slices.Clone(good), 0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := Decode(tt.in); !errors.Is(err, ErrCorrupt) {
				t.Errorf("Decode() err = %v, want ErrCorrupt", err)
			}
		})
	}
}

func TestSaveLoad(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if _, err := Load(dir); !errors.Is(err, ErrNotExist) || !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Load(fresh dir) = %v, want ErrNotExist (and fs.ErrNotExist)", err)
	}
	gens := []Manifest{
		{Gen: 0, Active: 0, NextID: 1, Segments: []uint32{0}},
		{Gen: 1, Active: 3, NextID: 4, Segments: []uint32{0, 1, 2, 3}},
		{Gen: 2, Active: 3, NextID: 6, Segments: []uint32{3, 4, 5}},
		{Gen: 3, NextID: 6},
	}
	for i := range gens {
		want := &gens[i]
		if err := Save(dir, want); err != nil {
			t.Fatalf("Save(gen %d) = %v", want.Gen, err)
		}
		if _, err := os.Stat(filepath.Join(dir, TempName)); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("gen %d: MANIFEST.tmp left behind: %v", want.Gen, err)
		}
		got, err := Load(dir)
		if err != nil {
			t.Fatalf("Load(gen %d) = %v", want.Gen, err)
		}
		if got.Gen != want.Gen || got.Active != want.Active || got.NextID != want.NextID ||
			!slices.Equal(got.Segments, want.Segments) {
			t.Fatalf("Load() = %+v, want %+v", got, want)
		}
	}
	// The file on disk is exactly Encode(m).
	b, err := os.ReadFile(filepath.Join(dir, FileName))
	if err != nil || !slices.Equal(b, Encode(&gens[len(gens)-1])) {
		t.Errorf("MANIFEST bytes differ from Encode: %v", err)
	}
	if err := Save(filepath.Join(dir, "missing"), &gens[0]); err == nil {
		t.Error("Save into a missing dir = nil")
	}
}

func TestSaveInvalid(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		m    Manifest
	}{
		{"unsorted", Manifest{Active: 1, NextID: 3, Segments: []uint32{2, 1}}},
		{"duplicate", Manifest{Active: 1, NextID: 3, Segments: []uint32{1, 1}}},
		{"active missing", Manifest{Active: 7, NextID: 3, Segments: []uint32{1, 2}}},
		{"next id too low", Manifest{Active: 2, NextID: 2, Segments: []uint32{1, 2}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			if err := Save(dir, &tt.m); !errors.Is(err, ErrInvalid) {
				t.Fatalf("Save() = %v, want ErrInvalid", err)
			}
			if ents, _ := os.ReadDir(dir); len(ents) != 0 {
				t.Errorf("Save wrote files for an invalid manifest: %v", ents)
			}
		})
	}
}

func TestCrashMidSave(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	good := &Manifest{Gen: 4, Active: 2, NextID: 3, Segments: []uint32{1, 2}}
	if err := Save(dir, good); err != nil {
		t.Fatal(err)
	}
	// A crash after writing MANIFEST.tmp but before the rename leaves a scratch file behind, complete
	// or torn; Load must keep serving the committed manifest and the next Save must replace it.
	for _, tmp := range [][]byte{Encode(&Manifest{Gen: 5, NextID: 3}), []byte("KVLM\x01torn"), nil} {
		if err := os.WriteFile(filepath.Join(dir, TempName), tmp, 0o644); err != nil {
			t.Fatal(err)
		}
		got, err := Load(dir)
		if err != nil || got.Gen != 4 || !slices.Equal(got.Segments, []uint32{1, 2}) {
			t.Fatalf("Load() with a stray tmp = %+v, %v", got, err)
		}
		next := &Manifest{Gen: 5, Active: 2, NextID: 4, Segments: []uint32{2, 3}}
		if err := Save(dir, next); err != nil {
			t.Fatalf("Save over a stray tmp = %v", err)
		}
		got, err = Load(dir)
		if err != nil || got.Gen != 5 || !slices.Equal(got.Segments, []uint32{2, 3}) {
			t.Fatalf("Load() after Save = %+v, %v", got, err)
		}
		if _, err := os.Stat(filepath.Join(dir, TempName)); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("MANIFEST.tmp left behind: %v", err)
		}
		if err := Save(dir, good); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLoadCorrupt(t *testing.T) {
	t.Parallel()
	good := Encode(&Manifest{Gen: 2, Active: 1, NextID: 2, Segments: []uint32{0, 1}})
	flip := func(i int) []byte {
		b := slices.Clone(good)
		b[i] ^= 0xff
		return b
	}
	tests := []struct {
		name string
		in   []byte
	}{
		{"empty file", nil},
		{"truncated", good[:len(good)-3]},
		{"bad crc", flip(len(good) - 2)},
		{"bad magic", flip(1)},
		{"flipped segment", flip(27)},
		{"other file", []byte("hello")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, FileName), tt.in, 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(dir); !errors.Is(err, ErrCorrupt) || errors.Is(err, ErrNotExist) {
				t.Errorf("Load() = %v, want ErrCorrupt", err)
			}
		})
	}
}

func TestOrphans(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	files := []string{
		"00000001.log", "00000001.hint", // live
		"00000002.log",                  // live, no hint
		"00000003.log", "00000003.hint", // orphan pair (crashed before the swap)
		"00000004.hint",     // orphan hint without a log
		"00000002.hint.tmp", // scratch: always an orphan, even for a live id
		"00000009.log.tmp",  // scratch
		"MANIFEST", "MANIFEST.tmp", "LOCK", "notes.txt", "1.log", "0000000a.log", "00000005.log.bak",
	}
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(dir, f), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// A sub-directory named like a segment is ignored, as is trash/ itself.
	if err := os.MkdirAll(filepath.Join(dir, TrashDir, "00000007.log"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "00000008.log"), 0o755); err != nil {
		t.Fatal(err)
	}
	got, err := Orphans(dir, &Manifest{Active: 2, NextID: 3, Segments: []uint32{1, 2}})
	if err != nil {
		t.Fatalf("Orphans() = %v", err)
	}
	want := []string{"00000002.hint.tmp", "00000003.hint", "00000003.log", "00000004.hint", "00000009.log.tmp"}
	if !slices.Equal(got, want) {
		t.Errorf("Orphans() = %v, want %v", got, want)
	}
	// With nothing live everything segment-shaped is an orphan.
	got, err = Orphans(dir, &Manifest{})
	if err != nil {
		t.Fatal(err)
	}
	want = []string{
		"00000001.hint", "00000001.log", "00000002.hint.tmp", "00000002.log",
		"00000003.hint", "00000003.log", "00000004.hint", "00000009.log.tmp",
	}
	if !slices.Equal(got, want) {
		t.Errorf("Orphans(empty manifest) = %v, want %v", got, want)
	}
	if _, err := Orphans(filepath.Join(dir, "missing"), &Manifest{}); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Orphans(missing dir) = %v, want ErrNotExist", err)
	}
}
