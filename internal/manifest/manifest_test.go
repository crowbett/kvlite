package manifest

import (
	"errors"
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
