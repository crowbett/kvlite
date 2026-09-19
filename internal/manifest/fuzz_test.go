package manifest

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"testing"
)

// FuzzManifest feeds arbitrary bytes to Decode and a structured manifest to Encode. Invariants: no
// panic on any input; a decode that succeeds re-encodes to the identical bytes (the layout is fixed,
// so the encoding is canonical); a manifest that encodes decodes to an equal value, whatever Validate
// says about it; Decode never allocates for a count the input cannot hold (the length check runs
// before the make).
func FuzzManifest(f *testing.F) {
	empty := Encode(&Manifest{})
	three := Encode(&Manifest{Gen: 7, Active: 3, NextID: 4, Segments: []uint32{1, 2, 3}})
	badCRC := bytes.Clone(three)
	badCRC[len(badCRC)-1] ^= 0xff
	shortCount := bytes.Clone(three)
	binary.LittleEndian.PutUint32(shortCount[21:25], 2)
	hugeCount := bytes.Clone(empty)
	binary.LittleEndian.PutUint32(hugeCount[21:25], 1<<31)
	badMagic := bytes.Clone(three)
	copy(badMagic, "NOPE")
	badVersion := bytes.Clone(three)
	badVersion[4] = 2
	for _, s := range [][]byte{nil, empty[:10], empty, three, badCRC, shortCount, hugeCount, badMagic, badVersion} {
		f.Add(s, uint64(0), uint32(0), uint32(0), []byte{})
	}
	f.Add([]byte{}, uint64(1), uint32(5), uint32(9), []byte{1, 0, 0, 0, 5, 0, 0, 0})
	f.Add([]byte{}, uint64(1<<63), uint32(0), uint32(0), []byte{0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, raw []byte, gen uint64, active, next uint32, segs []byte) {
		if m, err := Decode(raw); err != nil {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("Decode: not ErrCorrupt: %v", err)
			}
		} else {
			if !bytes.Equal(Encode(m), raw) {
				t.Fatalf("Decode/Encode not canonical:\n in % x\nout % x", raw, Encode(m))
			}
			if uint64(len(m.Segments)) != uint64(binary.LittleEndian.Uint32(raw[21:25])) {
				t.Fatalf("decoded %d segments, header says %d", len(m.Segments), binary.LittleEndian.Uint32(raw[21:25]))
			}
			_ = m.Validate() // any answer, no panic
		}

		if len(segs) > 4<<10 {
			segs = segs[:4<<10]
		}
		in := &Manifest{Gen: gen, Active: active, NextID: next}
		for i := 0; i+4 <= len(segs); i += 4 {
			in.Segments = append(in.Segments, binary.LittleEndian.Uint32(segs[i:]))
		}
		enc := Encode(in)
		if len(enc) != fixedSize+4*len(in.Segments) {
			t.Fatalf("Encode wrote %d bytes for %d segments", len(enc), len(in.Segments))
		}
		out, err := Decode(enc)
		if err != nil {
			t.Fatalf("Decode(Encode(%+v)) = %v", in, err)
		}
		if out.Gen != in.Gen || out.Active != in.Active || out.NextID != in.NextID || !slices.Equal(out.Segments, in.Segments) {
			t.Fatalf("round trip: %+v != %+v", out, in)
		}
		// Validate is a pure function of the fields and must agree before and after the trip.
		if (in.Validate() == nil) != (out.Validate() == nil) {
			t.Fatalf("Validate disagrees across the round trip: %v vs %v", in.Validate(), out.Validate())
		}
	})
}
