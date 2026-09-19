package record

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"
	"unsafe"
)

func TestConstants(t *testing.T) {
	t.Parallel()
	if MaxMeta != 31 {
		t.Errorf("MaxMeta = %d, want 31 (1 + 2*10 + 2*5)", MaxMeta)
	}
	if MaxPayload != MaxMeta+MaxKey+MaxValue || MaxRecord != HeaderSize+MaxPayload {
		t.Errorf("size constants inconsistent: payload %d record %d", MaxPayload, MaxRecord)
	}
	if MaxValue != 16<<20 {
		t.Errorf("MaxValue = %d, want 16 MiB (owner decision)", MaxValue)
	}
	if len(SegmentMagic) != 4 || len(HintMagic) != 4 || SegmentMagic == HintMagic {
		t.Errorf("magics must be 4 distinct bytes: %q %q", SegmentMagic, HintMagic)
	}
	if SegmentHeaderSize != 4+1+4+7 {
		t.Errorf("SegmentHeaderSize = %d, want 16", SegmentHeaderSize)
	}
	if unsafe.Sizeof(Flags(0)) != 1 {
		t.Errorf("Flags must be one byte")
	}
}

func TestFlags(t *testing.T) {
	t.Parallel()
	tests := []struct {
		f                            Flags
		tomb, exp, member, commitBit bool
	}{
		{0, false, false, false, false},
		{FlagTombstone, true, false, false, false},
		{FlagExpiry, false, true, false, false},
		{FlagBatchMember, false, false, true, false},
		{FlagBatchCommit, false, false, false, true},
		{FlagsMask, true, true, true, true},
	}
	for _, tt := range tests {
		if tt.f.Tombstone() != tt.tomb || tt.f.HasExpiry() != tt.exp ||
			tt.f.BatchMember() != tt.member || tt.f.BatchCommit() != tt.commitBit {
			t.Errorf("flags %08b: got %v %v %v %v", tt.f, tt.f.Tombstone(), tt.f.HasExpiry(),
				tt.f.BatchMember(), tt.f.BatchCommit())
		}
	}
	if FlagTombstone != 1 || FlagExpiry != 2 || FlagBatchMember != 4 || FlagBatchCommit != 8 {
		t.Errorf("flag bit order is part of the format: %d %d %d %d",
			FlagTombstone, FlagExpiry, FlagBatchMember, FlagBatchCommit)
	}
}

func TestChecksum(t *testing.T) {
	t.Parallel()
	// The standard crc32c check value.
	if got := Checksum([]byte("123456789")); got != 0xE3069283 {
		t.Errorf("Checksum(check) = %#x, want 0xe3069283 (Castagnoli)", got)
	}
	if Checksum(nil) != 0 {
		t.Errorf("Checksum(nil) = %#x, want 0", Checksum(nil))
	}
	if !Verify([]byte("abc"), Checksum([]byte("abc"))) || Verify([]byte("abc"), 1) {
		t.Errorf("Verify does not agree with Checksum")
	}
}

func TestHeader(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		in       []byte
		wantCRC  uint32
		wantPlen uint32
		wantErr  error
	}{
		{"short", []byte{1, 2, 3}, 0, 0, ErrTruncated},
		{"zero", make([]byte, 8), 0, 0, nil},
		{"little endian", []byte{0x78, 0x56, 0x34, 0x12, 0x10, 0, 0, 0}, 0x12345678, 16, nil},
		{"max payload", binary.LittleEndian.AppendUint32([]byte{0, 0, 0, 0}, MaxPayload), 0, MaxPayload, nil},
		{"too large", []byte{0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff}, 0, 0xffffffff, ErrTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			crc, plen, err := ParseHeader(tt.in)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if err != nil && errors.Is(err, ErrTruncated) {
				return
			}
			if crc != tt.wantCRC || plen != tt.wantPlen {
				t.Errorf("got (%#x, %d), want (%#x, %d)", crc, plen, tt.wantCRC, tt.wantPlen)
			}
		})
	}

	var buf [HeaderSize]byte
	PutHeader(buf[:], 0xdeadbeef, 42)
	crc, plen, err := ParseHeader(buf[:])
	if err != nil || crc != 0xdeadbeef || plen != 42 {
		t.Errorf("PutHeader/ParseHeader round trip: (%#x, %d, %v)", crc, plen, err)
	}
	if !bytes.Equal(buf[:4], []byte{0xef, 0xbe, 0xad, 0xde}) {
		t.Errorf("crc not little-endian: % x", buf[:4])
	}
	if MaxEncodedSize(MaxKey, MaxValue) != MaxRecord {
		t.Errorf("MaxEncodedSize(max, max) = %d, want MaxRecord %d", MaxEncodedSize(MaxKey, MaxValue), MaxRecord)
	}
}

func isStub(err error) bool { return err != nil && strings.HasPrefix(err.Error(), "stub:") }

// FuzzRecord is the format fuzzer (WS-A finishes the assertions when Append/Decode are real).
// Structured inputs round-trip through Append then Decode and must be field-identical; raw bytes go
// straight into Decode, which must never panic and must never return a Key or Value pointing outside
// the input slice. Seeds cover the empty/max key, a zero-length value, TTL set and unset, and every
// flag combination.
func FuzzRecord(f *testing.F) {
	maxKey := bytes.Repeat([]byte("k"), MaxKey)
	f.Add(uint8(0), uint64(0), uint64(0), []byte{}, []byte{}, []byte{})
	f.Add(uint8(0), uint64(1_700_000_000_000), uint64(0), []byte("k"), []byte("v"), []byte{0})
	f.Add(uint8(FlagExpiry), uint64(1_700_000_000_000), uint64(1_700_000_060_000), []byte("ttl"), []byte("v"), []byte{1, 2, 3})
	f.Add(uint8(FlagTombstone), uint64(1), uint64(0), []byte("gone"), []byte{}, []byte{0xff, 0xff, 0xff, 0xff})
	f.Add(uint8(FlagBatchMember), uint64(2), uint64(0), []byte("m"), []byte("v"), bytes.Repeat([]byte{0x80}, 12))
	f.Add(uint8(FlagBatchCommit), uint64(3), uint64(0), []byte{}, []byte{2, 7}, []byte{0x0f})
	f.Add(uint8(FlagsMask), uint64(4), uint64(5), maxKey, []byte("v"), maxKey[:64])
	f.Add(uint8(0), uint64(0), uint64(0), maxKey, bytes.Repeat([]byte("v"), 4096), []byte{})

	f.Fuzz(func(t *testing.T, flags uint8, tsMillis, expMillis uint64, key, value, raw []byte) {
		// Arbitrary bytes must never panic and never alias outside the input.
		if rec, err := Decode(raw); err == nil {
			assertAliases(t, raw, rec.Key, "Key")
			assertAliases(t, raw, rec.Value, "Value")
		}

		in := Record{
			Key:   key,
			Value: value,
			TS:    time.UnixMilli(int64(tsMillis & (1<<62 - 1))),
			Flags: Flags(flags) & FlagsMask,
		}
		if in.Flags.HasExpiry() {
			in.Expiry = time.UnixMilli(int64(expMillis & (1<<62 - 1)))
		}
		enc, err := Append(nil, in)
		if isStub(err) {
			t.Skip("stub: record.Append (WS-A)")
		}
		if err != nil {
			if len(key) > MaxKey || len(value) > MaxValue {
				return
			}
			t.Fatalf("Append(%+v) = %v", in, err)
		}
		crc, plen, err := ParseHeader(enc)
		if err != nil || int(plen) != len(enc)-HeaderSize || !Verify(enc[HeaderSize:], crc) {
			t.Fatalf("Append produced a bad header: crc %#x plen %d err %v len %d", crc, plen, err, len(enc))
		}
		out, err := Decode(enc[HeaderSize:])
		if err != nil {
			t.Fatalf("Decode(Append(r)) = %v", err)
		}
		if !bytes.Equal(out.Key, in.Key) || !bytes.Equal(out.Value, in.Value) || out.Flags != in.Flags ||
			!out.TS.Equal(in.TS) || !out.Expiry.Equal(in.Expiry) {
			t.Fatalf("round trip mismatch:\n in %+v\nout %+v", in, out)
		}
		assertAliases(t, enc, out.Key, "Key")
		assertAliases(t, enc, out.Value, "Value")
	})
}

func assertAliases(t *testing.T, buf, sub []byte, what string) {
	t.Helper()
	if len(sub) == 0 {
		return
	}
	start := uintptr(unsafe.Pointer(unsafe.SliceData(buf)))
	end := start + uintptr(len(buf))
	p := uintptr(unsafe.Pointer(unsafe.SliceData(sub)))
	if p < start || p+uintptr(len(sub)) > end {
		t.Fatalf("%s points outside the input slice", what)
	}
}
