package record

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"testing/iotest"
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

// Golden bytes: these pin the on-disk format. Changing them is a format change.
var (
	goldenRecord = Record{
		Key:    []byte("k"),
		Value:  []byte("v"),
		TS:     time.UnixMilli(1_700_000_000_000),
		Expiry: time.UnixMilli(1_700_000_060_000),
		Flags:  FlagExpiry,
	}
	goldenBytes = []byte{
		0xac, 0x10, 0x65, 0x30, // crc32c(payload) = 0x306510ac, little-endian
		0x11, 0x00, 0x00, 0x00, // plen = 17
		0x02,                               // flags: FlagExpiry
		0x80, 0xd0, 0x95, 0xff, 0xbc, 0x31, // tsMillis 1700000000000
		0xe0, 0xa4, 0x99, 0xff, 0xbc, 0x31, // expiryMillis 1700000060000
		0x01, 0x01, // keyLen, valLen
		'k', 'v',
	}
	goldenTombstone = Record{Key: []byte("gone"), TS: time.UnixMilli(1), Flags: FlagTombstone}
	goldenTombBytes = []byte{
		0x27, 0x36, 0x91, 0x30, // crc32c = 0x30913627
		0x08, 0x00, 0x00, 0x00, // plen = 8
		0x01,       // flags: FlagTombstone
		0x01,       // tsMillis 1
		0x04, 0x00, // keyLen 4, valLen 0
		'g', 'o', 'n', 'e',
	}
)

func TestGolden(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		rec  Record
		want []byte
	}{
		{"expiry", goldenRecord, goldenBytes},
		{"tombstone", goldenTombstone, goldenTombBytes},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := Append(nil, tt.rec)
			if err != nil {
				t.Fatalf("Append() = %v", err)
			}
			if !bytes.Equal(got, tt.want) {
				t.Fatalf("Append() =\n% x\nwant\n% x", got, tt.want)
			}
			if EncodedSize(tt.rec) != len(tt.want) {
				t.Errorf("EncodedSize() = %d, want %d", EncodedSize(tt.rec), len(tt.want))
			}
			dec, n, err := Parse(tt.want)
			if err != nil || n != len(tt.want) {
				t.Fatalf("Parse() = %d, %v", n, err)
			}
			assertEqualRecord(t, dec, tt.rec)
		})
	}
}

func assertEqualRecord(t *testing.T, got, want Record) {
	t.Helper()
	if !bytes.Equal(got.Key, want.Key) || !bytes.Equal(got.Value, want.Value) || got.Flags != want.Flags ||
		!got.TS.Equal(want.TS) || !got.Expiry.Equal(want.Expiry) {
		t.Fatalf("record mismatch:\n got %+v\nwant %+v", got, want)
	}
}

func TestRoundTrip(t *testing.T) {
	t.Parallel()
	ts := time.UnixMilli(1_700_000_000_123)
	exp := ts.Add(time.Hour)
	big := bytes.Repeat([]byte{0xab}, MaxValue)
	maxKey := bytes.Repeat([]byte("k"), MaxKey)
	tests := []struct {
		name string
		rec  Record
	}{
		{"zero record", Record{}},
		{"empty key and value", Record{TS: ts}},
		{"empty key with value", Record{Value: []byte("v"), TS: ts}},
		{"key with empty value", Record{Key: []byte("k"), TS: ts}},
		{"plain", Record{Key: []byte("key"), Value: []byte("value"), TS: ts}},
		{"max key", Record{Key: maxKey, Value: []byte("v"), TS: ts}},
		{"max value", Record{Key: []byte("k"), Value: big, TS: ts}},
		{"max both", Record{Key: maxKey, Value: big, TS: ts, Expiry: exp, Flags: FlagsMask}},
		{"negative ts", Record{Key: []byte("k"), TS: time.UnixMilli(-5)}},
		{"far future", Record{Key: []byte("k"), TS: time.UnixMilli(1 << 62), Expiry: time.UnixMilli(1<<63 - 1), Flags: FlagExpiry}},
		{"expiry ignored without flag", Record{Key: []byte("k"), TS: ts, Expiry: exp}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			enc, err := Append([]byte("prefix"), tt.rec)
			if err != nil {
				t.Fatalf("Append() = %v", err)
			}
			if !bytes.HasPrefix(enc, []byte("prefix")) {
				t.Fatal("Append clobbered the existing dst contents")
			}
			enc = enc[len("prefix"):]
			if len(enc) != EncodedSize(tt.rec) || len(enc) > MaxEncodedSize(len(tt.rec.Key), len(tt.rec.Value)) {
				t.Fatalf("len = %d, EncodedSize %d, MaxEncodedSize %d", len(enc), EncodedSize(tt.rec),
					MaxEncodedSize(len(tt.rec.Key), len(tt.rec.Value)))
			}
			want := tt.rec
			if !want.Flags.HasExpiry() {
				want.Expiry = time.Time{}
			}
			got, n, err := Parse(enc)
			if err != nil || n != len(enc) {
				t.Fatalf("Parse() = %d, %v", n, err)
			}
			assertEqualRecord(t, got, want)
			assertAliases(t, enc, got.Key, "Key")
			assertAliases(t, enc, got.Value, "Value")
			// Appending to the decoded views must never write into the input.
			snapshot := bytes.Clone(enc)
			_ = append(got.Key, 'x')
			_ = append(got.Value, 'x')
			if !bytes.Equal(enc, snapshot) {
				t.Fatal("append to a decoded view clobbered the input (missing capacity clip)")
			}
			got, m, buf, err := Read(bytes.NewReader(enc), nil)
			if err != nil || m != len(enc) {
				t.Fatalf("Read() = %d, %v", m, err)
			}
			assertEqualRecord(t, got, want)
			assertAliases(t, buf, got.Key, "Key")
			assertAliases(t, buf, got.Value, "Value")
		})
	}
}

func TestFlagCombos(t *testing.T) {
	t.Parallel()
	for f := Flags(0); f <= FlagsMask; f++ {
		rec := Record{Key: []byte("k"), Value: []byte{1, 2, 3}, TS: time.UnixMilli(7), Flags: f}
		if f.HasExpiry() {
			rec.Expiry = time.UnixMilli(9)
		}
		enc, err := Append(nil, rec)
		if err != nil {
			t.Fatalf("flags %04b: Append() = %v", f, err)
		}
		got, err := Decode(enc[HeaderSize:])
		if err != nil {
			t.Fatalf("flags %04b: Decode() = %v", f, err)
		}
		assertEqualRecord(t, got, rec)
		if got.Flags.HasExpiry() != (got.Expiry != time.Time{}) {
			t.Errorf("flags %04b: expiry presence does not follow the flag: %v", f, got.Expiry)
		}
	}
	for _, bad := range []Flags{FlagsMask + 1, 0x80, 0xff} {
		if _, err := Append(nil, Record{Flags: bad}); !errors.Is(err, ErrMalformed) {
			t.Errorf("Append(flags %#x) = %v, want ErrMalformed", bad, err)
		}
	}
}

func TestAppendLimits(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		rec     Record
		wantErr error
	}{
		{"key at max", Record{Key: make([]byte, MaxKey)}, nil},
		{"key over max", Record{Key: make([]byte, MaxKey+1)}, ErrTooLarge},
		{"value at max", Record{Value: make([]byte, MaxValue)}, nil},
		{"value over max", Record{Value: make([]byte, MaxValue+1)}, ErrTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dst := []byte{1, 2, 3}
			out, err := Append(dst, tt.rec)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Append() err = %v, want %v", err, tt.wantErr)
			}
			if err != nil && !bytes.Equal(out, dst) {
				t.Errorf("Append returned a modified dst on error")
			}
			if err == nil && len(out) > MaxRecord {
				t.Errorf("record of %d bytes exceeds MaxRecord", len(out))
			}
		})
	}
}

func TestDecodeErrors(t *testing.T) {
	t.Parallel()
	good, _ := Append(nil, goldenRecord)
	payload := good[HeaderSize:]
	tests := []struct {
		name    string
		in      []byte
		wantErr error
	}{
		{"nil", nil, ErrMalformed},
		{"empty", []byte{}, ErrMalformed},
		{"flags only", []byte{0}, ErrMalformed},
		{"unknown flag", []byte{0x10, 0, 0, 0}, ErrMalformed},
		{"ts varint cut", []byte{0, 0x80}, ErrMalformed},
		{"ts varint overflow", append([]byte{0}, bytes.Repeat([]byte{0x80}, 11)...), ErrMalformed},
		{"missing expiry", []byte{byte(FlagExpiry), 0}, ErrMalformed},
		{"missing lengths", []byte{0, 0}, ErrMalformed},
		{"non-minimal varint", []byte{0, 0xff, 0x00, 0, 0}, ErrMalformed},
		{"non-minimal keyLen", []byte{0, 0, 0x81, 0x00, 0, 'k'}, ErrMalformed},
		{"missing valLen", []byte{0, 0, 1}, ErrMalformed},
		{"key too large", append([]byte{0, 0}, binary.AppendUvarint(binary.AppendUvarint(nil, MaxKey+1), 0)...), ErrTooLarge},
		{"value too large", append([]byte{0, 0, 0}, binary.AppendUvarint(nil, MaxValue+1)...), ErrTooLarge},
		{"key past end", []byte{0, 0, 5, 0, 'a', 'b'}, ErrMalformed},
		{"value past end", []byte{0, 0, 1, 5, 'a', 'b'}, ErrMalformed},
		{"trailing bytes", append(bytes.Clone(payload), 0), ErrMalformed},
		{"payload cut", payload[:len(payload)-1], ErrMalformed},
		{"good", payload, nil},
		{"minimal", []byte{0, 0, 0, 0}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec, err := Decode(tt.in)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Decode() err = %v, want %v", err, tt.wantErr)
			}
			if err == nil {
				assertAliases(t, tt.in, rec.Key, "Key")
				assertAliases(t, tt.in, rec.Value, "Value")
			}
		})
	}
}

func TestParseAndRead(t *testing.T) {
	t.Parallel()
	good, _ := Append(nil, goldenRecord)
	flip := func(i int) []byte {
		b := bytes.Clone(good)
		b[i] ^= 0xff
		return b
	}
	tooLarge := bytes.Clone(good)
	binary.LittleEndian.PutUint32(tooLarge[4:8], MaxPayload+1)
	tests := []struct {
		name    string
		in      []byte
		wantN   int
		wantErr error
	}{
		{"complete", good, len(good), nil},
		{"complete plus trailing", append(bytes.Clone(good), good...), len(good), nil},
		{"empty", nil, 0, io.EOF},
		{"cut in crc", good[:2], 2, ErrTruncated},
		{"cut in plen", good[:6], 6, ErrTruncated},
		{"header only", good[:HeaderSize], HeaderSize, ErrTruncated},
		{"cut in key", good[:len(good)-2], len(good) - 2, ErrTruncated},
		{"cut in value", good[:len(good)-1], len(good) - 1, ErrTruncated},
		{"bad crc", flip(0), len(good), ErrChecksum},
		{"bad payload byte", flip(len(good) - 1), len(good), ErrChecksum},
		{"plen too large", tooLarge, HeaderSize, ErrTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Parse sees the whole buffer at once; an empty buffer is ErrTruncated there, not EOF.
			wantParse := tt.wantErr
			if errors.Is(wantParse, io.EOF) {
				wantParse = ErrTruncated
			}
			rec, n, err := Parse(tt.in)
			if !errors.Is(err, wantParse) || n != tt.wantN {
				t.Fatalf("Parse() = %d, %v; want %d, %v", n, err, tt.wantN, wantParse)
			}
			if err == nil {
				assertEqualRecord(t, rec, goldenRecord)
			}
			for _, name := range []string{"plain", "one byte at a time"} {
				var rd io.Reader = bytes.NewReader(tt.in)
				if name != "plain" {
					rd = iotest.OneByteReader(rd)
				}
				rec, n, _, err := Read(rd, make([]byte, 0, 4))
				if !errors.Is(err, tt.wantErr) || n != tt.wantN {
					t.Fatalf("Read(%s) = %d, %v; want %d, %v", name, n, err, tt.wantN, tt.wantErr)
				}
				if err == nil {
					assertEqualRecord(t, rec, goldenRecord)
				}
			}
		})
	}

	// A stream of records reads back in order, reusing one buffer, and ends with io.EOF exactly at the
	// boundary; the buffer grows to fit the largest record seen.
	var stream []byte
	recs := []Record{goldenRecord, goldenTombstone, {Key: []byte("big"), Value: make([]byte, 70000), TS: time.UnixMilli(3)}}
	for _, r := range recs {
		stream, _ = Append(stream, r)
	}
	rd := bytes.NewReader(stream)
	var buf []byte
	for i, want := range recs {
		got, _, b, err := Read(rd, buf)
		if err != nil {
			t.Fatalf("stream record %d: %v", i, err)
		}
		buf = b
		assertEqualRecord(t, got, want)
	}
	if _, n, _, err := Read(rd, buf); !errors.Is(err, io.EOF) || n != 0 {
		t.Fatalf("Read at end = %d, %v, want 0, io.EOF", n, err)
	}
	if cap(buf) < 70000+HeaderSize {
		t.Errorf("buffer did not grow: cap %d", cap(buf))
	}
	// A non-EOF reader error passes through untouched.
	boom := errors.New("boom")
	if _, _, _, err := Read(iotest.ErrReader(boom), nil); !errors.Is(err, boom) {
		t.Errorf("Read(ErrReader) = %v, want boom", err)
	}
}

func TestExpirySeconds(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		t    time.Time
		want uint32
	}{
		{"zero time", time.Time{}, 1},
		{"epoch", time.UnixMilli(0), 1},
		{"exact second", time.UnixMilli(10_000), 10},
		{"rounds up", time.UnixMilli(10_001), 11},
		{"just under", time.UnixMilli(10_999), 11},
		{"beyond 2106", time.Unix(1<<40, 0), 1<<32 - 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := ExpirySeconds(tt.t); got != tt.want {
				t.Errorf("ExpirySeconds() = %d, want %d", got, tt.want)
			}
		})
	}
	if (Record{Expiry: time.UnixMilli(10_000)}).ExpSec() != 0 {
		t.Error("ExpSec without FlagExpiry must be 0")
	}
	if (Record{Expiry: time.UnixMilli(10_000), Flags: FlagExpiry}).ExpSec() != 10 {
		t.Error("ExpSec with FlagExpiry must follow ExpirySeconds")
	}
}

// FuzzRecord is the format fuzzer. Structured inputs round-trip through Append then Decode and must be
// field-identical; raw bytes go straight into Decode and Parse, which must never panic and must never
// return a Key or Value pointing outside the input slice. Seeds cover the empty/max key, a zero-length
// value, TTL set and unset, and every flag combination.
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
	f.Add(uint8(0), uint64(0), uint64(0), []byte{}, []byte{}, goldenBytes)
	f.Add(uint8(0), uint64(0), uint64(0), []byte{}, []byte{}, goldenBytes[HeaderSize:])

	f.Fuzz(func(t *testing.T, flags uint8, tsMillis, expMillis uint64, key, value, raw []byte) {
		// Arbitrary bytes must never panic and never alias outside the input.
		if rec, err := Decode(raw); err == nil {
			assertAliases(t, raw, rec.Key, "Key")
			assertAliases(t, raw, rec.Value, "Value")
			// Whatever decodes must re-encode to the identical payload (canonical encoding).
			re, err := Append(nil, rec)
			if err != nil || !bytes.Equal(re[HeaderSize:], raw) {
				t.Fatalf("Decode/Append not canonical: %v\n in % x\nout % x", err, raw, re[HeaderSize:])
			}
		}
		if rec, n, err := Parse(raw); err == nil {
			if n > len(raw) {
				t.Fatalf("Parse consumed %d of %d bytes", n, len(raw))
			}
			assertAliases(t, raw, rec.Key, "Key")
			assertAliases(t, raw, rec.Value, "Value")
		} else if n < 0 || n > len(raw) {
			t.Fatalf("Parse error count %d out of range for %d bytes", n, len(raw))
		}
		if rec, n, _, err := Read(bytes.NewReader(raw), nil); err == nil {
			if n > len(raw) {
				t.Fatalf("Read consumed %d of %d bytes", n, len(raw))
			}
			if want, _, _ := Parse(raw); !bytes.Equal(rec.Key, want.Key) || !bytes.Equal(rec.Value, want.Value) {
				t.Fatal("Read and Parse disagree")
			}
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
		if err != nil {
			if len(key) > MaxKey || len(value) > MaxValue {
				return
			}
			t.Fatalf("Append(%+v) = %v", in, err)
		}
		if len(enc) != EncodedSize(in) {
			t.Fatalf("EncodedSize = %d, Append wrote %d", EncodedSize(in), len(enc))
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
		// Every strict prefix is a torn record, never a valid one and never a panic.
		for _, cut := range []int{0, 1, HeaderSize - 1, HeaderSize, len(enc) / 2, len(enc) - 1} {
			if cut >= len(enc) {
				continue
			}
			if _, _, err := Parse(enc[:cut]); !errors.Is(err, ErrTruncated) {
				t.Fatalf("Parse(prefix %d of %d) = %v, want ErrTruncated", cut, len(enc), err)
			}
		}
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
	if cap(sub) != len(sub) {
		t.Fatalf("%s has spare capacity %d: appending would alias the input", what, cap(sub)-len(sub))
	}
}

func BenchmarkAppend(b *testing.B) {
	rec := Record{Key: []byte("user:0000012345"), Value: bytes.Repeat([]byte("x"), 1024), TS: time.UnixMilli(1_700_000_000_000)}
	buf := make([]byte, 0, MaxEncodedSize(len(rec.Key), len(rec.Value)))
	b.SetBytes(int64(EncodedSize(rec)))
	b.ReportAllocs()
	for b.Loop() {
		var err error
		buf, err = Append(buf[:0], rec)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecode(b *testing.B) {
	enc, _ := Append(nil, Record{
		Key: []byte("user:0000012345"), Value: bytes.Repeat([]byte("x"), 1024),
		TS: time.UnixMilli(1_700_000_000_000), Expiry: time.UnixMilli(1_700_000_060_000), Flags: FlagExpiry,
	})
	payload := enc[HeaderSize:]
	b.SetBytes(int64(len(enc)))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Decode(payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParse(b *testing.B) {
	enc, _ := Append(nil, Record{Key: []byte("user:0000012345"), Value: bytes.Repeat([]byte("x"), 1024), TS: time.UnixMilli(1_700_000_000_000)})
	b.SetBytes(int64(len(enc)))
	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := Parse(enc); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRead(b *testing.B) {
	enc, _ := Append(nil, Record{Key: []byte("user:0000012345"), Value: bytes.Repeat([]byte("x"), 1024), TS: time.UnixMilli(1_700_000_000_000)})
	rd := bytes.NewReader(enc)
	buf := make([]byte, 0, len(enc))
	b.SetBytes(int64(len(enc)))
	b.ReportAllocs()
	for b.Loop() {
		rd.Reset(enc)
		var err error
		_, _, buf, err = Read(rd, buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}
