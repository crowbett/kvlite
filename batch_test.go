package kvlite

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/crowbett/kvlite/internal/record"
)

func TestBatchCommit(t *testing.T) {
	t.Parallel()
	clock := newTestClock()
	db := openEngine(t, WithClock(clock.Now))
	if err := db.Put([]byte("old"), []byte("gone soon")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("a"), []byte("before")); err != nil {
		t.Fatal(err)
	}
	b := db.Batch().
		Put([]byte("a"), []byte("1")).
		PutTTL([]byte("t"), []byte("2"), 30*time.Second).
		Delete([]byte("old")).
		Delete([]byte("never")).
		Put([]byte("a"), []byte("1-again")) // repeated key: last wins
	if b.Len() != 5 {
		t.Fatalf("Len() = %d, want 5", b.Len())
	}
	// Nothing is visible before Commit.
	if got, _ := db.Get([]byte("a")); string(got) != "before" {
		t.Errorf("a before Commit = %q", got)
	}
	if _, err := db.Get([]byte("t")); !errors.Is(err, ErrNotFound) {
		t.Errorf("t before Commit = %v", err)
	}
	if err := b.Commit(); err != nil {
		t.Fatalf("Commit() = %v", err)
	}
	if b.Len() != 0 {
		t.Errorf("Len() after Commit = %d, want 0", b.Len())
	}
	if got, err := db.Get([]byte("a")); err != nil || string(got) != "1-again" {
		t.Errorf("a = %q, %v", got, err)
	}
	if got, err := db.Get([]byte("t")); err != nil || string(got) != "2" {
		t.Errorf("t = %q, %v", got, err)
	}
	if _, err := db.Get([]byte("old")); !errors.Is(err, ErrNotFound) {
		t.Errorf("old = %v, want ErrNotFound", err)
	}
	clock.Advance(30 * time.Second)
	if _, err := db.Get([]byte("t")); !errors.Is(err, ErrNotFound) {
		t.Errorf("t after TTL = %v, want ErrNotFound", err)
	}
	s := db.Stats()
	if s.Keys != 2 || s.Puts != 2+3 || s.Deletes != 2 {
		t.Errorf("Stats keys %d puts %d deletes %d, want 2/5/2", s.Keys, s.Puts, s.Deletes)
	}
	ea, _ := db.kd.Get([]byte("a"))
	et, _ := db.kd.Get([]byte("t"))
	if s.LiveBytes != int64(ea.Size)+int64(et.Size) || s.TotalBytes != s.LiveBytes+s.DeadBytes {
		t.Errorf("bytes live %d dead %d total %d; entries a %d t %d", s.LiveBytes, s.DeadBytes, s.TotalBytes, ea.Size, et.Size)
	}

	// Reuse after Commit.
	if err := b.Put([]byte("z"), []byte("9")).Commit(); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get([]byte("z")); err != nil || string(got) != "9" {
		t.Errorf("z = %q, %v", got, err)
	}
}

// TestBatchOnDisk pins the record group layout recovery relies on: members flagged BatchMember (plus
// Tombstone/Expiry), then exactly one BatchCommit marker whose value is uvarint(count) uvarint(id).
func TestBatchOnDisk(t *testing.T) {
	t.Parallel()
	db := openEngine(t)
	err := db.Batch().
		Put([]byte("a"), []byte("1")).
		PutTTL([]byte("t"), []byte("2"), time.Minute).
		Delete([]byte("d")).
		Commit()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Batch().Put([]byte("second"), nil).Commit(); err != nil {
		t.Fatal(err)
	}
	recs := scanActive(t, db)
	if len(recs) != 6 {
		t.Fatalf("%d records on disk, want 3 members + marker + 1 member + marker", len(recs))
	}
	want := []struct {
		key   string
		flags record.Flags
	}{
		{"a", record.FlagBatchMember},
		{"t", record.FlagBatchMember | record.FlagExpiry},
		{"d", record.FlagBatchMember | record.FlagTombstone},
		{"", record.FlagBatchCommit},
		{"second", record.FlagBatchMember},
		{"", record.FlagBatchCommit},
	}
	for i, w := range want {
		if string(recs[i].Key) != w.key || recs[i].Flags != w.flags {
			t.Errorf("record %d = key %q flags %b, want %q %b", i, recs[i].Key, recs[i].Flags, w.key, w.flags)
		}
	}
	for i, wantCount := range map[int]uint64{3: 3, 5: 1} {
		count, n := binary.Uvarint(recs[i].Value)
		if n <= 0 || count != wantCount {
			t.Errorf("marker %d count = %d, want %d", i, count, wantCount)
		}
		id, m := binary.Uvarint(recs[i].Value[n:])
		if m <= 0 || id == 0 || n+m != len(recs[i].Value) {
			t.Errorf("marker %d id = %d (trailing %d bytes)", i, id, len(recs[i].Value)-n-m)
		}
	}
	// The two markers carry different ids.
	id1, _ := binary.Uvarint(recs[3].Value[1:])
	id2, _ := binary.Uvarint(recs[5].Value[1:])
	if id1 == id2 {
		t.Errorf("both markers carry batch id %d", id1)
	}
	// Keydir entries point at the members, sized exactly, and the marker is dead bytes.
	for _, k := range []string{"a", "t", "second"} {
		e, ok := db.kd.Get([]byte(k))
		if !ok {
			t.Fatalf("%s missing from keydir", k)
		}
		rec, _, err := db.readers[e.Seg].ReadAt(e.Off, e.Size)
		if err != nil || string(rec.Key) != k {
			t.Errorf("entry for %s reads back %q, %v", k, rec.Key, err)
		}
	}
	if s := db.Stats(); s.DeadBytes <= record.SegmentHeaderSize {
		t.Errorf("markers and the tombstone should be dead bytes: %+v", s)
	}
}

func TestBatchDiscard(t *testing.T) {
	t.Parallel()
	db := openEngine(t)
	b := db.Batch().Put([]byte("a"), []byte("1")).Delete([]byte("b"))
	b.Discard()
	if b.Len() != 0 {
		t.Errorf("Len() after Discard = %d", b.Len())
	}
	if err := b.Commit(); err != nil {
		t.Errorf("Commit() of a discarded batch = %v, want nil", err)
	}
	if _, err := db.Get([]byte("a")); !errors.Is(err, ErrNotFound) {
		t.Errorf("discarded write became visible: %v", err)
	}
	if s := db.Stats(); s.Puts != 0 || s.Deletes != 0 || s.TotalBytes != record.SegmentHeaderSize {
		t.Errorf("discarded batch touched the log: %+v", s)
	}
}

func TestBatchValidation(t *testing.T) {
	t.Parallel()
	db := openEngine(t)
	tests := []struct {
		name string
		b    func() *Batch
		want error
	}{
		{"invalid ttl", func() *Batch { return db.Batch().Put([]byte("ok"), nil).PutTTL([]byte("t"), nil, 0) }, ErrInvalidTTL},
		{"negative ttl", func() *Batch { return db.Batch().PutTTL([]byte("t"), nil, -1) }, ErrInvalidTTL},
		{"empty key", func() *Batch { return db.Batch().Put([]byte("ok"), nil).Put(nil, nil) }, ErrEmptyKey},
		{"empty delete key", func() *Batch { return db.Batch().Delete([]byte{}) }, ErrEmptyKey},
	}
	for _, tt := range tests { // sequential on purpose: the Stats check below sums them up
		b := tt.b()
		n := b.Len()
		if err := b.Commit(); !errors.Is(err, tt.want) {
			t.Errorf("%s: Commit() = %v, want %v", tt.name, err, tt.want)
		}
		if b.Len() != n {
			t.Errorf("%s: a failed Commit emptied the batch (%d -> %d)", tt.name, n, b.Len())
		}
	}
	if s := db.Stats(); s.Puts != 0 || s.TotalBytes != record.SegmentHeaderSize {
		t.Errorf("rejected batches reached the log: %+v", s)
	}
	if _, err := db.Get([]byte("ok")); !errors.Is(err, ErrNotFound) {
		t.Errorf("member of a rejected batch is visible: %v", err)
	}
}

func TestBatchTooLarge(t *testing.T) {
	t.Parallel()
	db := openEngine(t)
	db.opts.maxSegmentBytes = 4096
	b := db.Batch()
	for i := range 8 {
		b.Put([]byte("k"+strconv.Itoa(i)), bytes.Repeat([]byte("v"), 600))
	}
	err := b.Commit()
	if !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("Commit() of a 4.8 KiB group into 4 KiB segments = %v, want ErrValueTooLarge", err)
	}
	if b.Len() != 8 {
		t.Errorf("failed Commit emptied the batch")
	}
	if s := db.Stats(); s.Puts != 0 || s.Segments != 1 || s.TotalBytes != record.SegmentHeaderSize {
		t.Errorf("oversized batch touched the log: %+v", s)
	}
}

// TestBatchRotatesToFit is the group-fits-in-segment rule: a group that does not fit in the active
// segment goes whole into a fresh one; it never straddles two files.
func TestBatchRotatesToFit(t *testing.T) {
	t.Parallel()
	db := openEngine(t)
	db.opts.maxSegmentBytes = 4096
	for i := range 5 { // ~2.6 KiB in segment 0
		if err := db.Put([]byte("fill"+strconv.Itoa(i)), bytes.Repeat([]byte("f"), 500)); err != nil {
			t.Fatal(err)
		}
	}
	if s := db.Stats(); s.Segments != 1 {
		t.Fatalf("setup: %d segments", s.Segments)
	}
	b := db.Batch()
	for i := range 4 { // ~2.1 KiB: does not fit after the fill
		b.Put([]byte("b"+strconv.Itoa(i)), bytes.Repeat([]byte("b"), 500))
	}
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	s := db.Stats()
	if s.Segments != 2 || s.ActiveSegment != 1 {
		t.Fatalf("Stats segments %d active %d, want 2/1", s.Segments, s.ActiveSegment)
	}
	for i := range 4 {
		e, ok := db.kd.Get([]byte("b" + strconv.Itoa(i)))
		if !ok || e.Seg != 1 {
			t.Errorf("b%d in segment %d, want 1 (the whole group moved)", i, e.Seg)
		}
	}
	for i := range 5 {
		if e, _ := db.kd.Get([]byte("fill" + strconv.Itoa(i))); e.Seg != 0 {
			t.Errorf("fill%d moved to segment %d", i, e.Seg)
		}
	}
	// Segment 1 holds exactly the group: 4 members + marker.
	if recs := scanActive(t, db); len(recs) != 5 || !recs[4].Flags.BatchCommit() {
		t.Errorf("new segment holds %d records", len(recs))
	}
}

// TestBatchAtomicVisibility: readers that Get a then b must never see a's generation ahead of b's,
// since each batch writes both and the keydir applies the group in one step.
func TestBatchAtomicVisibility(t *testing.T) {
	t.Parallel()
	db := openEngine(t)
	db.opts.maxSegmentBytes = 16 << 10
	a, b := []byte("alpha"), []byte("beta")
	if err := db.Batch().Put(a, []byte("0")).Put(b, []byte("0")).Commit(); err != nil {
		t.Fatal(err)
	}
	gen := func(v []byte) int {
		n, err := strconv.Atoi(string(v))
		if err != nil {
			t.Errorf("bad value %q", v)
		}
		return n
	}
	var stop atomic.Bool
	var wg sync.WaitGroup
	var torn atomic.Int64
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				va, err1 := db.Get(a)
				vb, err2 := db.Get(b)
				if err1 != nil || err2 != nil {
					t.Errorf("Get = %v, %v", err1, err2)
					return
				}
				if gen(va) > gen(vb) {
					torn.Add(1)
				}
			}
		}()
	}
	for g := 1; g <= 1500; g++ {
		v := []byte(strconv.Itoa(g))
		if err := db.Batch().Put(a, v).Put(b, v).Commit(); err != nil {
			t.Fatal(err)
		}
	}
	stop.Store(true)
	wg.Wait()
	if n := torn.Load(); n != 0 {
		t.Fatalf("readers observed %d half-visible batches", n)
	}
}

func TestBatchClosedAndReadOnly(t *testing.T) {
	t.Parallel()
	db := openEngine(t)
	b := db.Batch().Put([]byte("a"), nil)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(); !errors.Is(err, ErrClosed) {
		t.Errorf("Commit after Close = %v, want ErrClosed", err)
	}
	ro := &DB{dir: "x", opts: defaultOptions()}
	ro.opts.readOnly = true
	if err := ro.Batch().Put([]byte("a"), nil).Commit(); !errors.Is(err, ErrReadOnly) {
		t.Errorf("Commit on read-only = %v, want ErrReadOnly", err)
	}
}

func BenchmarkPut(b *testing.B) {
	probeLanes()
	db, err := newDBForTest(b.TempDir(), installFakes)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	db.opts.maxSegmentBytes = 256 << 20
	val := bytes.Repeat([]byte("v"), 100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		if err := db.Put([]byte(fmt.Sprintf("key-%08d", i&0xffff)), val); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	probeLanes()
	db, err := newDBForTest(b.TempDir(), installFakes)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	val := bytes.Repeat([]byte("v"), 100)
	const n = 1 << 16
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%08d", i))
		if err := db.Put(keys[i], val); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		if _, err := db.Get(keys[i&(n-1)]); err != nil {
			b.Fatal(err)
		}
	}
}
