package keydir

import (
	"bytes"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPutGetDelete(t *testing.T) {
	t.Parallel()
	k := New(4, 0)
	if _, ok := k.Get([]byte("a")); ok {
		t.Fatal("Get on empty = found")
	}
	e1 := Entry{Seg: 1, Off: 16, Size: 30}
	if old, existed := k.Put([]byte("a"), e1); existed || old != (Entry{}) {
		t.Fatalf("first Put = (%+v, %v), want (zero, false)", old, existed)
	}
	if got, ok := k.Get([]byte("a")); !ok || got != e1 {
		t.Fatalf("Get = (%+v, %v), want (%+v, true)", got, ok, e1)
	}
	e2 := Entry{Seg: 1, Off: 46, Size: 31, ExpSec: 7}
	if old, existed := k.Put([]byte("a"), e2); !existed || old != e1 {
		t.Fatalf("second Put = (%+v, %v), want (%+v, true)", old, existed, e1)
	}
	if k.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", k.Len())
	}
	if old, existed := k.Delete([]byte("a")); !existed || old != e2 {
		t.Fatalf("Delete = (%+v, %v), want (%+v, true)", old, existed, e2)
	}
	if old, existed := k.Delete([]byte("a")); existed || old != (Entry{}) {
		t.Fatalf("second Delete = (%+v, %v), want (zero, false)", old, existed)
	}
	if _, ok := k.Get([]byte("a")); ok || k.Len() != 0 {
		t.Fatal("key still present after Delete")
	}
	// Keys are copied: mutating the caller's slice must not affect the map.
	key := []byte("mutable")
	k.Put(key, e1)
	key[0] = 'X'
	if _, ok := k.Get([]byte("mutable")); !ok {
		t.Error("map key aliases the caller's slice")
	}
	if _, ok := k.Get(key); ok {
		t.Error("mutated slice still matches")
	}
}

func TestCompareAndSwap(t *testing.T) {
	t.Parallel()
	from := Entry{Seg: 1, Off: 16, Size: 30}
	to := Entry{Seg: 9, Off: 16, Size: 30}
	newer := Entry{Seg: 2, Off: 100, Size: 30}
	tests := []struct {
		name    string
		setup   func(k *Keydir)
		want    bool
		wantEnt Entry
		wantOK  bool
	}{
		{"matches", func(k *Keydir) { k.Put([]byte("k"), from) }, true, to, true},
		{"rewritten mid-merge", func(k *Keydir) { k.Put([]byte("k"), newer) }, false, newer, true},
		{"deleted mid-merge", func(k *Keydir) {}, false, Entry{}, false},
		{"same seg different offset", func(k *Keydir) { k.Put([]byte("k"), Entry{Seg: 1, Off: 17, Size: 30}) }, false, Entry{Seg: 1, Off: 17, Size: 30}, true},
		{"expiry differs", func(k *Keydir) { k.Put([]byte("k"), Entry{Seg: 1, Off: 16, Size: 30, ExpSec: 5}) }, false, Entry{Seg: 1, Off: 16, Size: 30, ExpSec: 5}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			k := New(2, 0)
			tt.setup(k)
			if got := k.CompareAndSwap([]byte("k"), from, to); got != tt.want {
				t.Fatalf("CompareAndSwap() = %v, want %v", got, tt.want)
			}
			ent, ok := k.Get([]byte("k"))
			if ok != tt.wantOK || ent != tt.wantEnt {
				t.Errorf("after CAS Get = (%+v, %v), want (%+v, %v)", ent, ok, tt.wantEnt, tt.wantOK)
			}
		})
	}
}

func TestApply(t *testing.T) {
	t.Parallel()
	k := New(8, 0)
	k.Put([]byte("a"), Entry{Seg: 1, Off: 1, Size: 1})
	k.Put([]byte("gone"), Entry{Seg: 1, Off: 2, Size: 2})
	ups := []Update{
		{Key: []byte("a"), Entry: Entry{Seg: 2, Off: 10, Size: 5}},
		{Key: []byte("b"), Entry: Entry{Seg: 2, Off: 15, Size: 5}},
		{Key: []byte("gone"), Delete: true},
		{Key: []byte("missing"), Delete: true},
		{Key: []byte("a"), Entry: Entry{Seg: 2, Off: 20, Size: 6}}, // repeated key sees the earlier update
	}
	k.Apply(ups)
	wantOld := []struct {
		old     Entry
		existed bool
	}{
		{Entry{Seg: 1, Off: 1, Size: 1}, true},
		{Entry{}, false},
		{Entry{Seg: 1, Off: 2, Size: 2}, true},
		{Entry{}, false},
		{Entry{Seg: 2, Off: 10, Size: 5}, true},
	}
	for i, w := range wantOld {
		if ups[i].Old != w.old || ups[i].Existed != w.existed {
			t.Errorf("ups[%d] old = (%+v, %v), want (%+v, %v)", i, ups[i].Old, ups[i].Existed, w.old, w.existed)
		}
	}
	if e, ok := k.Get([]byte("a")); !ok || e != (Entry{Seg: 2, Off: 20, Size: 6}) {
		t.Errorf("a = (%+v, %v)", e, ok)
	}
	if e, ok := k.Get([]byte("b")); !ok || e != (Entry{Seg: 2, Off: 15, Size: 5}) {
		t.Errorf("b = (%+v, %v)", e, ok)
	}
	if _, ok := k.Get([]byte("gone")); ok {
		t.Error("gone still present")
	}
	if k.Len() != 2 {
		t.Errorf("Len() = %d, want 2", k.Len())
	}
	k.Apply(nil) // no-op
}

// TestApplyAtomic runs Applies that flip a pair of keys between two states while readers check that
// they never see the pair split. With -race this also proves the lock discipline.
func TestApplyAtomic(t *testing.T) {
	t.Parallel()
	k := New(64, 0)
	// Pick two keys in different shards so the multi-shard path is exercised.
	a, b := []byte("alpha"), []byte("beta")
	for i := 0; k.shardFor(a) == k.shardFor(b); i++ {
		b = []byte("beta" + strconv.Itoa(i))
	}
	k.Apply([]Update{{Key: a, Entry: Entry{Seg: 0}}, {Key: b, Entry: Entry{Seg: 0}}})

	var stop atomic.Bool
	var wg sync.WaitGroup
	var splits atomic.Int64
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				ea, _ := k.Get(a)
				eb, _ := k.Get(b)
				// Groups land atomically and generations only rise, so reading a before b can show
				// b at a newer generation (an Apply slipped between the two Gets) but never a ahead
				// of b: that would be a half-applied group.
				if ea.Seg > eb.Seg {
					splits.Add(1)
				}
			}
		}()
	}
	for gen := uint32(1); gen <= 2000; gen++ {
		k.Apply([]Update{{Key: a, Entry: Entry{Seg: gen}}, {Key: b, Entry: Entry{Seg: gen}}})
	}
	stop.Store(true)
	wg.Wait()
	if n := splits.Load(); n != 0 {
		t.Fatalf("readers observed %d half-applied groups", n)
	}
}

func TestSnapshot(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	k := New(16, 0)
	keys := []string{"b", "a", "abc", "ab", "zz", "expired", "ab-expired", "", "c"}
	for i, key := range keys {
		e := Entry{Seg: 1, Off: uint32(i), Size: 1}
		if key == "expired" || key == "ab-expired" {
			e.ExpSec = uint32(now.Unix()) - 1
		}
		k.Put([]byte(key), e)
	}
	tests := []struct {
		name   string
		prefix string
		want   []string
	}{
		{"full", "", []string{"", "a", "ab", "abc", "b", "c", "zz"}},
		{"prefix a", "a", []string{"a", "ab", "abc"}},
		{"prefix ab", "ab", []string{"ab", "abc"}},
		{"prefix none", "q", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := k.Snapshot([]byte(tt.prefix), now)
			if s.Len() != len(tt.want) {
				t.Fatalf("Len() = %d, want %d", s.Len(), len(tt.want))
			}
			for i, w := range tt.want {
				if got := s.Key(i); string(got) != w {
					t.Errorf("Key(%d) = %q, want %q", i, got, w)
				}
				if e, ok := k.Get([]byte(w)); !ok || e != s.Entry(i) {
					t.Errorf("Entry(%d) = %+v, want %+v", i, s.Entry(i), e)
				}
			}
			for i := 1; i < s.Len(); i++ {
				if bytes.Compare(s.Key(i-1), s.Key(i)) >= 0 {
					t.Errorf("not sorted at %d: %q >= %q", i, s.Key(i-1), s.Key(i))
				}
			}
		})
	}
}

func TestSnapshotIsCopy(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	k := New(2, 0)
	k.Put([]byte("a"), Entry{Seg: 1})
	k.Put([]byte("b"), Entry{Seg: 1})
	s := k.Snapshot(nil, now)
	k.Delete([]byte("a"))
	k.Put([]byte("b"), Entry{Seg: 2})
	if s.Len() != 2 || string(s.Key(0)) != "a" || s.Entry(1) != (Entry{Seg: 1}) {
		t.Error("snapshot changed after later mutations")
	}
	if empty := New(1, 0).Snapshot(nil, now); empty.Len() != 0 {
		t.Error("empty keydir snapshot not empty")
	}
}

func TestBytesEstimate(t *testing.T) {
	t.Parallel()
	k := New(8, 0)
	base := k.Bytes()
	if base <= 0 {
		t.Fatalf("Bytes() of empty = %d, want > 0", base)
	}
	k.Put([]byte("hello"), Entry{})
	if got := k.Bytes(); got != base+entryOverhead+5 {
		t.Errorf("Bytes() after one put = %d, want %d", got, base+entryOverhead+5)
	}
	k.Put([]byte("hello"), Entry{Seg: 1}) // overwrite: no key growth
	if got := k.Bytes(); got != base+entryOverhead+5 {
		t.Errorf("Bytes() after overwrite = %d, want %d", got, base+entryOverhead+5)
	}
	k.Delete([]byte("hello"))
	if got := k.Bytes(); got != base {
		t.Errorf("Bytes() after delete = %d, want %d", got, base)
	}
}

// TestBytesAtScale is the PLAN step-4 memory assertion: 1M keys, the estimate must be within a factor
// of two of the measured heap growth, and the per-key cost must stay under the ~110 B/key budget the
// ADR quotes for 24-byte keys.
func TestBytesAtScale(t *testing.T) {
	if testing.Short() {
		t.Skip("1M-key memory measurement skipped in -short")
	}
	const n = 1_000_000
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "user:%010d:profile:%02d", i, i%100) // 24 bytes
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	k := New(64, n)
	for i, key := range keys {
		k.Put(key, Entry{Seg: 1, Off: uint32(i), Size: 40})
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	measured := int64(after.HeapAlloc - before.HeapAlloc)
	est := k.Bytes()
	perKey := measured / n
	t.Logf("measured %d B (%d B/key), estimate %d B (%d B/key)", measured, perKey, est, est/n)
	if est < measured/2 || est > measured*2 {
		t.Errorf("Bytes() = %d, measured %d: off by more than 2x", est, measured)
	}
	if perKey > 130 {
		t.Errorf("%d B/key measured, want <= 130 (ADR budget ~110)", perKey)
	}
	runtime.KeepAlive(keys)
}

// TestConcurrentReadersWriter hammers one writer against many readers under -race. Every value a
// reader sees must be one the writer wrote for that key.
func TestConcurrentReadersWriter(t *testing.T) {
	t.Parallel()
	k := New(16, 0)
	const keys = 256
	var stop atomic.Bool
	var wg sync.WaitGroup
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			var key []byte
			for i := 0; !stop.Load(); i++ {
				key = strconv.AppendInt(key[:0], int64((i*7+r)%keys), 10)
				e, ok := k.Get(key)
				if ok && (e.Seg != 1 || e.Size != 8) {
					t.Errorf("reader saw torn entry %+v", e)
					return
				}
				if i%64 == 0 {
					_ = k.Snapshot(nil, time.Time{})
					_ = k.Bytes()
				}
			}
		}(r)
	}
	var key []byte
	for i := 0; i < 20000; i++ {
		key = strconv.AppendInt(key[:0], int64(i%keys), 10)
		switch i % 3 {
		case 0, 1:
			k.Put(key, Entry{Seg: 1, Off: uint32(i), Size: 8})
		default:
			k.Delete(key)
		}
		if i%10 == 0 {
			k.CompareAndSwap(key, Entry{Seg: 1, Off: uint32(i), Size: 8}, Entry{Seg: 1, Off: uint32(i + 1), Size: 8})
		}
	}
	stop.Store(true)
	wg.Wait()
}

func BenchmarkGet(b *testing.B) {
	k := New(64, 1<<20)
	const n = 1 << 20
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "key-%08d", i)
		k.Put(keys[i], Entry{Seg: 1, Off: uint32(i), Size: 32})
	}
	b.ReportAllocs()
	b.ResetTimer()
	var sink Entry
	for i := 0; b.Loop(); i++ {
		e, _ := k.Get(keys[i&(n-1)])
		sink = e
	}
	_ = sink
}

func BenchmarkGetParallel(b *testing.B) {
	k := New(64, 1<<20)
	const n = 1 << 20
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "key-%08d", i)
		k.Put(keys[i], Entry{Seg: 1, Off: uint32(i), Size: 32})
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k.Get(keys[i&(n-1)])
			i += 7919
		}
	})
}

func BenchmarkPut(b *testing.B) {
	k := New(64, 0)
	const n = 1 << 16
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "key-%08d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		k.Put(keys[i&(n-1)], Entry{Seg: 1, Off: uint32(i), Size: 32})
	}
}

func BenchmarkSnapshot100k(b *testing.B) {
	k := New(64, 100_000)
	for i := range 100_000 {
		k.Put(fmt.Appendf(nil, "key-%08d", i), Entry{Seg: 1, Off: uint32(i), Size: 32})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = k.Snapshot(nil, time.Time{})
	}
}

// BenchmarkGetHot measures the lookup itself with a cache-resident key set; BenchmarkGet at 1M keys
// is dominated by the DRAM miss of a random map slot.
func BenchmarkGetHot(b *testing.B) {
	k := New(64, 1<<12)
	const n = 1 << 12
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "key-%08d", i)
		k.Put(keys[i], Entry{Seg: 1, Off: uint32(i), Size: 32})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		k.Get(keys[i&(n-1)])
	}
}
