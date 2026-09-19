package bench

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/crowbett/kvlite"
)

// sizes is the value-size matrix from docs/PLAN.md "Benchmarks".
var sizes = []struct {
	name string
	n    int
}{
	{"100B", 100},
	{"1KiB", 1 << 10},
	{"16KiB", 16 << 10},
}

// openKeys is the key count preloaded for Get/Scan/Open benchmarks. The ADR asks for 1M keys on Open;
// 200k keeps a count=10 baseline run inside the .bench/README budget and scales linearly.
const openKeys = 200_000

func TestRunValidates(t *testing.T) {
	t.Parallel()
	if _, err := Run(context.Background(), Config{}); err == nil {
		t.Error("Run(N=0) = nil, want a validation error")
	}
	if _, err := Run(context.Background(), Config{N: 1, ValueSize: -1}); err == nil {
		t.Error("Run(ValueSize=-1) = nil, want a validation error")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Run(ctx, Config{N: 1}); !errors.Is(err, context.Canceled) {
		t.Errorf("Run(cancelled) = %v, want context.Canceled", err)
	}
}

func TestRunEndToEnd(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cfg  Config
	}{
		{"seq", Config{N: 500, ValueSize: 64, Readers: 1}},
		{"random readers", Config{N: 500, ValueSize: 64, Readers: 4, Random: true}},
		{"sync always", Config{N: 20, ValueSize: 16, Readers: 1, Sync: true}},
		{"empty value", Config{N: 50, ValueSize: 0, Readers: 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.cfg.Dir = t.TempDir()
			res, err := Run(context.Background(), tt.cfg)
			if err != nil {
				t.Fatalf("Run() = %v", err)
			}
			want := []string{"put", "get", "get-miss", "scan", "delete"}
			if len(res.Phases) != len(want) {
				t.Fatalf("phases = %d, want %d", len(res.Phases), len(want))
			}
			for i, p := range res.Phases {
				if p.Name != want[i] {
					t.Errorf("phase %d = %q, want %q", i, p.Name, want[i])
				}
				wantOps := tt.cfg.N
				if p.Name == "delete" {
					wantOps = tt.cfg.N / 2
				}
				if p.Ops != wantOps {
					t.Errorf("%s ops = %d, want %d", p.Name, p.Ops, wantOps)
				}
				// A few hundred page-cache reads can finish under the Windows clock resolution, so only
				// require a positive rate when a positive duration was measured.
				if p.Elapsed > 0 && p.OpsPerSec <= 0 {
					t.Errorf("%s ops/s = %v elapsed = %v, want > 0", p.Name, p.OpsPerSec, p.Elapsed)
				}
				if p.P99 < p.P50 {
					t.Errorf("%s p99 %v < p50 %v", p.Name, p.P99, p.P50)
				}
			}
			if res.DiskBefore <= 0 || res.DiskAfter <= 0 {
				t.Errorf("disk before/after = %d/%d, want > 0", res.DiskBefore, res.DiskAfter)
			}
			if res.DiskAfter > res.DiskBefore {
				t.Errorf("compaction grew the store: %d -> %d", res.DiskBefore, res.DiskAfter)
			}
			wantPolicy := "never"
			if tt.cfg.Sync {
				wantPolicy = "always"
			}
			if res.SyncPolicy != wantPolicy {
				t.Errorf("SyncPolicy = %q, want %q", res.SyncPolicy, wantPolicy)
			}
		})
	}
}

func TestRunTempDirRemoved(t *testing.T) {
	t.Parallel()
	res, err := Run(context.Background(), Config{N: 10, ValueSize: 8})
	if err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if res.Config.Dir != "" {
		t.Errorf("Config.Dir = %q, want empty (temp dir is private)", res.Config.Dir)
	}
}

func TestPercentiles(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		in       []time.Duration
		p50, p99 time.Duration
	}{
		{"empty", nil, 0, 0},
		{"one", []time.Duration{7}, 7, 7},
		{"two", []time.Duration{9, 1}, 1, 9},
		{"hundred", seq(100), 50, 99},
		{"unsorted", []time.Duration{5, 3, 1, 4, 2}, 3, 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p50, p99 := Percentiles(tt.in)
			if p50 != tt.p50 || p99 != tt.p99 {
				t.Errorf("Percentiles() = %v/%v, want %v/%v", p50, p99, tt.p50, tt.p99)
			}
		})
	}
}

func seq(n int) []time.Duration {
	out := make([]time.Duration, n)
	for i := range out {
		out[i] = time.Duration(n - i)
	}
	return out
}

func TestKeys(t *testing.T) {
	t.Parallel()
	const n = 1000
	seqKeys, rndKeys := Keys(n, false), Keys(n, true)
	if len(seqKeys) != n || len(rndKeys) != n {
		t.Fatalf("len = %d/%d, want %d", len(seqKeys), len(rndKeys), n)
	}
	seen := map[string]bool{}
	for i, k := range seqKeys {
		if len(k) != 16 {
			t.Fatalf("key %d = %q, want 16 bytes", i, k)
		}
		if i > 0 && string(seqKeys[i-1]) >= string(k) {
			t.Fatalf("sequential keys not ascending at %d: %q >= %q", i, seqKeys[i-1], k)
		}
		seen[string(k)] = true
	}
	same := 0
	for i, k := range rndKeys {
		if !seen[string(k)] {
			t.Fatalf("random key %q not in the sequential set", k)
		}
		if bytes.Equal(k, seqKeys[i]) {
			same++
		}
	}
	if same > n/10 {
		t.Errorf("random order matches sequential in %d/%d positions", same, n)
	}
	if !bytes.Equal(Keys(n, true)[0], rndKeys[0]) {
		t.Error("random shuffle is not reproducible")
	}
	rk := map[string]bool{}
	for i := range 10_000 {
		k := string(RandKey(i))
		if len(k) != 16 || rk[k] {
			t.Fatalf("RandKey(%d) = %q duplicate or wrong length", i, k)
		}
		rk[k] = true
	}
}

// TestHiResNow pins the property the latency percentiles depend on: the clock is monotonic and
// resolves well below a millisecond (time.Now's monotonic source on Windows steps at ~0.4-15 ms).
func TestHiResNow(t *testing.T) {
	t.Parallel()
	prev := HiResNow()
	minStep := int64(1 << 62)
	for range 200_000 {
		now := HiResNow()
		if now < prev {
			t.Fatalf("HiResNow went backwards: %d -> %d", prev, now)
		}
		if d := now - prev; d > 0 && d < minStep {
			minStep = d
		}
		prev = now
	}
	if minStep >= int64(100*time.Microsecond) {
		t.Errorf("smallest step = %v, want < 100µs", time.Duration(minStep))
	}
	start := HiResNow()
	time.Sleep(20 * time.Millisecond)
	if got := time.Duration(HiResNow() - start); got < 15*time.Millisecond || got > 500*time.Millisecond {
		t.Errorf("20ms sleep measured as %v", got)
	}
}

func TestDirSize(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a"), make([]byte, 100), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "sub"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "sub", "b"), make([]byte, 23), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := DirSize(dir)
	if err != nil || got != 123 {
		t.Errorf("DirSize() = %d, %v; want 123", got, err)
	}
	if _, err := DirSize(filepath.Join(dir, "missing")); err == nil {
		t.Error("DirSize(missing) = nil error")
	}
}

// --- benchmarks (perf-lab style: go test -run '^$' -bench . -benchmem -count N ./internal/bench/...) ---

// openBench opens a fresh store in a temp dir that the benchmark removes on cleanup.
func openBench(b *testing.B, opts ...kvlite.Option) *kvlite.DB {
	b.Helper()
	db, err := kvlite.Open(b.TempDir(), opts...)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })
	return db
}

// keysFor caps the preloaded key count so a value-size row never exceeds ~128 MiB on disk
// (100k keys at 100 B / 1 KiB, 8k keys at 16 KiB).
func keysFor(n, size int) int {
	return max(1000, min(n, (128<<20)/max(1, size)))
}

// preload writes keysFor(n, size) keys of size bytes and returns them.
func preload(b *testing.B, db *kvlite.DB, n, size int) [][]byte {
	b.Helper()
	n = keysFor(n, size)
	keys, value := Keys(n, false), Value(size)
	for _, k := range keys {
		if err := db.Put(k, value); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
	return keys
}

// BenchmarkPut appends b.N distinct keys: seq keys ascend, rand keys are hashed. Every iteration is a
// fresh key so the measured path is the pure append, never a superseded record.
func BenchmarkPut(b *testing.B) {
	for _, order := range []struct {
		name string
		key  func(i int) []byte
	}{{"seq", SeqKey}, {"rand", RandKey}} {
		for _, sz := range sizes {
			b.Run(order.name+"/"+sz.name, func(b *testing.B) {
				db := openBench(b)
				value := Value(sz.n)
				b.SetBytes(int64(sz.n))
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					if err := db.Put(order.key(i), value); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkPutSyncAlways is the fsync-per-write ceiling; it is the number the ADR warns dominates.
func BenchmarkPutSyncAlways(b *testing.B) {
	db := openBench(b, kvlite.WithSync(kvlite.SyncAlways))
	value := Value(1 << 10)
	b.SetBytes(1 << 10)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if err := db.Put(SeqKey(i), value); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGet reads preloaded keys: sequential and random access order per value size, then random
// 1 KiB reads at parallelism 1/8/16 (RunParallel scales GOMAXPROCS, so -cpu is the knob).
func BenchmarkGet(b *testing.B) {
	const n = 100_000
	for _, sz := range sizes {
		b.Run("seq/"+sz.name, func(b *testing.B) {
			db := openBench(b)
			keys := preload(b, db, n, sz.n)
			nk := len(keys)
			b.SetBytes(int64(sz.n))
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				if _, err := db.Get(keys[i%nk]); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("rand/"+sz.name, func(b *testing.B) {
			db := openBench(b)
			keys := preload(b, db, n, sz.n)
			nk := len(keys)
			order := Keys(nk, true)
			b.SetBytes(int64(sz.n))
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				if _, err := db.Get(order[i%nk]); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	b.Run("miss", func(b *testing.B) {
		db := openBench(b)
		keys := preload(b, db, n, 100)
		nk := len(keys)
		miss := missingKeys(nk)
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			if _, err := db.Get(miss[i%nk]); !errors.Is(err, kvlite.ErrNotFound) {
				b.Fatalf("Get(miss) = %v, want ErrNotFound", err)
			}
		}
	})
	for _, par := range []int{1, 8, 16} {
		b.Run(fmt.Sprintf("par%d/1KiB", par), func(b *testing.B) {
			db := openBench(b)
			keys := preload(b, db, n, 1<<10)
			nk := len(keys)
			order := Keys(nk, true)
			b.SetBytes(1 << 10)
			b.ReportAllocs()
			b.ResetTimer()
			// Exactly par goroutines share b.N reads (RunParallel would scale with GOMAXPROCS).
			var wg sync.WaitGroup
			errs := make([]error, par)
			for w := range par {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := w; i < b.N; i += par {
						if _, err := db.Get(order[i%nk]); err != nil {
							errs[w] = err
							return
						}
					}
				}()
			}
			wg.Wait()
			for _, err := range errs {
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkDelete appends tombstones for preloaded keys (cycling once the set is exhausted).
func BenchmarkDelete(b *testing.B) {
	const n = 100_000
	db := openBench(b)
	keys := preload(b, db, n, 100)
	nk := len(keys)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if err := db.Delete(keys[i%nk]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkScan iterates a full snapshot (100k keys, 8k at 16 KiB) per iteration, reading every
// value; the reported ns/op is per full scan and B/s counts value bytes.
func BenchmarkScan(b *testing.B) {
	const n = 100_000
	for _, sz := range sizes {
		b.Run(sz.name, func(b *testing.B) {
			db := openBench(b)
			nk := len(preload(b, db, n, sz.n))
			b.SetBytes(int64(nk) * int64(sz.n))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				snap, err := db.Snapshot()
				if err != nil {
					b.Fatal(err)
				}
				it := snap.Iter(nil)
				seen := 0
				for it.Next() {
					if _, err := it.Value(); err != nil {
						b.Fatal(err)
					}
					seen++
				}
				if err := it.Err(); err != nil {
					b.Fatal(err)
				}
				_ = snap.Close()
				if seen != nk {
					b.Fatalf("scan saw %d keys, want %d", seen, nk)
				}
			}
		})
	}
	b.Run("prefix/1KiB", func(b *testing.B) {
		db := openBench(b)
		preload(b, db, n, 1<<10)
		prefix := []byte("key-00000000001") // 10 keys
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			snap, err := db.Snapshot()
			if err != nil {
				b.Fatal(err)
			}
			it := snap.Iter(prefix)
			seen := 0
			for it.Next() {
				seen++
			}
			_ = snap.Close()
			if seen != 10 {
				b.Fatalf("prefix scan saw %d keys, want 10", seen)
			}
		}
	})
}

// BenchmarkOpen measures recovery of openKeys keys: "hints" opens a compacted store (hint-first,
// O(keys)); "nohints" opens the same store with its .hint files removed (log scan, O(bytes)).
func BenchmarkOpen(b *testing.B) {
	for _, mode := range []string{"hints", "nohints"} {
		b.Run(mode, func(b *testing.B) {
			dir := b.TempDir()
			db, err := kvlite.Open(dir)
			if err != nil {
				b.Fatal(err)
			}
			preload(b, db, openKeys, 100)
			if err := db.Compact(context.Background()); err != nil {
				b.Fatal(err)
			}
			if err := db.Close(); err != nil {
				b.Fatal(err)
			}
			if mode == "nohints" {
				hints, _ := filepath.Glob(filepath.Join(dir, "*.hint"))
				for _, h := range hints {
					if err := os.Remove(h); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				db, err := kvlite.Open(dir)
				if err != nil {
					b.Fatal(err)
				}
				if got := db.Stats().Keys; got != openKeys {
					b.Fatalf("Stats().Keys = %d, want %d", got, openKeys)
				}
				if err := db.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkBatch commits groups of 100 x 100 B puts.
func BenchmarkBatch(b *testing.B) {
	db := openBench(b)
	value := Value(100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		batch := db.Batch()
		for j := range 100 {
			batch.Put(SeqKey(i*100+j), value)
		}
		if err := batch.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}
