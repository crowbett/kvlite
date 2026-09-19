// Package bench holds the kvlite-only benchmark workload: Go benchmarks in *_test.go (perf-lab style,
// driven by scripts/bench.ps1 and compared with benchstat) plus Run, the same workload as a function so
// `kvlite bench` can execute it without test binaries. It is dependency-free; the bbolt/pebble
// comparison lives in the nested module bench/ (owner decision).
package bench

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"time"

	"github.com/crowbett/kvlite"
)

// Config describes one workload run.
type Config struct {
	// Dir is the store directory; empty means a temp dir removed afterwards.
	Dir string
	// N is the number of keys written, then read.
	N int
	// ValueSize is the value length in bytes.
	ValueSize int
	// Readers is the number of concurrent goroutines in the read phase.
	Readers int
	// Sync opens the store with SyncAlways.
	Sync bool
	// Random uses random instead of sequential keys.
	Random bool
}

// Phase is the measured result of one phase (put, get, delete, scan).
type Phase struct {
	// Name is "put", "get", "get-miss", "scan" or "delete".
	Name string
	// Ops is the number of operations, Elapsed the wall time, OpsPerSec the throughput.
	Ops       int
	Elapsed   time.Duration
	OpsPerSec float64
	// P50 and P99 are latency percentiles from a sorted sample (no HDR dependency).
	P50, P99 time.Duration
	// BytesPerOp is the allocation rate from runtime.MemStats deltas.
	BytesPerOp float64
}

// Result is the outcome of Run.
type Result struct {
	Config Config
	Phases []Phase
	// DiskBefore and DiskAfter are the on-disk size before and after a final Compact (disk amplification).
	DiskBefore, DiskAfter int64
	// SyncPolicy names the policy in effect, printed in every table because it dominates the numbers.
	SyncPolicy string
}

// maxSamples bounds the latency sample per phase; above it every k-th op is sampled.
const maxSamples = 1 << 18

// Run executes the workload described by cfg against a real kvlite store and returns the measurements.
// It honours ctx between phases.
//
// Phases, in order: put every key, get every key (hits, Readers goroutines), get N absent keys (misses),
// one full snapshot scan reading every value, delete every other key. DiskBefore is the directory size
// after the delete phase, DiskAfter the size after Compact.
func Run(ctx context.Context, cfg Config) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if cfg.N <= 0 {
		return Result{}, errors.New("bench: N must be positive")
	}
	if cfg.ValueSize < 0 {
		return Result{}, errors.New("bench: ValueSize must not be negative")
	}
	if cfg.Readers <= 0 {
		cfg.Readers = 1
	}

	dir := cfg.Dir
	if dir == "" {
		tmp, err := os.MkdirTemp("", "kvlite-bench-*")
		if err != nil {
			return Result{}, fmt.Errorf("bench: temp dir: %w", err)
		}
		defer os.RemoveAll(tmp)
		dir = tmp
	}

	policy := kvlite.SyncNever
	if cfg.Sync {
		policy = kvlite.SyncAlways
	}
	db, err := kvlite.Open(dir, kvlite.WithSync(policy))
	if err != nil {
		return Result{}, fmt.Errorf("bench: open: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = db.Close()
		}
	}()

	res := Result{Config: cfg, SyncPolicy: policy.String()}
	keys := Keys(cfg.N, cfg.Random)
	value := Value(cfg.ValueSize)

	put, err := measure(ctx, "put", cfg.N, 1, func(i int) error { return db.Put(keys[i], value) })
	if err != nil {
		return res, err
	}
	res.Phases = append(res.Phases, put)

	get, err := measure(ctx, "get", cfg.N, cfg.Readers, func(i int) error {
		_, err := db.Get(keys[i])
		return err
	})
	if err != nil {
		return res, err
	}
	res.Phases = append(res.Phases, get)

	missKeys := missingKeys(cfg.N)
	miss, err := measure(ctx, "get-miss", cfg.N, cfg.Readers, func(i int) error {
		_, err := db.Get(missKeys[i])
		if errors.Is(err, kvlite.ErrNotFound) {
			return nil
		}
		if err == nil {
			return errors.New("bench: get-miss returned a value")
		}
		return err
	})
	if err != nil {
		return res, err
	}
	res.Phases = append(res.Phases, miss)

	scan, err := measureScan(ctx, db, cfg.N)
	if err != nil {
		return res, err
	}
	res.Phases = append(res.Phases, scan)

	del, err := measure(ctx, "delete", cfg.N/2, 1, func(i int) error { return db.Delete(keys[2*i]) })
	if err != nil {
		return res, err
	}
	res.Phases = append(res.Phases, del)

	if res.DiskBefore, err = DirSize(dir); err != nil {
		return res, err
	}
	if err := db.Compact(ctx); err != nil {
		return res, fmt.Errorf("bench: compact: %w", err)
	}
	closed = true
	if err := db.Close(); err != nil {
		return res, fmt.Errorf("bench: close: %w", err)
	}
	if res.DiskAfter, err = DirSize(dir); err != nil {
		return res, err
	}
	return res, nil
}

// measure runs ops operations spread over workers goroutines and returns the phase measurement.
// op(i) is called exactly once for every i in [0, ops); with workers > 1 the indexes are split into
// contiguous ranges. Latencies are sampled every stride ops so the sample never exceeds maxSamples.
func measure(ctx context.Context, name string, ops, workers int, op func(i int) error) (Phase, error) {
	if err := ctx.Err(); err != nil {
		return Phase{}, err
	}
	if ops == 0 {
		return Phase{Name: name}, nil
	}
	if workers > ops {
		workers = ops
	}
	stride := max(1, ops/maxSamples)
	samples := make([][]time.Duration, workers)
	errs := make([]error, workers)

	var before runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	start := time.Now()

	var wg sync.WaitGroup
	for w := range workers {
		lo, hi := w*ops/workers, (w+1)*ops/workers
		wg.Add(1)
		go func() {
			defer wg.Done()
			lat := make([]time.Duration, 0, (hi-lo)/stride+1)
			for i := lo; i < hi; i++ {
				if i%stride == 0 {
					t0 := HiResNow()
					if err := op(i); err != nil {
						errs[w] = err
						break
					}
					lat = append(lat, time.Duration(HiResNow()-t0))
					continue
				}
				if err := op(i); err != nil {
					errs[w] = err
					break
				}
			}
			samples[w] = lat
		}()
	}
	wg.Wait()

	elapsed := time.Since(start)
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	for _, err := range errs {
		if err != nil {
			return Phase{}, fmt.Errorf("bench: %s: %w", name, err)
		}
	}
	return newPhase(name, ops, elapsed, slices.Concat(samples...), after.TotalAlloc-before.TotalAlloc), nil
}

// measureScan opens one snapshot and reads every key and value in order.
func measureScan(ctx context.Context, db *kvlite.DB, n int) (Phase, error) {
	if err := ctx.Err(); err != nil {
		return Phase{}, err
	}
	stride := max(1, n/maxSamples)
	lat := make([]time.Duration, 0, n/stride+1)

	var before runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	start := time.Now()

	snap, err := db.Snapshot()
	if err != nil {
		return Phase{}, fmt.Errorf("bench: scan: snapshot: %w", err)
	}
	ops := 0
	it := snap.Iter(nil)
	for {
		t0 := HiResNow()
		if !it.Next() {
			break
		}
		if _, err := it.Value(); err != nil {
			_ = snap.Close()
			return Phase{}, fmt.Errorf("bench: scan: value: %w", err)
		}
		if ops%stride == 0 {
			lat = append(lat, time.Duration(HiResNow()-t0))
		}
		ops++
	}
	if err := it.Err(); err != nil {
		_ = snap.Close()
		return Phase{}, fmt.Errorf("bench: scan: %w", err)
	}
	if err := snap.Close(); err != nil {
		return Phase{}, fmt.Errorf("bench: scan: close: %w", err)
	}

	elapsed := time.Since(start)
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	if ops != n {
		return Phase{}, fmt.Errorf("bench: scan visited %d keys, want %d", ops, n)
	}
	return newPhase("scan", ops, elapsed, lat, after.TotalAlloc-before.TotalAlloc), nil
}

func newPhase(name string, ops int, elapsed time.Duration, lat []time.Duration, allocated uint64) Phase {
	p := Phase{Name: name, Ops: ops, Elapsed: elapsed}
	if elapsed > 0 {
		p.OpsPerSec = float64(ops) / elapsed.Seconds()
	}
	if ops > 0 {
		p.BytesPerOp = float64(allocated) / float64(ops)
	}
	p.P50, p.P99 = Percentiles(lat)
	return p
}

// Percentiles sorts lat in place and returns its p50 and p99 (nearest-rank). Empty input yields zeros.
func Percentiles(lat []time.Duration) (p50, p99 time.Duration) {
	if len(lat) == 0 {
		return 0, 0
	}
	slices.Sort(lat)
	rank := func(p float64) time.Duration {
		i := int(p*float64(len(lat))+0.5) - 1
		return lat[max(0, min(i, len(lat)-1))]
	}
	return rank(0.50), rank(0.99)
}

// Keys returns n distinct 16-byte keys. Sequential keys are zero-padded decimal ("key-000000000042"),
// which sorts in insertion order; random keys are the same set in a seeded (reproducible) shuffle.
func Keys(n int, random bool) [][]byte {
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = SeqKey(i)
	}
	if random {
		rng := rand.New(rand.NewPCG(0x6b766c697465, uint64(n))) //nolint:gosec // deterministic workload
		rng.Shuffle(n, func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	}
	return keys
}

// SeqKey returns the i-th sequential key ("key-" + 12 decimal digits, 16 bytes).
func SeqKey(i int) []byte {
	return fmt.Appendf(nil, "key-%012d", i)
}

// RandKey returns a 16-byte key whose order is unrelated to i (splitmix64 of i, hex encoded), so a
// benchmark can generate its i-th random key without materialising the whole set.
func RandKey(i int) []byte {
	z := uint64(i) + 0x9e3779b97f4a7c15 //nolint:gosec // i is a loop index, not a size
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	z ^= z >> 31
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], z)
	return fmt.Appendf(nil, "%016x", b)
}

// missingKeys returns n keys guaranteed absent from Keys(n, _): a different prefix.
func missingKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "nokey%011d", i)
	}
	return keys
}

// Value returns a deterministic, incompressible value of size bytes (seeded PCG stream). A repeating
// pattern would let a compressing engine (pebble uses snappy) report a fraction of the real bytes.
func Value(size int) []byte {
	v := make([]byte, size)
	rng := rand.New(rand.NewPCG(0x76616c7565, uint64(size))) //nolint:gosec // deterministic workload
	var word [8]byte
	for i := 0; i < size; i += 8 {
		binary.LittleEndian.PutUint64(word[:], rng.Uint64())
		copy(v[i:], word[:]) // copy clips the final partial word
	}
	return v
}

// DirSize sums the sizes of every regular file below dir. Sizes are read through an open handle
// (File.Stat), because on NTFS the directory entry of a file another handle is still writing reports
// a stale size (typically 0) until that handle closes, which would hide the whole active segment. A
// file that cannot be opened (the LOCK file is held with share mode 0) falls back to the entry size.
func DirSize(dir string) (int64, error) {
	var total int64
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		if f, err := os.Open(path); err == nil { //nolint:gosec // path comes from WalkDir under dir
			info, err := f.Stat()
			_ = f.Close()
			if err != nil {
				return err
			}
			total += info.Size()
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("bench: dir size: %w", err)
	}
	return total, nil
}
