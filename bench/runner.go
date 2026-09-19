package bench

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"text/tabwriter"
	"time"

	ibench "github.com/crowbett/kvlite/internal/bench"
)

// Workload is the identical job every engine runs: put N sequential keys, get every key back, one full
// scan reading every value, then delete every other key. Dir size is sampled before and after Compact
// (where the engine supports it) so the table shows disk amplification, not just write throughput.
// Keys, values, latency sampling and the high-resolution clock are shared with internal/bench so the
// kvlite-only numbers and the cross-engine numbers come from the same code.
type Workload struct {
	N         int
	ValueSize int
}

// Measurement is one (engine, op, value size) row.
type Measurement struct {
	Engine     string
	Op         string
	ValueSize  int
	Ops        int
	Elapsed    time.Duration
	OpsPerSec  float64
	P50, P99   time.Duration
	BytesPerOp float64
}

// EngineResult is one engine's run of one Workload.
type EngineResult struct {
	Engine    string
	ValueSize int
	Rows      []Measurement
	// DiskBefore is the directory size after the delete phase, DiskAfter the size after Compact (if
	// supported) and Close.
	DiskBefore, DiskAfter int64
	Compacted             bool
}

const maxSamples = 1 << 18

// RunEngine opens engine in dir, runs w and returns the measurements. dir must be empty.
func RunEngine(ctx context.Context, engine, dir string, w Workload) (EngineResult, error) {
	res := EngineResult{Engine: engine, ValueSize: w.ValueSize}
	if w.N <= 0 {
		return res, errors.New("kvbench: N must be positive")
	}
	st, err := Open(engine, dir)
	if err != nil {
		return res, err
	}
	closed := false
	defer func() {
		if !closed {
			_ = st.Close()
		}
	}()

	keys := ibench.Keys(w.N, false)
	value := ibench.Value(w.ValueSize)

	row := func(op string, ops int, fn func(i int) error) error {
		m, err := measure(ctx, ops, fn)
		if err != nil {
			return fmt.Errorf("%s %s: %w", engine, op, err)
		}
		m.Engine, m.Op, m.ValueSize = engine, op, w.ValueSize
		res.Rows = append(res.Rows, m)
		return nil
	}
	if err := row("put", w.N, func(i int) error { return st.Put(keys[i], value) }); err != nil {
		return res, err
	}
	if err := row("get", w.N, func(i int) error {
		v, err := st.Get(keys[i])
		if err == nil && len(v) != w.ValueSize {
			return fmt.Errorf("got %d bytes, want %d", len(v), w.ValueSize)
		}
		return err
	}); err != nil {
		return res, err
	}
	if err := scanRow(ctx, &res, st, w); err != nil {
		return res, err
	}
	if err := row("delete", w.N/2, func(i int) error { return st.Delete(keys[2*i]) }); err != nil {
		return res, err
	}

	if res.DiskBefore, err = ibench.DirSize(dir); err != nil {
		return res, err
	}
	if c, ok := st.(Compactor); ok {
		if err := c.Compact(ctx); err != nil {
			return res, fmt.Errorf("%s compact: %w", engine, err)
		}
		res.Compacted = true
	}
	closed = true
	if err := st.Close(); err != nil {
		return res, fmt.Errorf("%s close: %w", engine, err)
	}
	if res.DiskAfter, err = ibench.DirSize(dir); err != nil {
		return res, err
	}
	return res, nil
}

func scanRow(ctx context.Context, res *EngineResult, st Store, w Workload) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stride := max(1, w.N/maxSamples)
	lat := make([]time.Duration, 0, w.N/stride+1)
	var before runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	start := time.Now()
	seen := 0
	last := ibench.HiResNow()
	err := st.Scan(nil, func(_, v []byte) bool {
		now := ibench.HiResNow()
		if seen%stride == 0 {
			lat = append(lat, time.Duration(now-last))
		}
		last = now
		seen++
		return len(v) == w.ValueSize
	})
	elapsed := time.Since(start)
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	if err != nil {
		return fmt.Errorf("%s scan: %w", res.Engine, err)
	}
	if seen != w.N {
		return fmt.Errorf("%s scan visited %d keys, want %d", res.Engine, seen, w.N)
	}
	m := newMeasurement(seen, elapsed, lat, after.TotalAlloc-before.TotalAlloc)
	m.Engine, m.Op, m.ValueSize = res.Engine, "scan", w.ValueSize
	res.Rows = append(res.Rows, m)
	return nil
}

func measure(ctx context.Context, ops int, fn func(i int) error) (Measurement, error) {
	if err := ctx.Err(); err != nil {
		return Measurement{}, err
	}
	stride := max(1, ops/maxSamples)
	lat := make([]time.Duration, 0, ops/stride+1)
	var before runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	start := time.Now()
	for i := range ops {
		if i%stride == 0 {
			t0 := ibench.HiResNow()
			if err := fn(i); err != nil {
				return Measurement{}, err
			}
			lat = append(lat, time.Duration(ibench.HiResNow()-t0))
			continue
		}
		if err := fn(i); err != nil {
			return Measurement{}, err
		}
	}
	elapsed := time.Since(start)
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	return newMeasurement(ops, elapsed, lat, after.TotalAlloc-before.TotalAlloc), nil
}

func newMeasurement(ops int, elapsed time.Duration, lat []time.Duration, allocated uint64) Measurement {
	m := Measurement{Ops: ops, Elapsed: elapsed}
	if elapsed > 0 {
		m.OpsPerSec = float64(ops) / elapsed.Seconds()
	}
	if ops > 0 {
		m.BytesPerOp = float64(allocated) / float64(ops)
	}
	m.P50, m.P99 = ibench.Percentiles(lat)
	return m
}

// WriteTable renders results as an aligned text table: one row per engine x op x value size, then one
// line per engine with the disk size before/after compaction.
func WriteTable(w io.Writer, results []EngineResult) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "engine\top\tvalue\tops\tops/s\tp50\tp99\tB/op")
	for _, r := range results {
		for _, m := range r.Rows {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%.0f\t%s\t%s\t%.0f\n",
				m.Engine, m.Op, humanBytes(int64(m.ValueSize)), m.Ops, m.OpsPerSec, fmtDur(m.P50), fmtDur(m.P99), m.BytesPerOp)
		}
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	fmt.Fprintln(w)
	tw = tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "engine\tvalue\tdisk before\tdisk after\tcompacted\tamplification")
	for _, r := range results {
		amp := "n/a"
		if r.DiskAfter > 0 {
			amp = fmt.Sprintf("%.2fx", float64(r.DiskBefore)/float64(r.DiskAfter))
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%v\t%s\n",
			r.Engine, humanBytes(int64(r.ValueSize)), humanBytes(r.DiskBefore), humanBytes(r.DiskAfter), r.Compacted, amp)
	}
	return tw.Flush()
}

func humanBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2fGiB", float64(n)/float64(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.2fMiB", float64(n)/float64(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1fKiB", float64(n)/float64(1<<10))
	default:
		return fmt.Sprintf("%dB", n)
	}
}

func fmtDur(d time.Duration) string {
	switch {
	case d < time.Millisecond:
		return fmt.Sprintf("%.1fµs", float64(d)/float64(time.Microsecond))
	case d < time.Second:
		return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond))
	default:
		return d.Round(time.Millisecond).String()
	}
}

// TempDir creates a scratch directory for one engine run under base (or the OS temp dir when empty).
func TempDir(base, engine string) (string, error) {
	if base != "" {
		if err := os.MkdirAll(base, 0o750); err != nil {
			return "", err
		}
	}
	return os.MkdirTemp(base, "kvbench-"+engine+"-*")
}
