// STUB written by the integrator to freeze the contract; workstream D (cli + bench + fuzz) owns this file
// and replaces the bodies, keeping every exported signature.

// Package bench holds the kvlite-only benchmark workload: Go benchmarks in *_test.go (perf-lab style,
// driven by scripts/bench.ps1 and compared with benchstat) plus Run, the same workload as a function so
// `kvlite bench` can execute it without test binaries. It is dependency-free; the bbolt/pebble
// comparison lives in the nested module bench/ (owner decision).
package bench

import (
	"context"
	"errors"
	"time"
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
	// Name is "put", "get", "delete" or "scan".
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

// Run executes the workload described by cfg against a real kvlite store and returns the measurements.
// It honours ctx between phases.
func Run(ctx context.Context, cfg Config) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if cfg.N <= 0 {
		return Result{}, errors.New("bench: N must be positive")
	}
	return Result{Config: cfg}, errors.New("stub: bench.Run")
}
