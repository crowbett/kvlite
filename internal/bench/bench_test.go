// STUB written by the integrator to freeze the contract; workstream D (cli + bench + fuzz) owns this file
// and replaces the bodies. Target matrix (docs/PLAN.md "Benchmarks"): Put/Get/Delete/Scan x 100 B /
// 1 KiB / 16 KiB values x sequential and random keys, Open recovery at 1M keys with and without hints,
// and Get at parallelism 1/8/16.

package bench

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestRunValidates(t *testing.T) {
	t.Parallel()
	if _, err := Run(context.Background(), Config{}); err == nil || strings.HasPrefix(err.Error(), "stub:") {
		t.Errorf("Run(N=0) = %v, want a validation error", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Run(ctx, Config{N: 1}); !errors.Is(err, context.Canceled) {
		t.Errorf("Run(cancelled) = %v, want context.Canceled", err)
	}
}

func BenchmarkPut(b *testing.B)  { b.Skip("stub: BenchmarkPut (WS-D)") }
func BenchmarkGet(b *testing.B)  { b.Skip("stub: BenchmarkGet (WS-D)") }
func BenchmarkScan(b *testing.B) { b.Skip("stub: BenchmarkScan (WS-D)") }
func BenchmarkOpen(b *testing.B) { b.Skip("stub: BenchmarkOpen (WS-D)") }
