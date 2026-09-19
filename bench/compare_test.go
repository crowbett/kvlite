// STUB written by the integrator to freeze the contract; workstream D (cli + bench + fuzz) owns this file
// and replaces the bodies. Target: the internal/bench matrix run through every Store in Engines, so
// `task compare` (go test -bench . inside bench/) yields one benchstat-comparable line per engine.

package bench

import (
	"errors"
	"testing"
)

func TestOpenUnknown(t *testing.T) {
	t.Parallel()
	if _, err := Open("nope", t.TempDir()); !errors.Is(err, ErrUnknownEngine) {
		t.Fatalf("Open(nope) = %v, want ErrUnknownEngine", err)
	}
	for _, e := range Engines {
		if _, err := Open(e, t.TempDir()); err == nil {
			t.Errorf("Open(%s) = nil error on stubs", e)
		}
	}
}

func BenchmarkPut(b *testing.B)  { b.Skip("stub: BenchmarkPut (WS-D)") }
func BenchmarkGet(b *testing.B)  { b.Skip("stub: BenchmarkGet (WS-D)") }
func BenchmarkScan(b *testing.B) { b.Skip("stub: BenchmarkScan (WS-D)") }
