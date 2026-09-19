// Package bench is a NESTED MODULE (github.com/crowbett/kvlite/bench, replace => ../) that
// compares kvlite with bbolt and pebble on an identical workload. It lives in its own module so the
// library's go.mod stays dependency-free and a root `go build ./...` never resolves bbolt or pebble.
package bench

import (
	"context"
	"errors"
	"fmt"
)

// Store is the seam every engine adapter implements. Scan visits keys with the prefix in key order and
// stops when fn returns false. Get returns ErrNotFound for an absent key in every engine.
type Store interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error
	Scan(prefix []byte, fn func(k, v []byte) bool) error
	Close() error
}

// Compactor is implemented by engines that can reclaim dead space on demand (kvlite, pebble). bbolt
// has no in-place compaction, so the runner reports its file size as-is.
type Compactor interface {
	Compact(ctx context.Context) error
}

// Engines lists the engine names Open accepts, in report order.
var Engines = []string{"kvlite", "bbolt", "pebble"}

// Errors shared by every adapter.
var (
	// ErrUnknownEngine is returned by Open for a name not in Engines.
	ErrUnknownEngine = errors.New("kvbench: unknown engine")
	// ErrNotFound is what every adapter's Get returns for an absent key.
	ErrNotFound = errors.New("kvbench: key not found")
)

// Open opens engine in dir.
func Open(engine, dir string) (Store, error) {
	switch engine {
	case "kvlite":
		return OpenKVLite(dir)
	case "bbolt":
		return OpenBolt(dir)
	case "pebble":
		return OpenPebble(dir)
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownEngine, engine)
	}
}

// PrefixUpperBound returns the smallest key greater than every key starting with prefix, or nil when
// no such bound exists (prefix is empty or all 0xff).
func PrefixUpperBound(prefix []byte) []byte {
	for i := len(prefix) - 1; i >= 0; i-- {
		if prefix[i] != 0xff {
			up := make([]byte, i+1)
			copy(up, prefix[:i+1])
			up[i]++
			return up
		}
	}
	return nil
}
