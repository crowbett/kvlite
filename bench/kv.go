// STUB written by the integrator to freeze the contract; workstream D (cli + bench + fuzz) owns this file
// and replaces the bodies, keeping every exported signature.

// Package bench is a NESTED MODULE (github.com/crowbett/kvlite/bench, replace => ../) that
// compares kvlite with bbolt and pebble on an identical workload. It lives in its own module so the
// library's go.mod stays dependency-free and a root `go build ./...` never resolves bbolt or pebble.
package bench

import (
	"errors"
	"fmt"
)

// Store is the seam every engine adapter implements. Scan visits keys with the prefix in key order and
// stops when fn returns false.
type Store interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error
	Scan(prefix []byte, fn func(k, v []byte) bool) error
	Close() error
}

// Engines lists the engine names Open accepts, in report order.
var Engines = []string{"kvlite", "bbolt", "pebble"}

// ErrUnknownEngine is returned by Open for a name not in Engines.
var ErrUnknownEngine = errors.New("kvbench: unknown engine")

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
