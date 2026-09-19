// STUB written by the integrator to freeze the contract; workstream D (cli + bench + fuzz) owns this file
// and replaces the body.

// Command kvbench runs the identical workload against kvlite, bbolt and pebble and prints ops/s,
// p50/p99, bytes/op, on-disk size before and after compaction and peak RSS, into
// .bench/compare-<date>.txt plus a README table.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/crowbett/kvlite/bench"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "kvbench:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("kvbench", flag.ContinueOnError)
	engines := fs.String("engines", strings.Join(bench.Engines, ","), "comma-separated engines to run")
	n := fs.Int("n", 200_000, "number of keys")
	value := fs.Int("value", 1024, "value size in bytes")
	dir := fs.String("dir", "", "working directory (default: temp)")
	out := fs.String("out", "", "write the table here as well as stdout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	for e := range strings.SplitSeq(*engines, ",") {
		if _, err := bench.Open(e, ""); errors.Is(err, bench.ErrUnknownEngine) {
			return err
		}
	}
	_, _, _, _ = n, value, dir, out
	return errors.New("not implemented: kvbench")
}
