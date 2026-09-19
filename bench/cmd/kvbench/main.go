// Command kvbench runs the identical workload (put N, get N, full scan, delete N/2, compact) against
// kvlite, bbolt and pebble and prints one table: engine, op, value size, ops/s, p50/p99, bytes/op,
// plus the on-disk size before and after compaction. Every engine runs with fsync disabled, matching
// kvlite's default SyncNever, so the numbers compare engine overhead, not the disk.
//
//	go run ./cmd/kvbench -engines kvlite,bbolt,pebble -n 200000 -sizes 100,1024,16384 -out ../.bench/compare-2026-09-19.txt
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/crowbett/kvlite/bench"
)

func main() {
	os.Exit(realMain())
}

// realMain is main without os.Exit so the signal handler's stop() runs before the process exits.
func realMain() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "kvbench:", err)
		return 1
	}
	return 0
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("kvbench", flag.ContinueOnError)
	fs.SetOutput(stderr)
	engines := fs.String("engines", strings.Join(bench.Engines, ","), "comma-separated engines to run")
	n := fs.Int("n", 200_000, "number of keys per engine and value size")
	sizesFlag := fs.String("sizes", "100,1024,16384", "comma-separated value sizes in bytes")
	dir := fs.String("dir", "", "parent of the per-engine working directories (default: OS temp)")
	out := fs.String("out", "", "also write the table to this file")
	keep := fs.Bool("keep", false, "keep the working directories after the run")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("kvbench takes no positional arguments")
	}
	if *n <= 0 {
		return errors.New("-n must be positive")
	}

	var names []string
	for e := range strings.SplitSeq(*engines, ",") {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		if !slices.Contains(bench.Engines, e) {
			return fmt.Errorf("%w: %q (known: %s)", bench.ErrUnknownEngine, e, strings.Join(bench.Engines, ","))
		}
		names = append(names, e)
	}
	if len(names) == 0 {
		return errors.New("-engines is empty")
	}
	var sizes []int
	for s := range strings.SplitSeq(*sizesFlag, ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		v, err := strconv.Atoi(s)
		if err != nil || v < 0 {
			return fmt.Errorf("-sizes: bad value size %q", s)
		}
		sizes = append(sizes, v)
	}
	if len(sizes) == 0 {
		return errors.New("-sizes is empty")
	}

	started := time.Now()
	var results []bench.EngineResult
	for _, size := range sizes {
		for _, engine := range names {
			if err := ctx.Err(); err != nil {
				return err
			}
			work, err := bench.TempDir(*dir, engine)
			if err != nil {
				return err
			}
			fmt.Fprintf(stderr, "==> %s value=%dB n=%d (%s)\n", engine, size, *n, work)
			res, err := bench.RunEngine(ctx, engine, work, bench.Workload{N: *n, ValueSize: size})
			if !*keep {
				_ = os.RemoveAll(work)
			}
			if err != nil {
				return err
			}
			results = append(results, res)
		}
	}

	header := fmt.Sprintf("kvbench %s  n=%d  sizes=%s  engines=%s  sync=never (all engines)  %s/%s go%s  %s\n\n",
		started.Format("2006-01-02 15:04"), *n, *sizesFlag, strings.Join(names, ","),
		runtime.GOOS, runtime.GOARCH, strings.TrimPrefix(runtime.Version(), "go"), time.Since(started).Round(time.Second))
	writers := []io.Writer{stdout}
	if *out != "" {
		f, err := os.Create(*out)
		if err != nil {
			return err
		}
		defer f.Close()
		writers = append(writers, f)
	}
	w := io.MultiWriter(writers...)
	if _, err := io.WriteString(w, header); err != nil {
		return err
	}
	return bench.WriteTable(w, results)
}
