// STUB written by the integrator to freeze the contract; workstream D (cli + bench + fuzz) owns this file
// and replaces the bodies, keeping every exported signature. Flag sets and argument validation are the
// contract; every Run returns "not implemented: <cmd>" until WS-D lands.

// Package kvcli implements the kvlite subcommands (get/put/del/scan/stats/compact/verify/bench) on top of
// the internal/cli dispatcher. All I/O goes through cli.Env; nothing here touches os.Stdout or os.Exit.
//
// Every command takes -dir (default "."), the store directory. Values are raw bytes unless -hex or
// -base64 is given; "-" as a value means stdin.
package kvcli

import (
	"context"
	"errors"
	"flag"
	"time"

	"github.com/crowbett/kvlite/internal/cli"
)

// Commands returns every kvlite subcommand, ready for cli.App.Register.
func Commands() []*cli.Command {
	return []*cli.Command{
		getCommand(),
		putCommand(),
		delCommand(),
		scanCommand(),
		statsCommand(),
		compactCommand(),
		verifyCommand(),
		benchCommand(),
	}
}

func notImplemented(name string) error { return errors.New("not implemented: " + name) }

// common holds the flags shared by every command.
type common struct {
	dir    string
	hex    bool
	base64 bool
}

func (c *common) dirFlag(fs *flag.FlagSet) {
	fs.StringVar(&c.dir, "dir", ".", "store directory")
}

func (c *common) encodingFlags(fs *flag.FlagSet) {
	fs.BoolVar(&c.hex, "hex", false, "keys and values are hex-encoded")
	fs.BoolVar(&c.base64, "base64", false, "keys and values are base64-encoded")
}

func (c *common) validate() error {
	if c.hex && c.base64 {
		return errors.New("-hex and -base64 are mutually exclusive")
	}
	return nil
}

// wrapUsage returns an error that matches cli.ErrUsage (exit code 2).
func wrapUsage(msg string) error {
	return &usageError{msg: msg}
}

type usageError struct{ msg string }

func (e *usageError) Error() string   { return "usage: " + e.msg }
func (e *usageError) Is(t error) bool { return t == cli.ErrUsage }

func getCommand() *cli.Command {
	var c common
	return &cli.Command{
		Name:    "get",
		Summary: "print the value of a key (exit 1 when absent)",
		Flags: func(fs *flag.FlagSet) {
			c.dirFlag(fs)
			c.encodingFlags(fs)
		},
		Run: func(_ context.Context, _ *cli.Env, args []string) error {
			if len(args) != 1 {
				return wrapUsage("get <key>")
			}
			if err := c.validate(); err != nil {
				return wrapUsage(err.Error())
			}
			return notImplemented("get")
		},
	}
}

func putCommand() *cli.Command {
	var (
		c    common
		ttl  time.Duration
		sync bool
	)
	return &cli.Command{
		Name:    "put",
		Summary: "store a value under a key (value from arg or stdin)",
		Flags: func(fs *flag.FlagSet) {
			c.dirFlag(fs)
			c.encodingFlags(fs)
			fs.DurationVar(&ttl, "ttl", 0, "expire the key after this duration (0 = never)")
			fs.BoolVar(&sync, "sync", false, "fsync before returning")
		},
		Run: func(_ context.Context, _ *cli.Env, args []string) error {
			if len(args) < 1 || len(args) > 2 {
				return wrapUsage("put <key> [value|-]")
			}
			if err := c.validate(); err != nil {
				return wrapUsage(err.Error())
			}
			if ttl < 0 {
				return wrapUsage("-ttl must not be negative")
			}
			return notImplemented("put")
		},
	}
}

func delCommand() *cli.Command {
	var c common
	return &cli.Command{
		Name:    "del",
		Summary: "delete a key (appends a tombstone)",
		Flags: func(fs *flag.FlagSet) {
			c.dirFlag(fs)
			c.encodingFlags(fs)
		},
		Run: func(_ context.Context, _ *cli.Env, args []string) error {
			if len(args) != 1 {
				return wrapUsage("del <key>")
			}
			if err := c.validate(); err != nil {
				return wrapUsage(err.Error())
			}
			return notImplemented("del")
		},
	}
}

func scanCommand() *cli.Command {
	var (
		c        common
		lo, hi   string
		limit    int
		keysOnly bool
		asJSON   bool
	)
	return &cli.Command{
		Name:    "scan",
		Summary: "list keys (and values) in key order from a snapshot",
		Flags: func(fs *flag.FlagSet) {
			c.dirFlag(fs)
			c.encodingFlags(fs)
			fs.StringVar(&lo, "lo", "", "range lower bound (inclusive)")
			fs.StringVar(&hi, "hi", "", "range upper bound (exclusive)")
			fs.IntVar(&limit, "limit", 0, "stop after this many keys (0 = all)")
			fs.BoolVar(&keysOnly, "keys-only", false, "print keys without values")
			fs.BoolVar(&asJSON, "json", false, "emit one JSON object per line")
		},
		Run: func(_ context.Context, _ *cli.Env, args []string) error {
			if len(args) > 1 {
				return wrapUsage("scan [prefix]")
			}
			if err := c.validate(); err != nil {
				return wrapUsage(err.Error())
			}
			if len(args) == 1 && (lo != "" || hi != "") {
				return wrapUsage("prefix and -lo/-hi are mutually exclusive")
			}
			if limit < 0 {
				return wrapUsage("-limit must not be negative")
			}
			return notImplemented("scan")
		},
	}
}

func statsCommand() *cli.Command {
	var (
		c      common
		asJSON bool
	)
	return &cli.Command{
		Name:    "stats",
		Summary: "print store statistics",
		Flags: func(fs *flag.FlagSet) {
			c.dirFlag(fs)
			fs.BoolVar(&asJSON, "json", false, "emit JSON")
		},
		Run: func(_ context.Context, _ *cli.Env, args []string) error {
			if len(args) != 0 {
				return wrapUsage("stats takes no arguments")
			}
			return notImplemented("stats")
		},
	}
}

func compactCommand() *cli.Command {
	var (
		c       common
		timeout time.Duration
	)
	return &cli.Command{
		Name:    "compact",
		Summary: "merge segments and reclaim dead space",
		Flags: func(fs *flag.FlagSet) {
			c.dirFlag(fs)
			fs.DurationVar(&timeout, "timeout", 0, "abort after this duration (0 = none)")
		},
		Run: func(_ context.Context, _ *cli.Env, args []string) error {
			if len(args) != 0 {
				return wrapUsage("compact takes no arguments")
			}
			if timeout < 0 {
				return wrapUsage("-timeout must not be negative")
			}
			return notImplemented("compact")
		},
	}
}

func verifyCommand() *cli.Command {
	var (
		c      common
		deep   bool
		asJSON bool
	)
	return &cli.Command{
		Name:    "verify",
		Summary: "check every segment crc and hint (read-only; exit 1 on any finding)",
		Flags: func(fs *flag.FlagSet) {
			c.dirFlag(fs)
			fs.BoolVar(&deep, "deep", false, "also cross-check every hint entry against its log record")
			fs.BoolVar(&asJSON, "json", false, "emit JSON")
		},
		Run: func(_ context.Context, _ *cli.Env, args []string) error {
			if len(args) != 0 {
				return wrapUsage("verify takes no arguments")
			}
			return notImplemented("verify")
		},
	}
}

func benchCommand() *cli.Command {
	var (
		c       common
		n       int
		value   int
		readers int
		sync    bool
		random  bool
	)
	return &cli.Command{
		Name:    "bench",
		Summary: "run the built-in kvlite workload (see bench/ for the bbolt/pebble comparison)",
		Flags: func(fs *flag.FlagSet) {
			fs.StringVar(&c.dir, "dir", "", "store directory (default: a temp dir, removed afterwards)")
			fs.IntVar(&n, "n", 100_000, "number of keys")
			fs.IntVar(&value, "value", 1024, "value size in bytes")
			fs.IntVar(&readers, "readers", 1, "concurrent reader goroutines")
			fs.BoolVar(&sync, "sync", false, "open with SyncAlways")
			fs.BoolVar(&random, "random", false, "random instead of sequential keys")
		},
		Run: func(_ context.Context, _ *cli.Env, args []string) error {
			if len(args) != 0 {
				return wrapUsage("bench takes no arguments")
			}
			if n <= 0 || value < 0 || readers <= 0 {
				return wrapUsage("-n and -readers must be positive, -value non-negative")
			}
			return notImplemented("bench")
		},
	}
}
