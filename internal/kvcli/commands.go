// Package kvcli implements the kvlite subcommands (get/put/del/scan/stats/compact/verify/bench) on top of
// the internal/cli dispatcher. All I/O goes through cli.Env; nothing here touches os.Stdout or os.Exit.
//
// Every command takes -dir (default "."), the store directory. Values are raw bytes unless -hex or
// -base64 is given; "-" as a value means stdin. get/scan/stats open the store read-only (no LOCK, so
// they work next to a running writer); verify never opens the store at all.
package kvcli

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"text/tabwriter"
	"time"

	"github.com/crowbett/kvlite"
	"github.com/crowbett/kvlite/internal/bench"
	"github.com/crowbett/kvlite/internal/cli"
	"github.com/crowbett/kvlite/internal/manifest"
	"github.com/crowbett/kvlite/internal/segment"
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

// ErrVerify is returned by verify when any finding was reported (exit 1).
var ErrVerify = errors.New("verify: store has problems")

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

// decode turns a command-line key or value into bytes per the encoding flags.
func (c *common) decode(what, s string) ([]byte, error) {
	switch {
	case c.hex:
		b, err := hex.DecodeString(s)
		if err != nil {
			return nil, wrapUsage(fmt.Sprintf("%s is not valid hex: %v", what, err))
		}
		return b, nil
	case c.base64:
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, wrapUsage(fmt.Sprintf("%s is not valid base64: %v", what, err))
		}
		return b, nil
	default:
		return []byte(s), nil
	}
}

// encode renders bytes for output per the encoding flags.
func (c *common) encode(b []byte) string {
	switch {
	case c.hex:
		return hex.EncodeToString(b)
	case c.base64:
		return base64.StdEncoding.EncodeToString(b)
	default:
		return string(b)
	}
}

// open opens the store at c.dir; readOnly skips the LOCK and refuses mutations.
func (c *common) open(readOnly bool, opts ...kvlite.Option) (*kvlite.DB, error) {
	if readOnly {
		opts = append(opts, kvlite.WithReadOnly())
	}
	db, err := kvlite.Open(c.dir, opts...)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", c.dir, err)
	}
	return db, nil
}

// joinClose folds a Close error into err when err is nil; used as `defer func() { err = joinClose(err, db.Close()) }()`.
func joinClose(err, cerr error) error {
	if cerr != nil && err == nil {
		return fmt.Errorf("close: %w", cerr)
	}
	return err
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
		Run: func(_ context.Context, env *cli.Env, args []string) (err error) {
			if len(args) != 1 {
				return wrapUsage("get <key>")
			}
			if err := c.validate(); err != nil {
				return wrapUsage(err.Error())
			}
			key, err := c.decode("key", args[0])
			if err != nil {
				return err
			}
			db, err := c.open(true)
			if err != nil {
				return err
			}
			defer func() { err = joinClose(err, db.Close()) }()
			v, err := db.Get(key)
			if err != nil {
				return fmt.Errorf("get %s: %w", args[0], err)
			}
			_, err = fmt.Fprintln(env.Stdout, c.encode(v))
			return err
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
		Run: func(_ context.Context, env *cli.Env, args []string) (err error) {
			if len(args) < 1 || len(args) > 2 {
				return wrapUsage("put <key> [value|-]")
			}
			if err := c.validate(); err != nil {
				return wrapUsage(err.Error())
			}
			if ttl < 0 {
				return wrapUsage("-ttl must not be negative")
			}
			key, err := c.decode("key", args[0])
			if err != nil {
				return err
			}
			var value []byte
			if len(args) == 2 && args[1] != "-" {
				if value, err = c.decode("value", args[1]); err != nil {
					return err
				}
			} else {
				raw, err := io.ReadAll(env.Stdin)
				if err != nil {
					return fmt.Errorf("read stdin: %w", err)
				}
				if c.hex || c.base64 {
					// Encoded stdin: tolerate a trailing newline from `echo`.
					s := string(raw)
					for s != "" && (s[len(s)-1] == '\n' || s[len(s)-1] == '\r') {
						s = s[:len(s)-1]
					}
					if value, err = c.decode("value", s); err != nil {
						return err
					}
				} else {
					value = raw
				}
			}
			policy := kvlite.SyncNever
			if sync {
				policy = kvlite.SyncAlways
			}
			db, err := c.open(false, kvlite.WithSync(policy))
			if err != nil {
				return err
			}
			defer func() { err = joinClose(err, db.Close()) }()
			if ttl > 0 {
				err = db.PutTTL(key, value, ttl)
			} else {
				err = db.Put(key, value)
			}
			if err != nil {
				return fmt.Errorf("put %s: %w", args[0], err)
			}
			return nil
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
		Run: func(_ context.Context, _ *cli.Env, args []string) (err error) {
			if len(args) != 1 {
				return wrapUsage("del <key>")
			}
			if err := c.validate(); err != nil {
				return wrapUsage(err.Error())
			}
			key, err := c.decode("key", args[0])
			if err != nil {
				return err
			}
			db, err := c.open(false)
			if err != nil {
				return err
			}
			defer func() { err = joinClose(err, db.Close()) }()
			if err := db.Delete(key); err != nil {
				return fmt.Errorf("del %s: %w", args[0], err)
			}
			return nil
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
		Run: func(ctx context.Context, env *cli.Env, args []string) (err error) {
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
			var prefix, loB, hiB []byte
			if len(args) == 1 {
				if prefix, err = c.decode("prefix", args[0]); err != nil {
					return err
				}
			}
			if lo != "" {
				if loB, err = c.decode("-lo", lo); err != nil {
					return err
				}
			}
			if hi != "" {
				if hiB, err = c.decode("-hi", hi); err != nil {
					return err
				}
			}
			db, err := c.open(true)
			if err != nil {
				return err
			}
			defer func() { err = joinClose(err, db.Close()) }()
			snap, err := db.Snapshot()
			if err != nil {
				return fmt.Errorf("snapshot: %w", err)
			}
			defer func() {
				if cerr := snap.Close(); cerr != nil && err == nil {
					err = fmt.Errorf("close snapshot: %w", cerr)
				}
			}()
			var it *kvlite.Iter
			if loB != nil || hiB != nil {
				it = snap.Range(loB, hiB)
			} else {
				it = snap.Iter(prefix)
			}
			enc := json.NewEncoder(env.Stdout)
			n := 0
			for it.Next() {
				if n&1023 == 0 {
					if err := ctx.Err(); err != nil {
						return err
					}
				}
				if limit > 0 && n >= limit {
					break
				}
				n++
				key := c.encode(it.Key())
				var value string
				if !keysOnly {
					v, err := it.Value()
					if err != nil {
						return fmt.Errorf("scan: value of %s: %w", key, err)
					}
					value = c.encode(v)
				}
				switch {
				case asJSON && keysOnly:
					err = enc.Encode(struct {
						Key string `json:"key"`
					}{key})
				case asJSON:
					err = enc.Encode(struct {
						Key   string `json:"key"`
						Value string `json:"value"`
					}{key, value})
				case keysOnly:
					_, err = fmt.Fprintln(env.Stdout, key)
				default:
					_, err = fmt.Fprintf(env.Stdout, "%s\t%s\n", key, value)
				}
				if err != nil {
					return err
				}
			}
			if err := it.Err(); err != nil {
				return fmt.Errorf("scan: %w", err)
			}
			return nil
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
		Run: func(_ context.Context, env *cli.Env, args []string) (err error) {
			if len(args) != 0 {
				return wrapUsage("stats takes no arguments")
			}
			db, err := c.open(true)
			if err != nil {
				return err
			}
			defer func() { err = joinClose(err, db.Close()) }()
			st := db.Stats()
			if asJSON {
				enc := json.NewEncoder(env.Stdout)
				enc.SetIndent("", "  ")
				return enc.Encode(st)
			}
			return printStats(env.Stdout, st)
		},
	}
}

// printStats writes st as aligned "name  value" lines.
func printStats(w io.Writer, st kvlite.Stats) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	row := func(name string, v any) { fmt.Fprintf(tw, "%s\t%v\n", name, v) }
	row("keys", st.Keys)
	row("segments", st.Segments)
	row("active_segment", st.ActiveSegment)
	row("live_bytes", st.LiveBytes)
	row("dead_bytes", st.DeadBytes)
	row("total_bytes", st.TotalBytes)
	row("dead_ratio", fmt.Sprintf("%.3f", st.DeadRatio))
	row("keydir_bytes", st.KeydirBytes)
	row("reclaimed_bytes", st.ReclaimedBytes)
	row("puts", st.Puts)
	row("deletes", st.Deletes)
	row("gets", st.Gets)
	row("hits", st.Hits)
	row("misses", st.Misses)
	row("expired_hits", st.ExpiredHits)
	row("syncs", st.Syncs)
	row("compactions", st.Compactions)
	last := "never"
	if !st.LastCompaction.IsZero() {
		last = st.LastCompaction.Format(time.RFC3339)
	}
	row("last_compaction", last)
	row("last_compaction_dur", st.LastCompactionDur)
	return tw.Flush()
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
		Run: func(ctx context.Context, env *cli.Env, args []string) (err error) {
			if len(args) != 0 {
				return wrapUsage("compact takes no arguments")
			}
			if timeout < 0 {
				return wrapUsage("-timeout must not be negative")
			}
			if timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}
			db, err := c.open(false)
			if err != nil {
				return err
			}
			defer func() { err = joinClose(err, db.Close()) }()
			before := db.Stats()
			start := time.Now()
			if err := db.Compact(ctx); err != nil {
				return fmt.Errorf("compact: %w", err)
			}
			took := time.Since(start)
			after := db.Stats()
			tw := tabwriter.NewWriter(env.Stdout, 0, 4, 2, ' ', 0)
			fmt.Fprintf(tw, "before\t%d bytes\t%d segments\t%.3f dead\n", before.TotalBytes, before.Segments, before.DeadRatio)
			fmt.Fprintf(tw, "after\t%d bytes\t%d segments\t%.3f dead\n", after.TotalBytes, after.Segments, after.DeadRatio)
			fmt.Fprintf(tw, "reclaimed\t%d bytes\t\t\n", before.TotalBytes-after.TotalBytes)
			fmt.Fprintf(tw, "took\t%s\t\t\n", took.Round(time.Millisecond))
			return tw.Flush()
		},
	}
}

// segmentReport is one segment's verify result, JSON-friendly.
type segmentReport struct {
	ID          uint32   `json:"id"`
	File        string   `json:"file"`
	Records     int64    `json:"records"`
	Bytes       int64    `json:"bytes"`
	Hint        string   `json:"hint"` // "ok", "none", "bad"
	HintEntries int      `json:"hint_entries,omitempty"`
	Findings    []string `json:"findings,omitempty"`
}

// verifyReport is the whole store's verify result.
type verifyReport struct {
	Dir      string          `json:"dir"`
	Manifest string          `json:"manifest"` // "ok", "missing", "corrupt"
	Active   uint32          `json:"active_segment"`
	Segments []segmentReport `json:"segments"`
	Orphans  []string        `json:"orphans,omitempty"`
	Findings []string        `json:"findings,omitempty"` // store-level findings
	Total    int             `json:"total_findings"`
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
		Run: func(ctx context.Context, env *cli.Env, args []string) error {
			if len(args) != 0 {
				return wrapUsage("verify takes no arguments")
			}
			rep, err := verifyStore(ctx, c.dir, deep)
			if err != nil {
				return err
			}
			if asJSON {
				enc := json.NewEncoder(env.Stdout)
				enc.SetIndent("", "  ")
				if err := enc.Encode(rep); err != nil {
					return err
				}
			} else if err := printVerify(env.Stdout, rep); err != nil {
				return err
			}
			if rep.Total > 0 {
				return fmt.Errorf("%w: %d finding(s)", ErrVerify, rep.Total)
			}
			return nil
		},
	}
}

// verifyStore walks dir without opening the DB: MANIFEST, orphans, every listed segment via
// segment.VerifySegment and, with deep, every hint entry against the record at its offset.
func verifyStore(ctx context.Context, dir string, deep bool) (*verifyReport, error) {
	rep := &verifyReport{Dir: dir, Manifest: "ok"}
	if _, err := os.Stat(dir); err != nil {
		return nil, fmt.Errorf("verify: %w", err)
	}
	m, err := manifest.Load(dir)
	var ids []uint32
	switch {
	case err == nil:
		ids = m.Segments
		rep.Active = m.Active
		orphans, oerr := manifest.Orphans(dir, m)
		if oerr != nil {
			return nil, fmt.Errorf("verify: orphans: %w", oerr)
		}
		rep.Orphans = orphans
		for _, o := range orphans {
			rep.Findings = append(rep.Findings, "orphan file not in MANIFEST: "+o)
		}
	case errors.Is(err, manifest.ErrNotExist), errors.Is(err, manifest.ErrCorrupt):
		rep.Manifest = "missing"
		if errors.Is(err, manifest.ErrCorrupt) {
			rep.Manifest = "corrupt"
		}
		ids, err = logIDs(dir)
		if err != nil {
			return nil, err
		}
		if len(ids) > 0 || rep.Manifest == "corrupt" {
			rep.Findings = append(rep.Findings, "MANIFEST "+rep.Manifest+"; verifying every *.log on disk")
		}
	default:
		return nil, fmt.Errorf("verify: manifest: %w", err)
	}

	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		sr := segmentReport{ID: id, File: segment.LogName(id), Hint: "none"}
		vr, err := segment.VerifySegment(dir, id)
		if err != nil {
			sr.Findings = append(sr.Findings, "cannot verify: "+err.Error())
			rep.Segments = append(rep.Segments, sr)
			continue
		}
		sr.Records, sr.Bytes = vr.Records, vr.Bytes
		if vr.HintPresent {
			sr.Hint = "bad"
			if vr.HintValid {
				sr.Hint = "ok"
			}
		}
		for _, f := range vr.Findings {
			where := "log"
			if f.Hint {
				where = "hint"
			}
			sr.Findings = append(sr.Findings, fmt.Sprintf("%s off=%d: %s", where, f.Off, f.Reason))
		}
		if deep && vr.HintPresent {
			n, findings := deepHintCheck(dir, id)
			sr.HintEntries = n
			sr.Findings = append(sr.Findings, findings...)
		}
		rep.Segments = append(rep.Segments, sr)
	}
	rep.Total = len(rep.Findings)
	for _, s := range rep.Segments {
		rep.Total += len(s.Findings)
	}
	return rep, nil
}

// deepHintCheck reads every hint entry of segment id and re-reads the record it points at, checking
// key and expiry agree. It is independent of VerifySegment's own cross-check.
func deepHintCheck(dir string, id uint32) (int, []string) {
	entries, err := segment.ReadHint(dir, id)
	if err != nil {
		return 0, []string{"hint unreadable: " + err.Error()}
	}
	r, err := segment.OpenReader(dir, id)
	if err != nil {
		return len(entries), []string{"log unreadable for hint check: " + err.Error()}
	}
	defer r.Close()
	var findings []string
	for _, e := range entries {
		rec, _, err := r.ReadAt(e.Off, e.Size)
		if err != nil {
			findings = append(findings, fmt.Sprintf("hint off=%d size=%d: read failed: %v", e.Off, e.Size, err))
			continue
		}
		if !bytes.Equal(rec.Key, e.Key) {
			findings = append(findings, fmt.Sprintf("hint off=%d: key mismatch (hint %q, log %q)", e.Off, e.Key, rec.Key))
		}
		var recExp uint32
		if rec.Flags.HasExpiry() && !rec.Expiry.IsZero() {
			recExp = uint32(rec.Expiry.Unix()) //nolint:gosec // ExpSec is defined as unix seconds in uint32
		}
		if recExp != e.ExpSec {
			findings = append(findings, fmt.Sprintf("hint off=%d: expiry mismatch (hint %d, log %d)", e.Off, e.ExpSec, recExp))
		}
		if rec.Flags.Tombstone() {
			findings = append(findings, fmt.Sprintf("hint off=%d: points at a tombstone", e.Off))
		}
	}
	return len(entries), findings
}

// logIDs lists the segment ids of every *.log in dir, ascending.
func logIDs(dir string) ([]uint32, error) {
	names, err := filepath.Glob(filepath.Join(dir, "*"+segment.LogSuffix))
	if err != nil {
		return nil, fmt.Errorf("verify: %w", err)
	}
	ids := make([]uint32, 0, len(names))
	for _, n := range names {
		if id, hint, ok := segment.ParseName(filepath.Base(n)); ok && !hint {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

func printVerify(w io.Writer, rep *verifyReport) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "dir\t%s\nmanifest\t%s\n", rep.Dir, rep.Manifest)
	for _, s := range rep.Segments {
		status := "ok"
		if len(s.Findings) > 0 {
			status = fmt.Sprintf("%d finding(s)", len(s.Findings))
		}
		active := ""
		if rep.Manifest == "ok" && s.ID == rep.Active {
			active = " (active)"
		}
		fmt.Fprintf(tw, "%s%s\trecords=%d\tbytes=%d\thint=%s\t%s\n", s.File, active, s.Records, s.Bytes, s.Hint, status)
		for _, f := range s.Findings {
			fmt.Fprintf(tw, "  !\t%s\n", f)
		}
	}
	for _, f := range rep.Findings {
		fmt.Fprintf(tw, "  !\t%s\n", f)
	}
	fmt.Fprintf(tw, "segments\t%d\nfindings\t%d\n", len(rep.Segments), rep.Total)
	return tw.Flush()
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
		Run: func(ctx context.Context, env *cli.Env, args []string) error {
			if len(args) != 0 {
				return wrapUsage("bench takes no arguments")
			}
			if n <= 0 || value < 0 || readers <= 0 {
				return wrapUsage("-n and -readers must be positive, -value non-negative")
			}
			res, err := bench.Run(ctx, bench.Config{
				Dir: c.dir, N: n, ValueSize: value, Readers: readers, Sync: sync, Random: random,
			})
			if err != nil {
				return err
			}
			return printBench(env.Stdout, res)
		},
	}
}

// printBench renders a bench.Result as a table.
func printBench(w io.Writer, res bench.Result) error {
	order := "sequential"
	if res.Config.Random {
		order = "random"
	}
	fmt.Fprintf(w, "kvlite bench: n=%d value=%dB readers=%d keys=%s sync=%s\n\n",
		res.Config.N, res.Config.ValueSize, res.Config.Readers, order, res.SyncPolicy)
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "phase\tops\tops/s\tp50\tp99\tB/op\t")
	for _, p := range res.Phases {
		fmt.Fprintf(tw, "%s\t%d\t%.0f\t%s\t%s\t%.0f\t\n",
			p.Name, p.Ops, p.OpsPerSec, fmtDur(p.P50), fmtDur(p.P99), p.BytesPerOp)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	amp := 0.0
	if res.DiskAfter > 0 {
		amp = float64(res.DiskBefore) / float64(res.DiskAfter)
	}
	_, err := fmt.Fprintf(w, "\ndisk before compact %d bytes, after %d bytes (%.2fx amplification)\n",
		res.DiskBefore, res.DiskAfter, amp)
	return err
}

// fmtDur prints a latency with microsecond resolution below a millisecond.
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
