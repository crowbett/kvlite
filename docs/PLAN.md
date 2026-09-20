# ADR-0001: kvlite - embedded bitcask-style key-value store (library + CLI)
Status: Accepted (owner decisions applied, step 1 scaffolded 2026-09-19) | Date: 2026-09-19 | Module: `github.com/crowbett/kvlite` (Go 1.27)

## Context
P11 in `~/dev/ROADMAP.md` Queue 2: "append-only log, in-memory keydir, hint files, crash-safe compaction, CRC, TTL, snapshot
iterator. Benchmarks vs bbolt and pebble on the same workload; fuzz test for the log format." Repo does not exist yet; the
integrator runs `~/dev/new-project.ps1 -Template go-cli -Name kvlite -Module github.com/crowbett/kvlite` and moves
this file to `docs/PLAN.md`. The `go-cli` template supplies `cmd/tool/main.go` (`signal.NotifyContext` -> `cli.New` ->
`Register(cli.Commands()...)` -> `os.Exit(app.Main(ctx, os.Args[1:]))`), `internal/cli` (`App`, `Command{Name,Summary,Flags,Run}`,
`Env{Stdout,Stderr,Stdin,Verbose}`, `ErrUsage`, exit codes 0/1/2/130), `.goreleaser.yaml` v2, `.golangci.yml` v2, `Taskfile.yml`,
and a **zero-dependency `go.mod`**. Bench style comes from `~/dev/go/perf-lab`: `internal/bench/*_bench_test.go`,
`scripts/bench.ps1` (`-Save baseline` / `-Compare baseline`, count>=6, writes `.bench/<name>.txt` + `.meta.txt` with CPU/Go/OS),
and its `.bench/README.md` rules (baseline changes only in a dedicated `perf(bench): refresh baseline` commit; unsaved runs go
to gitignored `results/bench/`). Target box: Ryzen 7 3700X, 16 logical cores, Windows 11, NTFS. Non-goals for v1: transactions
beyond one atomic `Batch`, secondary indexes, replication, network access, values larger than memory, MVCC.

## Decision
Classic bitcask: **writes append to one active segment; every live key sits in an in-memory keydir; a read is one map lookup
plus one `ReadAt`.** Values are inline in the log (v1: key <= 64 KiB, value <= 16 MiB, both fully in memory - no streaming API),
so a `Get` is exactly one syscall against the page cache. Durability is a knob (`always`/`interval`/`never`) because
`FlushFileBuffers` on NTFS costs ~1 ms and would dominate every small-value number we publish. Crash safety rests on three
rules: every record carries a crc32c over a length-prefixed payload; a bad record at the **tail of the active segment** is a
torn write and is truncated on `Open`, while a bad record anywhere else is `ErrCorrupt` unless `Open(WithRepair())`; and the
live segment set is named by a `MANIFEST` replaced atomically, so a compaction that dies mid-way leaves orphan files the next
`Open` deletes - never a half-visible key set.

The keydir is a 64-way sharded `map[string]Entry` with a pointer-free 16-byte entry and an `RWMutex` per shard: point reads
scale across cores while a single writer mutex serialises appends so offsets are assigned without CAS games. Ordered iteration
is not free in a hash map, so `Snapshot()` materialises a sorted contiguous copy of (key, entry) and pins the segments it
references by refcount; `Iter(prefix)`/`Range(lo,hi)` binary-search that slice. That is the stated v1 trade-off - point ops as
fast as a Go map, scans pay an O(keys) copy - and the keydir sits behind an unexported `index` interface so a B-tree or ART can
replace it in P16 without touching `DB`. Compaction merges all non-active segments into fresh ones, drops tombstones/expired/
superseded records, writes a `.hint` per output segment so the next `Open` is O(keys) not O(bytes), swaps the `MANIFEST` under
the writer lock, and re-points the keydir by compare-and-swap on `(segment, offset)` so a key overwritten mid-merge is never
rolled back.

## Package tree
```
cmd/kvlite/main.go     wiring only (template main): NotifyContext -> cli.New -> Register -> Main
internal/cli/          template subcommand framework, unchanged (its template commands.go is gone: kvcli imports cli,
                       so cli cannot import kvcli - cmd/kvlite registers kvcli.Commands() directly)
internal/kvcli/        get/put/del/scan/stats/compact/verify/bench implementations (all I/O through cli.Env)
db.go options.go errors.go stats.go batch.go  package kvlite: public API, writer path, TTL
open.go recovery.go snapshot.go compact.go    package kvlite: Open/recover, Snapshot, Compact
internal/record/       record encode/decode, crc32c, varints, size limits; FuzzRecord
internal/segment/      segment + hint file write/read/rotate/scan/verify; refcounted readers
internal/keydir/       sharded map, Entry, CompareAndSwap, packed sorted snapshot
internal/manifest/     MANIFEST read/write, atomic swap, generation, orphan detection
internal/fslock/       LOCK file: lock_windows.go (CreateFile share=0) / lock_unix.go (Flock)
internal/bench/        kvlite-only Go benchmarks (perf-lab style), zero deps, root module
bench/ + bench/cmd/kvbench/   NESTED MODULE (own go.mod): kv Store iface + kvlite/bbolt/pebble adapters + runner
```
Imports flow one way: `kvlite -> {record, segment, keydir, manifest, fslock}`, `segment -> record`, `keydir`/`manifest`/`fslock`
import nothing, `kvcli -> {kvlite, cli, internal/bench}`, `cmd/kvlite -> {cli, kvcli}`. No cycles, no `util`/`common`, no `pkg/`
(the only public surface is the root package). Root
`go.mod` stays dependency-free; `bench/go.mod` holds bbolt + pebble behind `replace github.com/.../kvlite => ../`.

## On-disk format
`LOCK`, `MANIFEST`, `NNNNNNNN.log`, `NNNNNNNN.hint`, `trash/` (deferred deletes). Little-endian throughout. Segment header
16 B: magic `"KVLT"` | version `1` | segID uint32 | 7 zero bytes. Record = `[crc32c:4][plen:uint32][payload:plen]`: the fixed
`plen` (bounds-checked against `maxRecord = 16 + maxKey + maxValue`) lets the reader do one `io.ReadFull` and makes a torn tail
unambiguous; crc32c is Castagnoli (SSE4.2 here) over the payload only.

| Field | Enc | Bytes | Notes |
|---|---|---|---|
| crc32c / plen | LE uint32 x2 | 8 | crc over payload; `plen > maxRecord` = corrupt |
| flags | u8 | 1 | b0 tombstone, b1 hasExpiry, b2 batchMember, b3 batchCommit |
| tsMillis / expiryMillis | uvarint | 1-18 | write time (merge tie-break); expiry present only when b1 |
| keyLen / valLen | uvarint | 2-8 | <= 64 KiB / <= 16 MiB (hard caps `record.MaxKey`/`record.MaxValue`; owner decision, options only lower them) |
| key, value | raw | keyLen+valLen | value empty for a tombstone; for batchCommit: uvarint count + uvarint batchID |

Flags precede the variable fields (deviation from the roadmap's field order) so the decoder knows whether `expiryMillis` is
present and can skip a tombstone body in one pass. Hint `NNNNNNNN.hint`: header `"KVLH"|1|segID|pad`, per key
`[flags:1][expSec:uvarint][off:uvarint][size:uvarint][keyLen:uvarint][key]`, footer `[crc32c(body):4][count:uint32]["KVLH"]` -
no per-record crc, because a missing or bad footer just means "ignore the hint, scan the log", which is always safe. `MANIFEST`
(KBs, rewritten whole): `"KVLM"|1|gen uint64|active uint32|nextID uint32|count uint32|segID[count]|crc32c`, written as
`MANIFEST.tmp` -> `Sync` -> `os.Rename` over `MANIFEST` (MoveFileEx/REPLACE_EXISTING on Windows). Directory fsync is skipped on
Windows (unsupported); the MANIFEST crc plus a "rescan `*.log`" fallback covers a lost rename.

## Interfaces and key types
```go
package kvlite   // DB is the only stateful public type; constructors return concrete types
type DB struct{ /* unexported */ }; type Option func(*options) error
type SyncPolicy uint8; const (SyncNever SyncPolicy = iota; SyncInterval; SyncAlways) // SyncNever is the default
func Open(dir string, opts ...Option) (*DB, error)
// Options: WithMaxSegmentBytes(int64) 256<<20 | WithSync(SyncPolicy) | WithSyncInterval(time.Duration) 1s
//   WithReadOnly() | WithRepair() | WithAutoCompact(deadRatio float64) off by default (owner decision); minDead and the
//   tick are the constants AutoCompactMinDead = 64<<20 and AutoCompactInterval = 5m, not options
//   WithMaxKeyBytes(int) 64<<10 cap | WithMaxValueBytes(int) 16<<20 cap (owner decision) | WithShards(int) 64, pow2
//   WithLogger(*slog.Logger) | WithClock(func() time.Time)  <- the TTL seam every test injects
func (db *DB) Get(key []byte) ([]byte, error)      // fresh copy; ErrNotFound when absent or expired
func (db *DB) Has(key []byte) (bool, error)
func (db *DB) Put(key, value []byte) error; func (db *DB) Delete(key []byte) error // Delete always appends a tombstone
func (db *DB) PutTTL(key, value []byte, ttl time.Duration) error   // ttl <= 0 -> ErrInvalidTTL
func (db *DB) Batch() *Batch; func (db *DB) Snapshot() (*Snapshot, error); func (db *DB) Compact(ctx context.Context) error
func (db *DB) Stats() Stats; func (db *DB) Sync() error; func (db *DB) Close() error
func (b *Batch) Put(key, value []byte) *Batch; func (b *Batch) Delete(key []byte) *Batch; func (b *Batch) Len() int
func (b *Batch) PutTTL(key, value []byte, ttl time.Duration) *Batch
func (b *Batch) Commit() error    // one contiguous record group + commit marker; never spans two segments
func (s *Snapshot) Get(key []byte) ([]byte, error); func (s *Snapshot) Len() int
func (s *Snapshot) Iter(prefix []byte) *Iter    // nil/empty prefix = full scan, in key order
func (s *Snapshot) Range(lo, hi []byte) *Iter   // [lo, hi); nil hi = unbounded
func (s *Snapshot) Close() error                // releases segment pins; leaking one blocks reclamation
func (it *Iter) Next() bool; func (it *Iter) Err() error; func (it *Iter) Key() []byte // Key valid until Next()
func (it *Iter) Value() ([]byte, error)         // lazy read from the pinned segment
type Stats struct {
	Keys, Segments int; ActiveSegment uint32; DeadRatio float64
	LiveBytes, DeadBytes, TotalBytes, KeydirBytes, ReclaimedBytes int64 // Keydir = shards+entries+key bytes
	Puts, Deletes, Gets, Hits, Misses, ExpiredHits, Syncs, Compactions uint64
	LastCompaction time.Time; LastCompactionDur time.Duration
}
// Sentinels, all errors.New("kvlite: ...") matched with errors.Is: ErrNotFound, ErrClosed, ErrCorrupt, ErrKeyTooLarge,
// ErrValueTooLarge, ErrEmptyKey, ErrLocked, ErrReadOnly, ErrInvalidTTL.
type CorruptError struct{ Seg uint32; Off int64; Reason string; Err error } // Is(err, ErrCorrupt) == true
// internal/keydir - 16 bytes per entry, pointer-free so the GC never scans it
type Entry struct{ Seg, Off, Size, ExpSec uint32 }  // ExpSec 0 = never; unix seconds (valid to 2106)
func New(shards, hint int) *Keydir; func (k *Keydir) Len() int; func (k *Keydir) Get(key []byte) (Entry, bool)
func (k *Keydir) Put(key []byte, e Entry) (old Entry, existed bool)  // Delete(key) has the same shape
func (k *Keydir) CompareAndSwap(key []byte, from, to Entry) bool     // compaction re-point; false = superseded
func (k *Keydir) Snapshot(prefix []byte, now time.Time) Sorted       // sorted, contiguous, skips expired
type Sorted struct{ /* keys []byte + offs []uint32 + ents []Entry; Len/Key(i)/Entry(i)/Search(k) via sort.Search */ }
// internal/record   const (MaxKey = 64<<10; MaxValue = 16<<20; HeaderSize = 8; MaxMeta = 31; MaxPayload; MaxRecord)
//                   + Checksum(payload) uint32, Verify(payload, crc) bool, PutHeader/ParseHeader, MaxEncodedSize (real, step 1)
type Record struct{ Key, Value []byte; TS, Expiry time.Time; Flags Flags }
func Append(dst []byte, r Record) ([]byte, error)                // encode into a reusable buffer
func Decode(payload []byte) (Record, error)                      // no copy; Key/Value alias payload
func Read(r io.Reader, buf []byte) (Record, int, []byte, error)  // record, bytes read, grown buf; + Verify(payload, crc)
// internal/segment - concrete types; any narrow interface lives in package kvlite, where it is consumed
func Create(dir string, id uint32) (*Writer, error); func OpenReader(dir string, id uint32) (*Reader, error)
func (w *Writer) Append(payload []byte) (off uint32, err error)  // caller holds the DB writer mutex
func (w *Writer) Size() int64; func (w *Writer) Sync() error; func (w *Writer) Close() error
func (r *Reader) ReadAt(off, size uint32) (record.Record, []byte, error)
func (r *Reader) Scan(fn func(off uint32, rec record.Record) error) error // sequential, 1 MiB buffered
func (r *Reader) Ref(); func (r *Reader) Unref()                 // refcount; 0 -> closable/deletable
func WriteHint(dir string, id uint32, it func(yield func(key []byte, e HintEntry) bool)) error
func ReadHint(dir string, id uint32) ([]HintEntry, error)        // footer crc mismatch -> error
func VerifySegment(dir string, id uint32) (VerifyReport, error)  // powers `kvlite verify`
```

## Data flow
1. **Put** - validate (empty/size) -> encode into the writer's reusable buffer -> `db.wmu.Lock()` -> rotate if
   `active.Size()+len(rec) > MaxSegmentBytes` -> `Append` -> `keydir.Put` (old entry's `Size` goes to `deadBytes`) -> `Sync()`
   if `SyncAlways` -> `wmu.Unlock()`. Validation and error translation happen in package `kvlite` only.
2. **Get** - `keydir.Get` under a shard RLock -> miss -> `ErrNotFound`; `ExpSec` in the past -> `ErrNotFound` + `ExpiredHits++`
   (lazy expiry; the record dies at the next compaction, never on the read path) -> `reader(e.Seg).ReadAt(e.Off, e.Size)` -> crc
   verify -> copy the value out. **Delete** - append a tombstone (so it survives a crash) -> `keydir.Delete`.
3. **Batch.Commit** - encode members with `batchMember` plus a trailing `batchCommit` carrying `(count, batchID)`; under `wmu`
   rotate first if the group does not fit, append, sync per policy, then apply every key (a crash before the marker lands leaves
   members recovery discards). **Snapshot** - `keydir.Snapshot(prefix, now)` under shard RLocks, then `Ref()` each referenced
   reader, so a compaction that finishes later cannot delete those files.

## Open / recovery procedure
```
1. MkdirAll(dir); fslock.Acquire(dir/LOCK) -> ErrLocked on contention (skipped in ReadOnly); sweep trash/ (best effort).
2. Read MANIFEST; crc bad or absent -> dir empty ? create gen 0 : WithRepair ? rebuild from sorted *.log : ErrCorrupt.
3. Move every *.log/*.hint not listed in MANIFEST into trash/ (orphans from a crashed compaction).
4. For each segment id ascending:
     <id>.hint validates (footer crc + count) and id != active -> load entries directly, O(keys)
     else Scan the log:  tombstone            -> keydir.Delete, deadBytes += size (+ old size)
                         batchMember          -> buffer; on batchCommit with matching count/id apply all, else drop
                         expired at load time -> skip, deadBytes += size
                         otherwise            -> keydir.Put (ascending id + ascending offset = last write wins)
     crc/short read at the tail of the LAST segment -> truncate the file there, log warn, continue
     crc failure anywhere else -> WithRepair ? scan forward to the next decodable record and count it in Stats
                                             : return &CorruptError{Seg, Off, ...}
5. Open the highest id for append at the truncated length (or Create one); LiveBytes from the keydir, DeadBytes = Total - Live.
6. Start the flusher goroutine (SyncInterval) and the auto-compactor (if enabled).
```

## Compaction algorithm
```
Compact(ctx):
  wmu.Lock(); rotate(); live := manifest.Segments() minus newActive; gen := manifest.Gen; wmu.Unlock()
  if len(live) == 0 { return nil };  dst := segment.Create(dir, allocID()); pending := []move{}
  for src in live ascending: src.Scan(func(off, rec):
      if ctx.Err() != nil { abort: delete dst files, return ctx.Err() }
      cur, ok := keydir.Get(rec.Key)
      if !ok || cur.Seg != src.ID || cur.Off != off { return nil }   // superseded or deleted -> drop
      if rec.Tombstone || expired(cur, now()) { return nil }         // tombstones never merge forward
      n := dst.Append(encode(rec)); pending = append(pending, move{rec.Key, cur, Entry{dst.ID, n, ...}})
      if dst.Size() >= maxSeg { dst.Sync(); WriteHint(dst); dst = segment.Create(dir, allocID()) })
  dst.Sync(); WriteHint(dst)                  // hints are fsynced BEFORE the MANIFEST swap
  wmu.Lock(); for m in pending { keydir.CompareAndSwap(m.key, m.from, m.to) } // false = rewritten mid-merge
    manifest.Replace(live -> merged, gen+1)     // MANIFEST.tmp -> Sync -> Rename
  wmu.Unlock()
  for src in live { src.Unref(); at refcount 0 -> Close, rename into trash/, delete (retry on Windows) }
```
Crash before the MANIFEST rename: the old set is still authoritative and the merged files are orphans removed by step 4 of
`Open`. Crash after: the inputs are the orphans. Both idempotent. Auto-compaction fires when `DeadRatio >= trigger &&
DeadBytes >= minDead`, at most one merge at a time.

## Concurrency and teardown
| Goroutine | Started by | Stopped by | On failure |
|---|---|---|---|
| interval flusher | `Open` when `SyncInterval` | `Close`: `close(stop)`, wait `done`, final `Sync` | sticky sync error returned by the next `Put`/`Sync` |
| auto-compactor | `Open` when trigger > 0 | `Close`: cancel `db.ctx`, wait `done` (5 s) | warn, delete partial output, retry next tick |
| caller's `Compact(ctx)` | user / `kvcli` | caller ctx or `db.ctx` | wrapped error; no MANIFEST change |
| trash sweeper | folded into the compactor tick and `Open` | same as compactor | best effort, debug log |

No goroutine per read or write: `Get` runs on the caller's goroutine, `Put` takes `db.wmu` (single writer, many readers).
`Close`: set `closed` (every API then returns `ErrClosed`) -> cancel `db.ctx` -> stop the flusher and `Sync()` -> wait up to 5 s
for snapshot refcounts to drop -> close every segment handle -> release `LOCK`; snapshots still open after that budget are
force-closed. `cmd/kvlite` threads the template's `signal.NotifyContext` ctx into `compact`/`scan`, so Ctrl+C exits 130.

## CLI (`internal/kvcli`, registered by `cmd/kvlite/main.go`)
`kvlite <cmd> -dir <path> ...`; `-dir` is a per-command flag (the template dispatcher has no per-app global flags) defaulting
to `.`; values are raw bytes unless `-hex`/`-base64`; `-` means stdin/stdout.

| Command | Flags | Behaviour |
|---|---|---|
| `get <key>` / `del <key>` | `-hex`, `-base64` | value to stdout, `ErrNotFound` -> exit 1 on stderr / appends a tombstone |
| `put <key> [value]` | `-ttl`, `-hex`, `-base64`, `-sync` | value from arg or stdin |
| `scan [prefix]` | `-lo`, `-hi`, `-limit`, `-keys-only`, `-json` | opens a `Snapshot`, honours ctx |
| `stats` / `compact` | `-json` / `-timeout` | `Stats` as table or JSON / `Compact(ctx)` + reclaimed bytes and duration |
| `verify` | `-deep`, `-json` | walks every segment (crc per record), cross-checks each hint against its log, reports orphans; exit 1 on any finding; read-only, never mutates |
| `bench` | `-n`, `-value`, `-readers`, `-sync`, `-random` | the built-in `internal/bench.Run` workload against a temp or given dir; kvlite only (owner decision: the bbolt/pebble comparison is `kvbench` in the nested module, never shelled out to) |

## Benchmarks
`internal/bench` (root module, zero deps, perf-lab style): `Put/Get/Delete/Scan` x 100 B / 1 KiB / 16 KiB values x sequential
and random keys, plus `Open` recovery at 1M keys with and without hints, and `Get` at parallelism 1/8/16. Baseline via
`pwsh -NoProfile -File scripts/bench.ps1 -Pkg ./internal/bench/... -Save baseline -Count 10`; `.bench/README.md` copied from
perf-lab, `results/bench/` gitignored. The cross-engine comparison lives in the **nested module `bench/`** (own `go.mod`,
`replace ../`) so the library's `go.mod` keeps zero deps and a root `go build ./...` never resolves bbolt or pebble. Decision:
nested module, **not** a `compare` build tag - a tag still puts the deps in the root `go.mod`/`go.sum`, which defeats the point
and makes `govulncheck` report pebble CVEs against kvlite. `bench/kv.go` declares the seam: `type Store interface { Get(key
[]byte) ([]byte, error); Put(key, value []byte) error; Delete(key []byte) error; Scan(prefix []byte, fn func(k, v []byte) bool)
error; Close() error }` with `OpenKVLite/OpenBolt/OpenPebble(dir string) (Store, error)`. `bench/cmd/kvbench` runs the identical
workload against all three and prints ops/s, p50/p99 (sorted sample slice, no HDR dep), bytes/op, on-disk size before and after
compaction (disk amplification) and peak RSS, into `.bench/compare-<date>.txt` plus a README table. `go test -bench` numbers go
through benchstat; the runner's latency table stays a committed markdown table (different shape).

## Fuzzing and crash tests
- **`FuzzRecord`** (`internal/record`): seeds cover empty/max key, zero-length value, TTL set and unset, every flag combo;
  asserts `Decode(Append(r))` is field-identical and that `Decode` on arbitrary bytes never panics and never returns a
  `Key`/`Value` pointing outside the input slice.
- **`FuzzDB`** (root): the corpus decodes into an op script (put/puttl/del/batch/compact/reopen) run against a real DB in
  `t.TempDir()` and against a `map[string]string` model on a fake clock; after every op and every reopen all model keys must
  match and all deleted keys must be `ErrNotFound`.
- `TestTruncatedTail`: write N records, `os.Truncate` the active segment at a random byte in the last record, reopen, assert
  everything before the cut reads back, the cut record is gone and new writes still work; table-driven over cut points (inside
  the crc, inside `plen`, inside the key). `TestCorruptMiddle`: flip a byte in segment 0 -> `Open` returns `ErrCorrupt` with the
  right `Seg`/`Off`, and `WithRepair()` opens, skips exactly that record and reports it in `Stats`. `TestCompactCrash`: a
  fault-injecting `segment.Writer` fails after k bytes; the DB reopens with the pre-compaction key set and no orphans.

## Implementation plan
1. **Integrator - scaffold. DONE 2026-09-19.** `new-project.ps1`, this file -> `docs/PLAN.md`, `cmd/tool` -> `cmd/kvlite`, repo
   `CLAUDE.md`, `.bench/` + `scripts/bench.ps1` from perf-lab, stubs carrying every signature above (each stub file opens with
   `// STUB written by the integrator ...; workstream X owns this file`; bodies return `errors.New("stub: <pkg>.<Func>")`).
   Real already: sentinels + `CorruptError`, the `Option` set, `record` constants/`Checksum`/`Verify`/header helpers +
   `FuzzRecord` seeds, `keydir.Entry` (+ 16-byte test) and `Sorted`, `manifest.Encode/Decode`, all of `fslock`, the `kvcli`
   flag sets, the `bench.Store` seam and the kvlite adapter. Verify `go build ./... ; go vet ./... ; go test -race ./... ;
   golangci-lint run ./... ; (cd bench; go build ./...)`.
2. **WS-A `internal/record`, `internal/fslock`, `internal/manifest`.** Encode/decode + crc32c + varints + limits; build-tagged
   lock; MANIFEST atomic swap; `TestLockedTwice` (second `Open` -> `ErrLocked`). Verify `go test ./internal/record/ -run . -fuzz
   FuzzRecord -fuzztime 60s` ; `go test ./internal/fslock/ ./internal/manifest/ -race`.
3. **WS-A `internal/segment`.** Writer/Reader/Scan/refcount, hint write+read with footer crc, rotation, `VerifySegment`.
   Verify `go test ./internal/segment/ -race -count=1`.
4. **WS-B `internal/keydir`.** Sharded map, `CompareAndSwap`, packed `Sorted`, `KeydirBytes`, 1M-key memory assertion via
   `runtime.ReadMemStats`. Verify `go test ./internal/keydir/ -race` ; `go test ./internal/keydir/ -bench . -benchmem`.
5. **WS-B `DB` core + `Batch`** (`db.go`, `options.go`, `errors.go`, `stats.go`, `batch.go`): `Open` happy path,
   `Put/Get/Delete/Has`, rotation, sync policies, fake-clock TTL, commit marker, group-fits-in-segment rule.
   Verify `go test . -race -run 'TestPutGet|TestTTL|TestRotate|TestBatch'`.
6. **WS-C `open.go`/`recovery.go`.** Hint-first load, scan fallback, tail truncation, `WithRepair`, orphan sweep.
   Verify `go test . -race -run 'TestRecover|TestTruncated|TestCorrupt' -count=3`.
7. **WS-C `snapshot.go`.** `Snapshot`/`Iter`/`Range`, pinning, snapshot-survives-compaction. Verify `go test . -race -run 'TestSnapshot|TestRange'`.
8. **WS-C `compact.go`.** The algorithm above, hints, MANIFEST swap, trash sweeper, `WithAutoCompact`, `TestCompactUnderWriters`
   (1 writer + 8 readers + compaction for 5 s, no lost keys). Verify `go test . -race -run TestCompact -count=3`.
9. **WS-D `internal/kvcli` + `FuzzDB`.** Eight subcommands against `t.TempDir()` with buffer `Env`s, plus the model fuzzer.
   Verify `go test ./internal/kvcli/ -race` ; `go test . -fuzz FuzzDB -fuzztime 120s`.
10. **WS-D benchmarks.** `internal/bench` plus the nested `bench/` module and `kvbench`. Verify `pwsh -NoProfile -File
    scripts/bench.ps1 -Pkg ./internal/bench/... -Count 6` ; `cd bench ; go run ./cmd/kvbench -engines kvlite,bbolt,pebble -n 200000`.
11. **Integrator - close out.** Wire `internal/cli/commands.go`, README with the measured table, goreleaser target, CI (root +
    nested module + fuzz smoke). Verify `go build ./... ; go vet ./... ; go test -race -count=1 ./... ; golangci-lint run ./...`.

## Fanout: 4 workstreams + integrator (exclusive file ownership)
- **Integrator** - `cmd/**`, `internal/cli/**`, `go.mod`/`go.sum`, `bench/go.mod`, `docs/**`, `README.md`, `CLAUDE.md`,
  `Taskfile.yml`, `.goreleaser.yaml`, `.github/**`, `scripts/**`, `.bench/**`. Steps 1 and 11; only it runs `go get`/`go mod tidy`.
- **WS-A "format"** - `internal/record/**`, `internal/segment/**`, `internal/manifest/**`, `internal/fslock/**`. Steps 2-3;
  publishes `internal/segment/testdata/` golden segments the other lanes read.
- **WS-B "engine"** - `internal/keydir/**` plus root-package `db.go`, `options.go`, `errors.go`, `stats.go`, `batch.go`. Steps 4-5.
- **WS-C "durability"** - root-package `open.go`, `recovery.go`, `snapshot.go`, `compact.go` (+ tests). Steps 6-8. Shares a
  package with WS-B, so ownership is **per file**; a needed change to a WS-B file is filed, not edited.
- **WS-D "cli + bench + fuzz"** - `internal/kvcli/**`, `internal/bench/**`, `bench/**` (except `go.mod`), `fuzz_test.go`,
  `testdata/fuzz/**`. Steps 9-10. Frozen after step 1 and changed only by the integrator together with this document:
  `keydir.Entry`, `record.Record` + the byte layout, `segment.Writer`/`Reader`, `kvlite.Stats`, the `Option` set.

## Alternatives considered
- **LSM (pebble-shaped) or B+tree (bbolt-shaped)**: better scans and bounded memory, but the roadmap asks for bitcask and those
  two are the baselines we measure against. Rejected.
- **Ordered keydir (B-tree / ART) from day one**: `Range` becomes O(log n) with no copy, but point ops lose 2-3x - the whole
  selling point - and the fuzz surface triples. Rejected for v1; the `index` seam keeps it a drop-in for P16.
- **mmap for reads**: fewer syscalls, but on Windows a mapped view blocks deletion, colliding head-on with compaction's reclaim
  step, and a fault on a truncated mapping is unrecoverable in Go. `ReadAt` + page cache is within noise at >= 1 KiB values.
- **Varint total length instead of a fixed `plen`**: ~3 bytes/record smaller, but forces byte-at-a-time reads before the bounds
  check and makes a torn header ambiguous. **A separate WAL for batches**: another file to fsync, order and recover, for no
  safety over an in-segment commit marker. **Eager TTL sweeper**: background I/O and a second writer path for no correctness
  gain. **`go.work` for the bench module**: implicit and easy to forget in CI; explicit `replace` is better. All rejected.

## Consequences
**Positive** - a `Get` is one map lookup plus one `ReadAt`; startup after a compaction is O(keys); zero dependencies; crash
semantics stated precisely and tested by construction; the format is fuzzable in isolation. **Negative** - the keydir holds
every key in RAM (~1.1 GB at 10M x 24-byte keys); range scans pay an O(keys) snapshot copy; write amplification is 2x for a key
that survives a compaction; a value sits in memory twice. **Follow-ups (P16)** - ordered index behind the `index` seam,
key-log/value-log split for large values, `io.Reader` streaming values, per-segment bloom filters, PGO on `kvbench`.

## Risks
- **Windows file locking and rename semantics.** A segment cannot be deleted while a handle is open, and Go's `os.Open` share
  mode does not reliably include `FILE_SHARE_DELETE`. Mitigation: never delete inline - refcount readers, rename into `trash/`,
  retry at the next compaction tick and at `Open`, treat a failed delete as a warning. `os.Rename` over `MANIFEST` works
  (MoveFileEx/REPLACE_EXISTING) but is not durable without a directory fsync NTFS does not offer - hence the MANIFEST crc and
  the `*.log` rescan fallback.
- **fsync cost.** ~1 ms per `FlushFileBuffers` here; `SyncAlways` caps writes near 1k ops/s and would make every published
  number meaningless. Mitigation: default `SyncNever`, recommend `SyncInterval(1s)`, print the policy in every bench table.
- **Keydir memory at 10M keys.** ~110 B/key (16 B entry + 16 B string header + key bytes + ~50 B map overhead) -> ~1.1 GB.
  Mitigation: measure in step 4, expose `KeydirBytes` in `Stats`/`kvlite stats`, note arena/interned keys as a P16 follow-up.
- **Clock for TTL.** Wall clock, so an NTP step or skewed host clock expires keys early or late and `time.Sleep` tests flake.
  Mitigation: `WithClock` in every test, expiry as an absolute unix second, documented "TTL is best-effort wall clock".
- **Compaction vs a concurrent writer.** Mitigation: merged output is only ever *adopted* by CAS on `(Seg, Off)`, so a key
  rewritten mid-merge keeps the newer active-segment record and the merged copy becomes dead bytes; `TestCompactUnderWriters`
  runs `-race -count=3` in CI. **Hint/log divergence** (a stale hint resurrecting old values): hints are written before the
  MANIFEST swap and only for segments immutable at that point, a hint for the active segment is ignored, and `kvlite verify`
  cross-checks every hint entry against the log it describes.

## Open questions for the owner - DECIDED 2026-09-19
1. **`Get` returns a fresh copy** (safe, one alloc per read). A pooled view / `GetInto(dst []byte)` is a P16 addition.
   Applied: `DB.Get`, `Snapshot.Get` and `Iter.Value` are documented as returning a copy.
2. **Value cap is 16 MiB** (`record.MaxValue = 16<<20`, `kvlite.MaxValueBytes`); `WithMaxValueBytes` can only lower it. The
   64 MiB / 1 GiB figures elsewhere in earlier drafts are superseded.
3. **Auto-compaction is OFF by default**, opt-in with `WithAutoCompact(deadRatio float64)`; `minDead` (64 MiB) and the tick
   (5 min) are the exported constants `AutoCompactMinDead` / `AutoCompactInterval`, not options. **`kvlite bench` runs only
   the dependency-free `internal/bench` workload**; the bbolt/pebble comparison is `kvbench` in the nested `bench/` module.

Scaffold deviations (integrator, step 1): `internal/cli/commands.go` is removed and `cmd/kvlite/main.go` registers
`kvcli.Commands()` because `kvcli -> cli` plus `cli -> kvcli` would be an import cycle; `-dir` is a per-command flag rather
than a global one; `record.MaxMeta` is the exact 31-byte worst case instead of the loose 16; `segment.Reader` gains
`ID()`/`Refs()`/`Close()` and `segment.Writer` gains `ID()`; `segment.HintEntry` carries `Key`; `keydir.Keydir` gains
`Bytes()` (backs `Stats.KeydirBytes`); `manifest` exposes `Encode/Decode/Load/Save/Orphans` + `Manifest{Gen, Active,
NextID, Segments}` and the `trash` dir name; `fslock` is `Acquire(path) (*Lock, error)` / `Lock.Release()` / `ErrLocked`.

Lane deviations (steps 2-10, recorded at close-out, 2026-09-19; all committed and green):
- **WS-A** `record` gained `Parse(b) (Record, n, error)` (decode one complete record from the front of a byte
  slice, telling truncated / too-large / checksum / malformed apart for the segment scanner) and `EncodedSize(r)`
  (exact on-disk size, used by the batch fit check);
  timestamps encode as the uvarint of the two's-complement millisecond value so every `time.Time` round-trips
  exactly; `segment.Reader` gained `ReadAtBuf`, `ScanFrom`, `Resync` and `Size`, `Writer` gained `Open` (reopen
  for append) and `Truncate`; the segment `ScanError{Tail bool}` tells a torn tail from mid-file corruption.
- **WS-B** the keydir applies a batch through one `Apply([]Update)` (all shards locked in order) so readers see
  all of a batch or none; `Stats.Repairs` was appended (WithRepair skip count). Measured keydir cost: 116 B/key
  at 1M x 16 B keys (`TestBytesAtScale`), under the 130 B budget.
- **WS-C** compaction reserves its output ids **before** rotating the active segment, so outputs sort below
  the new active segment and recovery's "ascending id = ascending write time" rule needs no per-record
  timestamp compare; a record that is **expired at load time is applied as a delete**, not skipped (skipping
  would let an older non-expiring value from an earlier segment resurrect); the five compaction stages
  (`rotated`/`merged`/`adopted`/`swapped`/`released`) are a test hook, each crash-tested.
- **WS-D** a batch has **no begin marker**: members carry `FlagBatchMember` and the trailing `FlagBatchCommit`
  marker holds `(count, batchID)`, so recovery buffers members until a marker with a matching count lands and
  drops the group otherwise; `kvbench`'s runner imports `internal/bench` (keys, values, `HiResNow`, percentiles,
  `DirSize`) so the kvlite-only and cross-engine numbers come from the same code - allowed because `bench/` is
  a nested module of the same repository path; `internal/bench` uses a QPC-based `HiResNow` on Windows because
  `time.Now`'s monotonic source steps at 0.5-15 ms there.
- **Integrator** the kvlite-only baseline is 200k keys on `Open` (not the ADR's 1M) so a `-count 10` run fits
  the `.bench/README.md` budget; `task fuzz` runs FuzzRecord 30 s, CI runs both fuzzers 20 s.

## Hardening (2026-09-19)

P16 pass over this repository (commits `13ff4b7..HEAD` on main). Baseline before touching anything: `go build`,
`go vet`, `golangci-lint run` (0 issues), `go test -race -count=3 ./...` all green; `cd bench; go build; go vet` green.

- **Vulnerabilities.** `govulncheck ./...` on the root module: none (zero dependencies). Bench module: GO-2026-5970
  (`golang.org/x/text` v0.14.0, infinite loop on invalid input) is present transitively via pebble and **not
  reachable** from `bench/`; left unbumped on purpose (the bench module pins the engines being compared).
- **Security review** (ASVS-lite; no network surface, so TLS/CORS/auth items do not apply). Fixed, each with a test:
  `put <key> -` read stdin with `io.ReadAll` and buffered an endless stream before the store could refuse it - now
  capped at `MaxValueBytes` (+1, 2x for hex/base64) and fails with `ErrValueTooLarge` (`TestPutStdinCapped`);
  segments, hints and MANIFEST were created 0644, so every value was world-readable on unix - now 0600
  (`segment.FileMode`, `manifest.FileMode`, `TestStoreFilesAreOwnerOnly`, mode assertions verified in
  `golang:1.27` on Linux); `kvlite verify` without a MANIFEST used `filepath.Glob` on the store path, so a directory
  named `data[1]` verified zero segments - now `os.ReadDir` (+ the `-deep` expiry check derives the log side through
  `record.ExpSec` like hints and the keydir do). Judged acceptable: error strings carry file paths (embedded
  library, no remote caller); `Resync`/`verify` are byte-granular and O(n * candidate) on a hostile file (repair
  and verify paths only, local files); every `uint32` conversion from disk is bounds-checked first (gosec on, nolint
  comments state the bound); the `-dir` flag is the user's own path (a local CLI, no traversal boundary).
- **Fuzz.** Targets and 60 s local runs: `FuzzRecord` 295,937 execs; `FuzzSegmentScan` (new) 9,900 - it writes a
  real file per input and Windows file I/O bounds it (Linux CI does ~7k/s); `FuzzHint` (new) 430,398; `FuzzParseName`
  (new) 11,078,416; `FuzzManifest` (new) 2,624,574; `FuzzCommitMarker` (new) 4,650,954; `FuzzDB` 465 (~8/s, as
  expected). **One crasher**, found by `FuzzSegmentScan` on CI's first run: a sealed segment whose last record
  fails its crc while its value contains a well-formed record. `Tail` is true, `VerifySegment` stops, but recovery
  under `WithRepair` resynced byte-wise into the corrupt record and adopted the decoy as a live key. Fixed in
  `scanSegment` (drop the tail, never resync past a `Tail` error), pinned by `TestRepairDecoyInCorruptTail` and the
  committed input `internal/segment/testdata/fuzz/FuzzSegmentScan/decoy-in-corrupt-tail`; the `Tail` contract in the
  segment package doc now says so. `task fuzz` and CI's fuzz-smoke job run every target.
- **Coverage** (statements, `-covermode=atomic`), core packages before -> after: root `kvlite` 87.3 -> 87.4 %,
  `record` 99.3 -> 99.3 %, `segment` 86.3 -> 86.4 %, `keydir` 100 -> 100 %, `manifest` 85.0 -> 85.0 %,
  `fslock` 91.7 -> 91.7 % (all already above the 80 % bar; `kvcli` 84.6 -> 88.1 %). No padding tests were added.
- **Perf follow-up done.** `segment.Reader` holds a small handle pool (`DefaultReadHandles`: min(NumCPU, 8) on
  Windows, 1 on unix) so parallel `Get`s no longer serialise on Go's per-handle `ReadAt` lock. A/B under the same
  load (`.bench/handles-2026-09-19-*.txt`): `Get/par8` -44 %, `par16` -60 %, `par1` unchanged. The full baseline
  was **not** refreshed: the box was shared with other test suites for the whole pass and a count=6 run on it would
  have replaced a clean reference with a noisy one. README's table keeps the committed baseline and says so.
- **CI.** Run 35477026324 (the fixes + fuzzers + pool) green on every job; run 35477852973 (docs) went red on
  fuzz-smoke with the crasher above, fixed in the next push (see the run id recorded in the final report). Jobs:
  test ubuntu + windows `-race`, lint, bench module, fuzz smoke of all seven targets, goreleaser check. Root suite
  also green in `golang:1.27` on Linux via Docker.
- **goreleaser.** `.goreleaser.yaml` v2 (linux/windows/darwin x amd64/arm64, `-trimpath -s -w`, version/commit/date
  stamped, archives + sha256 checksums); `goreleaser check` passes and `goreleaser release --snapshot --clean` builds
  all six targets locally (`kvlite -version` -> `0.0.1-next (de3da65, ...)`). No LICENSE file exists, so the archive
  glob `LICENSE*` matches nothing (a warning, not an error). No tag was created; `git tag v0.1.0 && git push --tags`
  would cut the first release.
