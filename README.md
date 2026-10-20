# kvlite

[![ci](https://github.com/crowbett/kvlite/actions/workflows/ci.yml/badge.svg)](https://github.com/crowbett/kvlite/actions/workflows/ci.yml) [![Go Reference](https://pkg.go.dev/badge/github.com/crowbett/kvlite.svg)](https://pkg.go.dev/github.com/crowbett/kvlite) [![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Embedded bitcask-style key-value store for Go, **zero dependencies**: append-only segments, an in-memory
keydir (one map lookup + one `ReadAt` per `Get`), hint files for O(keys) startup, crc32c on every record,
TTL, atomic batches, sorted snapshot iterators and crash-safe compaction. Library first, CLI second, benchmarked
against bbolt and pebble on the same workload (numbers below). Design record: `docs/PLAN.md` (ADR-0001).

```go
db, err := kvlite.Open("./data", kvlite.WithSync(kvlite.SyncInterval))
defer db.Close()
_ = db.Put([]byte("k"), []byte("v"))
v, err := db.Get([]byte("k"))                 // fresh copy; kvlite.ErrNotFound when absent or expired
_ = db.PutTTL([]byte("session"), v, time.Hour)
_ = db.Batch().Put([]byte("a"), v).Delete([]byte("k")).Commit()   // all-or-nothing, one commit marker
snap, _ := db.Snapshot(); defer snap.Close()  // pins the segments it references
for it := snap.Iter([]byte("prefix")); it.Next(); { k := it.Key(); v, _ := it.Value() }
_ = db.Compact(ctx)                           // merges, drops dead records, writes hints, swaps MANIFEST
```

## How it works

```
Put    validate -> encode (reusable buffer) -> wmu.Lock -> rotate if full -> Append -> keydir.Put -> [Sync] -> Unlock
Get    keydir.Get (shard RLock) -> expired? ErrNotFound -> reader(seg).ReadAt(off, size) -> crc -> copy out
Delete append a tombstone (survives a crash) -> keydir.Delete
Batch  members flagged batchMember + one trailing batchCommit{count, id}; recovery drops a group without its marker
Open   LOCK -> MANIFEST -> trash orphans -> per segment: valid .hint ? load : scan log -> truncate torn tail -> flusher
```

- **Keydir**: 64-way sharded `map[string]Entry`, `Entry` is 16 bytes and pointer-free (`Seg, Off, Size, ExpSec uint32`)
  so the GC never scans it. Measured 116 B/key at 1M x 16 B keys (`TestBytesAtScale`).
- **Records**: `[crc32c:4][plen:4][flags][ts uvarint][expiry uvarint?][klen][vlen][key][value]`, little-endian, crc over
  the payload only; `plen` is bounds-checked so a torn tail is unambiguous. Key <= 64 KiB, value <= 16 MiB.
- **Hints**: `NNNNNNNN.hint` per sealed segment with a footer crc + count; a bad or missing hint just means "scan the
  log" - always safe, never an error. Open at 200k keys: 57 ms with hints vs 79 ms scanning the logs (`BenchmarkOpen`).
- **Snapshots** materialise a sorted contiguous `(key, entry)` copy under shard RLocks and `Ref()` every segment they
  touch; `Iter(prefix)`/`Range(lo, hi)` binary-search that slice. Point ops are as fast as a Go map; scans pay O(keys).
- **Durability knob**: `SyncNever` (default, page cache), `SyncInterval` (flusher goroutine, 1 s), `SyncAlways`
  (`FlushFileBuffers` inside every write, ~1 ms on NTFS - see `PutSyncAlways`).

## On-disk format

`LOCK`, `MANIFEST`, `NNNNNNNN.log`, `NNNNNNNN.hint`, `trash/` (deferred deletes - NTFS refuses to unlink open files).

| File | Layout |
|---|---|
| segment header | `"KVLT"` \| version 1 \| segID u32 \| 7 zero bytes (16 B) |
| record | `[crc32c u32][plen u32][flags u8][tsMillis uvarint][expiryMillis uvarint if flag][keyLen][valLen][key][value]` |
| flags | b0 tombstone, b1 hasExpiry, b2 batchMember, b3 batchCommit (payload = uvarint count + uvarint batchID) |
| hint | `"KVLH"` header, per key `[flags][expSec][off][size][keyLen][key]`, footer `[crc32c(body)][count]["KVLH"]` |
| MANIFEST | `"KVLM"` \| 1 \| gen u64 \| active u32 \| nextID u32 \| count \| segID[count] \| crc32c; written as `.tmp` -> sync -> rename |

## Crash rules

1. Every record carries a crc32c; a record that fails at the **tail of the active segment** is a torn write: `Open`
   truncates the file there, logs a warning and continues (`TestTruncatedTail`, table-driven over cut points).
2. A bad record **anywhere else** is `ErrCorrupt` (`CorruptError{Seg, Off, Reason}`) unless `Open(WithRepair())`, which
   resyncs to the next decodable record and counts the skip in `Stats.Repairs`.
3. The live segment set is named by `MANIFEST`, replaced atomically. A compaction that dies before the rename leaves
   orphan output files; one that dies after leaves orphan inputs. `Open` moves either to `trash/`. Never a half-visible
   key set.
4. A batch without its commit marker is dropped whole at recovery; a record that is expired at load time is applied as
   a **delete**, not skipped, so an older non-expiring value in an earlier segment cannot resurrect.
5. Compaction reserves its output ids **before** rotating the active segment, so "ascending id = ascending write time"
   holds and recovery needs no per-record timestamp compare. Outputs are synced and hinted **before** the MANIFEST swap;
   the keydir is re-pointed by compare-and-swap on `(seg, off)` so a key overwritten mid-merge is never rolled back.
   The five stages (`rotated`/`merged`/`adopted`/`swapped`/`released`) are a test hook, each crash-tested.

## API

| | |
|---|---|
| `Open(dir, opts...)` | `WithMaxSegmentBytes` (256 MiB), `WithSync(SyncNever\|SyncInterval\|SyncAlways)`, `WithSyncInterval` (1 s), `WithReadOnly`, `WithRepair`, `WithAutoCompact(deadRatio)` (off; fires at >= 64 MiB dead, every 5 min), `WithMaxKeyBytes`/`WithMaxValueBytes` (lower only), `WithShards` (64), `WithLogger`, `WithClock` |
| `Get` `Has` `Put` `PutTTL` `Delete` | point ops; `ttl <= 0` -> `ErrInvalidTTL`; expiry is lazy (the read path returns `ErrNotFound`, compaction drops it) |
| `Batch().Put/PutTTL/Delete(...).Commit()` | one contiguous record group + commit marker; never spans two segments |
| `Snapshot()` -> `Get` `Len` `Iter(prefix)` `Range(lo, hi)` `Close` | `Iter.Key()` valid until `Next()`, `Value()` reads lazily from the pinned segment |
| `Compact(ctx)` `Sync()` `Stats()` `Close()` | `Close` waits up to 5 s for open snapshots, then force-closes them |
| sentinels | `ErrNotFound ErrClosed ErrCorrupt ErrKeyTooLarge ErrValueTooLarge ErrEmptyKey ErrLocked ErrReadOnly ErrInvalidTTL` (all `errors.Is`) |

## CLI

```powershell
go install ./cmd/kvlite            # or: task build
kvlite put -dir ./data k v ; kvlite get -dir ./data k ; kvlite del -dir ./data k
kvlite put -dir ./data -ttl 1h session - < token.bin          # value from stdin
kvlite scan -dir ./data -json ; kvlite scan -dir ./data -lo a -hi m -keys-only
kvlite stats -dir ./data -json ; kvlite compact -dir ./data -timeout 30s
kvlite verify -dir ./data -deep    # crc per record + every hint entry vs its log record; exit 1 on any finding
kvlite bench -n 200000 -value 1024 -readers 8 -random
```
Values are raw bytes unless `-hex`/`-base64`; `-` is stdin/stdout. Exit codes: `0` ok, `1` error (incl. not found
and verify findings), `2` usage, `130` interrupted (`scan`/`compact` honour Ctrl+C).

## Benchmarks

Machine: AMD Ryzen 7 3700X (8c/16t), Windows 11 / NTFS, Go 1.27.0. Every number below is copied from a committed
file (`.bench/baseline.txt`, `.bench/compare-2026-09-19*.txt`); nothing is typed by hand. `time.Now` steps at
366 us on this box, so the workload harness times each op with `QueryPerformanceCounter` (`internal/bench.HiResNow`).

### kvlite alone (`scripts/bench.ps1 -Save baseline -Count 6`, `SyncNever`, 200k keys)

`benchstat .bench/baseline.txt` (each op is one call on one goroutine unless `par*`; Scan and Open are whole-store ops):

| benchmark | time/op | allocs/op | note |
|---|---:|---:|---|
| Put seq / rand, 100 B | 5.05 µs ± 15% / 5.27 µs | 3 / 4 | 18.9 MiB/s |
| Put seq / rand, 1 KiB | 6.74 µs ± 14% / 6.67 µs | 3 / 4 | 145 MiB/s |
| Put seq / rand, 16 KiB | 24.9 µs ± 10% / 24.7 µs | 3 / 4 | 627 MiB/s |
| PutSyncAlways, 1 KiB | 391 µs ± 5% | 3 | the NTFS `FlushFileBuffers` floor |
| Get seq / rand, 100 B | 3.50 µs ± 11% / 6.04 µs | 1 | one `pread` against the page cache |
| Get seq / rand, 1 KiB | 3.96 µs ± 1% / 6.57 µs | 1 | |
| Get seq / rand, 16 KiB | 21.7 µs ± 5% / 24.9 µs | 1 | |
| Get miss | 67.9 ns ± 1% | 0 | keydir only |
| Get par1 / par8 / par16, 1 KiB | 6.56 / 13.0 / 13.1 µs | 1 | measured **before** the read handle pool - see the reading below |
| Delete | 4.05 µs ± 8% | 0 | tombstone append |
| Scan 200k keys, 100 B / 1 KiB / 16 KiB | 346 / 397 / 165 ms | 100k / 100k / 8.4k | snapshot copy + one read per key |
| Scan prefix (1/16 of the keys), 1 KiB | 28.4 ms ± 7% | 10 | binary search into the sorted snapshot |
| Open 200k keys, hints / no hints | 56.9 ms ± 36% / 79.2 ms ± 16% | 202k | hint load vs full log scan |
| Batch of 100 puts, 100 B | 477 µs ± 18% | 314 | ~4.8 µs per member |

### kvlite vs bbolt vs pebble (`bench/cmd/kvbench`, same workload, one process per engine, default sync policy of each)

`.bench/compare-2026-09-19.txt` (n = 200k keys per size, put -> get -> scan -> delete half, then compact) and
`.bench/compare-2026-09-19-16k.txt` (n = 20k). bbolt v1.5.0, pebble v1.1.5, all three with fsync off.

| value | op | kvlite ops/s (p50 / p99) | bbolt ops/s (p50 / p99) | pebble ops/s (p50 / p99) |
|---|---|---:|---:|---:|
| 100 B | put | **205,834** (4.0 / 19.1 µs) | 22,690 (38.5 / 101.8 µs) | **726,564** (1.1 / 1.9 µs) |
| 100 B | get | 269,452 (3.3 / 7.4 µs) | **888,139** (0.8 / 3.4 µs) | 57,816 (16.0 / 36.6 µs) |
| 100 B | scan | 278,358 (3.1 / 5.6 µs) | **7,992,327** (0.1 / 0.3 µs) | 2,790,712 (0.2 / 5.0 µs) |
| 100 B | delete | 227,545 (3.9 / 7.9 µs) | 17,266 (51.4 / 127.9 µs) | **820,314** (1.1 / 1.5 µs) |
| 1 KiB | put | 144,258 (4.7 / 55.0 µs) | 16,512 (54.7 / 165.9 µs) | **272,272** (1.2 / 3.0 µs) |
| 1 KiB | get | 218,022 (4.0 / 10.3 µs) | **469,632** (1.6 / 6.5 µs) | 50,577 (16.8 / 48.4 µs) |
| 1 KiB | scan | 229,832 (3.7 / 8.9 µs) | **6,865,939** (0.1 / 0.4 µs) | 558,166 (0.3 / 8.9 µs) |
| 1 KiB | delete | 215,896 (4.0 / 10.1 µs) | 15,632 (57.5 / 156.3 µs) | **820,953** (1.0 / 1.9 µs) |
| 16 KiB | put | **37,829** (12.1 / 93.9 µs) | 9,602 (84.8 / 397.7 µs) | 12,179 (5.7 / 56.4 µs) |
| 16 KiB | get | 83,591 (8.6 / 75.2 µs) | **96,603** (8.1 / 73.2 µs) | 31,781 (28.3 / 175.5 µs) |
| 16 KiB | scan | 90,777 (8.3 / 61.1 µs) | **3,611,086** (0.2 / 0.7 µs) | 97,941 (8.0 / 78.4 µs) |
| 16 KiB | delete | 232,834 (3.9 / 6.7 µs) | 9,799 (84.6 / 405.9 µs) | **904,584** (0.9 / 1.7 µs) |

| value | engine | disk after workload | after compaction | ratio |
|---|---|---:|---:|---:|
| 100 B | kvlite / bbolt / pebble | 28.5 / 64.0 / 18.6 MiB | 15.1 / 64.0 (no compaction) / 15.7 MiB | 1.89x / 1.00x / 1.18x |
| 1 KiB | kvlite / bbolt / pebble | 204.9 / 512.0 / 79.6 MiB | 103.3 / 512.0 / 45.5 MiB | 1.98x / 1.00x / 1.75x |
| 16 KiB | kvlite / bbolt / pebble | 313.5 / 512.0 / 327.7 MiB | 156.8 / 512.0 / 173.8 MiB | 2.00x / 1.00x / 1.89x |

Reading it honestly:

- **Writes**: kvlite beats bbolt by 9x (bbolt rewrites B+tree pages and its mmap file grows in 64 MiB steps) and loses to
  pebble by 2-3.5x at small values. Pebble batches into a memtable and never touches the disk on the put path; kvlite
  does one `WriteFile` per put, so the ~4 µs is the Windows syscall, not the encoder (3 allocs, 152 B/op). At 16 KiB
  the picture flips (kvlite 3x pebble): one contiguous append beats a memtable flush + L0 write.
- **Point reads**: bbolt wins 2-3x because a hit is a memcpy out of the mmap; kvlite pays one `pread` per `Get` and
  ~3.5-6 µs is the Windows `ReadFile` floor. Pebble is 4-5x slower than kvlite here (block cache + bloom + LSM levels).
  At 16 KiB the read is dominated by the copy and kvlite and bbolt tie.
- **Scans**: bbolt's cursor walks mmap'd leaf pages and is in a different league (8M keys/s). kvlite's snapshot iterator
  is one `pread` per key, so a scan costs the same as a get; that is the stated v1 trade-off (point ops first).
- **Parallel reads on Windows**: the baseline above (`Get/par8` 13.0 µs vs `par1` 6.6 µs; the CLI bench with 8 readers
  showed a 682 µs p99) predates the per-segment read handle pool. Go's `os.File.ReadAt` on Windows takes the handle's
  read **and** write lock around `ReadFile` (`internal/poll.FD.Pread` -> `readWriteLock`), so every reader of one
  segment used to serialise on one lock; a `segment.Reader` now opens min(NumCPU, 8) handles on Windows (1 on unix,
  where `pread` is lock-free) and picks one round-robin per read. A/B on the same box and load
  (`.bench/handles-2026-09-19-{before,after}.txt`, `-benchstat.txt`, count=6): `par8` 24.3 -> 13.6 µs (-44 %, p=0.015),
  `par16` 33.2 -> 13.2 µs (-60 %, p=0.002), `par1` unchanged. The box was loaded during that run (wide CIs), so the
  full baseline table has not been refreshed yet - see follow-ups.
- **Disk**: the workload leaves kvlite with ~2x its live size (every delete is a tombstone, every overwrite a dead record)
  until `Compact`, which brings it to live size + hints. Pebble's LSM is 1.2-1.9x for the same reason. bbolt never
  shrinks its file; 64 MiB for 20 MiB of data at 100 B, 512 MiB at 1 KiB.
- The `kvlite bench` CLI on the same box (8 readers, random keys, 1 KiB): put 139k/s, get 118k/s, get-miss 13.8M/s,
  scan 121k/s, delete 191k/s; `kvlite verify -deep` on the resulting store: 100,000 records, hint ok, 0 findings.

## Testing

```powershell
task test            # go test -race -count=1 ./... (root) ; cd bench ; go test -race ./...
task fuzz            # every parser 30 s each: FuzzRecord, FuzzSegmentScan, FuzzHint, FuzzParseName, FuzzManifest, FuzzCommitMarker
go test -run '^$' -fuzz='^FuzzDB$' -fuzztime=60s .        # op-script fuzz against a map model, with reopen
task compare         # cross-engine go-test benchmarks ; task kvbench -- -n 200000 -sizes 100,1024
```

- `FuzzRecord`: `Decode(Append(r))` field-identical; arbitrary bytes never panic and never alias outside the input.
- `FuzzSegmentScan`: arbitrary bytes as a segment file through `OpenReader`, a repairing `Scan`/`Resync`/`ScanFrom`
  walk, `ReadAt` and `VerifySegment`: every yielded record re-parses from its offset, `Tail=false` always has a record
  behind it, verify counts what the walk counted. `FuzzHint`/`FuzzManifest`: parse never panics, honours the footer
  count / length before allocating, round-trips through the encoder (the MANIFEST encoding is canonical).
  `FuzzParseName`, `FuzzCommitMarker`: the name and commit-marker parsers against their printers.
- `FuzzDB`: the corpus decodes into an op script (put/puttl/del/batch/compact/reopen) run against a real DB and a
  `map[string]string` model on a fake clock; every model key must match after every op and every reopen.
- Crash tests: torn tail at every cut point, corrupt mid-file with and without `WithRepair`, five compaction stages,
  batch without marker, lock contention, orphan sweep, evict-while-reading.
- CI (`.github/workflows/ci.yml`): ubuntu + windows `-race`, lint v2 + zero-dependency guard on the root `go.mod`,
  bench module build/test + a 2k-key kvbench smoke, 20 s of every fuzzer, `goreleaser check`.

## Follow-ups

- Keydir behind an `index` interface: an ART or B-tree would make `Snapshot()` O(1) instead of an O(keys) copy.
- Refresh `.bench/baseline.txt` on a quiet box (`scripts/bench.ps1 -Save baseline -Count 6`) so the table above reflects
  the read handle pool; an mmap read path would remove the per-read syscall entirely, but `Get` at ~3.5 us
  single-threaded is already the `ReadFile` floor.
- Space amplification before compaction is ~2x under a delete-heavy workload (tombstones and overwritten records stay
  until the next merge); `WithAutoCompact` bounds it, a size-tiered trigger per segment would bound it tighter.
- Values stream neither in nor out (16 MiB cap, fully in memory).

## License

MIT, see [LICENSE](LICENSE).
