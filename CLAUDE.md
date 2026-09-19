# kvlite

Embedded bitcask-style key-value store (library, root package `kvlite`) plus a CLI (`cmd/kvlite`).
Module `github.com/crowbett/kvlite`, Go 1.27, **zero dependencies in the root `go.mod`**.
Global Go conventions from `~/.claude/CLAUDE.md` apply; this file is the repo-specific part.
The design is ADR-0001 in `docs/PLAN.md` - read it before touching anything below `internal/`.

## Layout
```
db.go options.go errors.go stats.go batch.go     package kvlite: public API, writer path, TTL      (WS-B)
open.go recovery.go snapshot.go compact.go       package kvlite: Open/recover, Snapshot, Compact  (WS-C)
fuzz_test.go                                     FuzzDB model fuzzer                              (WS-D)
internal/record/    record encode/decode, crc32c, limits, FuzzRecord                              (WS-A)
internal/segment/   segment + hint files: Writer/Reader/Scan/refcount/VerifySegment              (WS-A)
internal/manifest/  MANIFEST encode/decode, atomic swap, orphan detection                         (WS-A)
internal/fslock/    LOCK file (Windows share-mode 0 / unix flock / O_EXCL fallback)              (WS-A)
internal/keydir/    sharded map, 16-byte pointer-free Entry, CompareAndSwap, packed Sorted       (WS-B)
internal/kvcli/     get/put/del/scan/stats/compact/verify/bench on top of internal/cli           (WS-D)
internal/bench/     kvlite-only benchmarks + the Run workload behind `kvlite bench`              (WS-D)
internal/cli/       template subcommand framework (App/Command/Env/ErrUsage); unchanged          (integrator)
cmd/kvlite/         wiring only: NotifyContext -> cli.New -> Register(kvcli.Commands()) -> Main   (integrator)
bench/              NESTED MODULE: kv.Store + kvlite/bbolt/pebble adapters + cmd/kvbench          (WS-D; go.mod integrator)
scripts/bench.ps1 .bench/   perf-lab style benchmark runner + committed baseline                  (integrator)
```
Imports flow one way: `kvlite -> {record, segment, keydir, manifest, fslock}`, `segment -> record`,
`keydir`/`manifest`/`fslock` import nothing, `kvcli -> {kvlite, cli, bench}`. No `util`, no `pkg/`.

## Crash rules (do not weaken)
- Every record carries a crc32c over a length-prefixed payload; `plen > record.MaxPayload` is corruption.
- A bad record at the **tail of the active segment** is a torn write: truncate on `Open`, warn, continue.
  A bad record anywhere else is `ErrCorrupt` (a `*CorruptError` with Seg/Off) unless `WithRepair()`.
- The live segment set is named only by `MANIFEST`, replaced atomically (`MANIFEST.tmp` -> Sync -> Rename).
  Compaction writes hints and fsyncs them **before** the swap; a crash on either side leaves orphans that
  the next `Open` moves to `trash/`, never a half-visible key set.
- Compaction adopts merged records only by `keydir.CompareAndSwap` on `(Seg, Off)`; a key rewritten
  mid-merge keeps the newer record. `Delete` always appends a tombstone. Tombstones never merge forward.
- Never delete a segment inline on Windows: refcount readers, rename into `trash/`, retry later.
- TTL is best-effort wall clock via `WithClock`; every TTL test injects the clock, never sleeps.

## Frozen contracts (change only together with docs/PLAN.md, by the integrator)
`keydir.Entry` (16 bytes, no pointers), `record.Record` + the byte layout and constants, `segment.Writer`/
`Reader`, `kvlite.Stats`, the `Option` set and every sentinel in `errors.go`. Every stub file starts with
`// STUB written by the integrator ...; workstream X owns this file` - replace bodies, keep signatures.

## Per-file ownership (fanout)
- **WS-A format**: `internal/record/**`, `internal/segment/**`, `internal/manifest/**`, `internal/fslock/**`.
- **WS-B engine**: `internal/keydir/**`, `db.go`, `options.go`, `errors.go`, `stats.go`, `batch.go`.
- **WS-C durability**: `open.go`, `recovery.go`, `snapshot.go`, `compact.go` (+ their tests). Shares the
  package with WS-B: a needed change to a WS-B file is filed, not edited.
- **WS-D cli+bench+fuzz**: `internal/kvcli/**`, `internal/bench/**`, `bench/**` (not `go.mod`),
  `fuzz_test.go`, `testdata/fuzz/**`.
- **Integrator**: `cmd/**`, `internal/cli/**`, `go.mod`/`go.sum`, `bench/go.mod`, `docs/**`, `README.md`,
  `CLAUDE.md`, `Taskfile.yml`, `.goreleaser.yaml`, `.github/**`, `scripts/**`, `.bench/**`. Only the
  integrator runs `go get` / `go mod tidy`.

## Conventions specific to this repo
- Commands: `func xxxCommand() *cli.Command` in `internal/kvcli/commands.go`, appended to `Commands()`;
  flags via closure vars, `-dir` on every command, output to `env.Stdout`, usage mistakes wrapped so
  `errors.Is(err, cli.ErrUsage)` (exit 2). Never `os.Stdout`/`os.Exit` inside a command; honour `ctx`.
- Tests: table-driven, `t.Parallel()`, `t.TempDir()`, buffer `cli.Env`s; no testify, no mocks.
- Benchmarks: `internal/bench` is perf-lab style. `scripts/bench.ps1 -Save baseline` only in a dedicated
  `perf(bench): refresh baseline` commit; unsaved runs go to gitignored `results/bench/`.
- `bench/` is a separate module: `cd bench` before `go build`/`go test`; root `go build ./...` must never
  pull bbolt or pebble.
- Commit messages are Conventional Commits (hookify enforces).

## Commands (Taskfile.yml)
`task build|install|run -- <args>`, `task test`, `task test:race`, `task vet`, `task lint`, `task fmt`,
`task fuzz` (FuzzRecord 30 s), `task bench` (internal/bench), `task compare` (bench/ module, all engines),
`task check`, `task release:check`, `task release:snapshot`.

## Definition of done
`go build ./...`, `go vet ./...`, `go test -race -count=1 ./...`, `golangci-lint run ./...` (0 issues),
`task fuzz` green, `(cd bench; go build ./...; go vet ./...)` and `task compare` run, `goreleaser check` if
`.goreleaser.yaml` changed. Say which you ran. A workstream is done when its stub bodies are gone
(`rg -n "stub:" <its files>` is empty) and its PLAN.md verify line passes.
