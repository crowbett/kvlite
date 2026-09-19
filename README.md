# kvlite

Embedded bitcask-style key-value store for Go: append-only segments, in-memory keydir, hint files,
crc32c on every record, TTL, atomic batches, sorted snapshots and crash-safe compaction. Zero dependencies.
**Status: scaffold - the API is frozen, the bodies are stubs** (see `docs/PLAN.md`, ADR-0001).

```go
db, err := kvlite.Open("./data", kvlite.WithSync(kvlite.SyncInterval))
defer db.Close()
_ = db.Put([]byte("k"), []byte("v"))
v, err := db.Get([]byte("k"))                 // fresh copy; kvlite.ErrNotFound when absent/expired
_ = db.PutTTL([]byte("session"), v, time.Hour)
_ = db.Batch().Put([]byte("a"), v).Delete([]byte("k")).Commit()
snap, _ := db.Snapshot(); defer snap.Close()
for it := snap.Iter([]byte("prefix")); it.Next(); { _, _ = it.Key(), it.Value }
_ = db.Compact(ctx)
```

## CLI

```powershell
go run ./cmd/kvlite put -dir ./data k v ; go run ./cmd/kvlite get -dir ./data k
go run ./cmd/kvlite scan -dir ./data -json ; go run ./cmd/kvlite stats -dir ./data
go run ./cmd/kvlite compact|verify|bench -dir ./data
```
Exit codes: `0` ok, `1` error (incl. key not found / verify findings), `2` usage, `130` interrupted.
`task fuzz` fuzzes the record format; `task compare` (nested `bench/` module) runs kvlite vs bbolt vs pebble.
