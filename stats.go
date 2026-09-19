// STUB written by the integrator to freeze the contract; workstream B (engine) owns this file and replaces
// the bodies, keeping every exported signature. Stats is a frozen contract (docs/PLAN.md): fields may be
// added at the end, never renamed or removed.

package kvlite

import "time"

// Stats is a point-in-time snapshot of the store's counters, returned by DB.Stats.
type Stats struct {
	// Keys is the number of live (non-deleted, non-expired at last check) keys in the keydir.
	Keys int
	// Segments is the number of segment files named by the MANIFEST, including the active one.
	Segments int
	// ActiveSegment is the id of the segment currently receiving appends.
	ActiveSegment uint32
	// DeadRatio is DeadBytes / TotalBytes (0 when TotalBytes is 0).
	DeadRatio float64

	// LiveBytes is the sum of record sizes referenced by the keydir.
	LiveBytes int64
	// DeadBytes is TotalBytes - LiveBytes: superseded, deleted and expired records.
	DeadBytes int64
	// TotalBytes is the on-disk size of every segment file.
	TotalBytes int64
	// KeydirBytes estimates the keydir's heap footprint: shards + entries + key bytes.
	KeydirBytes int64
	// ReclaimedBytes is the cumulative number of bytes freed by compaction.
	ReclaimedBytes int64

	// Puts counts successful Put/PutTTL calls and batch members.
	Puts uint64
	// Deletes counts successful Delete calls and batch deletes.
	Deletes uint64
	// Gets counts Get/Has calls.
	Gets uint64
	// Hits counts Gets that found a live value.
	Hits uint64
	// Misses counts Gets that found no key.
	Misses uint64
	// ExpiredHits counts Gets that found a key whose TTL had passed (reported as ErrNotFound).
	ExpiredHits uint64
	// Syncs counts fsync calls (explicit, per-write and interval).
	Syncs uint64
	// Compactions counts completed compactions.
	Compactions uint64

	// LastCompaction is when the last compaction finished (zero if never).
	LastCompaction time.Time
	// LastCompactionDur is how long the last compaction took.
	LastCompactionDur time.Duration
}
