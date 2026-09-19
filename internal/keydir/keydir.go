// STUB written by the integrator to freeze the contract; workstream B (engine) owns this file and replaces
// the bodies, keeping every exported signature. Entry, Entry.Expired, New, Len and Sorted are real and
// tested; Get/Put/Delete/CompareAndSwap/Snapshot/Bytes are stubs.

// Package keydir is kvlite's in-memory index: a sharded map[string]Entry with a pointer-free 16-byte
// entry so the GC never scans it, an RWMutex per shard so point reads scale across cores, and a packed
// sorted snapshot for ordered iteration. It imports nothing from the rest of the module.
package keydir

import (
	"bytes"
	"sort"
	"sync"
	"time"
)

// DefaultShards is the shard count used when New is given 0.
const DefaultShards = 64

// Entry locates one live record. It is exactly 16 bytes and contains no pointers (frozen contract).
type Entry struct {
	// Seg is the segment id, Off the record's absolute offset in it, Size its full on-disk length.
	Seg, Off, Size uint32
	// ExpSec is the expiry as unix seconds; 0 means never. Valid until 2106.
	ExpSec uint32
}

// Expired reports whether the entry's TTL has passed at now. Entries without a TTL never expire.
func (e Entry) Expired(now time.Time) bool {
	if e.ExpSec == 0 {
		return false
	}
	sec := now.Unix()
	return sec >= 0 && uint64(sec) >= uint64(e.ExpSec)
}

type shard struct {
	mu sync.RWMutex
	m  map[string]Entry
}

// Keydir is the sharded index. All methods are safe for concurrent use.
type Keydir struct {
	shards []shard
	mask   uint32
}

// New allocates a Keydir with shards shards (a power of two; 0 = DefaultShards) and a per-shard map
// capacity hint of hint/shards. It panics on a non-power-of-two count, which package kvlite validates
// in WithShards before it gets here.
func New(shards, hint int) *Keydir {
	if shards == 0 {
		shards = DefaultShards
	}
	if shards < 1 || shards&(shards-1) != 0 {
		panic("keydir: shard count must be a power of two")
	}
	k := &Keydir{shards: make([]shard, shards), mask: uint32(shards - 1)}
	per := hint / shards
	for i := range k.shards {
		k.shards[i].m = make(map[string]Entry, per)
	}
	return k
}

// Len returns the number of entries across all shards.
func (k *Keydir) Len() int {
	n := 0
	for i := range k.shards {
		s := &k.shards[i]
		s.mu.RLock()
		n += len(s.m)
		s.mu.RUnlock()
	}
	return n
}

// Get looks key up under the shard's read lock. It does not check expiry; callers use Entry.Expired.
func (k *Keydir) Get(key []byte) (Entry, bool) {
	_ = key
	_ = k.mask
	return Entry{}, false
}

// Put inserts or replaces key and returns the previous entry, if any (its Size becomes dead bytes).
func (k *Keydir) Put(key []byte, e Entry) (old Entry, existed bool) {
	_, _ = key, e
	_ = k.mask
	return Entry{}, false
}

// Delete removes key and returns the previous entry, if any.
func (k *Keydir) Delete(key []byte) (old Entry, existed bool) {
	_ = key
	_ = k.mask
	return Entry{}, false
}

// CompareAndSwap re-points key from from to to only if its current entry is still exactly from. It
// returns false when the key was rewritten or deleted mid-merge, in which case the merged copy is
// simply dead bytes.
func (k *Keydir) CompareAndSwap(key []byte, from, to Entry) bool {
	_, _, _ = key, from, to
	_ = k.mask
	return false
}

// Snapshot returns a sorted, contiguous copy of every (key, entry) whose key starts with prefix and
// whose TTL has not passed at now. Shards are read-locked one at a time, so the copy is consistent
// per shard, not globally; package kvlite takes it under the writer mutex when it needs more.
func (k *Keydir) Snapshot(prefix []byte, now time.Time) Sorted {
	_, _ = prefix, now
	_ = k.mask
	return Sorted{}
}

// Bytes estimates the heap footprint: shards + map overhead + entries + key bytes (Stats.KeydirBytes).
func (k *Keydir) Bytes() int64 {
	_ = k.mask
	return 0
}

// Sorted is a packed, immutable, key-ordered view: all keys in one byte slice, offs[i]..offs[i+1]
// delimiting key i, and ents[i] its Entry. Zero value is empty.
type Sorted struct {
	keys []byte
	offs []uint32 // len(ents)+1 boundaries
	ents []Entry
}

// newSorted packs already-sorted keys and entries; keys must be in ascending order.
func newSorted(keys [][]byte, ents []Entry) Sorted {
	s := Sorted{offs: make([]uint32, 1, len(keys)+1), ents: ents}
	for _, k := range keys {
		s.keys = append(s.keys, k...)
		s.offs = append(s.offs, uint32(len(s.keys))) //nolint:gosec // total key bytes < 4 GiB by construction
	}
	return s
}

// Len returns the number of keys.
func (s Sorted) Len() int { return len(s.ents) }

// Key returns key i, aliasing the packed buffer; do not modify.
func (s Sorted) Key(i int) []byte { return s.keys[s.offs[i]:s.offs[i+1]] }

// Entry returns entry i.
func (s Sorted) Entry(i int) Entry { return s.ents[i] }

// Search returns the smallest i such that Key(i) >= key, or Len() when no such key exists
// (sort.Search semantics; the seek primitive behind Iter and Range).
func (s Sorted) Search(key []byte) int {
	return sort.Search(s.Len(), func(i int) bool { return bytes.Compare(s.Key(i), key) >= 0 })
}
