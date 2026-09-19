// Package keydir is kvlite's in-memory index: a sharded map[string]Entry with a pointer-free 16-byte
// entry so the GC never scans it, an RWMutex per shard so point reads scale across cores, and a packed
// sorted snapshot for ordered iteration. It imports nothing from the rest of the module.
//
// Workstream B (engine) owns this file. Entry, Sorted and the constructor are a frozen contract
// (docs/PLAN.md); Apply and Update are WS-B additions used by Batch.Commit.
package keydir

import (
	"bytes"
	"hash/maphash"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
	"unsafe"
)

// DefaultShards is the shard count used when New is given 0.
const DefaultShards = 64

// entryOverhead is the estimated heap cost of one map slot beyond the key bytes: the 16-byte Entry,
// the 16-byte string header, the key's own allocation header and the swiss-map control byte and
// load-factor slack (~7/8 occupancy plus growth headroom). Calibrated against runtime.ReadMemStats in
// TestBytesAtScale (measured 115 B/key at 24-byte keys on go1.27/amd64).
const entryOverhead = 16 + 16 + 56

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
	mu       sync.RWMutex
	m        map[string]Entry
	keyBytes int64 // sum of len(key) over m, for Bytes
}

// Keydir is the sharded index. All methods are safe for concurrent use.
type Keydir struct {
	shards []shard
	mask   uint32
	seed   maphash.Seed
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
	k := &Keydir{shards: make([]shard, shards), mask: uint32(shards - 1), seed: maphash.MakeSeed()}
	per := hint / shards
	for i := range k.shards {
		k.shards[i].m = make(map[string]Entry, per)
	}
	return k
}

// shardFor picks the shard by a runtime memhash of key: allocation-free and well mixed, so the low
// bits are usable directly.
func (k *Keydir) shardFor(key []byte) *shard {
	h := maphash.Bytes(k.seed, key)
	return &k.shards[uint32(h)&k.mask] //nolint:gosec // truncation is the intent; mask bounds the index
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
// It performs no allocation: the map index uses the compiler's []byte-to-string lookup form.
func (k *Keydir) Get(key []byte) (Entry, bool) {
	s := k.shardFor(key)
	s.mu.RLock()
	e, ok := s.m[string(key)]
	s.mu.RUnlock()
	return e, ok
}

// Put inserts or replaces key and returns the previous entry, if any (its Size becomes dead bytes).
func (k *Keydir) Put(key []byte, e Entry) (old Entry, existed bool) {
	s := k.shardFor(key)
	s.mu.Lock()
	old, existed = s.put(key, e)
	s.mu.Unlock()
	return old, existed
}

// put is Put with the shard lock held.
func (s *shard) put(key []byte, e Entry) (old Entry, existed bool) {
	old, existed = s.m[string(key)]
	if !existed {
		s.keyBytes += int64(len(key))
	}
	s.m[string(key)] = e
	return old, existed
}

// Delete removes key and returns the previous entry, if any.
func (k *Keydir) Delete(key []byte) (old Entry, existed bool) {
	s := k.shardFor(key)
	s.mu.Lock()
	old, existed = s.del(key)
	s.mu.Unlock()
	return old, existed
}

// del is Delete with the shard lock held.
func (s *shard) del(key []byte) (old Entry, existed bool) {
	old, existed = s.m[string(key)]
	if existed {
		delete(s.m, string(key))
		s.keyBytes -= int64(len(key))
	}
	return old, existed
}

// CompareAndSwap re-points key from from to to only if its current entry is still exactly from. It
// returns false when the key was rewritten or deleted mid-merge, in which case the merged copy is
// simply dead bytes.
func (k *Keydir) CompareAndSwap(key []byte, from, to Entry) bool {
	s := k.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.m[string(key)]
	if !ok || cur != from {
		return false
	}
	s.m[string(key)] = to
	return true
}

// Update is one mutation for Apply: a Put of Entry, or a Delete when Delete is set. Apply fills Old
// and Existed with the entry the key had before, exactly as Put/Delete return them.
type Update struct {
	Key    []byte
	Entry  Entry
	Delete bool

	// Old and Existed are outputs.
	Old     Entry
	Existed bool
}

// Apply performs ups in order as one atomic step with respect to readers: every shard the updates
// touch is write-locked (in shard order, so concurrent Applies cannot deadlock) before the first
// mutation and released after the last, so a Get can never observe a half-applied group. Package
// kvlite calls it from Batch.Commit under the writer mutex. A key repeated in ups sees its earlier
// update as Old.
func (k *Keydir) Apply(ups []Update) {
	if len(ups) == 0 {
		return
	}
	// Collect the distinct shard indices, ascending.
	idx := make([]uint32, 0, min(len(ups), len(k.shards)))
	for i := range ups {
		idx = append(idx, uint32(maphash.Bytes(k.seed, ups[i].Key))&k.mask) //nolint:gosec // truncation intended
	}
	slices.Sort(idx)
	idx = slices.Compact(idx)
	for _, i := range idx {
		k.shards[i].mu.Lock()
	}
	for i := range ups {
		u := &ups[i]
		s := k.shardFor(u.Key)
		if u.Delete {
			u.Old, u.Existed = s.del(u.Key)
		} else {
			u.Old, u.Existed = s.put(u.Key, u.Entry)
		}
	}
	for _, i := range idx {
		k.shards[i].mu.Unlock()
	}
}

// Snapshot returns a sorted, contiguous copy of every (key, entry) whose key starts with prefix and
// whose TTL has not passed at now. Shards are read-locked one at a time, so the copy is consistent
// per shard, not globally; package kvlite takes it under the writer mutex when it needs more.
//
// Cost: O(keys) to walk the shards plus O(n log n) to sort the n matches, and one packed allocation
// of Σ len(key) + 20 bytes per match. This is the stated v1 trade-off: point operations are a map
// lookup, ordered scans pay a copy. Filtering by prefix only saves the sort and the copy, not the
// walk.
func (k *Keydir) Snapshot(prefix []byte, now time.Time) Sorted {
	pfx := string(prefix)
	var pairs []pair
	for i := range k.shards {
		s := &k.shards[i]
		s.mu.RLock()
		if pairs == nil {
			// First shard: size the buffer from an estimate of the total (per-shard counts are similar).
			pairs = make([]pair, 0, len(s.m)*len(k.shards))
		}
		for key, e := range s.m {
			if e.Expired(now) || (pfx != "" && !strings.HasPrefix(key, pfx)) {
				continue
			}
			pairs = append(pairs, pair{key, e})
		}
		s.mu.RUnlock()
	}
	slices.SortFunc(pairs, func(a, b pair) int { return strings.Compare(a.k, b.k) })
	return packSorted(pairs)
}

type pair struct {
	k string
	e Entry
}

// Bytes estimates the heap footprint: shards + map overhead + entries + key bytes (Stats.KeydirBytes).
// The map slot cost is the entryOverhead estimate; the figure is for capacity planning, not accounting.
func (k *Keydir) Bytes() int64 {
	n := int64(unsafe.Sizeof(*k)) + int64(len(k.shards))*int64(unsafe.Sizeof(shard{}))
	for i := range k.shards {
		s := &k.shards[i]
		s.mu.RLock()
		n += int64(len(s.m))*entryOverhead + s.keyBytes
		s.mu.RUnlock()
	}
	return n
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

// packSorted packs sorted pairs into one contiguous Sorted with exactly two allocations.
func packSorted(pairs []pair) Sorted {
	total := 0
	for i := range pairs {
		total += len(pairs[i].k)
	}
	s := Sorted{
		keys: make([]byte, 0, total),
		offs: make([]uint32, 1, len(pairs)+1),
		ents: make([]Entry, len(pairs)),
	}
	for i := range pairs {
		s.keys = append(s.keys, pairs[i].k...)
		s.offs = append(s.offs, uint32(len(s.keys))) //nolint:gosec // total key bytes < 4 GiB by construction
		s.ents[i] = pairs[i].e
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
