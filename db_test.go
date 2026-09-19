package kvlite

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/crowbett/kvlite/internal/record"
)

func TestPutGet(t *testing.T) {
	t.Parallel()
	db := openEngine(t)
	tests := []struct {
		name  string
		key   string
		value []byte
	}{
		{"simple", "hello", []byte("world")},
		{"empty value", "empty", []byte{}},
		{"nil value", "nilv", nil},
		{"binary key", "\x00\xff\x00", []byte{1, 2, 3}},
		{"large value", "big", bytes.Repeat([]byte("x"), 100_000)},
	}
	for _, tt := range tests {
		if err := db.Put([]byte(tt.key), tt.value); err != nil {
			t.Fatalf("%s: Put() = %v", tt.name, err)
		}
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := db.Get([]byte(tt.key))
			if err != nil {
				t.Fatalf("Get() = %v", err)
			}
			if got == nil || !bytes.Equal(got, tt.value) {
				t.Fatalf("Get() = %v (nil=%v), want %v", got, got == nil, tt.value)
			}
			ok, err := db.Has([]byte(tt.key))
			if err != nil || !ok {
				t.Fatalf("Has() = (%v, %v), want (true, nil)", ok, err)
			}
			// Fresh copy: mutating the result must not change what the store returns next time.
			if len(got) > 0 {
				got[0] ^= 0xff
				again, err := db.Get([]byte(tt.key))
				if err != nil || !bytes.Equal(again, tt.value) {
					t.Errorf("Get() after mutating the previous result = %v, %v", again, err)
				}
			}
		})
	}
}

func TestGetMissing(t *testing.T) {
	t.Parallel()
	db := openEngine(t)
	if _, err := db.Get([]byte("nope")); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get(absent) = %v, want ErrNotFound", err)
	}
	if ok, err := db.Has([]byte("nope")); ok || err != nil {
		t.Errorf("Has(absent) = (%v, %v), want (false, nil)", ok, err)
	}
	s := db.Stats()
	if s.Gets != 2 || s.Misses != 2 || s.Hits != 0 {
		t.Errorf("Stats = gets %d misses %d hits %d, want 2/2/0", s.Gets, s.Misses, s.Hits)
	}
}

func TestOverwrite(t *testing.T) {
	t.Parallel()
	db := openEngine(t)
	k := []byte("k")
	for i := range 5 {
		if err := db.Put(k, []byte(strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		}
		got, err := db.Get(k)
		if err != nil || string(got) != strconv.Itoa(i) {
			t.Fatalf("round %d: Get = %q, %v", i, got, err)
		}
	}
	s := db.Stats()
	if s.Keys != 1 || s.Puts != 5 {
		t.Errorf("Stats keys %d puts %d, want 1/5", s.Keys, s.Puts)
	}
	if s.DeadBytes <= 0 || s.LiveBytes <= 0 || s.TotalBytes != s.LiveBytes+s.DeadBytes {
		t.Errorf("bytes live %d dead %d total %d: overwrites must leave dead bytes", s.LiveBytes, s.DeadBytes, s.TotalBytes)
	}
}

func TestDelete(t *testing.T) {
	t.Parallel()
	db := openEngine(t)
	k := []byte("k")
	if err := db.Put(k, []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(k); err != nil {
		t.Fatalf("Delete() = %v", err)
	}
	if _, err := db.Get(k); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get after Delete = %v, want ErrNotFound", err)
	}
	if ok, _ := db.Has(k); ok {
		t.Error("Has after Delete = true")
	}
	if err := db.Delete([]byte("never-existed")); err != nil {
		t.Errorf("Delete(absent) = %v, want nil", err)
	}
	s := db.Stats()
	if s.Keys != 0 || s.Deletes != 2 || s.LiveBytes != 0 {
		t.Errorf("Stats keys %d deletes %d live %d, want 0/2/0", s.Keys, s.Deletes, s.LiveBytes)
	}
	// Both tombstones are on disk (recovery needs them), so they count as dead bytes.
	recs := scanActive(t, db)
	if len(recs) != 3 || !recs[1].Flags.Tombstone() || !recs[2].Flags.Tombstone() || len(recs[2].Value) != 0 {
		t.Errorf("active segment records = %d, want put + 2 tombstones", len(recs))
	}
	// Re-put after delete works.
	if err := db.Put(k, []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get(k); err != nil || string(got) != "v2" {
		t.Errorf("Get after re-put = %q, %v", got, err)
	}
}

func TestKeyValueCaps(t *testing.T) {
	t.Parallel()
	db := openEngine(t, WithMaxKeyBytes(8), WithMaxValueBytes(16))
	okKey, bigKey := bytes.Repeat([]byte("k"), 8), bytes.Repeat([]byte("k"), 9)
	okVal, bigVal := bytes.Repeat([]byte("v"), 16), bytes.Repeat([]byte("v"), 17)
	tests := []struct {
		name string
		op   func() error
		want error
	}{
		{"put ok", func() error { return db.Put(okKey, okVal) }, nil},
		{"put empty key", func() error { return db.Put(nil, okVal) }, ErrEmptyKey},
		{"put big key", func() error { return db.Put(bigKey, okVal) }, ErrKeyTooLarge},
		{"put big value", func() error { return db.Put(okKey, bigVal) }, ErrValueTooLarge},
		{"putttl big value", func() error { return db.PutTTL(okKey, bigVal, time.Second) }, ErrValueTooLarge},
		{"get empty key", func() error { _, err := db.Get(nil); return err }, ErrEmptyKey},
		{"get big key", func() error { _, err := db.Get(bigKey); return err }, ErrKeyTooLarge},
		{"has big key", func() error { _, err := db.Has(bigKey); return err }, ErrKeyTooLarge},
		{"delete empty key", func() error { return db.Delete([]byte{}) }, ErrEmptyKey},
		{"delete big key", func() error { return db.Delete(bigKey) }, ErrKeyTooLarge},
		{"batch big key", func() error { return db.Batch().Put(bigKey, okVal).Commit() }, ErrKeyTooLarge},
		{"batch big value", func() error { return db.Batch().Put(okKey, bigVal).Commit() }, ErrValueTooLarge},
		{"batch empty key", func() error { return db.Batch().Delete(nil).Commit() }, ErrEmptyKey},
	}
	for _, tt := range tests { // sequential on purpose: the Stats check below sums them up
		if err := tt.op(); !errors.Is(err, tt.want) {
			t.Errorf("%s: got %v, want %v", tt.name, err, tt.want)
		}
	}
	if s := db.Stats(); s.Puts != 1 || s.Deletes != 0 {
		t.Errorf("rejected writes reached the log: %+v", s)
	}
}

func TestTTL(t *testing.T) {
	t.Parallel()
	clock := newTestClock()
	db := openEngine(t, WithClock(clock.Now))
	k, v := []byte("ttl"), []byte("v")

	if err := db.PutTTL(k, v, 0); !errors.Is(err, ErrInvalidTTL) {
		t.Errorf("PutTTL(0) = %v, want ErrInvalidTTL", err)
	}
	if err := db.PutTTL(k, v, -time.Second); !errors.Is(err, ErrInvalidTTL) {
		t.Errorf("PutTTL(-1s) = %v, want ErrInvalidTTL", err)
	}
	if err := db.PutTTL(k, v, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	e, ok := db.kd.Get(k)
	if !ok || e.ExpSec != uint32(clock.Now().Unix()+10) {
		t.Fatalf("keydir entry = %+v, %v; want ExpSec now+10", e, ok)
	}
	recs := scanActive(t, db)
	if len(recs) != 1 || !recs[0].Flags.HasExpiry() || recs[0].Expiry.Unix() != int64(e.ExpSec) {
		t.Errorf("record expiry %v does not match keydir ExpSec %d", recs[0].Expiry, e.ExpSec)
	}

	clock.Advance(9 * time.Second)
	if got, err := db.Get(k); err != nil || !bytes.Equal(got, v) {
		t.Fatalf("Get 1s before expiry = %q, %v", got, err)
	}
	clock.Advance(time.Second)
	if _, err := db.Get(k); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get at expiry = %v, want ErrNotFound", err)
	}
	if ok, err := db.Has(k); ok || err != nil {
		t.Errorf("Has at expiry = (%v, %v), want (false, nil)", ok, err)
	}
	s := db.Stats()
	if s.ExpiredHits != 2 || s.Hits != 1 {
		t.Errorf("Stats expiredHits %d hits %d, want 2/1", s.ExpiredHits, s.Hits)
	}
	if s.Keys != 1 {
		t.Errorf("expiry is lazy: Keys = %d, want 1 (swept by compaction, not by Get)", s.Keys)
	}

	// A plain Put clears the TTL; a PutTTL over a live key replaces it.
	if err := db.Put(k, v); err != nil {
		t.Fatal(err)
	}
	clock.Advance(365 * 24 * time.Hour)
	if _, err := db.Get(k); err != nil {
		t.Errorf("Get after Put cleared the TTL = %v", err)
	}
	if e, _ := db.kd.Get(k); e.ExpSec != 0 {
		t.Errorf("ExpSec after plain Put = %d, want 0", e.ExpSec)
	}

	// Sub-second TTLs round up to the next whole second: never early.
	clock.Advance(500 * time.Millisecond) // now = X.5
	if err := db.PutTTL(k, v, 200*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	clock.Advance(499 * time.Millisecond) // X.999
	if _, err := db.Get(k); err != nil {
		t.Errorf("Get before the rounded-up second = %v", err)
	}
	clock.Advance(time.Millisecond) // X+1
	if _, err := db.Get(k); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get at the rounded-up second = %v, want ErrNotFound", err)
	}
}

func TestExpSec(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		t    time.Time
		want uint32
	}{
		{"zero time is never", time.Time{}, 0},
		{"whole second", time.Unix(100, 0), 100},
		{"rounds up", time.Unix(100, 1_000_000), 101},
		{"sub-millisecond truncates like the record does", time.Unix(100, 1), 100},
		{"clamps low", time.Unix(0, 0), 1},
		{"clamps high", time.Unix(1<<40, 0), 1<<32 - 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := expSec(tt.t); got != tt.want {
				t.Errorf("expSec() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestRotate(t *testing.T) {
	t.Parallel()
	db := openEngine(t)
	db.opts.maxSegmentBytes = 1024 // below the option's floor on purpose: tests rotate without 16 MiB of I/O
	val := bytes.Repeat([]byte("v"), 100)
	const n = 50
	for i := range n {
		if err := db.Put([]byte(fmt.Sprintf("key-%03d", i)), val); err != nil {
			t.Fatal(err)
		}
	}
	s := db.Stats()
	if s.Segments < 5 || s.ActiveSegment != uint32(s.Segments-1) {
		t.Fatalf("Stats segments %d active %d, want >= 5 segments and active = last", s.Segments, s.ActiveSegment)
	}
	db.rmu.RLock()
	m := db.man
	db.rmu.RUnlock()
	for i, id := range m.Segments {
		if id != uint32(i) {
			t.Errorf("manifest segments %v, want 0..%d", m.Segments, s.Segments-1)
			break
		}
	}
	if m.NextID != uint32(s.Segments) || m.Active != uint32(s.Segments-1) || m.Gen != uint64(s.Segments) {
		t.Errorf("manifest next %d active %d gen %d, want %d/%d/%d", m.NextID, m.Active, m.Gen, s.Segments, s.Segments-1, s.Segments)
	}
	if len(db.readers) != s.Segments {
		t.Errorf("%d readers open, want one per segment (%d)", len(db.readers), s.Segments)
	}
	// Every key still reads back, from whichever segment it landed in.
	segs := map[uint32]int{}
	for i := range n {
		k := []byte(fmt.Sprintf("key-%03d", i))
		got, err := db.Get(k)
		if err != nil || !bytes.Equal(got, val) {
			t.Fatalf("Get(%s) after rotation = %v", k, err)
		}
		e, _ := db.kd.Get(k)
		segs[e.Seg]++
	}
	if len(segs) != s.Segments {
		t.Errorf("keys spread over %d segments, want %d", len(segs), s.Segments)
	}
	for id, cnt := range segs {
		if cnt*len(val) > 1024 {
			t.Errorf("segment %d holds %d records: exceeds the rotation threshold", id, cnt)
		}
	}
	if s.TotalBytes != s.LiveBytes+s.DeadBytes || s.DeadBytes != int64(s.Segments)*record.SegmentHeaderSize {
		t.Errorf("bytes live %d dead %d total %d: dead should be exactly the %d prologues", s.LiveBytes, s.DeadBytes, s.TotalBytes, s.Segments)
	}
	// Rotation synced and closed the previous writer.
	if s.Syncs != uint64(s.Segments-1) {
		t.Errorf("Syncs = %d, want one per rotation (%d)", s.Syncs, s.Segments-1)
	}
}

func TestSyncPolicies(t *testing.T) {
	t.Parallel()
	t.Run("always", func(t *testing.T) {
		t.Parallel()
		db := openEngine(t, WithSync(SyncAlways))
		_ = db.Put([]byte("a"), []byte("1"))
		_ = db.Delete([]byte("a"))
		_ = db.Batch().Put([]byte("b"), []byte("2")).Commit()
		if s := db.Stats(); s.Syncs != 3 {
			t.Errorf("Syncs = %d, want 3 (one per write)", s.Syncs)
		}
	})
	t.Run("never", func(t *testing.T) {
		t.Parallel()
		db := openEngine(t, WithSync(SyncNever))
		for range 10 {
			_ = db.Put([]byte("a"), []byte("1"))
		}
		if s := db.Stats(); s.Syncs != 0 {
			t.Errorf("Syncs = %d, want 0", s.Syncs)
		}
		if err := db.Sync(); err != nil {
			t.Fatal(err)
		}
		if s := db.Stats(); s.Syncs != 1 {
			t.Errorf("Syncs after explicit Sync = %d, want 1", s.Syncs)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		if s := db.Stats(); s.Syncs != 2 {
			t.Errorf("Syncs after Close = %d, want 2 (Close syncs once)", s.Syncs)
		}
	})
	t.Run("interval", func(t *testing.T) {
		t.Parallel()
		db := openEngine(t, WithSync(SyncInterval), WithSyncInterval(2*time.Millisecond))
		if err := db.Put([]byte("a"), []byte("1")); err != nil {
			t.Fatal(err)
		}
		if !waitFor(t, 5*time.Second, func() bool { return db.Stats().Syncs >= 1 }) {
			t.Fatal("flusher never synced")
		}
		// Idle: the flusher must not fsync when nothing was appended.
		before := db.Stats().Syncs
		time.Sleep(20 * time.Millisecond)
		if after := db.Stats().Syncs; after != before {
			t.Errorf("idle flusher synced %d more times", after-before)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestBackgroundSyncErrorIsSticky(t *testing.T) {
	t.Parallel()
	db, fs := openFake(t, WithSync(SyncInterval), WithSyncInterval(time.Millisecond))
	boom := errors.New("disk on fire")
	fs.failSync.Store(&boom)
	if err := db.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if !waitFor(t, 5*time.Second, func() bool {
		return errors.Is(db.Put([]byte("a"), []byte("2")), boom)
	}) {
		t.Fatal("background sync failure never surfaced through Put")
	}
	if err := db.Sync(); !errors.Is(err, boom) {
		t.Errorf("Sync() while failing = %v, want the injected error", err)
	}
	fs.failSync.Store(nil)
	if err := db.Sync(); err != nil {
		t.Fatalf("Sync() after recovery = %v", err)
	}
	if err := db.Put([]byte("a"), []byte("3")); err != nil {
		t.Errorf("Put after a successful Sync = %v, want nil (sticky error cleared)", err)
	}
}

func TestAppendErrorPoisons(t *testing.T) {
	t.Parallel()
	db, fs := openFake(t)
	if err := db.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("short write")
	fs.failAppend.Store(&boom)
	if err := db.Put([]byte("b"), []byte("2")); !errors.Is(err, boom) {
		t.Fatalf("Put with failing Append = %v", err)
	}
	fs.failAppend.Store(nil)
	if err := db.Put([]byte("b"), []byte("2")); !errors.Is(err, boom) {
		t.Errorf("Put after a failed Append = %v, want the sticky error (torn tail needs a reopen)", err)
	}
	if err := db.Batch().Put([]byte("c"), nil).Commit(); !errors.Is(err, boom) {
		t.Errorf("Commit after a failed Append = %v, want the sticky error", err)
	}
	if _, err := db.Get([]byte("b")); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get(b) = %v, want ErrNotFound: a failed append must not reach the keydir", err)
	}
	if got, err := db.Get([]byte("a")); err != nil || string(got) != "1" {
		t.Errorf("reads keep working: Get(a) = %q, %v", got, err)
	}
}

func TestCloseThenErrClosed(t *testing.T) {
	t.Parallel()
	db := openEngine(t, WithSync(SyncInterval), WithSyncInterval(time.Millisecond))
	if err := db.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
	if err := db.Close(); !errors.Is(err, ErrClosed) {
		t.Errorf("second Close() = %v, want ErrClosed", err)
	}
	calls := map[string]func() error{
		"Get":    func() error { _, err := db.Get([]byte("a")); return err },
		"Has":    func() error { _, err := db.Has([]byte("a")); return err },
		"Put":    func() error { return db.Put([]byte("a"), nil) },
		"PutTTL": func() error { return db.PutTTL([]byte("a"), nil, time.Second) },
		"Delete": func() error { return db.Delete([]byte("a")) },
		"Sync":   db.Sync,
		"Commit": func() error { return db.Batch().Put([]byte("a"), nil).Commit() },
	}
	for name, fn := range calls {
		if err := fn(); !errors.Is(err, ErrClosed) {
			t.Errorf("%s after Close = %v, want ErrClosed", name, err)
		}
	}
	if len(db.readers) != 0 || db.active != nil || db.lock != nil {
		t.Error("Close left segments or the lock open")
	}
	_ = db.Stats() // still safe
}

func TestLockedTwice(t *testing.T) {
	t.Parallel()
	probeLanes()
	dir := t.TempDir()
	db, err := newDBForTest(dir, installFakes)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := newDBForTest(dir, installFakes); !errors.Is(err, ErrLocked) {
		t.Fatalf("second open = %v, want ErrLocked", err)
	}
}

func TestStats(t *testing.T) {
	t.Parallel()
	clock := newTestClock()
	db := openEngine(t, WithClock(clock.Now))
	if s := db.Stats(); s.Keys != 0 || s.Segments != 1 || s.ActiveSegment != 0 || s.KeydirBytes <= 0 ||
		s.TotalBytes != record.SegmentHeaderSize || s.DeadRatio != 1 {
		t.Errorf("fresh Stats = %+v", s)
	}
	_ = db.Put([]byte("a"), []byte("1"))
	_ = db.Put([]byte("b"), []byte("22"))
	_ = db.PutTTL([]byte("c"), []byte("333"), time.Second)
	_ = db.Put([]byte("a"), []byte("1111")) // supersedes
	_ = db.Delete([]byte("b"))
	_, _ = db.Get([]byte("a"))
	_, _ = db.Get([]byte("b"))
	clock.Advance(time.Second)
	_, _ = db.Get([]byte("c"))

	s := db.Stats()
	ea, _ := db.kd.Get([]byte("a"))
	ec, _ := db.kd.Get([]byte("c"))
	want := Stats{
		Keys: 2, Segments: 1, ActiveSegment: 0,
		LiveBytes: int64(ea.Size) + int64(ec.Size),
		Puts:      4, Deletes: 1, Gets: 3, Hits: 1, Misses: 1, ExpiredHits: 1,
	}
	if s.Keys != want.Keys || s.Segments != want.Segments || s.ActiveSegment != want.ActiveSegment ||
		s.LiveBytes != want.LiveBytes || s.Puts != want.Puts || s.Deletes != want.Deletes ||
		s.Gets != want.Gets || s.Hits != want.Hits || s.Misses != want.Misses || s.ExpiredHits != want.ExpiredHits {
		t.Errorf("Stats = %+v\nwant   %+v (LiveBytes, counters)", s, want)
	}
	if s.TotalBytes != s.LiveBytes+s.DeadBytes || s.DeadBytes <= record.SegmentHeaderSize ||
		s.DeadRatio <= 0 || s.DeadRatio >= 1 || s.DeadRatio != float64(s.DeadBytes)/float64(s.TotalBytes) {
		t.Errorf("byte accounting live %d dead %d total %d ratio %v", s.LiveBytes, s.DeadBytes, s.TotalBytes, s.DeadRatio)
	}
	if s.KeydirBytes <= 0 || s.Compactions != 0 || !s.LastCompaction.IsZero() || s.Repairs != 0 {
		t.Errorf("unexpected: keydir %d compactions %d last %v repairs %d", s.KeydirBytes, s.Compactions, s.LastCompaction, s.Repairs)
	}
	db.wmu.Lock()
	db.recordCompaction(1234, time.Now().Add(-time.Millisecond))
	db.wmu.Unlock()
	if s := db.Stats(); s.Compactions != 1 || s.ReclaimedBytes != 1234 || s.LastCompaction.IsZero() || s.LastCompactionDur <= 0 {
		t.Errorf("after recordCompaction: %+v", s)
	}
}

// TestConcurrentReadersWriters runs one Put/Delete writer, one Batch writer and several readers under
// -race. A reader must only ever see a value the writers wrote for that key.
func TestConcurrentReadersWriters(t *testing.T) {
	t.Parallel()
	db := openEngine(t)
	db.opts.maxSegmentBytes = 8 << 10 // rotate often
	const keys = 64
	key := func(i int) []byte { return []byte("k" + strconv.Itoa(i)) }
	val := func(i, gen int) []byte { return []byte("k" + strconv.Itoa(i) + ":" + strconv.Itoa(gen)) }

	var stop atomic.Bool
	var wg sync.WaitGroup
	for r := range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := r; !stop.Load(); i += 7 {
				k := key(i % keys)
				v, err := db.Get(k)
				switch {
				case errors.Is(err, ErrNotFound):
				case err != nil:
					t.Errorf("Get(%s) = %v", k, err)
					return
				case !bytes.HasPrefix(v, append(k, ':')):
					t.Errorf("Get(%s) = %q: value of another key", k, v)
					return
				}
				if i%17 == 0 {
					_, _ = db.Has(k)
					_ = db.Stats()
				}
			}
		}()
	}
	var writers sync.WaitGroup
	writers.Add(2)
	go func() {
		defer writers.Done()
		for gen := 0; gen < 3000; gen++ {
			i := gen % keys
			var err error
			if gen%5 == 4 {
				err = db.Delete(key(i))
			} else {
				err = db.Put(key(i), val(i, gen))
			}
			if err != nil {
				t.Errorf("write %d: %v", gen, err)
				return
			}
		}
	}()
	go func() {
		defer writers.Done()
		for gen := 0; gen < 500; gen++ {
			b := db.Batch()
			for j := range 4 {
				i := (gen*4 + j) % keys
				b.Put(key(i), val(i, gen))
			}
			b.Delete(key((gen + 3) % keys))
			if err := b.Commit(); err != nil {
				t.Errorf("commit %d: %v", gen, err)
				return
			}
		}
	}()
	writers.Wait()
	stop.Store(true)
	wg.Wait()
	if s := db.Stats(); s.Segments < 2 {
		t.Errorf("expected rotations under load, got %d segment(s)", s.Segments)
	}
}
