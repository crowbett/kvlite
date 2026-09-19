package kvlite

import (
	"bytes"
	"context"
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/crowbett/kvlite/internal/manifest"
	"github.com/crowbett/kvlite/internal/record"
	"github.com/crowbett/kvlite/internal/segment"
)

// churn writes n keys, rewrites every third and deletes every fifth, returning the model and the
// deleted keys.
func churn(t *testing.T, db *DB, n int) (model, []string) {
	t.Helper()
	m := fill(t, db, n)
	for i := 0; i < n; i += 3 {
		mustPut(t, db, key(i), []byte("v2-"+strconv.Itoa(i)))
		m.put(key(i), []byte("v2-"+strconv.Itoa(i)))
	}
	var deleted []string
	for i := 0; i < n; i += 5 {
		mustDelete(t, db, key(i))
		m.del(key(i))
		deleted = append(deleted, string(key(i)))
	}
	return m, deleted
}

func TestCompact(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openTest(t, dir)
	m, deleted := churn(t, db, 300)
	before := db.Stats()
	if before.DeadBytes == 0 || before.DeadRatio <= 0 {
		t.Fatalf("no dead bytes before compaction: %+v", before)
	}

	if err := db.Compact(t.Context()); err != nil {
		t.Fatalf("Compact = %v", err)
	}
	after := db.Stats()
	m.check(t, db, deleted)
	if after.Compactions != 1 || after.LastCompaction.IsZero() || after.ReclaimedBytes <= 0 {
		t.Errorf("compaction counters: %+v", after)
	}
	if after.Segments != 2 {
		t.Errorf("Segments = %d, want 2 (one output + the active)", after.Segments)
	}
	// Only two prologues are dead now; everything else is live.
	if after.DeadBytes != 2*record.SegmentHeaderSize {
		t.Errorf("DeadBytes = %d, want %d", after.DeadBytes, 2*record.SegmentHeaderSize)
	}
	if after.LiveBytes != before.LiveBytes {
		t.Errorf("LiveBytes changed: %d -> %d", before.LiveBytes, after.LiveBytes)
	}
	if after.TotalBytes != after.LiveBytes+after.DeadBytes || after.TotalBytes >= before.TotalBytes {
		t.Errorf("TotalBytes = %d (before %d)", after.TotalBytes, before.TotalBytes)
	}
	var onDisk int64
	for _, id := range db.man.Segments {
		onDisk += fileSize(t, logPath(dir, id))
		if id != after.ActiveSegment {
			if _, err := os.Stat(hintPath(dir, id)); err != nil {
				t.Errorf("no hint for output segment %d: %v", id, err)
			}
		}
	}
	if onDisk != after.TotalBytes {
		t.Errorf("TotalBytes = %d, files hold %d", after.TotalBytes, onDisk)
	}
	if err := db.Compact(t.Context()); err != nil {
		t.Fatalf("second Compact = %v", err)
	}
	m.check(t, db, deleted)
	if s := db.Stats(); s.Segments != 2 || s.Compactions != 2 {
		t.Errorf("after second compaction: %+v", s)
	}
	closeTest(t, db)
	noOrphans(t, dir)

	obs := observeRecovery(t, dir)
	db = openTest(t, dir)
	m.check(t, db, deleted)
	if hinted, scanned := obs.hinted(); len(hinted) != 1 || len(scanned) != 1 {
		t.Errorf("reopen after compaction: hinted %v scanned %v", hinted, scanned)
	}
	closeTest(t, db)
}

func TestCompactEmpty(t *testing.T) {
	t.Parallel()
	db := openTest(t, t.TempDir())
	defer closeTest(t, db)
	if err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	if s := db.Stats(); s.Segments != 1 || s.Compactions != 0 {
		t.Errorf("Stats after empty Compact = %+v", s)
	}
	mustPut(t, db, []byte("k"), []byte("v"))
	mustDelete(t, db, []byte("k"))
	if err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Everything was dead: no output segment at all.
	if s := db.Stats(); s.Segments != 1 || s.Keys != 0 || s.LiveBytes != 0 {
		t.Errorf("Stats after all-dead Compact = %+v", s)
	}
	wantNotFound(t, db, []byte("k"))
}

func TestCompactExpired(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	clk := newFakeClock()
	db := openTest(t, dir, clk.Option())
	mustPut(t, db, []byte("keep"), []byte("1"))
	if err := db.PutTTL([]byte("gone"), []byte("2"), time.Second); err != nil {
		t.Fatal(err)
	}
	if err := db.PutTTL([]byte("later"), []byte("3"), time.Hour); err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Second)
	if s := db.Stats(); s.Keys != 3 {
		t.Fatalf("Keys before compaction = %d (expiry is lazy)", s.Keys)
	}
	if err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	if s := db.Stats(); s.Keys != 2 {
		t.Errorf("Keys after compaction = %d, want 2", s.Keys)
	}
	wantNotFound(t, db, []byte("gone"))
	wantGet(t, db, []byte("later"), []byte("3"))
	closeTest(t, db)
	db = openTest(t, dir, clk.Option())
	wantNotFound(t, db, []byte("gone"))
	wantGet(t, db, []byte("keep"), []byte("1"))
	wantGet(t, db, []byte("later"), []byte("3"))
	// The TTL survived the merge.
	clk.Advance(2 * time.Hour)
	wantNotFound(t, db, []byte("later"))
	closeTest(t, db)
}

// TestCompactBatchMembers: merged batch members stand alone (flags stripped), so a reopen that scans the
// output does not discard them as an incomplete group.
func TestCompactBatchMembers(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openTest(t, dir)
	b := db.Batch()
	for i := range 20 {
		b.Put(key(i), val(i))
	}
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, id := range db.man.Segments {
		_ = os.Remove(hintPath(dir, id))
	}
	closeTest(t, db)
	db = openTest(t, dir)
	for i := range 20 {
		wantGet(t, db, key(i), val(i))
	}
	closeTest(t, db)
}

func TestCompactCancel(t *testing.T) {
	dir := t.TempDir()
	db := openTest(t, dir)
	defer closeTest(t, db)
	m, deleted := churn(t, db, 2000)
	before := db.Stats()

	ctx, cancel := context.WithCancel(t.Context())
	setCompactHook(t, func(h *DB, stage string) error {
		if h == db && stage == stageRotated {
			cancel()
		}
		return nil
	})
	if err := db.Compact(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Compact(cancelled) = %v, want context.Canceled", err)
	}
	m.check(t, db, deleted)
	s := db.Stats()
	if s.Compactions != 0 || s.ReclaimedBytes != 0 {
		t.Errorf("cancelled compaction updated counters: %+v", s)
	}
	if s.LiveBytes != before.LiveBytes {
		t.Errorf("LiveBytes changed by a cancelled compaction: %d -> %d", before.LiveBytes, s.LiveBytes)
	}
	noOrphans(t, dir)
	// The store keeps working and a later compaction succeeds.
	setCompactHook(t, nil)
	if err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	m.check(t, db, deleted)
	noOrphans(t, dir)
}

// TestCompactCrash simulates a crash at every compaction stage: the hook makes Compact return without
// any cleanup, the store is closed as is, and a fresh Open must see the pre-compaction key set (crash
// before the swap) or the merged one (after), with no orphan files left behind either way.
func TestCompactCrash(t *testing.T) {
	stages := []string{stageRotated, stageMerged, stageAdopted, stageSwapped, stageReleased}
	errCrash := errors.New("simulated crash")
	for _, stage := range stages {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			clk := newFakeClock()
			db := openTest(t, dir, clk.Option())
			m, deleted := churn(t, db, 400)
			if err := db.PutTTL([]byte("ttl"), []byte("x"), time.Second); err != nil {
				t.Fatal(err)
			}
			clk.Advance(5 * time.Second)
			oldSegs := slices.Clone(db.man.Segments)
			setCompactHook(t, func(h *DB, s string) error {
				if h == db && s == stage {
					return errCrash
				}
				return nil
			})
			if err := db.Compact(t.Context()); !errors.Is(err, errCrash) {
				t.Fatalf("Compact = %v, want the simulated crash", err)
			}
			setCompactHook(t, nil)
			// The in-memory store still serves the right data whatever the stage.
			m.values(t, db, deleted)
			wantNotFound(t, db, []byte("ttl"))
			closeTest(t, db)

			mf, err := manifest.Load(dir)
			if err != nil {
				t.Fatal(err)
			}
			swapped := stage == stageSwapped || stage == stageReleased
			if got := slices.ContainsFunc(oldSegs, func(id uint32) bool { return slices.Contains(mf.Segments, id) && id != mf.Active }); got == swapped {
				t.Errorf("MANIFEST after crash at %s names old segments = %v, want %v (segments %v, old %v)", stage, got, !swapped, mf.Segments, oldSegs)
			}

			obs := observeRecovery(t, dir)
			db = openTest(t, dir, clk.Option())
			m.check(t, db, deleted)
			wantNotFound(t, db, []byte("ttl"))
			if swapped {
				if hinted, _ := obs.hinted(); len(hinted) == 0 {
					t.Errorf("reopen after crash at %s did not use the merged hints", stage)
				}
			}
			mustPut(t, db, []byte("after"), []byte("crash"))
			m.put([]byte("after"), []byte("crash"))
			if err := db.Compact(t.Context()); err != nil {
				t.Fatalf("Compact after recovery = %v", err)
			}
			m.check(t, db, deleted)
			closeTest(t, db)
			noOrphans(t, dir)
		})
	}
}

// TestCompactSaveFails: a MANIFEST that cannot be written aborts the swap; the keydir is rolled back to
// the inputs and the outputs are discarded.
func TestCompactSaveFails(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openTest(t, dir)
	defer closeTest(t, db)
	m, deleted := churn(t, db, 200)
	errDisk := errors.New("disk full")
	orig := db.saveManifest
	calls := 0
	db.saveManifest = func(d string, mf *manifest.Manifest) error {
		calls++
		if calls == 2 { // the first Save is the rotation, the second the swap
			return errDisk
		}
		return orig(d, mf)
	}
	if err := db.Compact(t.Context()); !errors.Is(err, errDisk) {
		t.Fatalf("Compact = %v, want disk full", err)
	}
	m.check(t, db, deleted)
	noOrphans(t, dir)
	db.saveManifest = orig
	if err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	m.check(t, db, deleted)
	if s := db.Stats(); s.Segments != 2 {
		t.Errorf("Segments = %d", s.Segments)
	}
}

// TestCompactUnderWriters runs one writer, eight readers and back-to-back compactions together (-race)
// and checks no key is lost or rolled back.
func TestCompactUnderWriters(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openTest(t, dir)
	const keys = 2000
	dur := 2 * time.Second
	if testing.Short() {
		dur = 300 * time.Millisecond
	}

	var mu sync.Mutex
	m := model{}
	var deleted []string
	for i := range keys {
		mustPut(t, db, key(i), val(i))
		m.put(key(i), val(i))
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	errc := make(chan error, 16)
	wg.Add(1)
	go func() { // writer
		defer wg.Done()
		r := rand.New(rand.NewPCG(1, 2))
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			k := key(r.IntN(keys))
			mu.Lock()
			if r.IntN(4) == 0 {
				if err := db.Delete(k); err != nil {
					errc <- err
				}
				m.del(k)
			} else {
				v := []byte("w" + strconv.Itoa(i))
				if err := db.Put(k, v); err != nil {
					errc <- err
				}
				m.put(k, v)
			}
			mu.Unlock()
		}
	}()
	for g := range 8 {
		wg.Add(1)
		go func() { // readers: any answer must be a value the key held at some point, or not found
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(g), 7))
			for {
				select {
				case <-stop:
					return
				default:
				}
				k := key(r.IntN(keys))
				v, err := db.Get(k)
				if err != nil && !errors.Is(err, ErrNotFound) {
					errc <- err
					return
				}
				// Values are either val(i) (a run of one lowercase letter) or a writer's "w<n>".
				if err == nil && (len(v) == 0 || v[0] < 'a' || v[0] > 'z') {
					errc <- errors.New("unexpected value " + string(v))
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() { // compactor
		defer wg.Done()
		n := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := db.Compact(t.Context()); err != nil {
				errc <- err
				return
			}
			n++
			snap, err := db.Snapshot()
			if err != nil {
				errc <- err
				return
			}
			_, _ = collect(t, snap.Iter([]byte("key-1")))
			_ = snap.Close()
		}
	}()
	time.Sleep(dur)
	close(stop)
	wg.Wait()
	close(errc)
	for err := range errc {
		t.Errorf("concurrent error: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	for i := range keys {
		if _, ok := m[string(key(i))]; !ok {
			deleted = append(deleted, string(key(i)))
		}
	}
	m.check(t, db, deleted)
	if err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	m.check(t, db, deleted)
	s := db.Stats()
	if s.Compactions == 0 {
		t.Error("no compaction ran")
	}
	closeTest(t, db)
	noOrphans(t, dir)
	db = openTest(t, dir)
	m.check(t, db, deleted)
	closeTest(t, db)
}

// TestCompactRotatesOutputs: a low rotation threshold splits the merge into several outputs, each with
// a hint, all below the new active id.
func TestCompactRotatesOutputs(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openTest(t, dir, WithMaxSegmentBytes(record.MaxRecord))
	defer closeTest(t, db)
	big := bytes.Repeat([]byte("x"), 4<<20)
	m := model{}
	for i := range 12 { // ~48 MiB of live data across several segments
		k := []byte("big-" + strconv.Itoa(i))
		mustPut(t, db, k, big)
		m.put(k, big)
	}
	before := db.Stats()
	if before.Segments < 3 {
		t.Fatalf("expected rotation, got %d segments", before.Segments)
	}
	if err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	s := db.Stats()
	if s.Segments < 3 {
		t.Errorf("Segments after compaction = %d, want several outputs", s.Segments)
	}
	for _, id := range db.man.Segments {
		if id == s.ActiveSegment {
			continue
		}
		if id > s.ActiveSegment {
			t.Errorf("output %d sorts above the active segment %d", id, s.ActiveSegment)
		}
		if _, err := os.Stat(hintPath(dir, id)); err != nil {
			t.Errorf("output %d has no hint", id)
		}
		if fileSize(t, logPath(dir, id)) > record.MaxRecord+int64(len(big)) {
			t.Errorf("output %d exceeds the rotation threshold", id)
		}
	}
	for k, v := range m {
		got, err := db.Get([]byte(k))
		if err != nil || !bytes.Equal(got, v) {
			t.Fatalf("Get(%s) len %d err %v", k, len(got), err)
		}
	}
	noOrphans(t, dir)
}

// TestAutoCompact lowers the tick and the minimum dead bytes so the background compactor fires.
func TestAutoCompact(t *testing.T) {
	prevTick, prevMin := autoCompactTick.Load(), autoCompactMinDead.Load()
	autoCompactTick.Store(int64(10 * time.Millisecond))
	autoCompactMinDead.Store(1)
	t.Cleanup(func() {
		autoCompactTick.Store(prevTick)
		autoCompactMinDead.Store(prevMin)
	})
	dir := t.TempDir()
	db := openTest(t, dir, WithAutoCompact(0.3))
	m, deleted := churn(t, db, 200)
	if !db.shouldAutoCompact() {
		t.Fatalf("trigger not met: %+v", db.Stats())
	}
	deadline := time.Now().Add(5 * time.Second)
	for db.Stats().Compactions == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("auto-compaction never ran: %+v", db.Stats())
		}
		time.Sleep(10 * time.Millisecond)
	}
	m.check(t, db, deleted)
	closeTest(t, db)
	noOrphans(t, dir)
	// A read-only store never compacts, whatever the option says.
	ro := openTest(t, dir, WithReadOnly(), WithAutoCompact(0.01))
	time.Sleep(30 * time.Millisecond)
	if s := ro.Stats(); s.Compactions != 0 {
		t.Errorf("read-only auto-compaction ran: %+v", s)
	}
	closeTest(t, ro)
}

func TestCompactClosed(t *testing.T) {
	t.Parallel()
	db := openTest(t, t.TempDir())
	fill(t, db, 3)
	closeTest(t, db)
	if err := db.Compact(t.Context()); !errors.Is(err, ErrClosed) {
		t.Errorf("Compact on closed = %v", err)
	}
	if _, ok := compactLocks.Load(db); ok {
		t.Error("compaction lock leaked after Close")
	}
}

// TestCompactSerialised: two concurrent Compact calls never run their merges at the same time.
func TestCompactSerialised(t *testing.T) {
	dir := t.TempDir()
	db := openTest(t, dir)
	defer closeTest(t, db)
	m, deleted := churn(t, db, 500)
	var inMerge, overlap int32
	var mu sync.Mutex
	setCompactHook(t, func(h *DB, stage string) error {
		if h != db {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		switch stage {
		case stageRotated:
			inMerge++
			if inMerge > 1 {
				overlap++
			}
			time.Sleep(20 * time.Millisecond)
		case stageReleased:
			inMerge--
		}
		return nil
	})
	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := db.Compact(t.Context()); err != nil {
				t.Errorf("Compact = %v", err)
			}
		}()
	}
	wg.Wait()
	if overlap != 0 {
		t.Errorf("%d overlapping compactions", overlap)
	}
	m.check(t, db, deleted)
	if _, err := os.Stat(filepath.Join(dir, segment.LogName(db.Stats().ActiveSegment))); err != nil {
		t.Error(err)
	}
}
