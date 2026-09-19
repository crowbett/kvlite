package kvlite

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/crowbett/kvlite/internal/fslock"
	"github.com/crowbett/kvlite/internal/manifest"
	"github.com/crowbett/kvlite/internal/record"
	"github.com/crowbett/kvlite/internal/segment"
)

// --- helpers shared by the workstream C tests ---------------------------------------------------------

// fakeClock is the WithClock seam: TTL tests advance it instead of sleeping.
type fakeClock struct{ ns atomic.Int64 }

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.ns.Store(time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC).UnixNano())
	return c
}

func (c *fakeClock) Now() time.Time                  { return time.Unix(0, c.ns.Load()) }
func (c *fakeClock) Advance(d time.Duration)         { c.ns.Add(int64(d)) }
func (c *fakeClock) Option() Option                  { return WithClock(c.Now) }
func (c *fakeClock) Set(t time.Time)                 { c.ns.Store(t.UnixNano()) }
func (c *fakeClock) String() string                  { return c.Now().Format(time.RFC3339) }
func (c *fakeClock) Since(t time.Time) time.Duration { return c.Now().Sub(t) }

func openTest(t *testing.T, dir string, opts ...Option) *DB {
	t.Helper()
	db, err := Open(dir, opts...)
	if err != nil {
		t.Fatalf("Open(%s) = %v", dir, err)
	}
	return db
}

func closeTest(t *testing.T, db *DB) {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
}

func key(i int) []byte { return []byte("key-" + strconv.Itoa(i)) }

func val(i int) []byte { return bytes.Repeat([]byte{byte('a' + i%26)}, 8+i%40) }

func mustPut(t *testing.T, db *DB, k, v []byte) {
	t.Helper()
	if err := db.Put(k, v); err != nil {
		t.Fatalf("Put(%q) = %v", k, err)
	}
}

func mustDelete(t *testing.T, db *DB, k []byte) {
	t.Helper()
	if err := db.Delete(k); err != nil {
		t.Fatalf("Delete(%q) = %v", k, err)
	}
}

func wantGet(t *testing.T, db *DB, k, want []byte) {
	t.Helper()
	got, err := db.Get(k)
	if err != nil {
		t.Fatalf("Get(%q) = %v, want %q", k, err, want)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Get(%q) = %q, want %q", k, got, want)
	}
}

func wantNotFound(t *testing.T, db *DB, k []byte) {
	t.Helper()
	if _, err := db.Get(k); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(%q) = %v, want ErrNotFound", k, err)
	}
}

// model is a plain map the tests keep in step with the store.
type model map[string][]byte

func (m model) put(k, v []byte) { m[string(k)] = slices.Clone(v) }
func (m model) del(k []byte)    { delete(m, string(k)) }

// check verifies every model key reads back, every deleted key is absent and the keydir holds exactly
// the model's keys.
func (m model) check(t *testing.T, db *DB, deleted []string) {
	t.Helper()
	m.values(t, db, deleted)
	if got := db.Stats().Keys; got != len(m) {
		t.Fatalf("Stats().Keys = %d, want %d", got, len(m))
	}
}

// values is check without the key count (an expired key stays in the keydir until a compaction or
// reopen sweeps it).
func (m model) values(t *testing.T, db *DB, deleted []string) {
	t.Helper()
	for k, v := range m {
		wantGet(t, db, []byte(k), v)
	}
	for _, k := range deleted {
		if _, ok := m[k]; !ok {
			wantNotFound(t, db, []byte(k))
		}
	}
}

// fill writes n keys and returns the model.
func fill(t *testing.T, db *DB, n int) model {
	t.Helper()
	m := model{}
	for i := range n {
		mustPut(t, db, key(i), val(i))
		m.put(key(i), val(i))
	}
	return m
}

// segmentRecords lists (offset, size) of every record in dir/<id>.log.
func segmentRecords(t *testing.T, dir string, id uint32) [][2]int64 {
	t.Helper()
	r, err := segment.OpenReader(dir, id)
	if err != nil {
		t.Fatalf("OpenReader(%d) = %v", id, err)
	}
	defer r.Close()
	var out [][2]int64
	err = r.Scan(func(off uint32, rec record.Record) error {
		out = append(out, [2]int64{int64(off), int64(record.EncodedSize(rec))})
		return nil
	})
	if err != nil {
		t.Fatalf("Scan(%d) = %v", id, err)
	}
	return out
}

func logPath(dir string, id uint32) string  { return filepath.Join(dir, segment.LogName(id)) }
func hintPath(dir string, id uint32) string { return filepath.Join(dir, segment.HintName(id)) }

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return st.Size()
}

func flipByte(t *testing.T, path string, off int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var b [1]byte
	if _, err := f.ReadAt(b[:], off); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xFF
	if _, err := f.WriteAt(b[:], off); err != nil {
		t.Fatal(err)
	}
}

// noOrphans asserts every *.log/*.hint in dir is named by the MANIFEST and trash/ holds nothing.
func noOrphans(t *testing.T, dir string) {
	t.Helper()
	m, err := manifest.Load(dir)
	if err != nil {
		t.Fatalf("manifest.Load = %v", err)
	}
	orphans, err := manifest.Orphans(dir, m)
	if err != nil {
		t.Fatal(err)
	}
	if len(orphans) != 0 {
		t.Fatalf("orphans left on disk: %v (manifest %v)", orphans, m.Segments)
	}
	if ents, err := os.ReadDir(filepath.Join(dir, manifest.TrashDir)); err == nil && len(ents) != 0 {
		names := make([]string, 0, len(ents))
		for _, e := range ents {
			names = append(names, e.Name())
		}
		t.Fatalf("trash/ not empty: %v", names)
	}
}

// setCompactHook installs the compaction stage hook for the test's lifetime. Tests that use it must not
// run in parallel with each other; the hook filters on db so other parallel tests are unaffected.
func setCompactHook(t *testing.T, fn func(db *DB, stage string) error) {
	t.Helper()
	testHooks.mu.Lock()
	testHooks.compact = fn
	testHooks.mu.Unlock()
	t.Cleanup(func() {
		testHooks.mu.Lock()
		testHooks.compact = nil
		testHooks.mu.Unlock()
	})
}

// recoveryObserver records how each segment of dir was loaded by the next Open.
type recoveryObserver struct {
	mu   sync.Mutex
	dir  string
	seen map[uint32]bool // seg -> viaHint
}

func observeRecovery(t *testing.T, dir string) *recoveryObserver {
	t.Helper()
	o := &recoveryObserver{dir: dir, seen: map[uint32]bool{}}
	testHooks.mu.Lock()
	prev := testHooks.recovery
	testHooks.recovery = func(db *DB, seg uint32, viaHint bool) {
		if prev != nil {
			prev(db, seg, viaHint)
		}
		if db.dir != dir {
			return
		}
		o.mu.Lock()
		o.seen[seg] = viaHint
		o.mu.Unlock()
	}
	testHooks.mu.Unlock()
	t.Cleanup(func() {
		testHooks.mu.Lock()
		testHooks.recovery = prev
		testHooks.mu.Unlock()
	})
	return o
}

func (o *recoveryObserver) reset() {
	o.mu.Lock()
	clear(o.seen)
	o.mu.Unlock()
}

func (o *recoveryObserver) hinted() (hinted, scanned []uint32) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for seg, h := range o.seen {
		if h {
			hinted = append(hinted, seg)
		} else {
			scanned = append(scanned, seg)
		}
	}
	slices.Sort(hinted)
	slices.Sort(scanned)
	return hinted, scanned
}

// --- Open ---------------------------------------------------------------------------------------------

func TestOpenFresh(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "nested", "store")
	db := openTest(t, dir)
	for _, name := range []string{fslock.FileName, manifest.FileName, segment.LogName(0)} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("fresh store lacks %s: %v", name, err)
		}
	}
	s := db.Stats()
	if s.Segments != 1 || s.ActiveSegment != 0 || s.Keys != 0 || s.LiveBytes != 0 {
		t.Errorf("fresh Stats = %+v", s)
	}
	if s.TotalBytes != record.SegmentHeaderSize {
		t.Errorf("TotalBytes = %d, want the prologue %d", s.TotalBytes, record.SegmentHeaderSize)
	}
	closeTest(t, db)
	// The LOCK is released: a second Open succeeds.
	db = openTest(t, dir)
	closeTest(t, db)
	noOrphans(t, dir)
}

func TestOpenLocked(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openTest(t, dir)
	defer closeTest(t, db)
	if _, err := Open(dir); !errors.Is(err, ErrLocked) {
		t.Fatalf("second Open = %v, want ErrLocked", err)
	}
	// A read-only open does not take the LOCK.
	ro, err := Open(dir, WithReadOnly())
	if err != nil {
		t.Fatalf("Open(WithReadOnly) = %v", err)
	}
	if err := ro.Put([]byte("k"), []byte("v")); !errors.Is(err, ErrReadOnly) {
		t.Errorf("read-only Put = %v, want ErrReadOnly", err)
	}
	closeTest(t, ro)
}

func TestReopenPersistence(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	clk := newFakeClock()
	db := openTest(t, dir, clk.Option())
	m := fill(t, db, 200)
	var deleted []string
	for i := 0; i < 200; i += 7 {
		mustDelete(t, db, key(i))
		m.del(key(i))
		deleted = append(deleted, string(key(i)))
	}
	for i := 0; i < 200; i += 5 {
		mustPut(t, db, key(i), []byte("rewritten"))
		m.put(key(i), []byte("rewritten"))
	}
	if err := db.PutTTL([]byte("ttl-live"), []byte("x"), time.Hour); err != nil {
		t.Fatal(err)
	}
	m.put([]byte("ttl-live"), []byte("x"))
	if err := db.PutTTL([]byte("ttl-dead"), []byte("y"), time.Second); err != nil {
		t.Fatal(err)
	}
	if err := db.Batch().Put([]byte("b1"), []byte("1")).Put([]byte("b2"), []byte("2")).Delete(key(3)).Commit(); err != nil {
		t.Fatal(err)
	}
	m.put([]byte("b1"), []byte("1"))
	m.put([]byte("b2"), []byte("2"))
	m.del(key(3))
	deleted = append(deleted, string(key(3)))
	before := db.Stats()
	closeTest(t, db)

	clk.Advance(10 * time.Second)
	db = openTest(t, dir, clk.Option())
	defer closeTest(t, db)
	m.check(t, db, deleted)
	wantNotFound(t, db, []byte("ttl-dead"))
	after := db.Stats()
	if after.LiveBytes != before.LiveBytes-int64(len("y")+len("ttl-dead")+record.HeaderSize+record.MaxMeta)+0 && after.LiveBytes > before.LiveBytes {
		t.Errorf("LiveBytes after reopen %d > before %d", after.LiveBytes, before.LiveBytes)
	}
	if after.TotalBytes != before.TotalBytes {
		t.Errorf("TotalBytes after reopen = %d, want %d", after.TotalBytes, before.TotalBytes)
	}
	if after.Segments != before.Segments || after.ActiveSegment != before.ActiveSegment {
		t.Errorf("segment set changed across reopen: %+v -> %+v", before, after)
	}
	// New writes land after the recovered tail.
	mustPut(t, db, []byte("after"), []byte("reopen"))
	wantGet(t, db, []byte("after"), []byte("reopen"))
}

func TestOpenReadOnly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openTest(t, dir)
	m := fill(t, db, 50)
	closeTest(t, db)

	ro := openTest(t, dir, WithReadOnly())
	defer closeTest(t, ro)
	m.check(t, ro, nil)
	for _, err := range []error{
		ro.Put([]byte("k"), nil),
		ro.Delete(key(1)),
		ro.Compact(t.Context()),
		ro.Batch().Put([]byte("k"), nil).Commit(),
	} {
		if !errors.Is(err, ErrReadOnly) {
			t.Errorf("mutation on read-only store = %v, want ErrReadOnly", err)
		}
	}
	snap, err := ro.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snap.Len() != 50 {
		t.Errorf("read-only Snapshot.Len = %d, want 50", snap.Len())
	}
	_ = snap.Close()
	// A writer may open concurrently: read-only holds no lock.
	rw := openTest(t, dir)
	closeTest(t, rw)
}

func TestOpenReadOnlyFresh(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ro := openTest(t, dir, WithReadOnly())
	if s := ro.Stats(); s.Keys != 0 || s.Segments != 0 {
		t.Errorf("Stats = %+v", s)
	}
	wantNotFound(t, ro, []byte("nothing"))
	closeTest(t, ro)
	ents, _ := os.ReadDir(dir)
	if len(ents) != 0 {
		t.Errorf("read-only open of an empty dir created files: %v", ents)
	}
	if _, err := Open(filepath.Join(dir, "missing"), WithReadOnly()); err == nil {
		t.Error("read-only Open of a missing dir succeeded")
	}
}

func TestOpenOrphanSweep(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openTest(t, dir)
	m := fill(t, db, 10)
	closeTest(t, db)

	// Leftovers of a crashed compaction: an unlisted segment, its hint, a scratch hint, a stale hint
	// for the active segment and a stale trash entry.
	w, err := segment.Create(dir, 99)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	for _, name := range []string{segment.HintName(99), segment.HintName(0), segment.HintName(5) + segment.TempSuffix} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("junk"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(filepath.Join(dir, manifest.TrashDir), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, manifest.TrashDir, "00000042.log"), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}

	db = openTest(t, dir)
	m.check(t, db, nil)
	if s := db.Stats(); s.Segments != 1 {
		t.Errorf("Segments = %d, want 1", s.Segments)
	}
	closeTest(t, db)
	noOrphans(t, dir)
	for _, name := range []string{segment.LogName(99), segment.HintName(99), segment.HintName(0), segment.HintName(5) + segment.TempSuffix} {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			t.Errorf("%s survived the orphan sweep", name)
		}
	}
}

func TestOpenManifestMissing(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openTest(t, dir)
	m := fill(t, db, 20)
	closeTest(t, db)
	if err := os.Remove(filepath.Join(dir, manifest.FileName)); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open without MANIFEST = %v, want ErrCorrupt", err)
	}
	// Read-only tolerates it (rebuilds in memory only).
	ro := openTest(t, dir, WithReadOnly())
	m.check(t, ro, nil)
	closeTest(t, ro)
	if _, err := os.Stat(filepath.Join(dir, manifest.FileName)); err == nil {
		t.Error("read-only open wrote a MANIFEST")
	}
	db = openTest(t, dir, WithRepair())
	m.check(t, db, nil)
	mustPut(t, db, []byte("more"), []byte("data"))
	m.put([]byte("more"), []byte("data"))
	closeTest(t, db)
	db = openTest(t, dir)
	m.check(t, db, nil)
	closeTest(t, db)
}

func TestOpenManifestCorrupt(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openTest(t, dir)
	m := fill(t, db, 20)
	closeTest(t, db)
	flipByte(t, filepath.Join(dir, manifest.FileName), 9)
	if _, err := Open(dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open with corrupt MANIFEST = %v, want ErrCorrupt", err)
	}
	db = openTest(t, dir, WithRepair())
	m.check(t, db, nil)
	closeTest(t, db)
	db = openTest(t, dir)
	m.check(t, db, nil)
	closeTest(t, db)
}

func TestOpenActiveMissing(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openTest(t, dir)
	m := fill(t, db, 20)
	if err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	active := db.Stats().ActiveSegment
	closeTest(t, db)
	// A crash between the MANIFEST naming a new active segment and the file being created.
	if err := os.Remove(logPath(dir, active)); err != nil {
		t.Fatal(err)
	}
	db = openTest(t, dir)
	m.check(t, db, nil)
	mustPut(t, db, []byte("k"), []byte("v"))
	m.put([]byte("k"), []byte("v"))
	closeTest(t, db)
	noOrphans(t, dir)
	db = openTest(t, dir)
	m.check(t, db, nil)
	closeTest(t, db)
}

func TestOpenErrors(t *testing.T) {
	t.Parallel()
	if _, err := Open(""); err == nil {
		t.Error("Open(\"\") succeeded")
	}
	if _, err := Open(t.TempDir(), WithShards(3)); err == nil {
		t.Error("Open(WithShards(3)) succeeded")
	}
	// A file where the directory should be.
	f := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(f); err == nil {
		t.Error("Open(file) succeeded")
	}
}

func ExampleOpen() {
	dir, err := os.MkdirTemp("", "kvlite-example")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	db, err := Open(dir)
	if err != nil {
		panic(err)
	}
	if err := db.Put([]byte("hello"), []byte("world")); err != nil {
		panic(err)
	}
	if err := db.Close(); err != nil {
		panic(err)
	}
	db, err = Open(dir)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	v, err := db.Get([]byte("hello"))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(v))
	// Output: world
}
