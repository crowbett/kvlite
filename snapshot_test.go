package kvlite

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"
)

// collect drains an iterator into (key, value) pairs.
func collect(t *testing.T, it *Iter) (keys, vals [][]byte) {
	t.Helper()
	for it.Next() {
		k := slices.Clone(it.Key())
		v, err := it.Value()
		if err != nil {
			t.Fatalf("Value(%q) = %v", k, err)
		}
		keys = append(keys, k)
		vals = append(vals, v)
	}
	if err := it.Err(); err != nil {
		t.Fatalf("Iter.Err() = %v", err)
	}
	return keys, vals
}

func TestSnapshotIsolation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openTest(t, dir)
	defer closeTest(t, db)
	m := fill(t, db, 50)
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()

	// Mutate after the snapshot: overwrite, delete, add.
	mustPut(t, db, key(1), []byte("changed"))
	mustDelete(t, db, key(2))
	mustPut(t, db, []byte("new"), []byte("key"))

	if snap.Len() != 50 {
		t.Errorf("Len = %d, want 50", snap.Len())
	}
	for k, v := range m {
		got, err := snap.Get([]byte(k))
		if err != nil || !bytes.Equal(got, v) {
			t.Errorf("snap.Get(%q) = %q, %v; want %q", k, got, err, v)
		}
	}
	if _, err := snap.Get([]byte("new")); !errors.Is(err, ErrNotFound) {
		t.Errorf("snap.Get(new) = %v, want ErrNotFound", err)
	}
	keys, vals := collect(t, snap.Iter(nil))
	if len(keys) != 50 {
		t.Fatalf("iterated %d keys, want 50", len(keys))
	}
	for i := range keys {
		if !bytes.Equal(vals[i], m[string(keys[i])]) {
			t.Errorf("iter %q = %q, want %q", keys[i], vals[i], m[string(keys[i])])
		}
	}
	// The live store sees the mutations.
	wantGet(t, db, key(1), []byte("changed"))
	wantNotFound(t, db, key(2))

	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}
	if err := snap.Close(); err != nil {
		t.Errorf("second Close = %v", err)
	}
	if _, err := snap.Get(key(1)); !errors.Is(err, ErrClosed) {
		t.Errorf("Get after Close = %v, want ErrClosed", err)
	}
	it := snap.Iter(nil)
	if it.Next() || !errors.Is(it.Err(), ErrClosed) {
		t.Errorf("Iter after Close: Next %v Err %v", it.Next(), it.Err())
	}
	if _, err := it.Value(); !errors.Is(err, ErrClosed) {
		t.Errorf("Value after Close = %v", err)
	}
}

func TestSnapshotOrdering(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openTest(t, dir)
	t.Cleanup(func() { closeTest(t, db) })
	words := []string{"b", "a", "ab", "abc", "abd", "ac", "b\xff", "b\xff\xff", "c", "\xff", "\xff\xff", "a\x00", "zz", "z"}
	rand.Shuffle(len(words), func(i, j int) { words[i], words[j] = words[j], words[i] })
	for _, w := range words {
		mustPut(t, db, []byte(w), []byte("v:"+w))
	}
	sorted := slices.Clone(words)
	slices.Sort(sorted)
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = snap.Close() })

	tests := []struct {
		name string
		it   func() *Iter
		want []string
	}{
		{"all", func() *Iter { return snap.Iter(nil) }, sorted},
		{"empty prefix", func() *Iter { return snap.Iter([]byte{}) }, sorted},
		{"prefix a", func() *Iter { return snap.Iter([]byte("a")) }, []string{"a", "a\x00", "ab", "abc", "abd", "ac"}},
		{"prefix ab", func() *Iter { return snap.Iter([]byte("ab")) }, []string{"ab", "abc", "abd"}},
		{"prefix b\\xff", func() *Iter { return snap.Iter([]byte("b\xff")) }, []string{"b\xff", "b\xff\xff"}},
		{"prefix \\xff", func() *Iter { return snap.Iter([]byte("\xff")) }, []string{"\xff", "\xff\xff"}},
		{"prefix miss", func() *Iter { return snap.Iter([]byte("q")) }, nil},
		{"range all", func() *Iter { return snap.Range(nil, nil) }, sorted},
		{"range [ab, b)", func() *Iter { return snap.Range([]byte("ab"), []byte("b")) }, []string{"ab", "abc", "abd", "ac"}},
		{"range [b, nil)", func() *Iter { return snap.Range([]byte("b"), nil) }, []string{"b", "b\xff", "b\xff\xff", "c", "z", "zz", "\xff", "\xff\xff"}},
		{"range [nil, ab)", func() *Iter { return snap.Range(nil, []byte("ab")) }, []string{"a", "a\x00"}},
		{"range empty", func() *Iter { return snap.Range([]byte("c"), []byte("c")) }, nil},
		{"range inverted", func() *Iter { return snap.Range([]byte("z"), []byte("a")) }, nil},
		{"range miss", func() *Iter { return snap.Range([]byte("d"), []byte("y")) }, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			keys, vals := collect(t, tt.it())
			got := make([]string, len(keys))
			for i, k := range keys {
				got[i] = string(k)
				if !bytes.Equal(vals[i], []byte("v:"+got[i])) {
					t.Errorf("value for %q = %q", k, vals[i])
				}
			}
			if !slices.Equal(got, tt.want) {
				t.Errorf("keys = %q, want %q", got, tt.want)
			}
		})
	}
	it := snap.Iter(nil)
	if it.Key() != nil {
		t.Error("Key() before Next() != nil")
	}
	if _, err := it.Value(); err == nil {
		t.Error("Value() before Next() succeeded")
	}
	for it.Next() {
	}
	if it.Next() || it.Key() != nil || it.Err() != nil {
		t.Errorf("exhausted iterator: Next %v Key %v Err %v", it.Next(), it.Key(), it.Err())
	}
}

func TestSnapshotEmpty(t *testing.T) {
	t.Parallel()
	db := openTest(t, t.TempDir())
	defer closeTest(t, db)
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	if snap.Len() != 0 {
		t.Errorf("Len = %d", snap.Len())
	}
	if keys, _ := collect(t, snap.Iter(nil)); len(keys) != 0 {
		t.Errorf("keys = %q", keys)
	}
	if _, err := snap.Get([]byte("x")); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get = %v", err)
	}
	// The zero Snapshot behaves as closed.
	it := (&Snapshot{}).Iter(nil)
	if it.Next() || !errors.Is(it.Err(), ErrClosed) {
		t.Errorf("zero Snapshot iter: %v", it.Err())
	}
	if _, err := (&Snapshot{}).Get([]byte("x")); !errors.Is(err, ErrClosed) {
		t.Errorf("zero Snapshot Get = %v", err)
	}
	if err := (&Snapshot{}).Close(); err != nil {
		t.Errorf("zero Snapshot Close = %v", err)
	}
}

func TestSnapshotTTL(t *testing.T) {
	t.Parallel()
	clk := newFakeClock()
	db := openTest(t, t.TempDir(), clk.Option())
	defer closeTest(t, db)
	mustPut(t, db, []byte("a"), []byte("1"))
	if err := db.PutTTL([]byte("b"), []byte("2"), time.Second); err != nil {
		t.Fatal(err)
	}
	if err := db.PutTTL([]byte("c"), []byte("3"), time.Hour); err != nil {
		t.Fatal(err)
	}
	clk.Advance(5 * time.Second)
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	keys, _ := collect(t, snap.Iter(nil))
	if got := fmt.Sprint(keys); got != fmt.Sprint([][]byte{[]byte("a"), []byte("c")}) {
		t.Errorf("keys = %q, want a and c", keys)
	}
	if _, err := snap.Get([]byte("b")); !errors.Is(err, ErrNotFound) {
		t.Errorf("expired key visible in snapshot: %v", err)
	}
}

// TestSnapshotConcurrentWrites iterates a snapshot while a writer hammers the same keys (-race).
func TestSnapshotConcurrentWrites(t *testing.T) {
	t.Parallel()
	db := openTest(t, t.TempDir())
	defer closeTest(t, db)
	m := fill(t, db, 500)
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			k := key(i % 500)
			if i%5 == 0 {
				_ = db.Delete(k)
			} else {
				_ = db.Put(k, []byte("w"+strconv.Itoa(i)))
			}
		}
	}()
	for range 3 {
		keys, vals := collect(t, snap.Iter([]byte("key-1")))
		if len(keys) == 0 {
			t.Fatal("no keys with prefix key-1")
		}
		for i := range keys {
			if !bytes.Equal(vals[i], m[string(keys[i])]) {
				t.Fatalf("snapshot changed under a writer: %q = %q", keys[i], vals[i])
			}
		}
	}
	close(stop)
	wg.Wait()
}

// TestSnapshotSurvivesCompaction: a snapshot keeps reading values whose segments a later compaction
// merged away, and the files are reclaimed once it closes.
func TestSnapshotSurvivesCompaction(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openTest(t, dir)
	defer closeTest(t, db)
	m := fill(t, db, 100)
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	oldSegs := slices.Clone(db.man.Segments)

	for i := range 100 {
		mustPut(t, db, key(i), []byte("v2"))
	}
	for i := 0; i < 100; i += 2 {
		mustDelete(t, db, key(i))
	}
	if err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, id := range oldSegs {
		if _, err := os.Stat(logPath(dir, id)); err != nil {
			t.Errorf("segment %d deleted while a snapshot pins it: %v", id, err)
		}
	}
	keys, vals := collect(t, snap.Iter(nil))
	if len(keys) != 100 {
		t.Fatalf("snapshot has %d keys after compaction, want 100", len(keys))
	}
	for i := range keys {
		if !bytes.Equal(vals[i], m[string(keys[i])]) {
			t.Errorf("%q = %q after compaction, want %q", keys[i], vals[i], m[string(keys[i])])
		}
	}
	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}
	for _, id := range oldSegs {
		if _, err := os.Stat(logPath(dir, id)); err == nil {
			t.Errorf("segment %d still on disk after the last snapshot closed", id)
		}
	}
	// A second compaction and the live view are unaffected.
	if err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	for i := range 100 {
		if i%2 == 0 {
			wantNotFound(t, db, key(i))
		} else {
			wantGet(t, db, key(i), []byte("v2"))
		}
	}
	noOrphans(t, dir)
}

func TestSnapshotClosedDB(t *testing.T) {
	t.Parallel()
	db := openTest(t, t.TempDir())
	fill(t, db, 3)
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	it := snap.Iter(nil)
	if !it.Next() {
		t.Fatal("empty iterator")
	}
	// Closing the snapshot first keeps Close from waiting out its 5 s pin grace period.
	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}
	closeTest(t, db)
	if _, err := db.Snapshot(); !errors.Is(err, ErrClosed) {
		t.Errorf("Snapshot on closed DB = %v", err)
	}
	if _, err := it.Value(); !errors.Is(err, ErrClosed) {
		t.Errorf("Value through a closed snapshot = %v, want ErrClosed", err)
	}
	if it.Next() || !errors.Is(it.Err(), ErrClosed) {
		t.Errorf("Next through a closed snapshot: Err = %v", it.Err())
	}
}
