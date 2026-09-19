package kvlite

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/crowbett/kvlite/internal/record"
)

// FuzzDB decodes the input into an op script (put / put-ttl / delete / batch / get / snapshot-iter /
// compact / advance-clock / reopen / big put) and runs it against a real DB in t.TempDir() and against a
// map model on a fake clock. After every op the whole model is compared with the DB (every live key
// must Get its value, every deleted or expired key must be ErrNotFound); the same check runs after a
// final Close + Open. Expiry boundaries never coincide with the fake clock: the clock starts on an even
// second and only ever advances by even seconds, while every TTL is an odd number of seconds.
func FuzzDB(f *testing.F) {
	for _, s := range seedScripts() {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, script []byte) {
		runScript(t, script)
	})
}

// TestFuzzSeedsReplay runs every committed corpus file under testdata/fuzz/FuzzDB as a named subtest,
// so a regression shows the seed's name instead of a hash.
func TestFuzzSeedsReplay(t *testing.T) {
	t.Parallel()
	files, err := filepath.Glob(filepath.Join("testdata", "fuzz", "FuzzDB", "*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no seeds under testdata/fuzz/FuzzDB")
	}
	for _, file := range files {
		t.Run(filepath.Base(file), func(t *testing.T) {
			t.Parallel()
			script, err := readCorpusFile(file)
			if err != nil {
				t.Fatal(err)
			}
			runScript(t, script)
		})
	}
}

// readCorpusFile parses Go's fuzz corpus format: "go test fuzz v1" then one []byte("...") line.
func readCorpusFile(path string) ([]byte, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(strings.ReplaceAll(string(b), "\r\n", "\n"), "\n")
	if len(lines) < 2 || strings.TrimSpace(lines[0]) != "go test fuzz v1" {
		return nil, fmt.Errorf("%s: not a go fuzz corpus file", path)
	}
	line := strings.TrimSpace(lines[1])
	if !strings.HasPrefix(line, "[]byte(") || !strings.HasSuffix(line, ")") {
		return nil, fmt.Errorf("%s: expected []byte(...), got %q", path, line)
	}
	s, err := strconv.Unquote(line[len("[]byte(") : len(line)-1])
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return []byte(s), nil
}

// --- model ---

const (
	fuzzKeySpace = 16  // keys "k00".."k15": small so overwrites, deletes and TTLs collide constantly
	fuzzMaxOps   = 400 // bound per input so a fuzz iteration stays well under a second
	fuzzBigValue = 256 << 10
)

// modelVal is the model's view of one key.
type fuzzVal struct {
	value []byte
	exp   int64 // unix seconds, 0 = never
}

type fuzzModel struct {
	live map[string]fuzzVal
	// deleted remembers keys the DB has ever seen so the check covers explicit ErrNotFound too.
	seen map[string]bool
}

func (m *fuzzModel) put(k string, v []byte, exp int64) {
	m.live[k] = fuzzVal{value: bytes.Clone(v), exp: exp}
	m.seen[k] = true
}

func (m *fuzzModel) del(k string) {
	delete(m.live, k)
	m.seen[k] = true
}

// expired reports whether k is live in the model at now.
func (m *fuzzModel) get(k string, now int64) ([]byte, bool) {
	v, ok := m.live[k]
	if !ok || (v.exp != 0 && v.exp <= now) {
		return nil, false
	}
	return v.value, true
}

// liveKeys returns the sorted live keys at now.
func (m *fuzzModel) liveKeys(now int64) []string {
	keys := make([]string, 0, len(m.live))
	for k := range m.live {
		if _, ok := m.get(k, now); ok {
			keys = append(keys, k)
		}
	}
	fuzzSortStrings(keys)
	return keys
}

func fuzzSortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// fuzzClock is the WithClock seam: whole seconds, starts even, advances by even amounts.
type fuzzClock struct{ now time.Time }

func (c *fuzzClock) Now() time.Time { return c.now }
func (c *fuzzClock) unix() int64    { return c.now.Unix() }

// script is a cursor over the fuzz bytes.
type fuzzScript struct {
	b []byte
	i int
}

func (s *fuzzScript) more() bool { return s.i < len(s.b) }

func (s *fuzzScript) byte() byte {
	if s.i >= len(s.b) {
		return 0
	}
	v := s.b[s.i]
	s.i++
	return v
}

func (s *fuzzScript) key() string   { return "k" + strconv.Itoa(int(s.byte()%fuzzKeySpace)) }
func (s *fuzzScript) value() []byte { n := int(s.byte()); return bytes.Repeat([]byte{s.byte()}, n) }

// ttl is 1, 3, 5, ... 31 seconds: always odd.
func (s *fuzzScript) ttl() time.Duration { return time.Duration(2*int(s.byte()%16)+1) * time.Second }

// step is 2, 4, ... 32 seconds: always even.
func (s *fuzzScript) step() time.Duration { return time.Duration(2*(int(s.byte()%16)+1)) * time.Second }

// runScript executes one fuzz input against a fresh store and the model.
func runScript(t *testing.T, in []byte) {
	t.Helper()
	dir := t.TempDir()
	clock := &fuzzClock{now: time.Unix(1_800_000_000, 0).UTC()} // even second
	open := func() *DB {
		t.Helper()
		db, err := Open(dir,
			WithClock(clock.Now),
			WithMaxSegmentBytes(record.MaxRecord), // rotate as early as the format allows
		)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		return db
	}
	db := open()
	defer func() { _ = db.Close() }()

	m := &fuzzModel{live: map[string]fuzzVal{}, seen: map[string]bool{}}
	s := &fuzzScript{b: in}
	check := func(what string) {
		t.Helper()
		compareModel(t, db, m, clock.unix(), what)
	}

	for ops := 0; s.more() && ops < fuzzMaxOps; ops++ {
		op := s.byte()
		var err error
		what := ""
		switch op % 10 {
		case 0, 1: // put (weighted)
			k, v := s.key(), s.value()
			what = fmt.Sprintf("put %s (%d bytes)", k, len(v))
			if err = db.Put([]byte(k), v); err == nil {
				m.put(k, v, 0)
			}
		case 2: // put with TTL
			k, v, ttl := s.key(), s.value(), s.ttl()
			what = fmt.Sprintf("putttl %s %s", k, ttl)
			if err = db.PutTTL([]byte(k), v, ttl); err == nil {
				m.put(k, v, clock.now.Add(ttl).Unix())
			}
		case 3: // delete
			k := s.key()
			what = "delete " + k
			if err = db.Delete([]byte(k)); err == nil {
				m.del(k)
			}
		case 4: // batch of 1..4 members
			n := int(s.byte()%4) + 1
			b := db.Batch()
			type pend struct {
				k   string
				v   []byte
				exp int64
				del bool
			}
			var pending []pend
			for range n {
				switch s.byte() % 3 {
				case 0:
					k, v := s.key(), s.value()
					b.Put([]byte(k), v)
					pending = append(pending, pend{k: k, v: v})
				case 1:
					k, v, ttl := s.key(), s.value(), s.ttl()
					b.PutTTL([]byte(k), v, ttl)
					pending = append(pending, pend{k: k, v: v, exp: clock.now.Add(ttl).Unix()})
				case 2:
					k := s.key()
					b.Delete([]byte(k))
					pending = append(pending, pend{k: k, del: true})
				}
			}
			what = fmt.Sprintf("batch of %d", n)
			if err = b.Commit(); err == nil {
				for _, p := range pending {
					if p.del {
						m.del(p.k)
					} else {
						m.put(p.k, p.v, p.exp)
					}
				}
			}
		case 5: // get one key (the full compare below covers it; this exercises Has too)
			k := s.key()
			what = "get " + k
			want, ok := m.get(k, clock.unix())
			got, gerr := db.Get([]byte(k))
			has, herr := db.Has([]byte(k))
			switch {
			case herr != nil:
				err = herr
			case ok && (gerr != nil || !bytes.Equal(got, want) || !has):
				t.Fatalf("%s: Get = %q, %v Has = %v; want %q, live", what, got, gerr, has, want)
			case !ok && (!errors.Is(gerr, ErrNotFound) || has):
				t.Fatalf("%s: Get = %q, %v Has = %v; want ErrNotFound", what, got, gerr, has)
			}
		case 6: // snapshot iteration: full, prefix and range
			what = "snapshot-iter"
			err = compareSnapshot(t, db, m, clock.unix(), s.key())
		case 7: // compact
			what = "compact"
			err = db.Compact(context.Background())
		case 8: // advance the clock, or reopen
			if s.byte()%2 == 0 {
				d := s.step()
				clock.now = clock.now.Add(d)
				what = "advance " + d.String()
			} else {
				what = "reopen"
				if err = db.Close(); err == nil {
					db = open()
				}
			}
		case 9: // big value: enough of these rotate the active segment
			k := s.key()
			v := bytes.Repeat([]byte{s.byte()}, fuzzBigValue)
			what = fmt.Sprintf("put %s (%d bytes)", k, len(v))
			if err = db.Put([]byte(k), v); err == nil {
				m.put(k, v, 0)
			}
		}
		if err != nil {
			t.Fatalf("op %d %s: %v", ops, what, err)
		}
		check(fmt.Sprintf("after op %d (%s)", ops, what))
	}

	// Close + Open must reproduce the exact same view, then compaction must not change it either.
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db = open()
	check("after final reopen")
	if err := db.Compact(context.Background()); err != nil {
		t.Fatalf("final Compact: %v", err)
	}
	check("after final compact")
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db = open()
	check("after reopen post-compact")
	if got, want := db.Stats().Keys, len(m.liveKeys(clock.unix())); got != want {
		// Stats.Keys may still count keys that expired since the last check; it can never be below.
		if got < want {
			t.Fatalf("Stats().Keys = %d, want >= %d live keys", got, want)
		}
	}
}

// compareModel checks every key the model has ever seen against the DB.
func compareModel(t *testing.T, db *DB, m *fuzzModel, now int64, what string) {
	t.Helper()
	for k := range m.seen {
		want, ok := m.get(k, now)
		got, err := db.Get([]byte(k))
		switch {
		case ok && err != nil:
			t.Fatalf("%s: Get(%s) = %v, want %d-byte value", what, k, err, len(want))
		case ok && !bytes.Equal(got, want):
			t.Fatalf("%s: Get(%s) = %d bytes, want %d bytes (differs)", what, k, len(got), len(want))
		case !ok && !errors.Is(err, ErrNotFound):
			t.Fatalf("%s: Get(%s) = %q, %v; want ErrNotFound", what, k, got, err)
		}
	}
}

// compareSnapshot checks a full Iter, a prefix Iter and a Range against the model's sorted live set.
func compareSnapshot(t *testing.T, db *DB, m *fuzzModel, now int64, prefixKey string) error {
	t.Helper()
	snap, err := db.Snapshot()
	if err != nil {
		return err
	}
	defer func() { _ = snap.Close() }()
	live := m.liveKeys(now)
	if got := snap.Len(); got != len(live) {
		t.Fatalf("snapshot Len = %d, want %d live keys %v", got, len(live), live)
	}
	var got []string
	it := snap.Iter(nil)
	for it.Next() {
		k := string(it.Key())
		v, err := it.Value()
		if err != nil {
			return fmt.Errorf("Value(%s): %w", k, err)
		}
		want, ok := m.get(k, now)
		if !ok || !bytes.Equal(v, want) {
			t.Fatalf("snapshot Iter: %s = %d bytes, model live=%v %d bytes", k, len(v), ok, len(want))
		}
		got = append(got, k)
		if sv, err := snap.Get([]byte(k)); err != nil || !bytes.Equal(sv, v) {
			t.Fatalf("snapshot Get(%s) = %d bytes, %v; want %d bytes", k, len(sv), err, len(v))
		}
	}
	if err := it.Err(); err != nil {
		return err
	}
	if strings.Join(got, ",") != strings.Join(live, ",") {
		t.Fatalf("snapshot Iter keys = %v, want %v", got, live)
	}
	// Prefix: "k1" matches k1, k10..k15.
	prefix := prefixKey[:2]
	var wantP []string
	for _, k := range live {
		if strings.HasPrefix(k, prefix) {
			wantP = append(wantP, k)
		}
	}
	got = got[:0]
	it = snap.Iter([]byte(prefix))
	for it.Next() {
		got = append(got, string(it.Key()))
	}
	if err := it.Err(); err != nil {
		return err
	}
	if strings.Join(got, ",") != strings.Join(wantP, ",") {
		t.Fatalf("snapshot Iter(%q) = %v, want %v", prefix, got, wantP)
	}
	// Range [prefixKey, "k9"): every live key >= prefixKey and < "k9".
	lo, hi := prefixKey, "k9"
	var wantR []string
	for _, k := range live {
		if k >= lo && k < hi {
			wantR = append(wantR, k)
		}
	}
	got = got[:0]
	it = snap.Range([]byte(lo), []byte(hi))
	for it.Next() {
		got = append(got, string(it.Key()))
	}
	if err := it.Err(); err != nil {
		return err
	}
	if strings.Join(got, ",") != strings.Join(wantR, ",") {
		t.Fatalf("snapshot Range(%q, %q) = %v, want %v", lo, hi, got, wantR)
	}
	// Snapshot must survive later writes and a compaction: mutate, then re-read the pinned view.
	if len(live) > 0 {
		k := live[0]
		before, err := snap.Get([]byte(k))
		if err != nil {
			return err
		}
		if err := db.Put([]byte(k), append(bytes.Clone(before), '!')); err != nil {
			return err
		}
		m.put(k, append(bytes.Clone(before), '!'), 0) // a plain Put clears any earlier TTL
		if err := db.Compact(context.Background()); err != nil {
			return err
		}
		after, err := snap.Get([]byte(k))
		if err != nil || !bytes.Equal(after, before) {
			t.Fatalf("snapshot changed under a write+compact: %s = %d bytes, %v; want %d bytes", k, len(after), err, len(before))
		}
	}
	return nil
}

// seedScripts returns the in-code seeds (the same ones committed under testdata/fuzz/FuzzDB).
func seedScripts() [][]byte {
	// Encoding reminder: op%10 -> 0,1 put(k,len,fill) 2 putttl(k,len,fill,ttl) 3 del(k) 4 batch(n,...)
	// 5 get(k) 6 snapiter(k) 7 compact 8 clock(sel[,step]) 9 bigput(k,fill).
	return [][]byte{
		{},
		{0, 1, 3, 'a', 5, 1},
		{0, 1, 4, 'a', 0, 2, 4, 'b', 6, 1, 3, 1, 5, 1, 6, 1},
		{2, 1, 4, 'a', 0, 8, 0, 0, 5, 1, 8, 0, 15, 5, 1, 6, 0},
		{4, 3, 0, 1, 4, 'a', 1, 2, 4, 'b', 3, 2, 3, 6, 0, 7, 6, 0, 8, 1, 6, 0},
		{0, 1, 4, 'a', 0, 1, 4, 'b', 0, 1, 4, 'c', 7, 5, 1, 8, 1, 5, 1, 7, 6, 0},
		{9, 1, 'x', 9, 2, 'y', 9, 3, 'z', 9, 4, 'w', 7, 6, 0, 8, 1, 6, 0, 9, 1, 'q', 7, 6, 0},
		{2, 1, 4, 'a', 15, 2, 2, 4, 'b', 0, 8, 0, 0, 6, 0, 8, 0, 15, 6, 0, 7, 6, 0, 8, 1, 6, 0},
	}
}

// FuzzCommitMarker pins the commit-marker decoder recovery runs on every FlagBatchCommit value:
// arbitrary bytes never panic and never yield a count above MaxInt32, and every marker the writer
// encodes (uvarint count + uvarint batchID) decodes to its count.
func FuzzCommitMarker(f *testing.F) {
	f.Add([]byte{}, uint32(0), uint64(0))
	f.Add([]byte{1, 42}, uint32(1), uint64(42))
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01, 0}, uint32(100), uint64(1<<63))
	f.Add([]byte{0x80}, uint32(math.MaxInt32), uint64(7))
	f.Fuzz(func(t *testing.T, raw []byte, count uint32, id uint64) {
		if n, ok := decodeCommit(raw); ok && (n < 0 || n > math.MaxInt32) {
			t.Fatalf("decodeCommit(% x) = %d, outside [0, MaxInt32]", raw, n)
		}
		enc := binary.AppendUvarint(binary.AppendUvarint(nil, uint64(count)), id)
		n, ok := decodeCommit(enc)
		if count > math.MaxInt32 {
			if ok {
				t.Fatalf("decodeCommit accepted count %d", count)
			}
			return
		}
		if !ok || n != int(count) {
			t.Fatalf("decodeCommit(count=%d, id=%d) = %d, %v", count, id, n, ok)
		}
	})
}
