package kvlite

// In-memory stand-ins for the workstream-A packages (record encode/decode, segment files, MANIFEST) so
// the engine tests can run while those bodies are stubs, plus the shared test helpers. openEngine probes
// each package once per process and swaps in a fake only for what is still stubbed; once record,
// segment, manifest and Open are real the same tests run unchanged against the real files. The fakes
// stay useful afterwards for fault injection (failing Append/Sync) that real files cannot provide.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/crowbett/kvlite/internal/manifest"
	"github.com/crowbett/kvlite/internal/record"
	"github.com/crowbett/kvlite/internal/segment"
)

func isStub(err error) bool { return err != nil && strings.Contains(err.Error(), "stub:") }

var probe struct {
	once                                        sync.Once
	recordReal, segmentReal, manifestReal, open bool
}

// probeLanes finds out which of the other lanes' bodies are real. It runs once per test binary.
func probeLanes() {
	probe.once.Do(func() {
		_, err := record.Append(nil, record.Record{Key: []byte("k"), TS: time.Unix(1, 0)})
		probe.recordReal = !isStub(err)
		dir, err := os.MkdirTemp("", "kvlite-probe")
		if err != nil {
			return
		}
		defer os.RemoveAll(dir)
		if w, err := segment.Create(dir, 0); !isStub(err) {
			probe.segmentReal = true
			if w != nil {
				_ = w.Close()
			}
		}
		probe.manifestReal = !isStub(manifest.Save(dir, &manifest.Manifest{Segments: []uint32{0}}))
		if probe.recordReal && probe.segmentReal && probe.manifestReal {
			db, err := Open(filepath.Join(dir, "open"))
			if err == nil {
				_ = db.Close()
			}
			probe.open = !isStub(err)
		}
	})
}

// openEngine opens a fresh store in a temp dir: through Open when every other lane is real, otherwise
// through newDBForTest with fakes for whatever is still a stub. It is closed at cleanup.
func openEngine(t *testing.T, opts ...Option) *DB {
	t.Helper()
	probeLanes()
	dir := t.TempDir()
	var (
		db  *DB
		err error
	)
	if probe.open {
		db, err = Open(dir, opts...)
	} else {
		db, err = newDBForTest(dir, installFakes, opts...)
	}
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil && !errors.Is(err, ErrClosed) {
			t.Errorf("close: %v", err)
		}
	})
	return db
}

// openFake always builds the store on a memFS (real record codec when available) so a test can inject
// faults. It is closed at cleanup.
func openFake(t *testing.T, opts ...Option) (*DB, *memFS) {
	t.Helper()
	probeLanes()
	fs := newMemFS()
	db, err := newDBForTest(t.TempDir(), fs.install, opts...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil && !errors.Is(err, ErrClosed) {
			t.Errorf("close: %v", err)
		}
	})
	return db, fs
}

// installFakes replaces exactly the seams whose real implementation is still a stub. A stub record
// codec forces the fake segment too, since a real Reader would decode with the stub.
func installFakes(db *DB) {
	fs := newMemFS()
	if !probe.segmentReal || !probe.recordReal {
		db.createSegment = fs.create
		db.openSegment = fs.open
	}
	if !probe.manifestReal {
		db.saveManifest = fs.save
	}
	if !probe.recordReal {
		db.encode = testEncode
	}
}

// testClock is the WithClock seam the engine TTL tests use; nothing here sleeps.
type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func newTestClock() *testClock { return &testClock{t: time.Unix(1_700_000_000, 0)} }

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// memFS is an in-memory segment store and manifest log.
type memFS struct {
	mu   sync.Mutex
	segs map[uint32]*memSeg
	mans []*manifest.Manifest

	failAppend atomic.Pointer[error] // every Append fails with this while set
	failSync   atomic.Pointer[error] // every Sync fails with this while set
	syncs      atomic.Int64
}

type memSeg struct {
	mu   sync.RWMutex
	id   uint32
	data []byte
}

func newMemFS() *memFS { return &memFS{segs: make(map[uint32]*memSeg)} }

// install points every seam of db at the memFS (the real record codec when lane A has landed).
func (fs *memFS) install(db *DB) {
	db.createSegment = fs.create
	db.openSegment = fs.open
	db.saveManifest = fs.save
	if !probe.recordReal {
		db.encode = testEncode
	}
}

func (fs *memFS) create(_ string, id uint32) (segWriter, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if _, ok := fs.segs[id]; ok {
		return nil, fmt.Errorf("segment %d: %w", id, os.ErrExist)
	}
	s := &memSeg{id: id}
	s.data = append(s.data, record.SegmentMagic...)
	s.data = append(s.data, record.FormatVersion)
	s.data = binary.LittleEndian.AppendUint32(s.data, id)
	s.data = append(s.data, make([]byte, record.SegmentHeaderSize-9)...)
	fs.segs[id] = s
	return &memWriter{fs: fs, s: s}, nil
}

func (fs *memFS) open(_ string, id uint32) (segReader, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	s, ok := fs.segs[id]
	if !ok {
		return nil, fmt.Errorf("segment %d: %w", id, os.ErrNotExist)
	}
	return &memReader{fs: fs, s: s}, nil
}

func (fs *memFS) save(_ string, m *manifest.Manifest) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	c := *m
	c.Segments = slices.Clone(m.Segments)
	fs.mans = append(fs.mans, &c)
	return nil
}

type memWriter struct {
	fs     *memFS
	s      *memSeg
	closed bool
}

func (w *memWriter) ID() uint32 { return w.s.id }

func (w *memWriter) Append(rec []byte) (uint32, error) {
	if w.closed {
		return 0, os.ErrClosed
	}
	if p := w.fs.failAppend.Load(); p != nil {
		return 0, *p
	}
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	off := len(w.s.data)
	w.s.data = append(w.s.data, rec...)
	return uint32(off), nil
}

func (w *memWriter) Size() int64 {
	w.s.mu.RLock()
	defer w.s.mu.RUnlock()
	return int64(len(w.s.data))
}

func (w *memWriter) Sync() error {
	if w.closed {
		return os.ErrClosed
	}
	if p := w.fs.failSync.Load(); p != nil {
		return *p
	}
	w.fs.syncs.Add(1)
	return nil
}

func (w *memWriter) Close() error {
	if w.closed {
		return nil
	}
	err := w.Sync()
	w.closed = true
	return err
}

type memReader struct {
	fs     *memFS
	s      *memSeg
	refs   atomic.Int32
	closed atomic.Bool
}

func (r *memReader) ID() uint32 { return r.s.id }

func (r *memReader) ReadAt(off, size uint32) (record.Record, []byte, error) {
	if r.closed.Load() {
		return record.Record{}, nil, os.ErrClosed
	}
	r.s.mu.RLock()
	if int(off)+int(size) > len(r.s.data) || size < record.HeaderSize {
		r.s.mu.RUnlock()
		return record.Record{}, nil, segment.ErrOutOfRange
	}
	buf := slices.Clone(r.s.data[off : off+size])
	r.s.mu.RUnlock()
	rec, err := decodeFull(buf)
	return rec, buf, err
}

func (r *memReader) Scan(fn func(off uint32, rec record.Record) error) error {
	if r.closed.Load() {
		return os.ErrClosed
	}
	r.s.mu.RLock()
	data := slices.Clone(r.s.data)
	r.s.mu.RUnlock()
	off := record.SegmentHeaderSize
	for off < len(data) {
		_, plen, err := record.ParseHeader(data[off:])
		if err != nil {
			return fmt.Errorf("scan at %d: %w", off, err)
		}
		end := off + record.HeaderSize + int(plen)
		if end > len(data) {
			return fmt.Errorf("scan at %d: %w", off, record.ErrTruncated)
		}
		rec, err := decodeFull(data[off:end])
		if err != nil {
			return fmt.Errorf("scan at %d: %w", off, err)
		}
		if err := fn(uint32(off), rec); err != nil {
			return err
		}
		off = end
	}
	return nil
}

func (r *memReader) Ref()      { r.refs.Add(1) }
func (r *memReader) Unref()    { r.refs.Add(-1) }
func (r *memReader) Refs() int { return int(r.refs.Load()) }
func (r *memReader) Close() error {
	r.closed.Store(true)
	return nil
}

// decodeFull checks and decodes one complete on-disk record (header + payload).
func decodeFull(buf []byte) (record.Record, error) {
	crc, plen, err := record.ParseHeader(buf)
	if err != nil {
		return record.Record{}, err
	}
	if int(plen)+record.HeaderSize != len(buf) {
		return record.Record{}, record.ErrMalformed
	}
	payload := buf[record.HeaderSize:]
	if !record.Verify(payload, crc) {
		return record.Record{}, record.ErrChecksum
	}
	if probe.recordReal {
		return record.Decode(payload)
	}
	return testDecode(payload)
}

// testEncode/testDecode implement the documented payload layout (docs/PLAN.md "On-disk format") for
// use until record.Append/Decode land: flags | tsMillis | [expiryMillis] | keyLen | valLen | key | value.
func testEncode(dst []byte, r record.Record) ([]byte, error) {
	if len(r.Key) > record.MaxKey || len(r.Value) > record.MaxValue {
		return dst, record.ErrTooLarge
	}
	start := len(dst)
	dst = append(dst, make([]byte, record.HeaderSize)...)
	dst = append(dst, byte(r.Flags))
	dst = binary.AppendUvarint(dst, uint64(r.TS.UnixMilli()))
	if r.Flags.HasExpiry() {
		dst = binary.AppendUvarint(dst, uint64(r.Expiry.UnixMilli()))
	}
	dst = binary.AppendUvarint(dst, uint64(len(r.Key)))
	dst = binary.AppendUvarint(dst, uint64(len(r.Value)))
	dst = append(dst, r.Key...)
	dst = append(dst, r.Value...)
	payload := dst[start+record.HeaderSize:]
	record.PutHeader(dst[start:], record.Checksum(payload), uint32(len(payload)))
	return dst, nil
}

func testDecode(p []byte) (record.Record, error) {
	if len(p) < 1 {
		return record.Record{}, record.ErrMalformed
	}
	r := record.Record{Flags: record.Flags(p[0])}
	if r.Flags&^record.FlagsMask != 0 {
		return record.Record{}, record.ErrMalformed
	}
	p = p[1:]
	next := func() (uint64, error) {
		v, n := binary.Uvarint(p)
		if n <= 0 {
			return 0, record.ErrMalformed
		}
		p = p[n:]
		return v, nil
	}
	ts, err := next()
	if err != nil {
		return record.Record{}, err
	}
	r.TS = time.UnixMilli(int64(ts))
	if r.Flags.HasExpiry() {
		exp, err := next()
		if err != nil {
			return record.Record{}, err
		}
		r.Expiry = time.UnixMilli(int64(exp))
	}
	kl, err := next()
	if err != nil {
		return record.Record{}, err
	}
	vl, err := next()
	if err != nil {
		return record.Record{}, err
	}
	if kl > record.MaxKey || vl > record.MaxValue {
		return record.Record{}, record.ErrTooLarge
	}
	if uint64(len(p)) != kl+vl {
		return record.Record{}, record.ErrMalformed
	}
	r.Key = p[:kl]
	r.Value = p[kl:]
	return r, nil
}

// scanActive returns every record in the active segment in file order, via the reader table.
func scanActive(t *testing.T, db *DB) []record.Record {
	t.Helper()
	db.rmu.RLock()
	r := db.readers[db.man.Active]
	db.rmu.RUnlock()
	if r == nil {
		t.Fatal("no reader for the active segment")
	}
	var recs []record.Record
	err := r.Scan(func(_ uint32, rec record.Record) error {
		c := rec
		c.Key = slices.Clone(rec.Key)
		c.Value = slices.Clone(rec.Value)
		recs = append(recs, c)
		return nil
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	return recs
}

// waitFor polls cond every millisecond until it holds or the deadline passes.
func waitFor(t *testing.T, d time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
	return true
}
