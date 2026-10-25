// Command webdemo is kvlite's in-browser demo bridge, compiled to js/wasm by
// crowbett.github.io. The page gives the binary an in-memory file system, so
// the real engine writes real segment, hint and MANIFEST files; the bridge
// drives it and reads those files back with the engine's own segment reader.
// Contract: https://github.com/crowbett/crowbett.github.io/blob/main/docs/demo-contract.md
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/crowbett/kvlite"
	"github.com/crowbett/kvlite/internal/fslock"
	"github.com/crowbett/kvlite/internal/manifest"
	"github.com/crowbett/kvlite/internal/record"
	"github.com/crowbett/kvlite/internal/segment"
)

const (
	maxKey     = 64
	maxValue   = 256
	maxTear    = 4096
	maxRecords = 200 // per segment, newest kept
	maxKeys    = 50
	maxLog     = 20
)

// Demo is one store in dir. The worker makes one call at a time, so it has no lock.
type Demo struct {
	dir  string
	db   *kvlite.DB
	logs []string
}

// KeyParams names a key, and the value for a put.
type KeyParams struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// CrashParams drops the open store without Close, as a killed process would,
// and cuts Tear bytes off the end of the active segment before reopening.
type CrashParams struct {
	Tear int64 `json:"tear"`
}

// View is the store after a call: the engine's Stats, every file on disk, the
// records in each segment, the current keys, and the engine's own log lines.
type View struct {
	Stats    Stats      `json:"stats"`
	Files    []File     `json:"files"`
	Segments []Segment  `json:"segments"`
	Keys     []KV       `json:"keys"`
	KeysMore int        `json:"keysMore"`
	Log      []string   `json:"log"`
	Get      *GetResult `json:"get,omitempty"`
}

// Stats is the subset of kvlite.Stats the page shows.
type Stats struct {
	Keys           int     `json:"keys"`
	Segments       int     `json:"segments"`
	ActiveSegment  uint32  `json:"activeSegment"`
	LiveBytes      int64   `json:"liveBytes"`
	DeadBytes      int64   `json:"deadBytes"`
	DeadRatio      float64 `json:"deadRatio"`
	ReclaimedBytes int64   `json:"reclaimedBytes"`
	Compactions    uint64  `json:"compactions"`
}

// File is one file under the store directory.
type File struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

// Segment is one log file as the segment reader decodes it. Records holds the
// newest maxRecords; More counts older ones left out. Torn is the reader's
// error when the scan stopped at a bad record.
type Segment struct {
	ID      uint32 `json:"id"`
	Active  bool   `json:"active"`
	Hint    bool   `json:"hint"`
	Bytes   int64  `json:"bytes"`
	Records []Rec  `json:"records"`
	More    int    `json:"more"`
	Torn    string `json:"torn,omitempty"`
}

// Rec is one decoded record: its offset and encoded size in the file.
type Rec struct {
	Off   uint32 `json:"off"`
	Size  int    `json:"size"`
	Kind  string `json:"kind"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

// KV is a current key and value from a snapshot.
type KV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// GetResult is the answer to a get.
type GetResult struct {
	Key   string `json:"key"`
	Found bool   `json:"found"`
	Value string `json:"value"`
}

var demo = &Demo{dir: "/kvlite"}

// Handle answers one bridge call. Errors come back as {"error": "..."}.
func Handle(method string, params []byte) any { return demo.Handle(method, params) }

// Handle answers one call on d.
func (d *Demo) Handle(method string, params []byte) (res any) {
	defer func() {
		if v := recover(); v != nil {
			res = errorResult(fmt.Errorf("internal: %v", v))
		}
	}()
	v, err := d.handle(method, params)
	if err != nil {
		return errorResult(err)
	}
	return v
}

func errorResult(err error) map[string]string {
	return map[string]string{"error": err.Error()}
}

func (d *Demo) handle(method string, params []byte) (View, error) {
	if d.db == nil {
		if err := d.open(); err != nil {
			return View{}, err
		}
	}
	var get *GetResult
	switch method {
	case "state":
	case "put", "delete", "get":
		var p KeyParams
		if err := json.Unmarshal(params, &p); err != nil {
			return View{}, err
		}
		if p.Key == "" || len(p.Key) > maxKey {
			return View{}, fmt.Errorf("key must be 1 to %d bytes", maxKey)
		}
		if len(p.Value) > maxValue {
			return View{}, fmt.Errorf("value must be at most %d bytes", maxValue)
		}
		var err error
		switch method {
		case "put":
			err = d.db.Put([]byte(p.Key), []byte(p.Value))
		case "delete":
			err = d.db.Delete([]byte(p.Key))
		default:
			get = &GetResult{Key: p.Key}
			var v []byte
			v, err = d.db.Get([]byte(p.Key))
			if errors.Is(err, kvlite.ErrNotFound) {
				err = nil
			} else if err == nil {
				get.Found, get.Value = true, string(v)
			}
		}
		if err != nil {
			return View{}, err
		}
	case "compact":
		if err := d.db.Compact(context.Background()); err != nil {
			return View{}, err
		}
	case "crash":
		var p CrashParams
		if err := json.Unmarshal(params, &p); err != nil {
			return View{}, err
		}
		if p.Tear < 0 || p.Tear > maxTear {
			return View{}, fmt.Errorf("tear must be 0 to %d bytes", maxTear)
		}
		if err := d.crash(p.Tear); err != nil {
			return View{}, err
		}
	case "reset":
		if err := d.db.Close(); err != nil {
			return View{}, err
		}
		d.db, d.logs = nil, nil
		if err := os.RemoveAll(d.dir); err != nil {
			return View{}, err
		}
		if err := d.open(); err != nil {
			return View{}, err
		}
	default:
		return View{}, fmt.Errorf("unknown method %q", method)
	}
	v, err := d.view()
	v.Get = get
	return v, err
}

func (d *Demo) open() error {
	if err := os.MkdirAll(d.dir, 0o750); err != nil {
		return err
	}
	db, err := kvlite.Open(d.dir, kvlite.WithLogger(slog.New(&logHandler{d: d})))
	if err != nil {
		return err
	}
	d.db = db
	return nil
}

// crash abandons the open DB without Close. The browser build has no lock
// primitive, so kvlite's O_EXCL LOCK file survives the crash; it is removed by
// hand, as internal/fslock says an operator must. Then Open recovers.
func (d *Demo) crash(tear int64) error {
	active := d.db.Stats().ActiveSegment
	d.db = nil
	if err := os.Remove(filepath.Join(d.dir, fslock.FileName)); err != nil {
		return err
	}
	if tear > 0 {
		path := filepath.Join(d.dir, segment.LogName(active))
		fi, err := os.Stat(path)
		if err != nil {
			return err
		}
		if err := os.Truncate(path, max(record.SegmentHeaderSize, fi.Size()-tear)); err != nil {
			return err
		}
	}
	return d.open()
}

func (d *Demo) view() (View, error) {
	s := d.db.Stats()
	v := View{
		Stats: Stats{
			Keys: s.Keys, Segments: s.Segments, ActiveSegment: s.ActiveSegment,
			LiveBytes: s.LiveBytes, DeadBytes: s.DeadBytes, DeadRatio: s.DeadRatio,
			ReclaimedBytes: s.ReclaimedBytes, Compactions: s.Compactions,
		},
		Files:    []File{},
		Segments: []Segment{},
		Keys:     []KV{},
		Log:      slices.Clone(d.logs),
	}
	if err := d.files(&v); err != nil {
		return View{}, err
	}
	if err := d.keys(&v); err != nil {
		return View{}, err
	}
	return v, nil
}

func (d *Demo) files(v *View) error {
	hints := map[uint32]bool{}
	var logs []uint32
	for _, sub := range []string{"", manifest.TrashDir} {
		ents, err := os.ReadDir(filepath.Join(d.dir, sub))
		if errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return err
		}
		for _, e := range ents {
			if e.IsDir() {
				continue
			}
			fi, err := e.Info()
			if err != nil {
				return err
			}
			v.Files = append(v.Files, File{Name: filepath.Join(sub, e.Name()), Size: fi.Size()})
			if id, hint, ok := segment.ParseName(e.Name()); ok && sub == "" {
				if hint {
					hints[id] = true
				} else {
					logs = append(logs, id)
				}
			}
		}
	}
	slices.Sort(logs)
	for _, id := range logs {
		seg, err := d.segment(id)
		if err != nil {
			return err
		}
		seg.Active = id == v.Stats.ActiveSegment
		seg.Hint = hints[id]
		v.Segments = append(v.Segments, seg)
	}
	return nil
}

// segment decodes one log with the engine's reader.
func (d *Demo) segment(id uint32) (Segment, error) {
	r, err := segment.OpenReader(d.dir, id)
	if err != nil {
		return Segment{}, err
	}
	defer r.Close()
	seg := Segment{ID: id, Records: []Rec{}}
	if seg.Bytes, err = r.Size(); err != nil {
		return Segment{}, err
	}
	err = r.Scan(func(off uint32, rec record.Record) error {
		seg.Records = append(seg.Records, Rec{Off: off, Size: record.EncodedSize(rec), Kind: kind(rec.Flags), Key: string(rec.Key), Value: value(rec)})
		return nil
	})
	if se := (*segment.ScanError)(nil); errors.As(err, &se) {
		seg.Torn = se.Error()
	} else if err != nil {
		return Segment{}, err
	}
	if n := len(seg.Records) - maxRecords; n > 0 {
		seg.More = n
		seg.Records = seg.Records[n:]
	}
	return seg, nil
}

func kind(f record.Flags) string {
	switch {
	case f.BatchCommit():
		return "commit"
	case f.Tombstone():
		return "delete"
	case f.BatchMember():
		return "batch put"
	default:
		return "put"
	}
}

func value(rec record.Record) string {
	if rec.Flags.Tombstone() || rec.Flags.BatchCommit() {
		return ""
	}
	return string(rec.Value)
}

func (d *Demo) keys(v *View) error {
	snap, err := d.db.Snapshot()
	if err != nil {
		return err
	}
	defer snap.Close()
	v.KeysMore = max(0, snap.Len()-maxKeys)
	it := snap.Iter(nil)
	for len(v.Keys) < maxKeys && it.Next() {
		val, err := it.Value()
		if err != nil {
			return err
		}
		v.Keys = append(v.Keys, KV{Key: string(it.Key()), Value: string(val)})
	}
	return it.Err()
}

// logHandler keeps the engine's last maxLog log lines for the page.
type logHandler struct {
	d     *Demo
	attrs []slog.Attr
}

func (h *logHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *logHandler) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder
	b.WriteString(r.Level.String() + " " + r.Message)
	write := func(a slog.Attr) bool {
		fmt.Fprintf(&b, " %s=%v", a.Key, a.Value)
		return true
	}
	for _, a := range h.attrs {
		write(a)
	}
	r.Attrs(write)
	h.d.logs = append(h.d.logs, b.String())
	if n := len(h.d.logs) - maxLog; n > 0 {
		h.d.logs = h.d.logs[n:]
	}
	return nil
}

func (h *logHandler) WithAttrs(as []slog.Attr) slog.Handler {
	return &logHandler{d: h.d, attrs: append(slices.Clone(h.attrs), as...)}
}

func (h *logHandler) WithGroup(string) slog.Handler { return h }
