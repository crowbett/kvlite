package main

import (
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"testing"
)

// newDemo opens a demo store in a temp dir and closes it when the test ends.
func newDemo(t *testing.T) *Demo {
	t.Helper()
	d := &Demo{dir: t.TempDir()}
	t.Cleanup(func() {
		if d.db != nil {
			_ = d.db.Close()
		}
	})
	return d
}

func call(t *testing.T, d *Demo, method, params string) View {
	t.Helper()
	switch v := d.Handle(method, []byte(params)).(type) {
	case View:
		return v
	default:
		t.Fatalf("%s %s: %v", method, params, v)
		return View{}
	}
}

func kinds(s Segment) string {
	out := make([]string, len(s.Records))
	for i, r := range s.Records {
		out[i] = r.Kind + ":" + r.Key
	}
	return strings.Join(out, " ")
}

func TestPutDeleteGet(t *testing.T) {
	t.Parallel()
	d := newDemo(t)
	call(t, d, "put", `{"key":"a","value":"1"}`)
	call(t, d, "put", `{"key":"b","value":"2"}`)
	call(t, d, "put", `{"key":"a","value":"3"}`)
	v := call(t, d, "delete", `{"key":"b"}`)
	if len(v.Segments) != 1 || !v.Segments[0].Active {
		t.Fatalf("segments %+v, want one active", v.Segments)
	}
	if got := kinds(v.Segments[0]); got != "put:a put:b put:a delete:b" {
		t.Errorf("records %q", got)
	}
	r := v.Segments[0].Records
	if r[0].Off != 16 || r[1].Off != r[0].Off+uint32(r[0].Size) {
		t.Errorf("offsets %+v, want back to back after the 16-byte header", r)
	}
	if v.Stats.Keys != 1 || v.Stats.DeadBytes == 0 || len(v.Keys) != 1 || v.Keys[0] != (KV{"a", "3"}) {
		t.Errorf("stats %+v keys %+v", v.Stats, v.Keys)
	}
	if g := call(t, d, "get", `{"key":"a"}`).Get; g == nil || !g.Found || g.Value != "3" {
		t.Errorf("get a: %+v", g)
	}
	if g := call(t, d, "get", `{"key":"b"}`).Get; g == nil || g.Found {
		t.Errorf("get b after delete: %+v", g)
	}
}

func TestCompact(t *testing.T) {
	t.Parallel()
	d := newDemo(t)
	for i := range 6 {
		call(t, d, "put", fmt.Sprintf(`{"key":"k%d","value":"v%d"}`, i%2, i))
	}
	before := call(t, d, "state", `{}`)
	v := call(t, d, "compact", `{}`)
	if v.Stats.Compactions != 1 || v.Stats.ReclaimedBytes == 0 {
		t.Errorf("after compact: %+v", v.Stats)
	}
	var merged *Segment
	for i, s := range v.Segments {
		if s.ID == before.Stats.ActiveSegment {
			t.Errorf("input segment %d still on disk", s.ID)
		}
		if !s.Active {
			merged = &v.Segments[i]
		}
	}
	if merged == nil || !merged.Hint || kinds(*merged) != "put:k0 put:k1" {
		t.Errorf("merged segment %+v, want k0 and k1 with a hint", merged)
	}
	if v.Keys[0] != (KV{"k0", "v4"}) || v.Keys[1] != (KV{"k1", "v5"}) {
		t.Errorf("keys %+v", v.Keys)
	}
}

func TestCrash(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("crash removes the LOCK file the dropped store still holds open, which Windows refuses; the demo runs as js/wasm")
	}
	d := newDemo(t)
	call(t, d, "put", `{"key":"a","value":"1"}`)
	call(t, d, "put", `{"key":"b","value":"2"}`)
	v := call(t, d, "crash", `{"tear":0}`)
	if len(v.Keys) != 2 {
		t.Errorf("clean crash lost keys: %+v", v.Keys)
	}
	v = call(t, d, "crash", `{"tear":3}`)
	if len(v.Keys) != 1 || v.Keys[0].Key != "a" {
		t.Errorf("torn crash: keys %+v, want only a", v.Keys)
	}
	if !strings.Contains(strings.Join(v.Log, "\n"), "truncated torn tail") {
		t.Errorf("log %q, want the torn-tail warning", v.Log)
	}
	if got := kinds(v.Segments[len(v.Segments)-1]); got != "put:a" {
		t.Errorf("active segment after recovery %q", got)
	}
}

func TestReset(t *testing.T) {
	t.Parallel()
	d := newDemo(t)
	call(t, d, "put", `{"key":"a","value":"1"}`)
	v := call(t, d, "reset", `{}`)
	if v.Stats.Keys != 0 || len(v.Keys) != 0 || len(v.Log) != 0 || len(v.Segments) != 1 || len(v.Segments[0].Records) != 0 {
		t.Errorf("after reset: %+v", v)
	}
}

func TestRecordCap(t *testing.T) {
	t.Parallel()
	d := newDemo(t)
	var v View
	for i := range maxRecords + 5 {
		v = call(t, d, "put", fmt.Sprintf(`{"key":"k%d","value":"v"}`, i))
	}
	s := v.Segments[0]
	if len(s.Records) != maxRecords || s.More != 5 || s.Records[len(s.Records)-1].Key != fmt.Sprintf("k%d", maxRecords+4) {
		t.Errorf("got %d records, %d more, last %q", len(s.Records), s.More, s.Records[len(s.Records)-1].Key)
	}
	if len(v.Keys) != maxKeys || v.KeysMore != maxRecords+5-maxKeys {
		t.Errorf("keys %d, more %d", len(v.Keys), v.KeysMore)
	}
}

func TestHandleErrors(t *testing.T) {
	t.Parallel()
	d := newDemo(t)
	long := strings.Repeat("x", maxValue+1)
	cases := []struct{ method, params, want string }{
		{"nope", `{}`, `unknown method "nope"`},
		{"put", `{`, "unexpected end"},
		{"put", `{"key":"","value":"v"}`, "key must be 1 to 64 bytes"},
		{"put", `{"key":"` + long[:maxKey+1] + `"}`, "key must be 1 to 64 bytes"},
		{"put", `{"key":"k","value":"` + long + `"}`, "value must be at most 256 bytes"},
		{"crash", `{"tear":-1}`, "tear must be 0 to 4096 bytes"},
		{"crash", `{"tear":4097}`, "tear must be 0 to 4096 bytes"},
	}
	for _, c := range cases {
		got, ok := d.Handle(c.method, []byte(c.params)).(map[string]string)
		if !ok || !strings.Contains(got["error"], c.want) {
			t.Errorf("%s %s: got %v, want an error containing %q", c.method, c.params, got, c.want)
		}
	}
}

// TestViewHasNoNulls pins that every list is an array, never null: the page
// reads .length on each.
func TestViewHasNoNulls(t *testing.T) {
	t.Parallel()
	b, err := json.Marshal(newDemo(t).Handle("state", []byte(`{}`)))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "null") {
		t.Errorf("view has a null: %s", b)
	}
}
