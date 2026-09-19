package bench

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

var sizes = []struct {
	name string
	n    int
}{
	{"100B", 100},
	{"1KiB", 1 << 10},
	{"16KiB", 16 << 10},
}

// openStore opens engine in a temp dir and closes it on cleanup.
func openStore(tb testing.TB, engine string) Store {
	tb.Helper()
	st, err := Open(engine, tb.TempDir())
	if err != nil {
		tb.Fatalf("Open(%s): %v", engine, err)
	}
	tb.Cleanup(func() { _ = st.Close() })
	return st
}

func TestOpenUnknown(t *testing.T) {
	t.Parallel()
	if _, err := Open("nope", t.TempDir()); !errors.Is(err, ErrUnknownEngine) {
		t.Fatalf("Open(nope) = %v, want ErrUnknownEngine", err)
	}
}

// TestStoreContract runs the same put/get/delete/scan script through every engine.
func TestStoreContract(t *testing.T) {
	t.Parallel()
	for _, engine := range Engines {
		t.Run(engine, func(t *testing.T) {
			t.Parallel()
			st := openStore(t, engine)
			if _, err := st.Get([]byte("missing")); !errors.Is(err, ErrNotFound) {
				t.Fatalf("Get(missing) = %v, want ErrNotFound", err)
			}
			for _, k := range []string{"a/1", "a/2", "b/1", "a/0"} {
				if err := st.Put([]byte(k), []byte("v-"+k)); err != nil {
					t.Fatalf("Put(%s): %v", k, err)
				}
			}
			v, err := st.Get([]byte("a/1"))
			if err != nil || string(v) != "v-a/1" {
				t.Fatalf("Get(a/1) = %q, %v", v, err)
			}
			if err := st.Put([]byte("a/1"), []byte("new")); err != nil {
				t.Fatal(err)
			}
			if v, _ := st.Get([]byte("a/1")); string(v) != "new" {
				t.Fatalf("overwrite not visible: %q", v)
			}
			if err := st.Delete([]byte("a/2")); err != nil {
				t.Fatal(err)
			}
			if _, err := st.Get([]byte("a/2")); !errors.Is(err, ErrNotFound) {
				t.Fatalf("Get(deleted) = %v, want ErrNotFound", err)
			}
			var got []string
			if err := st.Scan([]byte("a/"), func(k, v []byte) bool {
				got = append(got, string(k)+"="+string(v))
				return true
			}); err != nil {
				t.Fatal(err)
			}
			if want := "a/0=v-a/0 a/1=new"; strings.Join(got, " ") != want {
				t.Fatalf("Scan(a/) = %q, want %q", got, want)
			}
			got = nil
			if err := st.Scan(nil, func(k, _ []byte) bool {
				got = append(got, string(k))
				return len(got) < 2
			}); err != nil {
				t.Fatal(err)
			}
			if want := "a/0 a/1"; strings.Join(got, " ") != want {
				t.Fatalf("Scan(nil, stop after 2) = %q, want %q", got, want)
			}
			if c, ok := st.(Compactor); ok {
				if err := c.Compact(context.Background()); err != nil {
					t.Fatalf("Compact: %v", err)
				}
				if v, _ := st.Get([]byte("b/1")); string(v) != "v-b/1" {
					t.Fatalf("value lost across compact: %q", v)
				}
			}
		})
	}
}

func TestRunEngine(t *testing.T) {
	t.Parallel()
	for _, engine := range Engines {
		t.Run(engine, func(t *testing.T) {
			t.Parallel()
			res, err := RunEngine(context.Background(), engine, t.TempDir(), Workload{N: 300, ValueSize: 64})
			if err != nil {
				t.Fatalf("RunEngine: %v", err)
			}
			want := []string{"put", "get", "scan", "delete"}
			if len(res.Rows) != len(want) {
				t.Fatalf("rows = %d, want %d", len(res.Rows), len(want))
			}
			for i, m := range res.Rows {
				wantOps := 300
				if m.Op == "delete" {
					wantOps = 150
				}
				// A 300-key mmap scan can finish under the Windows clock resolution, so only require a
				// positive rate when a positive duration was measured.
				if m.Op != want[i] || m.Engine != engine || m.Ops != wantOps || (m.Elapsed > 0 && m.OpsPerSec <= 0) {
					t.Errorf("row %d = %+v", i, m)
				}
			}
			if res.DiskBefore <= 0 || res.DiskAfter <= 0 {
				t.Errorf("disk = %d/%d, want > 0", res.DiskBefore, res.DiskAfter)
			}
			var buf bytes.Buffer
			if err := WriteTable(&buf, []EngineResult{res}); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(buf.String(), engine) || !strings.Contains(buf.String(), "disk before") {
				t.Errorf("table:\n%s", buf.String())
			}
		})
	}
	if _, err := RunEngine(context.Background(), "bbolt", t.TempDir(), Workload{}); err == nil {
		t.Error("RunEngine(N=0) = nil error")
	}
}

func TestPrefixUpperBound(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in, want []byte
	}{
		{nil, nil},
		{[]byte("a"), []byte("b")},
		{[]byte("ab\xff"), []byte("ac")},
		{[]byte("\xff\xff"), nil},
	}
	for _, tt := range tests {
		if got := PrefixUpperBound(tt.in); !bytes.Equal(got, tt.want) {
			t.Errorf("PrefixUpperBound(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// --- benchmarks: one benchstat-comparable line per engine x op x value size ---

// preload writes up to n keys of size bytes, capped so one row never exceeds ~128 MiB on disk
// (50k keys at 100 B / 1 KiB, 8k at 16 KiB), and returns the keys written.
func preload(b *testing.B, st Store, n, size int) [][]byte {
	b.Helper()
	n = max(1000, min(n, (128<<20)/max(1, size)))
	keys := make([][]byte, n)
	value := bytes.Repeat([]byte("v"), size)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "key-%012d", i)
		if err := st.Put(keys[i], value); err != nil {
			b.Fatal(err)
		}
	}
	return keys
}

func BenchmarkPut(b *testing.B) {
	for _, engine := range Engines {
		for _, sz := range sizes {
			b.Run(engine+"/"+sz.name, func(b *testing.B) {
				st := openStore(b, engine)
				value := bytes.Repeat([]byte("v"), sz.n)
				b.SetBytes(int64(sz.n))
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					if err := st.Put(fmt.Appendf(nil, "key-%012d", i), value); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkGet(b *testing.B) {
	const n = 50_000
	for _, engine := range Engines {
		for _, sz := range sizes {
			b.Run(engine+"/"+sz.name, func(b *testing.B) {
				st := openStore(b, engine)
				keys := preload(b, st, n, sz.n)
				nk := len(keys)
				b.SetBytes(int64(sz.n))
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					if _, err := st.Get(keys[(i*7919)%nk]); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkScan(b *testing.B) {
	const n = 50_000
	for _, engine := range Engines {
		for _, sz := range sizes {
			b.Run(engine+"/"+sz.name, func(b *testing.B) {
				st := openStore(b, engine)
				nk := len(preload(b, st, n, sz.n))
				b.SetBytes(int64(nk) * int64(sz.n))
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					seen := 0
					if err := st.Scan(nil, func(_, _ []byte) bool { seen++; return true }); err != nil {
						b.Fatal(err)
					}
					if seen != nk {
						b.Fatalf("scan saw %d, want %d", seen, nk)
					}
				}
			})
		}
	}
}
