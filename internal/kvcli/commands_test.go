package kvcli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/crowbett/kvlite"
	"github.com/crowbett/kvlite/internal/cli"
)

// run dispatches args through the real cli.App with buffer streams and returns stdout.
func run(t *testing.T, stdin string, args ...string) (stdout string, err error) {
	t.Helper()
	app := cli.New("kvlite", "test")
	app.Register(Commands()...)
	var out, errb bytes.Buffer
	env := &cli.Env{Stdout: &out, Stderr: &errb, Stdin: strings.NewReader(stdin)}
	err = app.Run(context.Background(), env, args)
	return out.String(), err
}

// seed creates a store in a fresh temp dir with the given keys and returns the dir.
func seed(t *testing.T, kv map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	db, err := kvlite.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for k, v := range kv {
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return dir
}

func TestUsage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		args    []string
		wantOut string
		wantErr error
	}{
		{"version", []string{"-version"}, "kvlite test\n", nil},
		{"help lists commands", []string{"help"}, "usage: kvlite", nil},
		{"no command", nil, "", cli.ErrUsage},
		{"unknown", []string{"nope"}, "", cli.ErrUsage},

		{"get no key", []string{"get"}, "", cli.ErrUsage},
		{"get two keys", []string{"get", "a", "b"}, "", cli.ErrUsage},
		{"get hex and base64", []string{"get", "-hex", "-base64", "k"}, "", cli.ErrUsage},
		{"get bad hex", []string{"get", "-hex", "zz"}, "", cli.ErrUsage},
		{"get bad base64", []string{"get", "-base64", "!!"}, "", cli.ErrUsage},
		{"get bad flag", []string{"get", "-bogus", "k"}, "", cli.ErrUsage},

		{"put no key", []string{"put"}, "", cli.ErrUsage},
		{"put three args", []string{"put", "k", "v", "x"}, "", cli.ErrUsage},
		{"put negative ttl", []string{"put", "-ttl", "-1s", "k", "v"}, "", cli.ErrUsage},
		{"put bad hex value", []string{"put", "-hex", "6b", "zz"}, "", cli.ErrUsage},

		{"del no key", []string{"del"}, "", cli.ErrUsage},

		{"scan prefix and range", []string{"scan", "-lo", "a", "p"}, "", cli.ErrUsage},
		{"scan two args", []string{"scan", "a", "b"}, "", cli.ErrUsage},
		{"scan negative limit", []string{"scan", "-limit", "-1"}, "", cli.ErrUsage},

		{"stats args", []string{"stats", "x"}, "", cli.ErrUsage},
		{"compact args", []string{"compact", "x"}, "", cli.ErrUsage},
		{"compact negative timeout", []string{"compact", "-timeout", "-1s"}, "", cli.ErrUsage},
		{"verify args", []string{"verify", "x"}, "", cli.ErrUsage},
		{"bench args", []string{"bench", "x"}, "", cli.ErrUsage},
		{"bench zero n", []string{"bench", "-n", "0"}, "", cli.ErrUsage},
		{"bench negative value", []string{"bench", "-value", "-1"}, "", cli.ErrUsage},
		{"bench zero readers", []string{"bench", "-readers", "0"}, "", cli.ErrUsage},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			out, err := run(t, "", tt.args...)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if !strings.HasPrefix(out, tt.wantOut) {
				t.Errorf("stdout = %q, want prefix %q", out, tt.wantOut)
			}
		})
	}
}

func TestEveryCommandRegistered(t *testing.T) {
	t.Parallel()
	want := []string{"get", "put", "del", "scan", "stats", "compact", "verify", "bench"}
	got := map[string]bool{}
	for _, c := range Commands() {
		if c.Name == "" || c.Summary == "" || c.Run == nil || c.Flags == nil {
			t.Errorf("command %+v is incomplete", c)
		}
		got[c.Name] = true
	}
	for _, n := range want {
		if !got[n] {
			t.Errorf("command %q missing", n)
		}
	}
	if len(got) != len(want) {
		t.Errorf("got %d commands, want %d", len(got), len(want))
	}
}

func TestGetPutDel(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		steps   [][]string // run in order against one dir
		stdin   string
		wantOut string // stdout of the last step
		wantErr error  // error of the last step
	}{
		{"put then get", [][]string{{"put", "k", "v"}, {"get", "k"}}, "", "v\n", nil},
		{"overwrite", [][]string{{"put", "k", "v1"}, {"put", "k", "v2"}, {"get", "k"}}, "", "v2\n", nil},
		{"missing key", [][]string{{"get", "nope"}}, "", "", kvlite.ErrNotFound},
		{"put from stdin dash", [][]string{{"put", "k", "-"}, {"get", "k"}}, "from stdin\n", "from stdin\n\n", nil},
		{"put from stdin implicit", [][]string{{"put", "k"}, {"get", "k"}}, "raw", "raw\n", nil},
		{"hex round trip", [][]string{{"put", "-hex", "6b6579", "00ff"}, {"get", "-hex", "6b6579"}}, "", "00ff\n", nil},
		{"hex key raw get", [][]string{{"put", "-hex", "6b", "76"}, {"get", "k"}}, "", "v\n", nil},
		{"base64 round trip", [][]string{{"put", "-base64", "a2V5", "AP8="}, {"get", "-base64", "a2V5"}}, "", "AP8=\n", nil},
		{"hex stdin trailing newline", [][]string{{"put", "-hex", "6b", "-"}, {"get", "k"}}, "76\n", "v\n", nil},
		{"del then get", [][]string{{"put", "k", "v"}, {"del", "k"}, {"get", "k"}}, "", "", kvlite.ErrNotFound},
		{"del absent ok", [][]string{{"del", "ghost"}}, "", "", nil},
		{"put sync", [][]string{{"put", "-sync", "k", "v"}, {"get", "k"}}, "", "v\n", nil},
		{"put ttl still live", [][]string{{"put", "-ttl", "1h", "k", "v"}, {"get", "k"}}, "", "v\n", nil},
		{"empty key rejected", [][]string{{"put", "", "v"}}, "", "", kvlite.ErrEmptyKey},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			var out string
			var err error
			for i, step := range tt.steps {
				args := append([]string{step[0], "-dir", dir}, step[1:]...)
				out, err = run(t, tt.stdin, args...)
				if i < len(tt.steps)-1 && err != nil {
					t.Fatalf("step %d %v: %v", i, step, err)
				}
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("last err = %v, want %v", err, tt.wantErr)
			}
			if err != nil && errors.Is(err, cli.ErrUsage) {
				t.Fatalf("store error must not be a usage error: %v", err)
			}
			if out != tt.wantOut {
				t.Errorf("stdout = %q, want %q", out, tt.wantOut)
			}
		})
	}
}

func TestScan(t *testing.T) {
	t.Parallel()
	dir := seed(t, map[string]string{
		"a/1": "one", "a/2": "two", "a/3": "three", "b/1": "bee", "c": "sea",
	})
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"all", nil, "a/1\tone\na/2\ttwo\na/3\tthree\nb/1\tbee\nc\tsea\n"},
		{"prefix", []string{"a/"}, "a/1\tone\na/2\ttwo\na/3\tthree\n"},
		{"prefix no match", []string{"zz"}, ""},
		{"limit", []string{"-limit", "2"}, "a/1\tone\na/2\ttwo\n"},
		{"keys only", []string{"-keys-only", "a/"}, "a/1\na/2\na/3\n"},
		{"range lo hi", []string{"-lo", "a/2", "-hi", "b/1"}, "a/2\ttwo\na/3\tthree\n"},
		{"range lo only", []string{"-lo", "b"}, "b/1\tbee\nc\tsea\n"},
		{"range hi only", []string{"-hi", "a/3"}, "a/1\tone\na/2\ttwo\n"},
		{"hex output", []string{"-hex", "63"}, "63\t736561\n"},
		{"json", []string{"-json", "b/"}, `{"key":"b/1","value":"bee"}` + "\n"},
		{"json keys only", []string{"-json", "-keys-only", "-limit", "1"}, `{"key":"a/1"}` + "\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			out, err := run(t, "", append([]string{"scan", "-dir", dir}, tt.args...)...)
			if err != nil {
				t.Fatalf("scan: %v", err)
			}
			if out != tt.want {
				t.Errorf("stdout = %q, want %q", out, tt.want)
			}
		})
	}
}

func TestScanHonoursContext(t *testing.T) {
	t.Parallel()
	dir := seed(t, map[string]string{"k": "v"})
	app := cli.New("kvlite", "test")
	app.Register(Commands()...)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out, errb bytes.Buffer
	err := app.Run(ctx, &cli.Env{Stdout: &out, Stderr: &errb, Stdin: strings.NewReader("")}, []string{"scan", "-dir", dir})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("scan(cancelled ctx) = %v, want context.Canceled", err)
	}
}

func TestStats(t *testing.T) {
	t.Parallel()
	dir := seed(t, map[string]string{"a": "1", "b": "2", "c": "3"})

	t.Run("table", func(t *testing.T) {
		t.Parallel()
		out, err := run(t, "", "stats", "-dir", dir)
		if err != nil {
			t.Fatalf("stats: %v", err)
		}
		for _, want := range []string{"keys", "segments", "total_bytes", "dead_ratio", "last_compaction"} {
			if !strings.Contains(out, want) {
				t.Errorf("stats output lacks %q:\n%s", want, out)
			}
		}
		if !strings.Contains(out, "keys            3\n") && !strings.Contains(out, "keys  ") {
			t.Errorf("keys row not aligned/present:\n%s", out)
		}
		for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
			if len(strings.Fields(line)) < 2 {
				t.Errorf("line %q is not a name/value pair", line)
			}
		}
	})
	t.Run("json", func(t *testing.T) {
		t.Parallel()
		out, err := run(t, "", "stats", "-json", "-dir", dir)
		if err != nil {
			t.Fatalf("stats -json: %v", err)
		}
		var st kvlite.Stats
		if err := json.Unmarshal([]byte(out), &st); err != nil {
			t.Fatalf("not JSON: %v\n%s", err, out)
		}
		if st.Keys != 3 {
			t.Errorf("Keys = %d, want 3", st.Keys)
		}
	})
}

func TestCompact(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// Overwrite the same keys many times so there is dead space to reclaim.
	db, err := kvlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	value := bytes.Repeat([]byte("x"), 1024)
	for round := range 20 {
		for i := range 50 {
			if err := db.Put([]byte("k"+string(rune('a'+i%26))), append(value, byte(round))); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	out, err := run(t, "", "compact", "-dir", dir, "-timeout", "1m")
	if err != nil {
		t.Fatalf("compact: %v\n%s", err, out)
	}
	for _, want := range []string{"before", "after", "reclaimed", "took"} {
		if !strings.Contains(out, want) {
			t.Errorf("compact output lacks %q:\n%s", want, out)
		}
	}
	// Live data must survive.
	got, err := run(t, "", "get", "-dir", dir, "ka")
	if err != nil || !strings.HasSuffix(got, string(rune(19))+"\n") {
		t.Errorf("get after compact = %q, %v", got, err)
	}
	// Compacting an already-compact store is a no-op, not an error.
	if _, err := run(t, "", "compact", "-dir", dir); err != nil {
		t.Errorf("second compact: %v", err)
	}
}

func TestVerify(t *testing.T) {
	t.Parallel()

	t.Run("clean store", func(t *testing.T) {
		t.Parallel()
		dir := seed(t, map[string]string{"a": "1", "b": "2"})
		out, err := run(t, "", "verify", "-dir", dir)
		if err != nil {
			t.Fatalf("verify: %v\n%s", err, out)
		}
		if !strings.HasSuffix(strings.TrimSpace(out), "\nfindings               0") &&
			!regexp.MustCompile(`(?m)^findings\s+0$`).MatchString(out) {
			t.Errorf("expected zero findings:\n%s", out)
		}
	})
	t.Run("clean store deep json after compact", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		db, err := kvlite.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		for i := range 100 {
			if err := db.Put([]byte{'k', byte(i)}, []byte("v")); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Compact(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		out, err := run(t, "", "verify", "-dir", dir, "-deep", "-json")
		if err != nil {
			t.Fatalf("verify -deep: %v\n%s", err, out)
		}
		var rep verifyReport
		if err := json.Unmarshal([]byte(out), &rep); err != nil {
			t.Fatalf("not JSON: %v\n%s", err, out)
		}
		if rep.Total != 0 || rep.Manifest != "ok" || len(rep.Segments) == 0 {
			t.Errorf("report = %+v, want clean", rep)
		}
		hints := 0
		for _, s := range rep.Segments {
			if s.Hint == "ok" {
				hints++
			}
		}
		if hints == 0 {
			t.Errorf("no valid hint after compaction: %+v", rep.Segments)
		}
	})
	t.Run("corrupt byte is a finding", func(t *testing.T) {
		t.Parallel()
		dir := seed(t, map[string]string{"alpha": strings.Repeat("v", 200), "beta": "2"})
		logs, _ := filepath.Glob(filepath.Join(dir, "*.log"))
		if len(logs) == 0 {
			t.Fatal("no segment file written")
		}
		b, err := os.ReadFile(logs[0])
		if err != nil {
			t.Fatal(err)
		}
		b[len(b)/2] ^= 0xff
		if err := os.WriteFile(logs[0], b, 0o600); err != nil {
			t.Fatal(err)
		}
		out, err := run(t, "", "verify", "-dir", dir)
		if !errors.Is(err, ErrVerify) {
			t.Fatalf("verify(corrupt) = %v, want ErrVerify\n%s", err, out)
		}
		if errors.Is(err, cli.ErrUsage) {
			t.Fatal("finding must not be a usage error")
		}
		if !strings.Contains(out, "!") {
			t.Errorf("no finding line printed:\n%s", out)
		}
	})
	t.Run("orphan is a finding", func(t *testing.T) {
		t.Parallel()
		dir := seed(t, map[string]string{"a": "1"})
		if err := os.WriteFile(filepath.Join(dir, "00009999.log"), []byte("junk"), 0o600); err != nil {
			t.Fatal(err)
		}
		out, err := run(t, "", "verify", "-dir", dir, "-json")
		if !errors.Is(err, ErrVerify) {
			t.Fatalf("verify(orphan) = %v, want ErrVerify\n%s", err, out)
		}
		if !strings.Contains(out, "00009999.log") {
			t.Errorf("orphan not named:\n%s", out)
		}
	})
	t.Run("missing dir", func(t *testing.T) {
		t.Parallel()
		_, err := run(t, "", "verify", "-dir", filepath.Join(t.TempDir(), "nope"))
		if err == nil || errors.Is(err, cli.ErrUsage) {
			t.Fatalf("verify(missing) = %v, want a plain error", err)
		}
	})
}

func TestBench(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		args []string
	}{
		{"defaults small", []string{"-n", "200", "-value", "32"}},
		{"random readers dir", []string{"-n", "100", "-value", "0", "-readers", "3", "-random"}},
		{"sync", []string{"-n", "10", "-value", "8", "-sync"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			args := append([]string{"bench", "-dir", t.TempDir()}, tt.args...)
			out, err := run(t, "", args...)
			if err != nil {
				t.Fatalf("bench: %v\n%s", err, out)
			}
			for _, want := range []string{"phase", "put", "get", "get-miss", "scan", "delete", "ops/s", "p99", "disk before compact"} {
				if !strings.Contains(out, want) {
					t.Errorf("bench output lacks %q:\n%s", want, out)
				}
			}
		})
	}
}

func TestPrintBenchFormatsDurations(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in   time.Duration
		want string
	}{
		{500 * time.Nanosecond, "0.5µs"},
		{1500 * time.Microsecond, "1.50ms"},
		{2 * time.Second, "2s"},
	}
	for _, tt := range tests {
		if got := fmtDur(tt.in); got != tt.want {
			t.Errorf("fmtDur(%v) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
