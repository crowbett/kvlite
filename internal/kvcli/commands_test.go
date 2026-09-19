package kvcli

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/crowbett/kvlite/internal/cli"
)

func run(t *testing.T, args ...string) (stdout string, err error) {
	t.Helper()
	app := cli.New("kvlite", "test")
	app.Register(Commands()...)
	var out, errb bytes.Buffer
	env := &cli.Env{Stdout: &out, Stderr: &errb, Stdin: strings.NewReader("")}
	err = app.Run(context.Background(), env, args)
	return out.String(), err
}

func TestCommands(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		args     []string
		wantOut  string
		wantErr  error
		wantText string
	}{
		{"version", []string{"-version"}, "kvlite test\n", nil, ""},
		{"help lists commands", []string{"help"}, "usage: kvlite", nil, ""},
		{"no command", nil, "", cli.ErrUsage, ""},
		{"unknown", []string{"nope"}, "", cli.ErrUsage, ""},

		{"get", []string{"get", "k"}, "", nil, "not implemented: get"},
		{"get dir hex", []string{"get", "-dir", "x", "-hex", "6b"}, "", nil, "not implemented: get"},
		{"get no key", []string{"get"}, "", cli.ErrUsage, ""},
		{"get hex and base64", []string{"get", "-hex", "-base64", "k"}, "", cli.ErrUsage, ""},
		{"get bad flag", []string{"get", "-bogus", "k"}, "", cli.ErrUsage, ""},

		{"put", []string{"put", "k", "v"}, "", nil, "not implemented: put"},
		{"put stdin", []string{"put", "-ttl", "5s", "-sync", "k"}, "", nil, "not implemented: put"},
		{"put no key", []string{"put"}, "", cli.ErrUsage, ""},
		{"put negative ttl", []string{"put", "-ttl", "-1s", "k", "v"}, "", cli.ErrUsage, ""},

		{"del", []string{"del", "k"}, "", nil, "not implemented: del"},
		{"del no key", []string{"del"}, "", cli.ErrUsage, ""},

		{"scan", []string{"scan"}, "", nil, "not implemented: scan"},
		{"scan prefix", []string{"scan", "-limit", "10", "-keys-only", "-json", "p"}, "", nil, "not implemented: scan"},
		{"scan range", []string{"scan", "-lo", "a", "-hi", "z"}, "", nil, "not implemented: scan"},
		{"scan prefix and range", []string{"scan", "-lo", "a", "p"}, "", cli.ErrUsage, ""},
		{"scan two args", []string{"scan", "a", "b"}, "", cli.ErrUsage, ""},

		{"stats", []string{"stats", "-json"}, "", nil, "not implemented: stats"},
		{"stats args", []string{"stats", "x"}, "", cli.ErrUsage, ""},

		{"compact", []string{"compact", "-timeout", "1m"}, "", nil, "not implemented: compact"},
		{"compact args", []string{"compact", "x"}, "", cli.ErrUsage, ""},

		{"verify", []string{"verify", "-deep", "-json"}, "", nil, "not implemented: verify"},
		{"verify args", []string{"verify", "x"}, "", cli.ErrUsage, ""},

		{"bench", []string{"bench", "-n", "10", "-value", "16", "-readers", "2", "-sync", "-random"}, "", nil, "not implemented: bench"},
		{"bench zero n", []string{"bench", "-n", "0"}, "", cli.ErrUsage, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			out, err := run(t, tt.args...)
			switch {
			case tt.wantText != "":
				if err == nil || err.Error() != tt.wantText {
					t.Fatalf("err = %v, want %q", err, tt.wantText)
				}
				if errors.Is(err, cli.ErrUsage) {
					t.Fatalf("stub error must not be a usage error")
				}
			case !errors.Is(err, tt.wantErr):
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
