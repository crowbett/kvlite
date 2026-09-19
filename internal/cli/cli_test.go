package cli

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

func run(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	app := New("tool", "test")
	app.Register(Commands()...)
	var out, errb bytes.Buffer
	env := &Env{Stdout: &out, Stderr: &errb, Stdin: strings.NewReader("")}
	err = app.Run(context.Background(), env, args)
	return out.String(), errb.String(), err
}

func TestCommands(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		args    []string
		wantOut string
		wantErr error
	}{
		{"greet default", []string{"greet"}, "hello, world\n", nil},
		{"greet flags", []string{"greet", "-name", "neon", "-shout"}, "HELLO, NEON\n", nil},
		{"echo", []string{"echo", "a", "b"}, "a b\n", nil},
		{"echo no args", []string{"echo"}, "", ErrUsage},
		{"version", []string{"-version"}, "tool test\n", nil},
		{"no command", nil, "", ErrUsage},
		{"unknown", []string{"nope"}, "", ErrUsage},
		{"help", []string{"help"}, "usage: tool", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			out, _, err := run(t, tt.args...)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if !strings.HasPrefix(out, tt.wantOut) {
				t.Errorf("stdout = %q, want prefix %q", out, tt.wantOut)
			}
		})
	}
}
