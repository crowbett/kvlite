package cli

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"
	"testing"
)

// echoCommand is a test-only command exercising flags, positional args and usage errors.
func echoCommand() *Command {
	var shout bool
	return &Command{
		Name:    "echo",
		Summary: "print its arguments",
		Flags:   func(fs *flag.FlagSet) { fs.BoolVar(&shout, "shout", false, "UPPERCASE") },
		Run: func(_ context.Context, env *Env, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("%w: echo needs at least one argument", ErrUsage)
			}
			msg := strings.Join(args, " ")
			if shout {
				msg = strings.ToUpper(msg)
			}
			env.Logf("echoing %d args", len(args))
			_, err := fmt.Fprintln(env.Stdout, msg)
			return err
		},
	}
}

func run(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	app := New("tool", "test")
	app.Register(echoCommand())
	var out, errb bytes.Buffer
	env := &Env{Stdout: &out, Stderr: &errb, Stdin: strings.NewReader("")}
	err = app.Run(context.Background(), env, args)
	return out.String(), errb.String(), err
}

func TestDispatcher(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		args    []string
		wantOut string
		wantErr error
		wantLog string
	}{
		{"echo", []string{"echo", "a", "b"}, "a b\n", nil, ""},
		{"echo flag", []string{"echo", "-shout", "a"}, "A\n", nil, ""},
		{"echo verbose", []string{"-v", "echo", "a"}, "a\n", nil, "echoing 1 args\n"},
		{"echo no args", []string{"echo"}, "", ErrUsage, ""},
		{"echo bad flag", []string{"echo", "-nope"}, "", ErrUsage, ""},
		{"version", []string{"-version"}, "tool test\n", nil, ""},
		{"no command", nil, "", ErrUsage, ""},
		{"unknown", []string{"nope"}, "", ErrUsage, ""},
		{"help", []string{"help"}, "usage: tool", nil, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			out, errOut, err := run(t, tt.args...)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if !strings.HasPrefix(out, tt.wantOut) {
				t.Errorf("stdout = %q, want prefix %q", out, tt.wantOut)
			}
			if tt.wantLog != "" && errOut != tt.wantLog {
				t.Errorf("stderr = %q, want %q", errOut, tt.wantLog)
			}
		})
	}
}

func TestMainExitCodes(t *testing.T) {
	t.Parallel()
	app := New("tool", "test")
	app.Register(&Command{
		Name: "fail", Summary: "always fails",
		Run: func(_ context.Context, _ *Env, _ []string) error { return errors.New("boom") },
	}, &Command{
		Name: "cancel", Summary: "reports cancellation",
		Run: func(_ context.Context, _ *Env, _ []string) error { return context.Canceled },
	})
	tests := []struct {
		args []string
		want int
	}{
		{[]string{"-version"}, 0},
		{[]string{"fail"}, 1},
		{[]string{"nope"}, 2},
		{[]string{"cancel"}, 130},
	}
	for _, tt := range tests {
		if got := app.Main(context.Background(), tt.args); got != tt.want {
			t.Errorf("Main(%v) = %d, want %d", tt.args, got, tt.want)
		}
	}
}
