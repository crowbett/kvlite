// Package cli implements a tiny dependency-free subcommand framework on top of flag.
package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"text/tabwriter"
)

// ErrUsage signals the caller should print usage and exit 2.
var ErrUsage = errors.New("usage")

// Command is a single subcommand.
type Command struct {
	Name    string
	Summary string
	// Flags registers command-specific flags on fs. Optional.
	Flags func(fs *flag.FlagSet)
	// Run executes the command with the positional args left after flag parsing.
	Run func(ctx context.Context, env *Env, args []string) error
}

// Env is passed to every command: IO streams plus global flags.
type Env struct {
	Stdout  io.Writer
	Stderr  io.Writer
	Stdin   io.Reader
	Verbose bool
}

// Logf writes to stderr only when --verbose is set.
func (e *Env) Logf(format string, args ...any) {
	if e.Verbose {
		fmt.Fprintf(e.Stderr, format+"\n", args...)
	}
}

// App owns the command set and global flags.
type App struct {
	Name     string
	Version  string
	commands map[string]*Command
}

// New creates an App.
func New(name, version string) *App {
	return &App{Name: name, Version: version, commands: map[string]*Command{}}
}

// Register adds commands to the app.
func (a *App) Register(cmds ...*Command) {
	for _, c := range cmds {
		a.commands[c.Name] = c
	}
}

// Main parses args (without the program name), dispatches, and returns an exit code.
func (a *App) Main(ctx context.Context, args []string) int {
	env := &Env{Stdout: os.Stdout, Stderr: os.Stderr, Stdin: os.Stdin}
	err := a.Run(ctx, env, args)
	switch {
	case err == nil:
		return 0
	case errors.Is(err, ErrUsage):
		return 2
	case errors.Is(err, context.Canceled):
		fmt.Fprintln(env.Stderr, "interrupted")
		return 130
	default:
		fmt.Fprintf(env.Stderr, "%s: %v\n", a.Name, err)
		return 1
	}
}

// Run is Main without the exit-code mapping, for tests.
func (a *App) Run(ctx context.Context, env *Env, args []string) error {
	global := flag.NewFlagSet(a.Name, flag.ContinueOnError)
	global.SetOutput(env.Stderr)
	global.BoolVar(&env.Verbose, "v", false, "verbose output")
	global.BoolVar(&env.Verbose, "verbose", false, "verbose output")
	showVersion := global.Bool("version", false, "print version and exit")
	global.Usage = func() { a.usage(env.Stderr, global) }

	if err := global.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return ErrUsage
		}
		return fmt.Errorf("%w: %w", ErrUsage, err)
	}
	if *showVersion {
		fmt.Fprintf(env.Stdout, "%s %s\n", a.Name, a.Version)
		return nil
	}

	rest := global.Args()
	if len(rest) == 0 {
		a.usage(env.Stderr, global)
		return ErrUsage
	}

	name, cmdArgs := rest[0], rest[1:]
	if name == "help" {
		a.usage(env.Stdout, global)
		return nil
	}
	cmd, ok := a.commands[name]
	if !ok {
		fmt.Fprintf(env.Stderr, "unknown command %q\n\n", name)
		a.usage(env.Stderr, global)
		return ErrUsage
	}

	fs := flag.NewFlagSet(a.Name+" "+cmd.Name, flag.ContinueOnError)
	fs.SetOutput(env.Stderr)
	if cmd.Flags != nil {
		cmd.Flags(fs)
	}
	fs.Usage = func() {
		fmt.Fprintf(env.Stderr, "usage: %s %s [flags] [args]\n\n%s\n\n", a.Name, cmd.Name, cmd.Summary)
		fs.PrintDefaults()
	}
	if err := fs.Parse(cmdArgs); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return ErrUsage
		}
		return fmt.Errorf("%w: %w", ErrUsage, err)
	}
	return cmd.Run(ctx, env, fs.Args())
}

func (a *App) usage(w io.Writer, global *flag.FlagSet) {
	fmt.Fprintf(w, "usage: %s [global flags] <command> [flags] [args]\n\ncommands:\n", a.Name)
	names := make([]string, 0, len(a.commands))
	for n := range a.commands {
		names = append(names, n)
	}
	sort.Strings(names)
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	for _, n := range names {
		fmt.Fprintf(tw, "  %s\t%s\n", n, a.commands[n].Summary)
	}
	fmt.Fprintf(tw, "  help\tshow this help\n")
	_ = tw.Flush()
	fmt.Fprintln(w, "\nglobal flags:")
	global.PrintDefaults()
	fmt.Fprintf(w, "\nrun %q for command help\n", a.Name+" <command> -h")
}
