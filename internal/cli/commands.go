package cli

import (
	"context"
	"flag"
	"fmt"
	"strings"
)

// Commands returns the built-in command set. Add new subcommands here.
func Commands() []*Command {
	return []*Command{
		greetCommand(),
		echoCommand(),
	}
}

func greetCommand() *Command {
	var (
		name  string
		shout bool
	)
	return &Command{
		Name:    "greet",
		Summary: "print a greeting",
		Flags: func(fs *flag.FlagSet) {
			fs.StringVar(&name, "name", "world", "who to greet")
			fs.BoolVar(&shout, "shout", false, "UPPERCASE the greeting")
		},
		Run: func(_ context.Context, env *Env, _ []string) error {
			msg := "hello, " + name
			if shout {
				msg = strings.ToUpper(msg)
			}
			env.Logf("greeting %s", name)
			_, err := fmt.Fprintln(env.Stdout, msg)
			return err
		},
	}
}

func echoCommand() *Command {
	return &Command{
		Name:    "echo",
		Summary: "print its arguments",
		Run: func(_ context.Context, env *Env, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("%w: echo needs at least one argument", ErrUsage)
			}
			_, err := fmt.Fprintln(env.Stdout, strings.Join(args, " "))
			return err
		},
	}
}
