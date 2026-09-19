// Command tool is a minimal subcommand CLI built on the standard flag package.
package main

import (
	"context"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"

	"github.com/crowbett/kvlite/internal/cli"
)

// Set by goreleaser via -ldflags. Falls back to module build info for `go install`.
var (
	version = ""
	commit  = ""
	date    = ""
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	app := cli.New("tool", buildVersion())
	app.Register(cli.Commands()...)
	os.Exit(app.Main(ctx, os.Args[1:]))
}

func buildVersion() string {
	v := version
	if v == "" {
		v = "dev"
		if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
			v = bi.Main.Version
		}
	}
	if commit != "" {
		v += " (" + commit
		if date != "" {
			v += ", " + date
		}
		v += ")"
	}
	return v
}
