// Command kvlite is the CLI for the kvlite embedded key-value store: get/put/del/scan/stats/compact/
// verify/bench against a store directory. Wiring only; the commands live in internal/kvcli.
package main

import (
	"context"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"

	"github.com/crowbett/kvlite/internal/cli"
	"github.com/crowbett/kvlite/internal/kvcli"
)

// Set by goreleaser via -ldflags. Falls back to module build info for `go install`.
var (
	version = ""
	commit  = ""
	date    = ""
)

func main() {
	os.Exit(run())
}

// run is main without os.Exit so the signal handler's stop() runs before the process exits.
func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	app := cli.New("kvlite", buildVersion())
	app.Register(kvcli.Commands()...)
	return app.Main(ctx, os.Args[1:])
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
