# kvlite

Dependency-free Go CLI: `flag` + a tiny subcommand dispatcher in `internal/cli` (no cobra).
Module `github.com/crowbett/kvlite`. Global Go conventions from `~/.claude/CLAUDE.md` apply; this is the repo-specific part.

## Layout
- `cmd/kvlite/main.go` - `signal.NotifyContext`, `cli.New(name, version)`, `app.Register(cli.Commands()...)`,
  `os.Exit(app.Main(ctx, os.Args[1:]))`. Version comes from `-ldflags -X main.version` or build info.
- `internal/cli/cli.go` - `App`, `Command{Name, Summary, Flags, Run}`, `Env{Stdout, Stderr, Stdin, Verbose}`,
  `ErrUsage`, usage printing, exit-code mapping (0 ok, 1 error, 2 usage, 130 interrupted).
- `internal/cli/commands.go` - `Commands()` returns the command list; one `xxxCommand()` constructor per subcommand.
- `internal/cli/cli_test.go` - table-driven `TestCommands` driving `App.Run` with buffers.
- `.goreleaser.yaml` (v2, 5 os/arch targets), `.golangci.yml` (v2), `Taskfile.yml`.

## Commands (Taskfile.yml)
- `task build` (-> `bin/`), `task install`, `task run -- greet -name neon`
- `task test` (`go test -count=1 ./...`), `task test:race` (needs a C compiler), `task vet`, `task lint`, `task check`
- `task release:check` (`goreleaser check`), `task release:snapshot` (local `dist/` for all targets)

## Conventions specific to this repo
- Every subcommand is a `func xxxCommand() *Command` in `commands.go`, appended to `Commands()`.
  Flags are captured by closure variables registered in `Flags`; positional args arrive in `Run`.
  `/add-command` does the wiring plus test rows.
- Output goes to `env.Stdout`; diagnostics to `env.Stderr`; `env.Logf` only under `-v`. Never `fmt.Print*` to
  `os.Stdout` directly and never `os.Exit` inside a command - return an error (`ErrUsage`-wrapped for usage mistakes).
- Honour `ctx` in anything that blocks (I/O, subprocesses, sleeps) so Ctrl+C returns 130.
- Tests call `App.Run` with an `Env` of buffers, never the real process streams; add a table row per new
  behaviour (default, flags, usage error).
- Commit messages are Conventional Commits; goreleaser groups the changelog by `feat`/`fix`/`perf`.

## Definition of done
`go build ./...`, `go vet ./...`, `go test -count=1 ./...` (add `-race` when a C compiler is present, say which),
`golangci-lint run ./...` green; `goreleaser check` if `.goreleaser.yaml` changed. `/go-test`, `/go-lint`, `/go-release`.
