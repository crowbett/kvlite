# kvlite

Dependency-free Go CLI skeleton: `flag` + a tiny subcommand dispatcher. No cobra.

```
cmd/kvlite/main.go        entrypoint, signal handling, version from ldflags or build info
internal/cli/cli.go     App / Command / Env: global flags, dispatch, usage, exit codes
internal/cli/commands.go  your subcommands (greet, echo as examples)
internal/cli/cli_test.go  table-driven tests through App.Run
.goreleaser.yaml        v2 config: linux/windows/darwin x amd64/arm64, archives, checksums, changelog
Taskfile.yml            task build|install|run|test|lint|release:snapshot
```

## Usage

```powershell
go run ./cmd/kvlite greet -name neon -shout
go run ./cmd/kvlite -v echo hello world
go run ./cmd/kvlite -version
go run ./cmd/kvlite help
```

Exit codes: `0` ok, `1` runtime error, `2` usage error, `130` interrupted.

## Add a subcommand

In `internal/cli/commands.go`:

```go
func fooCommand() *Command {
    var n int
    return &Command{
        Name:    "foo",
        Summary: "does foo",
        Flags:   func(fs *flag.FlagSet) { fs.IntVar(&n, "n", 1, "how many") },
        Run: func(ctx context.Context, env *Env, args []string) error {
            _, err := fmt.Fprintln(env.Stdout, "foo", n, args)
            return err
        },
    }
}
```

then append it to `Commands()`.

## Release

```powershell
go install github.com/goreleaser/goreleaser/v2@latest
goreleaser check
goreleaser release --snapshot --clean   # local dist/ build for all targets
git tag v0.1.0; git push --tags          # CI/goreleaser release from a tag
```

Commit messages in Conventional Commits style (`feat:`, `fix:`, `perf:`) are grouped in the changelog.
