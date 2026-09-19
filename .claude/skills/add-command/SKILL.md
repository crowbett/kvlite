---
name: add-command
description: "Add a subcommand to THIS repo's flag-based dispatcher: xxxCommand() constructor in internal/cli/commands.go, registration in Commands(), table rows in TestCommands (cli_test.go), README usage line, then build/vet/test. Use when the user says 'add a command', 'new subcommand', 'add a <verb> command', or 'wire up <name>'."
argument-hint: "<name> [\"summary\"] [-flag:type=default ...]"
arguments: [name, summary]
shell: powershell
allowed-tools: PowerShell(go build *) PowerShell(go vet *) PowerShell(go test *) PowerShell(go run *) PowerShell(go version) PowerShell(golangci-lint run *) PowerShell(Get-ChildItem *) PowerShell(Select-String *) PowerShell(Measure-Object *) PowerShell(Select-Object *) PowerShell(Write-Output *) Bash(go build *) Bash(go vet *) Bash(go test *) Bash(go run *) Bash(golangci-lint run *)
---

# add-command

Command: `$name`  Summary: `$summary`  Raw: `$ARGUMENTS`

## Current commands
!`Select-String -Path internal\cli\commands.go -Pattern 'Name:\s+"'; Write-Output 'test table rows:'; Select-String -Path internal\cli\cli_test.go -Pattern '^\s*\{"' | Measure-Object | Select-Object -ExpandProperty Count; Get-ChildItem cmd -Directory -Name; go version`

## Files (do not invent others)
| What | Where |
|---|---|
| `func <name>Command() *Command` | `internal/cli/commands.go` |
| registration | `Commands()` slice in `internal/cli/commands.go` |
| test rows | `TestCommands` table in `internal/cli/cli_test.go` |
| usage docs | `README.md` "Usage" block |

## Procedure
1. Read `internal/cli/commands.go` (see `greetCommand`) and `cli_test.go` (`run` helper) before writing.
2. **Constructor** in `commands.go`, in the same shape as `greetCommand`:
   ```go
   func <name>Command() *Command {
       var n int
       return &Command{
           Name:    "<name>",
           Summary: "<one line, lower case, no trailing period>",
           Flags:   func(fs *flag.FlagSet) { fs.IntVar(&n, "n", 1, "how many") },
           Run: func(ctx context.Context, env *Env, args []string) error {
               // validate args -> return fmt.Errorf("%w: <name> needs ...", ErrUsage)
               // do work honouring ctx; write results to env.Stdout, diagnostics via env.Logf
               _, err := fmt.Fprintln(env.Stdout, ...)
               return err
           },
       }
   }
   ```
   Rules: flags via closure vars; no `os.Stdout`/`os.Exit`; `ErrUsage`-wrap argument mistakes (exit 2);
   anything blocking selects on `ctx.Done()`; real logic that grows past ~40 lines moves to
   `internal/<domain>` and the command just calls it.
3. **Register**: append `<name>Command(),` to the slice in `Commands()` (keep alphabetical-ish order).
4. **Tests**: add rows to `TestCommands`: default behaviour, each flag, a usage error (`wantErr: ErrUsage`).
   Output assertions use `wantOut` prefix matching; if the command reads stdin, extend the `run` helper's
   `Env.Stdin` rather than adding a new helper.
5. **README**: add a `go run ./cmd/<bin> <name> ...` example under "Usage".
6. **Verify** (report exactly what ran):
   ```powershell
   go build ./...; go vet ./...; go test -count=1 ./...
   golangci-lint run ./...
   go run ./cmd/<bin> <name> -h
   ```

## Done when
`<name>` shows in `help` output, test rows pass, vet + lint clean, README updated.
Report: files touched, the flag set, and the exit codes the command can return.
