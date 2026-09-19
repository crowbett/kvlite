# .bench

- `baseline.txt` is the committed reference output of `internal/bench` (`go test -bench` format, read by benchstat); `baseline.meta.txt` records the CPU, Go version, OS and exact command that produced it.
- Refresh with `pwsh -NoProfile -File scripts/bench.ps1 -Save baseline` (count 10, 15-25 min; close browsers/music first, the timings are load-sensitive); compare a change with `scripts/bench.ps1 -Compare baseline [-Run '<regex>']`.
- `baseline.txt` only changes in a dedicated `perf(bench): refresh baseline` commit that touches nothing else, so a benchstat diff is never mixed with code changes.
- Unsaved runs land in `results/bench/<timestamp>.txt` (gitignored); other `-Save <name>` files are scratch and should not be committed.
- Numbers are only comparable on the same machine: check `.meta.txt` against your box before trusting a `vs base` column.
