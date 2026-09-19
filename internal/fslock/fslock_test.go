package fslock

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// helperEnv names the lock path a re-executed test binary should try to acquire (see TestHelperAcquire).
const helperEnv = "KVLITE_FSLOCK_HELPER_PATH"

func TestAcquireRelease(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), FileName)

	l1, err := Acquire(path)
	if err != nil {
		t.Fatalf("first Acquire() = %v", err)
	}
	if l1.Path() != path {
		t.Errorf("Path() = %q, want %q", l1.Path(), path)
	}
	if _, err := Acquire(path); !errors.Is(err, ErrLocked) {
		t.Fatalf("second Acquire() = %v, want ErrLocked", err)
	}
	if err := l1.Release(); err != nil {
		t.Fatalf("Release() = %v", err)
	}
	if err := l1.Release(); err != nil {
		t.Errorf("second Release() = %v, want nil (idempotent)", err)
	}

	l2, err := Acquire(path)
	if err != nil {
		t.Fatalf("Acquire() after release = %v", err)
	}
	t.Cleanup(func() { _ = l2.Release() })
	// While held, other handles are refused on Windows (exclusive share mode), so a plain read of the
	// file must not succeed there; on unix flock is advisory and the read goes through.
	if _, err := os.ReadFile(path); err != nil && runtime.GOOS != "windows" {
		t.Errorf("read of a held lock file on %s = %v, want nil (advisory lock)", runtime.GOOS, err)
	}
	if err := l2.Release(); err != nil {
		t.Fatalf("Release() = %v", err)
	}
	// After release the file stays behind (except on the O_EXCL fallback, which removes it) and holds
	// the last owner's pid for diagnostics.
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		t.Fatalf("read lock file: %v", err)
	}
	if got := strings.TrimSpace(string(b)); got != strconv.Itoa(os.Getpid()) {
		t.Errorf("lock file holds %q, want pid %d", got, os.Getpid())
	}
}

// TestLockedTwice is the in-process contention check the ADR names: a second Acquire of a held lock is
// ErrLocked, and many concurrent attempts admit exactly one holder.
func TestLockedTwice(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), FileName)
	l, err := Acquire(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Release() })
	if _, err := Acquire(path); !errors.Is(err, ErrLocked) {
		t.Fatalf("Acquire() while held = %v, want ErrLocked", err)
	}
	if err := l.Release(); err != nil {
		t.Fatal(err)
	}
	// Race 16 goroutines for a free lock: exactly one wins, the rest see ErrLocked.
	const n = 16
	results := make(chan *Lock, n)
	for range n {
		go func() {
			l, err := Acquire(path)
			if err != nil && !errors.Is(err, ErrLocked) {
				t.Errorf("Acquire() = %v", err)
			}
			results <- l
		}()
	}
	var winners []*Lock
	for range n {
		if l := <-results; l != nil {
			winners = append(winners, l)
		}
	}
	if len(winners) != 1 {
		t.Fatalf("%d goroutines hold the lock, want 1", len(winners))
	}
	_ = winners[0].Release()
}

// TestStaleLockFile: the lock lives in the open handle, not in the file, so a LOCK left behind by a
// crashed process (whatever pid it names) is simply taken over. Only the O_EXCL fallback platform
// treats a leftover file as held, and that platform is not one we ship for.
func TestStaleLockFile(t *testing.T) {
	t.Parallel()
	if runtime.GOOS != "windows" && !isUnix() {
		t.Skip("O_EXCL fallback: a leftover lock file is held until removed by hand")
	}
	path := filepath.Join(t.TempDir(), FileName)
	if err := os.WriteFile(path, []byte("999999999\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	l, err := Acquire(path)
	if err != nil {
		t.Fatalf("Acquire() over a stale lock file = %v", err)
	}
	if err := l.Release(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(b)); got != strconv.Itoa(os.Getpid()) {
		t.Errorf("lock file holds %q after takeover, want pid %d", got, os.Getpid())
	}
}

func isUnix() bool {
	switch runtime.GOOS {
	case "linux", "darwin", "freebsd", "netbsd", "openbsd", "dragonfly", "solaris", "illumos", "aix":
		return true
	}
	return false
}

// TestHelperAcquire is the body of the child process in TestSecondProcess: it tries the lock named by
// helperEnv once and reports the outcome on stdout. It is a no-op in a normal test run.
func TestHelperAcquire(t *testing.T) {
	path := os.Getenv(helperEnv)
	if path == "" {
		t.Skip("helper: not invoked by TestSecondProcess")
	}
	l, err := Acquire(path)
	switch {
	case errors.Is(err, ErrLocked):
		os.Stdout.WriteString("LOCKED\n")
	case err != nil:
		os.Stdout.WriteString("ERROR " + err.Error() + "\n")
	default:
		os.Stdout.WriteString("ACQUIRED\n")
		_ = l.Release()
	}
}

// TestSecondProcess is the cross-process guarantee: it re-executes this test binary as a child that
// runs only TestHelperAcquire against a lock the parent holds (must be refused) and then against a
// released one (must succeed).
func TestSecondProcess(t *testing.T) {
	t.Parallel()
	exe, err := os.Executable()
	if err != nil {
		t.Skip("no test binary path:", err)
	}
	path := filepath.Join(t.TempDir(), FileName)
	run := func() string {
		t.Helper()
		cmd := exec.CommandContext(t.Context(), exe, "-test.run=^TestHelperAcquire$", "-test.v=false")
		cmd.Env = append(os.Environ(), helperEnv+"="+path)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("child process: %v\n%s", err, out)
		}
		for line := range strings.Lines(string(out)) {
			line = strings.TrimSpace(line)
			if line == "LOCKED" || line == "ACQUIRED" || strings.HasPrefix(line, "ERROR") {
				return line
			}
		}
		t.Fatalf("child process printed no verdict:\n%s", out)
		return ""
	}

	l, err := Acquire(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := run(); got != "LOCKED" {
		t.Fatalf("child while parent holds the lock: %s, want LOCKED", got)
	}
	if err := l.Release(); err != nil {
		t.Fatal(err)
	}
	if got := run(); got != "ACQUIRED" {
		t.Fatalf("child after release: %s, want ACQUIRED", got)
	}
	// The parent can take it back after the child exited: the child's handle died with it.
	l, err = Acquire(path)
	if err != nil {
		t.Fatalf("Acquire() after the child exited = %v", err)
	}
	_ = l.Release()
}

func TestAcquireBadDir(t *testing.T) {
	t.Parallel()
	_, err := Acquire(filepath.Join(t.TempDir(), "missing", FileName))
	if err == nil || errors.Is(err, ErrLocked) {
		t.Fatalf("Acquire in a missing dir = %v, want a non-ErrLocked error", err)
	}
}
