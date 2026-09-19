package fslock

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

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

func TestAcquireBadDir(t *testing.T) {
	t.Parallel()
	_, err := Acquire(filepath.Join(t.TempDir(), "missing", FileName))
	if err == nil || errors.Is(err, ErrLocked) {
		t.Fatalf("Acquire in a missing dir = %v, want a non-ErrLocked error", err)
	}
}
