// Package fslock implements the single-writer LOCK file of a kvlite directory. The lock is held by an
// open file handle, so the OS releases it when the process dies: an exclusive share mode on Windows
// (CreateFile with dwShareMode 0), flock(LOCK_EX|LOCK_NB) on unix, and O_EXCL creation elsewhere.
// It imports nothing from the rest of the module.
//
// Stale locks: the file holds the owner's pid for diagnostics only. On Windows and unix a LOCK left
// behind by a crashed process is not held by anyone (the handle died with the process) and the next
// Acquire simply takes it over, rewriting the pid; nothing ever needs to be removed by hand. Only the
// O_EXCL fallback (platforms with neither primitive) treats the file's existence as the lock, so a
// crash there leaves ErrLocked until the file is deleted. Contention is never a wait: Acquire returns
// ErrLocked immediately, whether the holder is another process or another Lock in this one.
package fslock

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

// FileName is the lock file name inside the store directory.
const FileName = "LOCK"

// ErrLocked means another process (or another Lock in this process) holds the file.
var ErrLocked = errors.New("fslock: locked by another process")

// Lock is a held lock. Release it exactly once.
type Lock struct {
	path string
	f    *os.File
	once sync.Once
	err  error
}

// Acquire takes the lock at path, creating the file if needed. It never blocks: contention is ErrLocked.
func Acquire(path string) (*Lock, error) {
	path = filepath.Clean(path)
	f, err := open(path)
	if err != nil {
		if errors.Is(err, ErrLocked) {
			return nil, ErrLocked
		}
		return nil, fmt.Errorf("fslock: acquire %s: %w", path, err)
	}
	// Best effort: record the owner for humans reading the directory.
	if err := f.Truncate(0); err == nil {
		_, _ = f.WriteAt([]byte(strconv.Itoa(os.Getpid())+"\n"), 0)
	}
	return &Lock{path: path, f: f}, nil
}

// Path returns the lock file path.
func (l *Lock) Path() string { return l.path }

// Release drops the lock. It is idempotent and returns the first error.
func (l *Lock) Release() error {
	l.once.Do(func() {
		if err := release(l.f); err != nil {
			l.err = fmt.Errorf("fslock: release %s: %w", l.path, err)
		}
	})
	return l.err
}
