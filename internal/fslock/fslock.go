// STUB written by the integrator to freeze the contract; workstream A (format) owns this file and replaces
// the bodies, keeping every exported signature. This package is real and tested (see lock_*.go for the
// per-platform primitive); WS-A may harden it but must keep Acquire/Release/ErrLocked.

// Package fslock implements the single-writer LOCK file of a kvlite directory. The lock is held by an
// open file handle, so the OS releases it when the process dies: an exclusive share mode on Windows
// (CreateFile with dwShareMode 0), flock(LOCK_EX|LOCK_NB) on unix, and O_EXCL creation elsewhere.
// The file holds the owner's pid for diagnostics only. It imports nothing from the rest of the module.
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
