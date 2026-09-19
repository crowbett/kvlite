//go:build windows

package fslock

import (
	"errors"
	"os"
	"syscall"
)

// errSharingViolation is ERROR_SHARING_VIOLATION (32), which package syscall does not name.
const errSharingViolation = syscall.Errno(32)

// open creates or opens path with an exclusive share mode: any other CreateFile on it fails with
// ERROR_SHARING_VIOLATION until the handle is closed, which the kernel does when the process exits.
func open(path string) (*os.File, error) {
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	h, err := syscall.CreateFile(p,
		syscall.GENERIC_READ|syscall.GENERIC_WRITE,
		0, // no sharing
		nil,
		syscall.OPEN_ALWAYS,
		syscall.FILE_ATTRIBUTE_NORMAL,
		0)
	if err != nil {
		if errors.Is(err, errSharingViolation) {
			return nil, ErrLocked
		}
		return nil, err
	}
	return os.NewFile(uintptr(h), path), nil
}

func release(f *os.File) error { return f.Close() }
