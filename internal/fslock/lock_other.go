//go:build !windows && !unix

package fslock

import (
	"errors"
	"io/fs"
	"os"
)

// open falls back to O_EXCL creation on platforms without an advisory lock primitive. A crash leaves
// the file behind and the next Acquire reports ErrLocked until it is removed by hand.
func open(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return nil, ErrLocked
		}
		return nil, err
	}
	return f, nil
}

func release(f *os.File) error {
	err := f.Close()
	if rerr := os.Remove(f.Name()); err == nil {
		err = rerr
	}
	return err
}
