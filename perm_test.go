package kvlite

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	"github.com/crowbett/kvlite/internal/fslock"
	"github.com/crowbett/kvlite/internal/manifest"
	"github.com/crowbett/kvlite/internal/record"
	"github.com/crowbett/kvlite/internal/segment"
)

// TestStoreFilesAreOwnerOnly pins the on-disk permission of everything the store creates: the
// directory is 0750 and every file (LOCK, MANIFEST, *.log, *.hint) is 0600, because segments hold
// user data and must not be world-readable. Windows has no mode bits, so there the test only checks
// that each kind of file was actually produced (the walk still exercises every path).
func TestStoreFilesAreOwnerOnly(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	dir := filepath.Join(root, "store") // created by Open (MkdirAll)
	db := openTest(t, dir, WithMaxSegmentBytes(record.MaxRecord))
	big := bytes.Repeat([]byte("x"), 4<<20)
	for i := range 4 { // several segments, so Compact writes hints
		mustPut(t, db, []byte("k-"+strconv.Itoa(i)), big)
	}
	if err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Check with the store open (LOCK held) and every writer/hint in place.
	seen := map[string]bool{}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		name := d.Name()
		switch {
		case d.IsDir():
			if path != dir && name != manifest.TrashDir {
				t.Errorf("unexpected directory %s", path)
			}
			if runtime.GOOS != "windows" && info.Mode().Perm() != 0o750 {
				t.Errorf("%s: dir mode %o, want 0750", path, info.Mode().Perm())
			}
			return nil
		case name == fslock.FileName:
			seen["lock"] = true
		case name == manifest.FileName:
			seen["manifest"] = true
		default:
			if _, hint, ok := segment.ParseName(name); ok {
				if hint {
					seen["hint"] = true
				} else {
					seen["log"] = true
				}
			} else if filepath.Dir(path) == dir {
				t.Errorf("unexpected file %s", path)
			}
		}
		if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
			t.Errorf("%s: mode %o, want 0600", path, info.Mode().Perm())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	closeTest(t, db)
	for _, kind := range []string{"lock", "manifest", "log", "hint"} {
		if !seen[kind] {
			t.Errorf("no %s file was created", kind)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, manifest.FileName)); err != nil {
		t.Fatal(err)
	}
}
