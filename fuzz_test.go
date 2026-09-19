// STUB written by the integrator to freeze the contract; workstream D (cli + bench + fuzz) owns this file
// and replaces the body. FuzzDB decodes the corpus into an op script (put/puttl/del/batch/compact/reopen)
// run against a real DB in t.TempDir() and a map[string]string model on a fake clock; after every op and
// every reopen all model keys must match and all deleted keys must be ErrNotFound.

package kvlite

import "testing"

func FuzzDB(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 1, 2, 3})
	f.Fuzz(func(t *testing.T, script []byte) {
		_ = script
		t.Skip("stub: FuzzDB (WS-D)")
	})
}
