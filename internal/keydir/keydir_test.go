package keydir

import (
	"bytes"
	"testing"
	"time"
	"unsafe"
)

func TestEntryLayout(t *testing.T) {
	t.Parallel()
	if got := unsafe.Sizeof(Entry{}); got != 16 {
		t.Fatalf("unsafe.Sizeof(Entry{}) = %d, want 16 (frozen contract)", got)
	}
	if got := unsafe.Alignof(Entry{}); got != 4 {
		t.Errorf("unsafe.Alignof(Entry{}) = %d, want 4", got)
	}
}

func TestEntryExpired(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 500_000_000)
	tests := []struct {
		name string
		e    Entry
		want bool
	}{
		{"never", Entry{}, false},
		{"future", Entry{ExpSec: 1_700_000_001}, false},
		{"exact second", Entry{ExpSec: 1_700_000_000}, true},
		{"past", Entry{ExpSec: 1_600_000_000}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.e.Expired(now); got != tt.want {
				t.Errorf("Expired() = %v, want %v", got, tt.want)
			}
		})
	}
	if (Entry{ExpSec: 1}).Expired(time.Unix(-5, 0)) {
		t.Error("negative clock must not expire entries")
	}
}

func TestNew(t *testing.T) {
	t.Parallel()
	k := New(0, 1000)
	if len(k.shards) != DefaultShards || k.mask != DefaultShards-1 || k.Len() != 0 {
		t.Errorf("New(0, ...) = %d shards mask %d len %d", len(k.shards), k.mask, k.Len())
	}
	k = New(8, 0)
	if len(k.shards) != 8 || k.mask != 7 {
		t.Errorf("New(8, 0) = %d shards mask %d", len(k.shards), k.mask)
	}
	defer func() {
		if recover() == nil {
			t.Error("New(3, 0) did not panic")
		}
	}()
	New(3, 0)
}

func TestSorted(t *testing.T) {
	t.Parallel()
	keys := [][]byte{[]byte(""), []byte("a"), []byte("ab"), []byte("b"), []byte("zz")}
	ents := make([]Entry, len(keys))
	for i := range ents {
		ents[i] = Entry{Seg: 1, Off: uint32(i * 10), Size: 10}
	}
	s := newSorted(keys, ents)
	if s.Len() != len(keys) {
		t.Fatalf("Len() = %d, want %d", s.Len(), len(keys))
	}
	for i, k := range keys {
		if !bytes.Equal(s.Key(i), k) || s.Entry(i) != ents[i] {
			t.Errorf("Key/Entry(%d) = %q %+v, want %q %+v", i, s.Key(i), s.Entry(i), k, ents[i])
		}
	}
	tests := []struct {
		key  string
		want int
	}{
		{"", 0}, {"a", 1}, {"aa", 2}, {"ab", 2}, {"abc", 3}, {"b", 3}, {"c", 4}, {"zz", 4}, {"zzz", 5},
	}
	for _, tt := range tests {
		if got := s.Search([]byte(tt.key)); got != tt.want {
			t.Errorf("Search(%q) = %d, want %d", tt.key, got, tt.want)
		}
	}
	var empty Sorted
	if empty.Len() != 0 || empty.Search([]byte("x")) != 0 {
		t.Error("zero Sorted must be empty")
	}
}
