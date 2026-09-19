package kvlite

import (
	"errors"
	"fmt"
	"testing"
)

func TestCorruptError(t *testing.T) {
	t.Parallel()
	inner := errors.New("crc mismatch")
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"bare", &CorruptError{Seg: 3, Off: 128}, "kvlite: corrupt segment 3 at offset 128"},
		{"reason", &CorruptError{Seg: 0, Off: 16, Reason: "checksum"}, "kvlite: corrupt segment 0 at offset 16: checksum"},
		{
			"wrapped", fmt.Errorf("open: %w", &CorruptError{Seg: 7, Off: 1, Reason: "truncated", Err: inner}),
			"open: kvlite: corrupt segment 7 at offset 1: truncated: crc mismatch",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.err.Error(); got != tt.want {
				t.Errorf("Error() = %q, want %q", got, tt.want)
			}
			if !errors.Is(tt.err, ErrCorrupt) {
				t.Errorf("errors.Is(err, ErrCorrupt) = false, want true")
			}
			var ce *CorruptError
			if !errors.As(tt.err, &ce) {
				t.Errorf("errors.As(err, *CorruptError) = false, want true")
			}
			if errors.Is(tt.err, ErrNotFound) {
				t.Errorf("errors.Is(err, ErrNotFound) = true, want false")
			}
		})
	}
	if !errors.Is(&CorruptError{Err: inner}, inner) {
		t.Errorf("CorruptError does not unwrap to its Err")
	}
}

func TestSentinelsDistinct(t *testing.T) {
	t.Parallel()
	all := []error{
		ErrNotFound, ErrClosed, ErrCorrupt, ErrKeyTooLarge, ErrValueTooLarge,
		ErrEmptyKey, ErrLocked, ErrReadOnly, ErrInvalidTTL,
	}
	seen := map[string]bool{}
	for i, a := range all {
		if seen[a.Error()] {
			t.Errorf("duplicate sentinel text %q", a.Error())
		}
		seen[a.Error()] = true
		for j, b := range all {
			if i != j && errors.Is(a, b) {
				t.Errorf("sentinel %v matches %v", a, b)
			}
		}
	}
}
