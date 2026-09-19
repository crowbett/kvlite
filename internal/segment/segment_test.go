package segment

import "testing"

func TestNames(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		in       string
		wantID   uint32
		wantHint bool
		wantOK   bool
	}{
		{"log", "00000007.log", 7, false, true},
		{"hint", "00000007.hint", 7, true, true},
		{"max", "04294967295.log", 0, false, false},
		{"big id", "99999999.log", 99999999, false, true},
		{"short", "7.log", 0, false, false},
		{"other", "MANIFEST", 0, false, false},
		{"lock", "LOCK", 0, false, false},
		{"tmp", "00000007.log.tmp", 0, false, false},
		{"alpha", "0000000a.log", 0, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			id, hint, ok := ParseName(tt.in)
			if ok != tt.wantOK || id != tt.wantID || hint != tt.wantHint {
				t.Errorf("ParseName(%q) = (%d, %v, %v), want (%d, %v, %v)",
					tt.in, id, hint, ok, tt.wantID, tt.wantHint, tt.wantOK)
			}
		})
	}
	if LogName(7) != "00000007.log" || HintName(7) != "00000007.hint" || LogName(123456789) != "123456789.log" {
		t.Errorf("LogName/HintName: %q %q %q", LogName(7), HintName(7), LogName(123456789))
	}
}

func TestReaderRefs(t *testing.T) {
	t.Parallel()
	r := &Reader{id: 3}
	if r.ID() != 3 || r.Refs() != 0 {
		t.Fatalf("fresh reader: id %d refs %d", r.ID(), r.Refs())
	}
	r.Ref()
	r.Ref()
	r.Unref()
	if r.Refs() != 1 {
		t.Errorf("Refs() = %d, want 1", r.Refs())
	}
	r.Unref()
	r.Unref()
	if r.Refs() != 0 {
		t.Errorf("Refs() after over-unref = %d, want 0", r.Refs())
	}
	if !(VerifyReport{}).OK() || (VerifyReport{Findings: []Finding{{Reason: "x"}}}).OK() {
		t.Error("VerifyReport.OK wrong")
	}
}
