package kvlite

import (
	"log/slog"
	"testing"
	"time"

	"github.com/crowbett/kvlite/internal/record"
)

func TestOptions(t *testing.T) {
	t.Parallel()
	fixed := func() time.Time { return time.Unix(1_700_000_000, 0) }
	logger := slog.New(slog.DiscardHandler)
	tests := []struct {
		name    string
		opts    []Option
		wantErr bool
		check   func(t *testing.T, o options)
	}{
		{"defaults", nil, false, func(t *testing.T, o options) {
			t.Helper()
			if o.maxSegmentBytes != DefaultMaxSegmentBytes || o.sync != SyncNever ||
				o.syncInterval != DefaultSyncInterval || o.maxKey != MaxKeyBytes ||
				o.maxValue != MaxValueBytes || o.shards != DefaultShards ||
				o.autoCompact != 0 || o.readOnly || o.repair || o.logger == nil || o.clock == nil {
				t.Errorf("unexpected defaults: %+v", o)
			}
		}},
		{"all set", []Option{
			WithMaxSegmentBytes(record.MaxRecord), WithSync(SyncInterval), WithSyncInterval(50 * time.Millisecond),
			WithReadOnly(), WithRepair(), WithAutoCompact(0.5), WithMaxKeyBytes(32), WithMaxValueBytes(1 << 20),
			WithShards(8), WithLogger(logger), WithClock(fixed),
		}, false, func(t *testing.T, o options) {
			t.Helper()
			if o.maxSegmentBytes != record.MaxRecord || o.sync != SyncInterval ||
				o.syncInterval != 50*time.Millisecond || !o.readOnly || !o.repair || o.autoCompact != 0.5 ||
				o.maxKey != 32 || o.maxValue != 1<<20 || o.shards != 8 || o.logger != logger ||
				!o.clock().Equal(fixed()) {
				t.Errorf("options not applied: %+v", o)
			}
		}},
		{"nil option skipped", []Option{nil, WithShards(2)}, false, nil},
		{"segment too small", []Option{WithMaxSegmentBytes(record.MaxRecord - 1)}, true, nil},
		{"bad sync", []Option{WithSync(SyncAlways + 1)}, true, nil},
		{"zero interval", []Option{WithSyncInterval(0)}, true, nil},
		{"auto compact zero", []Option{WithAutoCompact(0)}, true, nil},
		{"auto compact over one", []Option{WithAutoCompact(1.5)}, true, nil},
		{"key zero", []Option{WithMaxKeyBytes(0)}, true, nil},
		{"key over cap", []Option{WithMaxKeyBytes(MaxKeyBytes + 1)}, true, nil},
		{"value over cap", []Option{WithMaxValueBytes(MaxValueBytes + 1)}, true, nil},
		{"shards not pow2", []Option{WithShards(3)}, true, nil},
		{"nil logger", []Option{WithLogger(nil)}, true, nil},
		{"nil clock", []Option{WithClock(nil)}, true, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			o := defaultOptions()
			err := o.apply(tt.opts)
			if (err != nil) != tt.wantErr {
				t.Fatalf("apply() err = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.check != nil {
				tt.check(t, o)
			}
		})
	}
}

func TestSyncPolicyString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		p    SyncPolicy
		want string
	}{
		{SyncNever, "never"}, {SyncInterval, "interval"}, {SyncAlways, "always"}, {SyncPolicy(9), "SyncPolicy(9)"},
	}
	for _, tt := range tests {
		if got := tt.p.String(); got != tt.want {
			t.Errorf("%d.String() = %q, want %q", tt.p, got, tt.want)
		}
	}
}
