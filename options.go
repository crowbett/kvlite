// Workstream B (engine) owns this file. The Option set is a frozen contract (docs/PLAN.md): the
// unexported options struct may grow, no With* signature changes.

package kvlite

import (
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/crowbett/kvlite/internal/record"
)

// Defaults and hard caps for the options. The caps are format limits (see internal/record) and cannot be
// raised by an option.
const (
	// DefaultMaxSegmentBytes is the size at which the active segment rotates (256 MiB).
	DefaultMaxSegmentBytes int64 = 256 << 20
	// DefaultSyncInterval is the flusher period under SyncInterval.
	DefaultSyncInterval = time.Second
	// DefaultShards is the keydir shard count.
	DefaultShards = 64
	// MaxKeyBytes is the hard cap on a key (64 KiB); WithMaxKeyBytes can only lower it.
	MaxKeyBytes = record.MaxKey
	// MaxValueBytes is the hard cap on a value (16 MiB, owner decision); WithMaxValueBytes can only lower it.
	MaxValueBytes = record.MaxValue
	// AutoCompactMinDead is the minimum DeadBytes before auto-compaction fires (64 MiB).
	AutoCompactMinDead int64 = 64 << 20
	// AutoCompactInterval is how often the auto-compactor re-evaluates its trigger.
	AutoCompactInterval = 5 * time.Minute
)

// SyncPolicy says when appended records are fsynced.
type SyncPolicy uint8

// Sync policies. SyncNever is the default because FlushFileBuffers costs ~1 ms on NTFS.
const (
	// SyncNever leaves durability to the OS page cache; a power loss may lose recent writes.
	SyncNever SyncPolicy = iota
	// SyncInterval fsyncs from a background goroutine every WithSyncInterval.
	SyncInterval
	// SyncAlways fsyncs inside every Put/Delete/Batch.Commit.
	SyncAlways
)

// String implements fmt.Stringer.
func (p SyncPolicy) String() string {
	switch p {
	case SyncNever:
		return "never"
	case SyncInterval:
		return "interval"
	case SyncAlways:
		return "always"
	default:
		return "SyncPolicy(" + strconv.Itoa(int(p)) + ")"
	}
}

// Option configures Open. Options are applied in order; an invalid value makes Open fail.
type Option func(*options) error

type options struct {
	maxSegmentBytes int64
	sync            SyncPolicy
	syncInterval    time.Duration
	readOnly        bool
	repair          bool
	autoCompact     float64 // dead ratio trigger; 0 = off
	maxKey          int
	maxValue        int
	shards          int
	logger          *slog.Logger
	clock           func() time.Time
}

func defaultOptions() options {
	return options{
		maxSegmentBytes: DefaultMaxSegmentBytes,
		sync:            SyncNever,
		syncInterval:    DefaultSyncInterval,
		maxKey:          MaxKeyBytes,
		maxValue:        MaxValueBytes,
		shards:          DefaultShards,
		logger:          slog.New(slog.DiscardHandler),
		clock:           time.Now,
	}
}

func (o *options) apply(opts []Option) error {
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(o); err != nil {
			return err
		}
	}
	return nil
}

// WithMaxSegmentBytes sets the rotation threshold of the active segment. n must be at least one maximal
// record (record.MaxRecord) so that any single record fits.
func WithMaxSegmentBytes(n int64) Option {
	return func(o *options) error {
		if n < record.MaxRecord {
			return errors.New("kvlite: WithMaxSegmentBytes: below record.MaxRecord")
		}
		o.maxSegmentBytes = n
		return nil
	}
}

// WithSync sets the durability policy.
func WithSync(p SyncPolicy) Option {
	return func(o *options) error {
		if p > SyncAlways {
			return errors.New("kvlite: WithSync: unknown policy")
		}
		o.sync = p
		return nil
	}
}

// WithSyncInterval sets the flusher period used under SyncInterval.
func WithSyncInterval(d time.Duration) Option {
	return func(o *options) error {
		if d <= 0 {
			return errors.New("kvlite: WithSyncInterval: must be positive")
		}
		o.syncInterval = d
		return nil
	}
}

// WithReadOnly opens without the LOCK file and rejects every mutation with ErrReadOnly.
func WithReadOnly() Option {
	return func(o *options) error {
		o.readOnly = true
		return nil
	}
}

// WithRepair makes Open skip corrupt records instead of returning ErrCorrupt and rebuild a missing or
// corrupt MANIFEST from the segment files on disk.
func WithRepair() Option {
	return func(o *options) error {
		o.repair = true
		return nil
	}
}

// WithAutoCompact enables background compaction (off by default, owner decision). It fires when
// Stats.DeadRatio >= deadRatio and DeadBytes >= AutoCompactMinDead, checked every AutoCompactInterval.
// deadRatio must be in (0, 1].
func WithAutoCompact(deadRatio float64) Option {
	return func(o *options) error {
		if deadRatio <= 0 || deadRatio > 1 {
			return errors.New("kvlite: WithAutoCompact: deadRatio must be in (0, 1]")
		}
		o.autoCompact = deadRatio
		return nil
	}
}

// WithMaxKeyBytes lowers the key size limit; n must be in [1, MaxKeyBytes].
func WithMaxKeyBytes(n int) Option {
	return func(o *options) error {
		if n < 1 || n > MaxKeyBytes {
			return errors.New("kvlite: WithMaxKeyBytes: must be in [1, MaxKeyBytes]")
		}
		o.maxKey = n
		return nil
	}
}

// WithMaxValueBytes lowers the value size limit; n must be in [1, MaxValueBytes].
func WithMaxValueBytes(n int) Option {
	return func(o *options) error {
		if n < 1 || n > MaxValueBytes {
			return errors.New("kvlite: WithMaxValueBytes: must be in [1, MaxValueBytes]")
		}
		o.maxValue = n
		return nil
	}
}

// WithShards sets the keydir shard count; n must be a power of two.
func WithShards(n int) Option {
	return func(o *options) error {
		if n < 1 || n&(n-1) != 0 {
			return errors.New("kvlite: WithShards: must be a power of two")
		}
		o.shards = n
		return nil
	}
}

// WithLogger sets the structured logger (default: discard).
func WithLogger(l *slog.Logger) Option {
	return func(o *options) error {
		if l == nil {
			return errors.New("kvlite: WithLogger: nil logger")
		}
		o.logger = l
		return nil
	}
}

// WithClock injects the clock used for record timestamps and TTL expiry. Every TTL test uses it.
func WithClock(now func() time.Time) Option {
	return func(o *options) error {
		if now == nil {
			return errors.New("kvlite: WithClock: nil clock")
		}
		o.clock = now
		return nil
	}
}
