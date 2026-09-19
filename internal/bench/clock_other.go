//go:build !windows

package bench

import "time"

var hiResBase = time.Now()

// HiResNow returns monotonic nanoseconds since process start. On Linux/macOS time.Now's monotonic
// reading is already nanosecond-resolution (clock_gettime), so no OS-specific source is needed.
func HiResNow() int64 { return int64(time.Since(hiResBase)) }
