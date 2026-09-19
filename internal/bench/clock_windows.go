//go:build windows

package bench

import (
	"sync"
	"syscall"
	"unsafe"
)

// time.Now's monotonic reading on Windows comes from the interrupt clock (~0.4-15 ms steps), which
// rounds every sub-millisecond operation to zero. QueryPerformanceCounter ticks at ~10 MHz or better,
// so per-op latencies stay meaningful. Zero dependencies: kernel32 via package syscall.
var (
	qpcOnce  sync.Once
	qpcProc  *syscall.LazyProc
	qpfFreq  int64
	qpcStart int64
)

func qpcInit() {
	k32 := syscall.NewLazyDLL("kernel32.dll")
	qpcProc = k32.NewProc("QueryPerformanceCounter")
	freqProc := k32.NewProc("QueryPerformanceFrequency")
	_, _, _ = freqProc.Call(uintptr(unsafe.Pointer(&qpfFreq))) //nolint:gosec // out-param of a kernel32 call
	if qpfFreq <= 0 {
		qpfFreq = 1
	}
	_, _, _ = qpcProc.Call(uintptr(unsafe.Pointer(&qpcStart))) //nolint:gosec // out-param of a kernel32 call
}

// HiResNow returns monotonic nanoseconds since the first call, from QueryPerformanceCounter.
func HiResNow() int64 {
	qpcOnce.Do(qpcInit)
	var c int64
	_, _, _ = qpcProc.Call(uintptr(unsafe.Pointer(&c))) //nolint:gosec // out-param of a kernel32 call
	ticks := c - qpcStart
	// (ticks * 1e9) / freq without overflow for runs shorter than ~290 years at 1 GHz.
	sec := ticks / qpfFreq
	rem := ticks % qpfFreq
	return sec*1_000_000_000 + rem*1_000_000_000/qpfFreq
}
