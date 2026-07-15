// util.go
package main

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sync"
)

// sink prevents dead-code elimination of memory reads.
var sink uint64

// barrier provides a simple start gate for goroutines.
type barrier struct {
	mu      sync.Mutex
	cond    *sync.Cond
	count   int
	total   int
	release bool
}

func newBarrier(n int) *barrier {
	b := &barrier{total: n}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// arrive signals that one goroutine has arrived at the barrier.
// Blocks until all n goroutines have arrived, then releases all at once.
func (b *barrier) arrive() {
	b.mu.Lock()
	b.count++
	if b.count == b.total {
		b.release = true
		b.cond.Broadcast()
	} else {
		for !b.release {
			b.cond.Wait()
		}
	}
	b.mu.Unlock()
}

// buildChaseChain writes a random-permutation pointer chain into buf.
// Each cache line's first 8 bytes hold the index of the next cache line to visit.
// Returns the starting index.
func buildChaseChain(buf []byte) int {
	const lineSize = 64
	numLines := len(buf) / lineSize
	if numLines < 2 {
		panic("buffer too small for pointer chase")
	}

	perm := rand.Perm(numLines)

	for i := 0; i < numLines; i++ {
		cur := perm[i]
		next := perm[(i+1)%numLines]
		off := cur * lineSize
		buf[off+0] = byte(next)
		buf[off+1] = byte(next >> 8)
		buf[off+2] = byte(next >> 16)
		buf[off+3] = byte(next >> 24)
		buf[off+4] = byte(next >> 32)
		buf[off+5] = byte(next >> 40)
		buf[off+6] = byte(next >> 48)
		buf[off+7] = byte(next >> 56)
	}

	return perm[0]
}

// chaseSteps walks n steps through a pointer-chase chain in buf.
// Returns the final index (used as a sink to prevent optimisation).
//
//go:noinline
func chaseSteps(buf []byte, startIdx, steps int) int {
	const lineSize = 64
	idx := startIdx
	for i := 0; i < steps; i++ {
		off := idx * lineSize
		idx = int(buf[off]) |
			int(buf[off+1])<<8 |
			int(buf[off+2])<<16 |
			int(buf[off+3])<<24 |
			int(buf[off+4])<<32 |
			int(buf[off+5])<<40 |
			int(buf[off+6])<<48 |
			int(buf[off+7])<<56
	}
	return idx
}

// AccessWidth controls how many bytes are read per cache line in sequentialScanSlice.
type AccessWidth int

const (
	Access1Byte  AccessWidth = 1
	Access8Bytes AccessWidth = 8
)

// sequentialScanSlice reads one cache line at a time from buf, consuming either 1 or 8
// bytes per line depending on width.  iters controls how many full passes are
// made.  Returns an accumulator used as a sink.
//
//go:noinline
func sequentialScanSlice(buf []byte, iters int, width AccessWidth) uint64 {
	const lineSize = 64
	n := len(buf)
	var acc uint64
	switch width {
	case Access8Bytes:
		for iter := 0; iter < iters; iter++ {
			for off := 0; off+8 <= n; off += lineSize {
				acc += uint64(buf[off]) |
					uint64(buf[off+1])<<8 |
					uint64(buf[off+2])<<16 |
					uint64(buf[off+3])<<24 |
					uint64(buf[off+4])<<32 |
					uint64(buf[off+5])<<40 |
					uint64(buf[off+6])<<48 |
					uint64(buf[off+7])<<56
			}
		}
	default: // Access1Byte
		for iter := 0; iter < iters; iter++ {
			for off := 0; off < n; off += lineSize {
				acc += uint64(buf[off])
			}
		}
	}
	return acc
}

// randomScanSlice reads from buf at pre-computed random offsets.
// Each offset is cache-line-aligned. Returns an accumulator as a sink.
//
//go:noinline
func randomScanSlice(buf []byte, offsets []int, passes int, width AccessWidth) uint64 {
	var acc uint64
	switch width {
	case Access8Bytes:
		for p := 0; p < passes; p++ {
			for _, off := range offsets {
				acc += uint64(buf[off]) |
					uint64(buf[off+1])<<8 |
					uint64(buf[off+2])<<16 |
					uint64(buf[off+3])<<24 |
					uint64(buf[off+4])<<32 |
					uint64(buf[off+5])<<40 |
					uint64(buf[off+6])<<48 |
					uint64(buf[off+7])<<56
			}
		}
	default: // Access1Byte
		for p := 0; p < passes; p++ {
			for _, off := range offsets {
				acc += uint64(buf[off])
			}
		}
	}
	return acc
}

// writeCSVHeader prints the CSV header to stdout.
func writeCSVHeader() {
	fmt.Println("trial,bench,goroutines,metric,value,unit")
}

// writeCSVRow prints one result row to stdout.
func writeCSVRow(trial int, bench string, goroutines int,metric string, value float64, unit string) {
	fmt.Printf("%d,%s,%d,%s,%.4f,%s\n", trial, bench, goroutines, metric, value, unit)
}

// defaultGoroutines returns runtime.GOMAXPROCS(0) if n <= 0.
func defaultGoroutines(n int) int {
	if n <= 0 {
		return runtime.GOMAXPROCS(0)
	}
	return n
}

// fatalf prints a message to stderr and exits.
func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
