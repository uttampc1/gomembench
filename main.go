// main.go
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
)

func main() {
	// ── Flags ────────────────────────────────────────────────────────────
	bench := flag.String("bench", "stream-sequential",
		"benchmark to run: stream-sequential | stream-random | latency-idle | latency-loaded")
	totalGB := flag.Int("total-gb", 24,
		"total working set in GB (divided equally across goroutines)")
	n := flag.Int("n", 0,
		"number of goroutines (default: GOMAXPROCS)")
	trials := flag.Int("trials", 5,
		"number of trials per measurement")
	passes := flag.Int("passes", 0,
		"number of passes per goroutine per trial (default: auto from probe, 0 = auto)")
	probeBufGB := flag.Int("probe-buf-gb", 0,
		"probe buffer size in GB (default: auto from L3 size, min 1 GB)")
	width := flag.Int("width", 1,
		"bytes read per cache line in stream benchmark: 1 or 8")
	verbose := flag.Bool("v", false,
		"print per-trial timing details (start window, finish window) to stderr")
	flag.Parse()

	// ── Validate ─────────────────────────────────────────────────────────
	if *totalGB <= 0 {
		fatalf("--total-gb must be > 0")
	}
	if *trials <= 0 {
		fatalf("--trials must be > 0")
	}
	if *width != 1 && *width != 8 {
		fatalf("--width must be 1 or 8")
	}
	if *passes < 0 {
		fatalf("--passes must be >= 0 (0 = auto)")
	}

	// ── Discover system topology ──────────────────────────────────────────
	info := Discover()
	info.Print()

	// ── Resolve common parameters ─────────────────────────────────────────
	totalBytes := *totalGB * (1 << 30)
	goroutines := defaultGoroutines(*n)

	// ── Print run parameters to stderr ───────────────────────────────────
	fmt.Fprintf(os.Stderr, "bench         : %s\n", *bench)
	fmt.Fprintf(os.Stderr, "total-gb      : %d GB (%d bytes)\n", *totalGB, totalBytes)
	fmt.Fprintf(os.Stderr, "goroutines    : %d\n", goroutines)
	fmt.Fprintf(os.Stderr, "trials        : %d\n", *trials)
	if *passes > 0 {
		fmt.Fprintf(os.Stderr, "passes        : %d (user-specified)\n", *passes)
	} else {
		fmt.Fprintf(os.Stderr, "passes        : auto\n")
	}
	fmt.Fprintf(os.Stderr, "width         : %d byte(s) per cache line\n", *width)
	fmt.Fprintf(os.Stderr, "GOMAXPROCS    : %d\n", runtime.GOMAXPROCS(0))
	fmt.Fprintln(os.Stderr)

	// ── Disable GC for benchmark duration ────────────────────────────────
	debug.SetGCPercent(-1)
	runtime.GC()

	// ── Dispatch ──────────────────────────────────────────────────────────
	aw := Access1Byte
	if *width == 8 {
		aw = Access8Bytes
	}

	switch *bench {
	case "stream-sequential":
		StreamBenchSequential(totalBytes, goroutines, *trials, *passes, aw, *verbose)

	case "stream-random":
		StreamBenchRandom(totalBytes, goroutines, *trials, *passes, aw, *verbose)

	case "latency-idle":
		probeBufBytes := resolveProbeBuf(*probeBufGB, info)
		fmt.Fprintf(os.Stderr, "probe-buf     : %.2f GB (%d bytes)\n",
			float64(probeBufBytes)/float64(1<<30), probeBufBytes)
		LatencyIdleBench(probeBufBytes, *trials)

	case "latency-loaded":
		probeBufBytes := resolveProbeBuf(*probeBufGB, info)
		fmt.Fprintf(os.Stderr, "probe-buf     : %.2f GB (%d bytes)\n",
			float64(probeBufBytes)/float64(1<<30), probeBufBytes)
		LatencyLoadedBench(totalBytes, probeBufBytes, goroutines, *trials, aw)

	default:
		fatalf("unknown --bench value %q; choose stream | latency-idle | latency-loaded", *bench)
	}
}

// resolveProbeBuf returns the probe buffer size in bytes.
func resolveProbeBuf(probeBufGB int, info SysInfo) int {
	if probeBufGB > 0 {
		return probeBufGB * (1 << 30)
	}
	return info.DefaultProbeBufBytes()
}
