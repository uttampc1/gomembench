// stream.go
package main

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"time"
)

const cacheLineSize = 64

// goroutineResult holds the timing and accumulator for one goroutine's run.
type goroutineResult struct {
	acc    uint64
	routineStart time.Time // when goroutine begins executing (before barrier)
	measureStart time.Time // after barrier, just before scan
	measureEnd   time.Time // just after scan completes
	routineEnd   time.Time // when goroutine function is about to return
}

// StreamBench runs a sequential read benchmark with n goroutines over a fixed
// total buffer.
//
// Each goroutine runs multiple passes through its slice so that the total
// measurement duration is long enough that all goroutines fully overlap.
// The number of passes is chosen so that each goroutine runs for at least
// minDuration; actual pass count is computed from a short single-pass probe.
//
// Each goroutine records its own start and finish time immediately around its
// scan loop.  After all goroutines finish, we take min(start) and max(finish)
// to get the true wall-clock elapsed time from first-start to last-finish.
//
// Reported throughput counts 64 physical bytes per cache-line access
// regardless of how many payload bytes sequentialScanSlice reads from each line.
func StreamBenchSequential(totalBytes, n, trials int, fixedPasses int, width AccessWidth, verbose bool) {
	const minDuration = 10 * time.Second // target minimum scan time per trial

	if n <= 0 {
		n = runtime.GOMAXPROCS(0)
	}

	// Round down so every slice is exactly cacheLineSize-aligned.
	perGoroutine := (totalBytes / n / cacheLineSize) * cacheLineSize
	if perGoroutine == 0 {
		fatalf("stream: buffer too small for %d goroutines", n)
	}
	actual := perGoroutine * n
	buf := make([]byte, actual)

	linesPerGoroutine := perGoroutine / cacheLineSize

	// First-touch: let each goroutine fault in its own pages so the OS maps
	// them on whichever NUMA node that goroutine's OS thread sits on.
	// User controls NUMA placement externally via numactl / taskset.
	{
		var wg sync.WaitGroup
		wg.Add(n)
		for g := 0; g < n; g++ {
			g := g
			go func() {
				defer wg.Done()
				sl := buf[g*perGoroutine : (g+1)*perGoroutine]
				for i := 0; i < len(sl); i += 4096 {
					sl[i] = 0
				}
			}()
		}
		wg.Wait()
	}

	// Probe pass: measure how long one goroutine takes for one pass so we
	// can compute the number of passes needed to reach minDuration.
	var passes int
	if fixedPasses > 0 {
		passes = fixedPasses
		fmt.Fprintf(os.Stderr, "passes: %d (user-specified)\n", passes)
	} else {
		// Probe pass: measure how long one goroutine takes for one pass so we
		// can compute the number of passes needed to reach minDuration.
		probeStart := time.Now()
		probeAcc := sequentialScanSlice(buf[:perGoroutine], 1, width)
		sink += probeAcc
		probeElapsed := time.Since(probeStart)

		passes = 1
		if probeElapsed > 0 && probeElapsed < minDuration {
			passes = int(minDuration/probeElapsed) + 1
		}

		fmt.Fprintf(os.Stderr, "probe: one pass = %v, using %d passes per trial (~%v per goroutine)\n",
			probeElapsed.Round(time.Millisecond),
			passes,
			(time.Duration(passes)*probeElapsed).Round(time.Millisecond))
	}

	writeCSVHeader()
	fmt.Println("==============================================")

	results := make([]goroutineResult, n)

	for t := 1; t <= trials; t++ {
		b := newBarrier(n)
		var endWg sync.WaitGroup
		endWg.Add(n)

		for g := 0; g < n; g++ {
			g := g
			go func() {
				defer endWg.Done()
				sl := buf[g*perGoroutine : (g+1)*perGoroutine]
        results[g].routineStart = time.Now()

				b.arrive() // block until every goroutine is ready

				results[g].measureStart = time.Now()
				results[g].acc = sequentialScanSlice(sl, passes, width)
				results[g].measureEnd = time.Now()
				results[g].routineEnd = time.Now()
			}()
		}

		endWg.Wait()

		// Fold accumulators into the global sink so the compiler cannot
		// treat the reads as dead code.
		for _, r := range results {
			sink += r.acc
		}

	 	// Aggregate: min(measureStart) to max(measureEnd)
		earliestMeasure := results[0].measureStart
		latestMeasure := results[0].measureEnd
		for i := 1; i < n; i++ {
			if results[i].measureStart.Before(earliestMeasure) {
				earliestMeasure = results[i].measureStart
			}
			if results[i].measureEnd.After(latestMeasure) {
				latestMeasure = results[i].measureEnd
			}
		}
		elapsed := latestMeasure.Sub(earliestMeasure)

		physicalBytes := float64(n) * float64(linesPerGoroutine) * float64(cacheLineSize) * float64(passes)
		gbps := physicalBytes / elapsed.Seconds() / 1e9

		totalAccesses := float64(n) * float64(linesPerGoroutine) * float64(passes)
		cacheLineAccessesPerSec := totalAccesses / elapsed.Seconds()

		payloadBytes := float64(n) * float64(linesPerGoroutine) * float64(width) * float64(passes)
    payloadGbps := payloadBytes / elapsed.Seconds() / 1e9

		writeCSVRow(t, "stream-sequential", n, "throughput", gbps, "GB/s")
		writeCSVRow(t, "stream-sequential", n, "cache_line_acceses_per_sec", cacheLineAccessesPerSec/1e6, "M_acc/s")
		writeCSVRow(t, "stream-sequential", n, "payload_throughput", payloadGbps, "GB/s")
	  fmt.Println("----------------------------------------------")

    if verbose {
			// Print per-goroutine header once (first trial)
			if t == 1 {
				fmt.Fprintf(os.Stderr,
					"trial,routine_id,routine_start_ms,measure_start_ms,measure_end_ms,routine_end_ms,scan_duration_ms,routine_throughput_gbps\n")
				fmt.Fprintf(os.Stderr,
					"-------------------------------------------------------------------------------------------------------------------------\n")
			}

			// All times relative to earliest measureStart
			ref := earliestMeasure
			perGoroutinePhysical := float64(linesPerGoroutine) * float64(cacheLineSize) * float64(passes)

			for g := 0; g < n; g++ {
				r := results[g]
				scanDur := r.measureEnd.Sub(r.measureStart)
				rThroughput := perGoroutinePhysical / scanDur.Seconds() / 1e9

				fmt.Fprintf(os.Stderr,
					"%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.4f\n",
					t,
					g,
					float64(r.routineStart.Sub(ref).Microseconds())/1000.0,
					float64(r.measureStart.Sub(ref).Microseconds())/1000.0,
					float64(r.measureEnd.Sub(ref).Microseconds())/1000.0,
					float64(r.routineEnd.Sub(ref).Microseconds())/1000.0,
					float64(scanDur.Microseconds())/1000.0,
					rThroughput,
				)
			}
			fmt.Fprintf(os.Stderr,
				"-------------------------------------------------------------------------------------------------------------------------\n")
		}
	}
}

// StreamBenchRandom runs a random read throughput benchmark with n goroutines
// over a fixed total buffer.
//
// Each goroutine pre-builds a random offset array covering every cache line
// in its slice (one access per line, random order). During timing, goroutines
// walk the offset array reading from their buffer slice.
//
// Reported throughput counts 64 physical bytes per cache-line access.
// Also reports accesses per second as a secondary metric.
func StreamBenchRandom(totalBytes, n, trials int, fixedPasses int, width AccessWidth, verbose bool) {
	const minDuration = 10 * time.Second // target minimum scan time per trial

	if n <= 0 {
		n = runtime.GOMAXPROCS(0)
	}

	// Round down so every slice is exactly cacheLineSize-aligned.
	perGoroutine := (totalBytes / n / cacheLineSize) * cacheLineSize
	if perGoroutine == 0 {
		fatalf("stream: buffer too small for %d goroutines", n)
	}
	actual := perGoroutine * n
	buf := make([]byte, actual)

  linesPerGoroutine := perGoroutine / cacheLineSize

	// First-touch: let each goroutine fault in its own pages so the OS maps
	// them on whichever NUMA node that goroutine's OS thread sits on.
	// User controls NUMA placement externally via numactl / taskset.
	{
		var wg sync.WaitGroup
		wg.Add(n)
		for g := 0; g < n; g++ {
			g := g
			go func() {
				defer wg.Done()
				sl := buf[g*perGoroutine : (g+1)*perGoroutine]
				for i := 0; i < len(sl); i += 4096 {
					sl[i] = 0
				}
			}()
		}
		wg.Wait()
	}

	// Build per-goroutine random offset arrays if needed.
	// Each array contains every cache-line offset in the goroutine's slice,
	// shuffled into random order.
	var perGoroutineOffsets [][]int
	perGoroutineOffsets = make([][]int, n)
	for g := 0; g < n; g++ {
	  offsets := make([]int, linesPerGoroutine)
		for i := 0; i < linesPerGoroutine; i++ {
			offsets[i] = i * cacheLineSize
		}
		// Shuffle using a per-goroutine seed so each has a different pattern.
		rng := rand.New(rand.NewSource(int64(g) + 42))
		rng.Shuffle(len(offsets), func(i, j int) {
			offsets[i], offsets[j] = offsets[j], offsets[i]
		})
	  perGoroutineOffsets[g] = offsets
	}
	// Probe pass to determine number of passes.
	var passes int
	if fixedPasses > 0 {
		passes = fixedPasses
		fmt.Fprintf(os.Stderr, "passes: %d (user-specified)\n", passes)
	} else {
		probeStart := time.Now()
		probeAcc := randomScanSlice(buf[:perGoroutine], perGoroutineOffsets[0], 1, width)
		sink += probeAcc
		probeElapsed := time.Since(probeStart)

		passes = 1
		if probeElapsed > 0 && probeElapsed < minDuration {
			passes = int(minDuration/probeElapsed) + 1
		}

		fmt.Fprintf(os.Stderr, "probe: one pass = %v, using %d passes per trial (~%v per goroutine)\n",
			probeElapsed.Round(time.Millisecond),
			passes,
			(time.Duration(passes)*probeElapsed).Round(time.Millisecond))
	}

 	results := make([]goroutineResult, n)

	for t := 1; t <= trials; t++ {
		b := newBarrier(n)
		var endWg sync.WaitGroup
		endWg.Add(n)

		for g := 0; g < n; g++ {
			g := g
			go func() {
				defer endWg.Done()
				sl := buf[g*perGoroutine : (g+1)*perGoroutine]
        results[g].routineStart = time.Now()

				b.arrive()

				results[g].measureStart = time.Now()
				results[g].acc = randomScanSlice(sl, perGoroutineOffsets[g], passes, width)
				results[g].measureEnd = time.Now()
				results[g].routineEnd = time.Now()
			}()
		}

		endWg.Wait()

		for _, r := range results {
			sink += r.acc
		}

	 	// Aggregate: min(measureStart) to max(measureEnd)
		earliestMeasure := results[0].measureStart
		latestMeasure := results[0].measureEnd
		for i := 1; i < n; i++ {
			if results[i].measureStart.Before(earliestMeasure) {
				earliestMeasure = results[i].measureStart
			}
			if results[i].measureEnd.After(latestMeasure) {
				latestMeasure = results[i].measureEnd
			}
		}
		elapsed := latestMeasure.Sub(earliestMeasure)

		physicalBytes := float64(n) * float64(linesPerGoroutine) * float64(cacheLineSize) * float64(passes)
		gbps := physicalBytes / elapsed.Seconds() / 1e9

		totalAccesses := float64(n) * float64(linesPerGoroutine) * float64(passes)
		cacheLineAccessesPerSec := totalAccesses / elapsed.Seconds()

		payloadBytes := float64(n) * float64(linesPerGoroutine) * float64(width) * float64(passes)
    payloadGbps := payloadBytes / elapsed.Seconds() / 1e9

		writeCSVRow(t, "stream-random", n, "throughput", gbps, "GB/s")
		writeCSVRow(t, "stream-random", n, "cache_line_acceses_per_sec", cacheLineAccessesPerSec/1e6, "M_acc/s")
		writeCSVRow(t, "stream-random", n, "payload_throughput", payloadGbps, "GB/s")
    if verbose {
			// Print per-goroutine header once (first trial)
			if t == 1 {
				fmt.Fprintf(os.Stderr,
					"trial,routine_id,routine_start_ms,measure_start_ms,measure_end_ms,routine_end_ms,scan_duration_ms,routine_throughput_gbps\n")
			}

			// All times relative to earliest measureStart
			ref := earliestMeasure
			perGoroutinePhysical := float64(linesPerGoroutine) * float64(cacheLineSize) * float64(passes)

			for g := 0; g < n; g++ {
				r := results[g]
				scanDur := r.measureEnd.Sub(r.measureStart)
				rThroughput := perGoroutinePhysical / scanDur.Seconds() / 1e9

				fmt.Fprintf(os.Stderr,
					"%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.4f\n",
					t,
					g,
					float64(r.routineStart.Sub(ref).Microseconds())/1000.0,
					float64(r.measureStart.Sub(ref).Microseconds())/1000.0,
					float64(r.measureEnd.Sub(ref).Microseconds())/1000.0,
					float64(r.routineEnd.Sub(ref).Microseconds())/1000.0,
					float64(scanDur.Microseconds())/1000.0,
					rThroughput,
				)
			}
		}
  }
}
