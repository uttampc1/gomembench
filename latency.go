// latency.go
package main

import (
	"runtime"
	"sync"
	"time"
)

const (
	// defaultProbeBufSize is the default size of the pointer-chase buffer.
	// Must exceed total L3 size to ensure every hop is a genuine DRAM miss.
	// Default 1 GB is sufficient for this Genoa system (768 MB total L3).
	// For larger systems (e.g. GNR with 512 cores and larger L3), pass a
	// larger value via the -probe-buf-gb flag.
	defaultProbeBufSize = 1 << 30 // 1 GB

	// probeHopsIdle is the number of pointer-chase hops per idle-latency trial.
	chaseHopsIdle = 10_000_000

	// probeBatchSize is how many hops the probe goroutine walks between
	// checks of the done channel in loaded-latency mode.  Walking 1000 hops
	// before checking amortises the select overhead to < 0.1% of hop time.
	probeBatchSize = 1000

	// loadedDuration is how long each loaded-latency trial runs.
	loadedDuration = 5 * time.Second
)

// LatencyIdleBench measures pointer-chase latency with no background load.
//
// A single goroutine walks a random-permutation chain through a buffer of
// probeBufBytes bytes.  Because each hop's address depends on the previous
// result the hardware prefetcher cannot help, so every hop is a genuine
// serialised DRAM access.
func LatencyIdleBench(probeBufBytes, trials int) {
	buf := make([]byte, probeBufBytes)

	// First-touch the probe buffer before timing starts.
	for i := 0; i < len(buf); i += 4096 {
		buf[i] = 0
	}

	startIdx := buildChaseChain(buf)

	writeCSVHeader()

	for t := 1; t <= trials; t++ {
		start := time.Now()
		finalIdx := chaseSteps(buf, startIdx, chaseHopsIdle)
		elapsed := time.Since(start)

		sink += uint64(finalIdx)

		nsPerHop := float64(elapsed.Nanoseconds()) / float64(chaseHopsIdle)
		writeCSVRow(t, "latency-idle", 1, "latency", nsPerHop, "ns")
	}
}

// LatencyLoadedBench measures pointer-chase latency while n injector
// goroutines continuously stream through a shared buffer.
//
// All n+1 goroutines (n injectors + 1 probe) start together via a channel-
// based gate.  After loadedDuration the coordinator closes the done channel;
// injectors stop at their next pass boundary, the probe stops after its
// current batch of probeBatchSize hops.
//
// Two CSV rows are written per trial:
//
//	latency-loaded, n, t, throughput, <GB/s>,  GB/s
//	latency-loaded, n, t, latency,    <ns/hop>, ns
func LatencyLoadedBench(totalBytes, probeBufBytes, n, trials int, width AccessWidth) {
	if n <= 0 {
		n = runtime.GOMAXPROCS(0)
	}

	// ── Injector buffer ──────────────────────────────────────────────────
	perInjector := (totalBytes / n / cacheLineSize) * cacheLineSize
	if perInjector == 0 {
		fatalf("latency-loaded: injector buffer too small for %d goroutines", n)
	}
	injBuf := make([]byte, perInjector*n)

	// ── Probe buffer ─────────────────────────────────────────────────────
	probeBuf := make([]byte, probeBufBytes)

	// ── First-touch both buffers ──────────────────────────────────────────
	{
		var wg sync.WaitGroup
		wg.Add(n)
		for g := 0; g < n; g++ {
			g := g
			go func() {
				defer wg.Done()
				sl := injBuf[g*perInjector : (g+1)*perInjector]
				for i := 0; i < len(sl); i += 4096 {
					sl[i] = 0
				}
			}()
		}
		wg.Wait()
		for i := 0; i < len(probeBuf); i += 4096 {
			probeBuf[i] = 0
		}
	}

	probeStart := buildChaseChain(probeBuf)

	// injResult holds per-goroutine timing and byte-count for one trial.
	type injResult struct {
		physicalBytes float64
		start         time.Time
		finish        time.Time
	}
	injResults := make([]injResult, n)

	writeCSVHeader()

	for t := 1; t <= trials; t++ {
		done := make(chan struct{})

		// Gate: workers signal ready via the buffered channel; coordinator
		// drains it and then closes start to release everyone at once.
		ready := make(chan struct{}, n+1)
		start := make(chan struct{})

		var (
			probeHops    int64
			probeElapsed time.Duration
			allDone      sync.WaitGroup
		)
		allDone.Add(n + 1)

		// ── Injector goroutines ───────────────────────────────────────────
		for g := 0; g < n; g++ {
			g := g
			go func() {
				defer allDone.Done()
				sl := injBuf[g*perInjector : (g+1)*perInjector]
				linesPerPass := len(sl) / cacheLineSize

				ready <- struct{}{}
				<-start

				injResults[g].start = time.Now()
				var acc uint64
				var bytes float64
				for {
					select {
					case <-done:
						injResults[g].finish = time.Now()
						injResults[g].physicalBytes = bytes
						sink += acc
						return
					default:
					}
					acc += sequentialScanSlice(sl, 1, width)
					bytes += float64(linesPerPass) * float64(cacheLineSize)
				}
			}()
		}

		// ── Probe goroutine ───────────────────────────────────────────────
		go func() {
			defer allDone.Done()

			ready <- struct{}{}
			<-start

			idx := probeStart
			var hops int64
			probeT0 := time.Now()
			for {
				select {
				case <-done:
					probeElapsed = time.Since(probeT0)
					probeHops = hops
					sink += uint64(idx)
					return
				default:
				}
				// Walk a fixed batch before checking done again.
				// This amortises the select overhead to a negligible
				// fraction of the total hop time.
				idx = chaseSteps(probeBuf, idx, probeBatchSize)
				hops += probeBatchSize
			}
		}()

		// ── Coordinator: release workers, time the run ────────────────────
		for i := 0; i < n+1; i++ {
			<-ready
		}
		close(start)

		time.Sleep(loadedDuration)
		close(done)

		allDone.Wait()

		// ── Aggregate injector bandwidth ──────────────────────────────────
		earliest := injResults[0].start
		latest := injResults[0].finish
		var totalPhysical float64
		for _, r := range injResults {
			if r.start.Before(earliest) {
				earliest = r.start
			}
			if r.finish.After(latest) {
				latest = r.finish
			}
			totalPhysical += r.physicalBytes
		}
		injElapsed := latest.Sub(earliest)
		var gbps float64
		if injElapsed > 0 {
			gbps = totalPhysical / injElapsed.Seconds() / 1e9
		}

		// ── Probe latency ─────────────────────────────────────────────────
		var nsPerHop float64
		if probeHops > 0 {
			nsPerHop = float64(probeElapsed.Nanoseconds()) / float64(probeHops)
		}

		writeCSVRow(t, "latency-loaded", n, "throughput", gbps, "GB/s")
		writeCSVRow(t, "latency-loaded", n, "latency", nsPerHop, "ns")
	}
}
