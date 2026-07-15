Here is the updated README with the `payload_throughput` metric included:

---

# Go Memory Benchmark (gomembench)

A methodology-correct memory bandwidth and latency benchmark for Go workloads on multi-socket NUMA systems.

## Goals

Measure how much memory bandwidth and what access latency a Go application can extract from modern server hardware, with particular focus on:

- **Sequential read throughput** — streaming reads with hardware prefetcher assistance
- **Random read throughput** — independent random reads that defeat the prefetcher
- **Idle latency** — serialized pointer-chase latency with no background load
- **Loaded latency** — pointer-chase latency while the memory system is under full bandwidth pressure
- **NUMA behavior** — how Go's NUMA-unaware runtime affects performance across sockets
- **Core scaling** — how throughput changes as goroutine count increases

## Building

```bash
mkdir gomembench
cd gomembench
go mod init gomembench
# copy all .go files into this directory
go build -o bench .
```

## Source Files

| File | Purpose |
|---|---|
| `main.go` | Flag parsing, topology printing, benchmark dispatch |
| `stream.go` | Sequential and random throughput benchmarks |
| `latency.go` | Idle and loaded latency benchmarks |
| `util.go` | Barrier, scan functions, pointer-chase setup, CSV helpers |
| `sysinfo.go` | Hardware topology discovery from sysfs |

## Command-Line Flags

| Flag | Default | Description |
|---|---|---|
| `-bench` | `stream-sequential` | Benchmark: `stream-sequential`, `stream-random`, `latency-idle`, `latency-loaded` |
| `-total-gb` | `24` | Total working set in GB (divided equally across goroutines) |
| `-n` | GOMAXPROCS | Number of goroutines |
| `-trials` | `5` | Number of independent measurement trials |
| `-passes` | `0` (auto) | Passes per goroutine per trial; 0 = auto-compute for ~10s minimum |
| `-width` | `1` | Bytes read per cache line: 1 or 8 |
| `-probe-buf-gb` | `0` (auto) | Probe buffer size in GB for latency benchmarks; 0 = auto from L3 |
| `-v` | `false` | Print per-goroutine timing details to stderr |

## Output Format

CSV to stdout with header:

```
trial#,bench,goroutines,metric,value,unit
```

### Metrics reported per trial

| Benchmark | Metrics |
|---|---|
| `stream-sequential` | `throughput` (GB/s), `payload_throughput` (GB/s), `accesses_per_sec` (M_acc/s) |
| `stream-random` | `throughput` (GB/s), `payload_throughput` (GB/s), `accesses_per_sec` (M_acc/s) |
| `latency-idle` | `latency` (ns) |
| `latency-loaded` | `throughput` (GB/s), `latency` (ns) |

System topology and run parameters are printed to stderr so they do not contaminate the CSV.

## Throughput Accounting

Every memory access fetches a full 64-byte cache line from DRAM regardless of how many bytes the application reads from that line. Three throughput metrics capture different views of the same physical activity:

### `throughput` (GB/s) — physical cache-line bytes

What the memory system actually moved:

```
throughput = goroutines × cache_lines_per_goroutine × 64 × passes / elapsed_seconds
```

This is the hardware-level view. It matches what tools like Intel MLC report.

### `payload_throughput` (GB/s) — application-consumed bytes

What the application actually read from each cache line:

```
payload_throughput = goroutines × cache_lines_per_goroutine × width × passes / elapsed_seconds
```

Where `width` is 1 or 8 bytes depending on the `-width` flag. This is what the original benchmark reported as "throughput."

The ratio between `throughput` and `payload_throughput` shows how much bandwidth the application "wastes" by reading only a fraction of each fetched cache line:

| `-width` | Payload per line | `payload / throughput` ratio |
|---|---|---|
| 1 | 1 byte | 1.6% |
| 8 | 8 bytes | 12.5% |

### `accesses_per_sec` (M_acc/s) — cache-line visits

How many cache lines were touched per second:

```
accesses_per_sec = goroutines × cache_lines_per_goroutine × passes / elapsed_seconds
```

All three metrics describe the same physical traffic from different perspectives:

```
throughput = accesses_per_sec × 64
payload_throughput = accesses_per_sec × width
```

## Latency Methodology

Latency benchmarks use an in-place pointer-chase structure: each 64-byte cache line stores the index of the next cache line to visit, forming a random-permutation cycle through the entire buffer. Each hop depends on the result of the previous load, so the CPU cannot pipeline or prefetch ahead. Measured time per hop is genuine serialized DRAM access latency.

The probe buffer defaults to `max(1 GB, 2 × total_L3_cache)` to ensure every hop misses all cache levels.

## Example Runs

### Sequential throughput, single socket

```bash
numactl --cpunodebind=0 --membind=0 \
  ./bench -bench stream-sequential -total-gb 24 -n 96 -trials 5
```

### Sequential throughput, both sockets (Go runtime decides placement)

```bash
./bench -bench stream-sequential -total-gb 24 -n 192 -trials 5
```

### Random throughput, single socket

```bash
numactl --cpunodebind=0 --membind=0 \
  ./bench -bench stream-random -total-gb 24 -n 96 -trials 5
```

### Idle latency, local memory

```bash
numactl --cpunodebind=0 --membind=0 \
  ./bench -bench latency-idle -trials 5
```

### Idle latency, remote memory (NUMA penalty)

```bash
numactl --cpunodebind=0 --membind=1 \
  ./bench -bench latency-idle -trials 5
```

### Loaded latency, local memory

```bash
numactl --cpunodebind=0 --membind=0 \
  ./bench -bench latency-loaded -n 96 -trials 5
```

### Loaded latency, remote memory

```bash
numactl --cpunodebind=0 --membind=1 \
  ./bench -bench latency-loaded -n 96 -trials 5
```

### Core scaling sweep

```bash
for n in 1 2 4 8 16 32 48 64 96; do
  numactl --cpunodebind=0 --membind=0 \
    ./bench -bench stream-sequential -total-gb 24 -n $n -trials 3
done
```

### Two-socket comparison (two separate processes)

```bash
numactl --cpunodebind=0 --membind=0 \
  ./bench -bench stream-sequential -total-gb 24 -n 96 -trials 5 &
numactl --cpunodebind=1 --membind=1 \
  ./bench -bench stream-sequential -total-gb 24 -n 96 -trials 5 &
wait
```

### Per-goroutine timing details

```bash
numactl --cpunodebind=0 --membind=0 \
  ./bench -bench stream-sequential -total-gb 24 -n 96 -trials 1 -passes 100 -v \
  2>verbose.csv
```

The verbose output (to stderr) contains one row per goroutine per trial:

```
trial,routine_id,routine_start_ms,measure_start_ms,measure_end_ms,routine_end_ms,scan_duration_ms,routine_throughput_gbps
```

### Quick test with short duration

```bash
./bench -bench stream-sequential -total-gb 1 -n 4 -trials 1 -passes 1
```

## Typical Results (AMD EPYC 9654, 2-socket, 96 cores/socket, SMT off)

### Sequential Throughput

| Configuration | Throughput |
|---|---|
| Single socket, 96 goroutines, local memory | ~366 GB/s |
| Single process, both sockets, 192 goroutines | ~372 GB/s |
| Two processes, one per socket | ~711 GB/s combined |
| MLC peak injection bandwidth | ~740 GB/s |

### Random Throughput

| Configuration | Throughput |
|---|---|
| Single socket, 96 goroutines, local memory | ~303 GB/s |
| Random / Sequential ratio (same socket) | ~83% |

### Idle Latency

| Configuration | Latency |
|---|---|
| Local (CPU node 0, memory node 0) | ~138 ns |
| Remote (CPU node 0, memory node 1) | ~238 ns |
| NUMA penalty ratio | 1.73x |
| MLC NUMA penalty ratio | 1.74x |

### Loaded Latency

| Configuration | Injector BW | Probe Latency |
|---|---|---|
| Local, 96 injectors | ~366 GB/s | ~1450 ns |
| Remote, 96 injectors | ~116 GB/s | ~1970 ns |

## Key Findings

1. **Single-socket bandwidth matches MLC** — 366 GB/s vs MLC's 370 GB/s (1.3% gap)
2. **Single Go process across both sockets loses ~50% of available bandwidth** — 372 GB/s vs 711 GB/s from two pinned processes, demonstrating Go runtime NUMA unawareness
3. **NUMA latency penalty matches MLC** — 1.73x vs MLC's 1.74x ratio
4. **Random access is 83% of sequential on Genoa** — the out-of-order engine provides substantial memory-level parallelism even without prefetcher help
5. **Per-goroutine throughput variation within one socket is only 2.5%** — the socket is internally very uniform
6. **Per-goroutine variation across two sockets is 34%** — bimodal distribution from mixed local/remote memory access
