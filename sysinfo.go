// sysinfo.go
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// SysInfo holds hardware topology discovered at runtime from /sys and /proc.
type SysInfo struct {
	TotalL3Bytes    int64   // sum of all unique L3 cache instances in bytes
	NumNUMANodes    int     // number of NUMA nodes
	CPUsPerNode     []int   // number of CPUs on each node (indexed by node ID)
	MemPerNodeBytes []int64 // physical memory per node in bytes
	TotalCPUs       int     // total number of online CPUs
}

// Discover reads hardware topology from sysfs and procfs.
// Missing files produce zero values with a warning rather than aborting.
func Discover() SysInfo {
	var info SysInfo
	info.TotalL3Bytes = discoverL3()
	info.NumNUMANodes, info.CPUsPerNode, info.MemPerNodeBytes = discoverNUMA()
	info.TotalCPUs = discoverOnlineCPUs()
	return info
}

// Print writes a human-readable summary of the discovered topology to stderr.
func (s SysInfo) Print() {
	fmt.Fprintf(os.Stderr, "=== System Topology ===\n")
	fmt.Fprintf(os.Stderr, "Online CPUs       : %d\n", s.TotalCPUs)
	fmt.Fprintf(os.Stderr, "NUMA nodes        : %d\n", s.NumNUMANodes)
	for i, cpus := range s.CPUsPerNode {
		mem := s.MemPerNodeBytes[i]
		fmt.Fprintf(os.Stderr, "  node%-2d           : %d CPUs, %.1f GB RAM\n",
			i, cpus, float64(mem)/1e9)
	}
	fmt.Fprintf(os.Stderr, "Total L3 cache    : %.1f MB (%d bytes)\n",
		float64(s.TotalL3Bytes)/1e6, s.TotalL3Bytes)
	fmt.Fprintf(os.Stderr, "=======================\n\n")
}

// DefaultProbeBufBytes returns a probe buffer size guaranteed to exceed the
// total L3 cache so every pointer-chase hop is a genuine DRAM miss.
// Minimum is 1 GB; if 2x L3 is larger, that value is used instead.
func (s SysInfo) DefaultProbeBufBytes() int {
	const minSize = 1 << 30 // 1 GB floor
	twice := s.TotalL3Bytes * 2
	if twice > int64(minSize) {
		return int(twice)
	}
	return minSize
}

// ──────────────────────────────────────────────────────────────────────────────
// Internal helpers
// ──────────────────────────────────────────────────────────────────────────────

// discoverL3 returns the total L3 cache size in bytes.
//
// Deduplication strategy: two cache index directories represent the same
// physical cache instance if and only if they have identical shared_cpu_list
// content.  We normalise the cpu-list string and use it as the map key so
// that each unique physical L3 slice is counted exactly once.
func discoverL3() int64 {
	const cacheBase = "/sys/devices/system/cpu"

	cpuDirs, err := filepath.Glob(filepath.Join(cacheBase, "cpu[0-9]*"))
	if err != nil || len(cpuDirs) == 0 {
		warnf("sysinfo: cannot glob %s: %v", cacheBase, err)
		return 0
	}

	// seen maps normalised shared_cpu_list → true so each L3 instance is
	// counted once regardless of how many CPU directories expose it.
	seen := make(map[string]bool)
	var total int64

	for _, cpuDir := range cpuDirs {
		indexDirs, err := filepath.Glob(filepath.Join(cpuDir, "cache", "index*"))
		if err != nil {
			continue
		}
		for _, idxDir := range indexDirs {
			// Only interested in L3.
			level, err := readSysFile(filepath.Join(idxDir, "level"))
			if err != nil || strings.TrimSpace(level) != "3" {
				continue
			}

			// Use shared_cpu_list as the unique identity for this cache
			// instance.  Normalise whitespace so formatting differences
			// between kernels do not create false duplicates.
			sharedCPUs, err := readSysFile(filepath.Join(idxDir, "shared_cpu_list"))
			if err != nil {
				continue
			}
			key := strings.TrimSpace(sharedCPUs)
			if seen[key] {
				continue
			}
			seen[key] = true

			sizeStr, err := readSysFile(filepath.Join(idxDir, "size"))
			if err != nil {
				continue
			}
			bytes, err := parseCacheSize(strings.TrimSpace(sizeStr))
			if err != nil {
				warnf("sysinfo: cannot parse cache size %q: %v", sizeStr, err)
				continue
			}
			total += bytes
		}
	}
	return total
}

// discoverNUMA returns the number of NUMA nodes, CPUs per node, and memory
// per node (in bytes) by reading /sys/devices/system/node/.
func discoverNUMA() (numNodes int, cpusPerNode []int, memPerNode []int64) {
	const nodeBase = "/sys/devices/system/node"

	nodeDirs, err := filepath.Glob(filepath.Join(nodeBase, "node[0-9]*"))
	if err != nil || len(nodeDirs) == 0 {
		warnf("sysinfo: cannot glob %s: %v", nodeBase, err)
		return 0, nil, nil
	}

	numNodes = len(nodeDirs)
	cpusPerNode = make([]int, numNodes)
	memPerNode = make([]int64, numNodes)

	for _, nodeDir := range nodeDirs {
		base := filepath.Base(nodeDir) // e.g. "node0"
		idxStr := strings.TrimPrefix(base, "node")
		idx, err := strconv.Atoi(idxStr)
		if err != nil || idx < 0 || idx >= numNodes {
			warnf("sysinfo: unexpected node dir %q", nodeDir)
			continue
		}

		cpuListStr, err := readSysFile(filepath.Join(nodeDir, "cpulist"))
		if err == nil {
			cpusPerNode[idx] = countCPUsInList(strings.TrimSpace(cpuListStr))
		}

		meminfo, err := readSysFile(filepath.Join(nodeDir, "meminfo"))
		if err == nil {
			memPerNode[idx] = parseNodeMemTotal(meminfo)
		}
	}
	return numNodes, cpusPerNode, memPerNode
}

// discoverOnlineCPUs counts online CPUs from /sys/devices/system/cpu/online.
func discoverOnlineCPUs() int {
	s, err := readSysFile("/sys/devices/system/cpu/online")
	if err != nil {
		warnf("sysinfo: cannot read cpu/online: %v", err)
		return 0
	}
	return countCPUsInList(strings.TrimSpace(s))
}

// ──────────────────────────────────────────────────────────────────────────────
// Parsing helpers
// ──────────────────────────────────────────────────────────────────────────────

// parseCacheSize converts a kernel cache-size string to bytes.
// Recognised suffixes: K, M, G (case-insensitive).  No suffix means bytes.
func parseCacheSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return 0, fmt.Errorf("empty string")
	}
	suffix := s[len(s)-1]
	var multiplier int64 = 1
	numStr := s
	switch suffix {
	case 'K', 'k':
		multiplier = 1024
		numStr = s[:len(s)-1]
	case 'M', 'm':
		multiplier = 1024 * 1024
		numStr = s[:len(s)-1]
	case 'G', 'g':
		multiplier = 1024 * 1024 * 1024
		numStr = s[:len(s)-1]
	}
	val, err := strconv.ParseInt(strings.TrimSpace(numStr), 10, 64)
	if err != nil {
		return 0, err
	}
	return val * multiplier, nil
}

// countCPUsInList counts CPUs described by a Linux cpulist string,
// e.g. "0-95,96-191" or "0,2,4".
func countCPUsInList(s string) int {
	if s == "" {
		return 0
	}
	total := 0
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if idx := strings.Index(part, "-"); idx >= 0 {
			lo, err1 := strconv.Atoi(part[:idx])
			hi, err2 := strconv.Atoi(part[idx+1:])
			if err1 == nil && err2 == nil && hi >= lo {
				total += hi - lo + 1
			}
		} else {
			total++
		}
	}
	return total
}

// parseNodeMemTotal extracts the MemTotal value in bytes from a NUMA node's
// meminfo file.  The relevant line looks like:
//
//	Node 0 MemTotal:        1583618048 kB
func parseNodeMemTotal(meminfo string) int64 {
	for _, line := range strings.Split(meminfo, "\n") {
		if !strings.Contains(line, "MemTotal") {
			continue
		}
		fields := strings.Fields(line)
		// ["Node", "0", "MemTotal:", "<value>", "<unit>"]
		if len(fields) < 4 {
			continue
		}
		val, err := strconv.ParseInt(fields[len(fields)-2], 10, 64)
		if err != nil {
			continue
		}
		switch strings.ToLower(fields[len(fields)-1]) {
		case "kb":
			return val * 1024
		case "mb":
			return val * 1024 * 1024
		case "gb":
			return val * 1024 * 1024 * 1024
		default:
			return val
		}
	}
	return 0
}

// readSysFile reads the entire content of a sysfs/procfs file as a string.
func readSysFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// warnf prints a warning to stderr without aborting.
func warnf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "warning: "+format+"\n", args...)
}
