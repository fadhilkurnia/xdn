// bench_fuse_writer.go - Workload generator for FUSE filesystem benchmarking.
//
// Spawns N goroutines writing to a FUSE-mounted directory to stress-test
// fuselog's statediff capture under concurrent multi-threaded writes.
//
// Workload profiles:
//   seq-large    : 1 file, sequential 16KB writes, 100MB total (InnoDB tablespace sim)
//   rand-small   : 10 files pre-allocated 10MB each, random 512B writes (WAL sim)
//   many-files   : Create 1000 files of 1-10KB in nested dirs (WordPress uploads)
//   create-unlink: Create -> write 4KB -> unlink, repeat 10K times (temp files, tests pruning)
//   mixed        : 5 data files + 1 WAL + 2 temp threads concurrently (MySQL composite)
//
// Usage:
//   go run bench_fuse_writer.go --dir <mount> --profile <name> --threads <N> --duration <secs>

package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"math/big"
	mrand "math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Stats struct {
	TotalOps   int64
	TotalBytes int64
	Latencies  []time.Duration
}

func main() {
	dir := flag.String("dir", "", "Target FUSE mount directory")
	profile := flag.String("profile", "seq-large", "Workload profile: seq-large, rand-small, many-files, create-unlink, mixed")
	threads := flag.Int("threads", 1, "Number of concurrent writer threads")
	duration := flag.Int("duration", 10, "Duration in seconds")
	flag.Parse()

	if *dir == "" {
		fmt.Fprintln(os.Stderr, "Error: --dir is required")
		os.Exit(1)
	}

	if err := os.MkdirAll(*dir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating dir: %v\n", err)
		os.Exit(1)
	}

	deadline := time.Now().Add(time.Duration(*duration) * time.Second)

	fmt.Printf("bench_fuse_writer: profile=%s threads=%d duration=%ds dir=%s\n",
		*profile, *threads, *duration, *dir)

	switch *profile {
	case "seq-large":
		runSeqLarge(*dir, *threads, deadline)
	case "rand-small":
		runRandSmall(*dir, *threads, deadline)
	case "many-files":
		runManyFiles(*dir, *threads, deadline)
	case "create-unlink":
		runCreateUnlink(*dir, *threads, deadline)
	case "mixed":
		runMixed(*dir, deadline)
	default:
		fmt.Fprintf(os.Stderr, "Unknown profile: %s\n", *profile)
		os.Exit(1)
	}
}

func printStats(label string, stats *Stats, duration time.Duration) {
	secs := duration.Seconds()
	mbps := float64(stats.TotalBytes) / (1024 * 1024) / secs
	opsPerSec := float64(stats.TotalOps) / secs

	sort.Slice(stats.Latencies, func(i, j int) bool {
		return stats.Latencies[i] < stats.Latencies[j]
	})

	n := len(stats.Latencies)
	var p50, p95, p99 time.Duration
	if n > 0 {
		p50 = stats.Latencies[n*50/100]
		p95 = stats.Latencies[n*95/100]
		p99 = stats.Latencies[n*99/100]
	}

	fmt.Printf("\n=== %s ===\n", label)
	fmt.Printf("  Total ops:       %d\n", stats.TotalOps)
	fmt.Printf("  Total bytes:     %d (%.2f MB)\n", stats.TotalBytes, float64(stats.TotalBytes)/(1024*1024))
	fmt.Printf("  Throughput:      %.2f MB/s\n", mbps)
	fmt.Printf("  Ops/sec:         %.2f\n", opsPerSec)
	fmt.Printf("  Latency p50:     %v\n", p50)
	fmt.Printf("  Latency p95:     %v\n", p95)
	fmt.Printf("  Latency p99:     %v\n", p99)
}

// randBytes returns n random bytes
func randBytes(n int) []byte {
	buf := make([]byte, n)
	rand.Read(buf)
	return buf
}

func randInt(max int) int {
	n, _ := rand.Int(rand.Reader, big.NewInt(int64(max)))
	return int(n.Int64())
}

// seq-large: 1 shared file, sequential 16KB writes
func runSeqLarge(dir string, threads int, deadline time.Time) {
	fname := filepath.Join(dir, "seq_large.dat")
	f, err := os.Create(fname)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating file: %v\n", err)
		return
	}
	defer f.Close()

	const writeSize = 16 * 1024 // 16KB
	buf := randBytes(writeSize)

	var totalOps int64
	var totalBytes int64
	var mu sync.Mutex
	var allLatencies []time.Duration
	var wg sync.WaitGroup

	start := time.Now()
	for t := 0; t < threads; t++ {
		wg.Add(1)
		go func(tid int) {
			defer wg.Done()
			var localLat []time.Duration
			var localOps, localBytes int64
			offset := int64(tid) * writeSize

			for time.Now().Before(deadline) {
				t0 := time.Now()
				_, err := f.WriteAt(buf, offset)
				lat := time.Since(t0)
				if err != nil {
					continue
				}
				localLat = append(localLat, lat)
				localOps++
				localBytes += writeSize
				offset += int64(threads) * writeSize
			}

			atomic.AddInt64(&totalOps, localOps)
			atomic.AddInt64(&totalBytes, localBytes)
			mu.Lock()
			allLatencies = append(allLatencies, localLat...)
			mu.Unlock()
		}(t)
	}
	wg.Wait()
	elapsed := time.Since(start)

	stats := &Stats{
		TotalOps:   atomic.LoadInt64(&totalOps),
		TotalBytes: atomic.LoadInt64(&totalBytes),
		Latencies:  allLatencies,
	}
	printStats(fmt.Sprintf("seq-large (threads=%d)", threads), stats, elapsed)
	os.Remove(fname)
}

// rand-small: 10 pre-allocated files, random 512B writes
func runRandSmall(dir string, threads int, deadline time.Time) {
	const numFiles = 10
	const fileSize = 10 * 1024 * 1024 // 10MB
	const writeSize = 512

	files := make([]*os.File, numFiles)
	for i := 0; i < numFiles; i++ {
		fname := filepath.Join(dir, fmt.Sprintf("rand_small_%d.dat", i))
		f, err := os.Create(fname)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating file %d: %v\n", i, err)
			return
		}
		f.Truncate(int64(fileSize))
		files[i] = f
	}
	defer func() {
		for _, f := range files {
			name := f.Name()
			f.Close()
			os.Remove(name)
		}
	}()

	buf := randBytes(writeSize)
	var totalOps int64
	var totalBytes int64
	var mu sync.Mutex
	var allLatencies []time.Duration
	var wg sync.WaitGroup

	start := time.Now()
	for t := 0; t < threads; t++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := mrand.New(mrand.NewSource(time.Now().UnixNano()))
			var localLat []time.Duration
			var localOps, localBytes int64

			for time.Now().Before(deadline) {
				fi := rng.Intn(numFiles)
				off := int64(rng.Intn(fileSize - writeSize))
				t0 := time.Now()
				_, err := files[fi].WriteAt(buf, off)
				lat := time.Since(t0)
				if err != nil {
					continue
				}
				localLat = append(localLat, lat)
				localOps++
				localBytes += writeSize
			}

			atomic.AddInt64(&totalOps, localOps)
			atomic.AddInt64(&totalBytes, localBytes)
			mu.Lock()
			allLatencies = append(allLatencies, localLat...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	stats := &Stats{
		TotalOps:   atomic.LoadInt64(&totalOps),
		TotalBytes: atomic.LoadInt64(&totalBytes),
		Latencies:  allLatencies,
	}
	printStats(fmt.Sprintf("rand-small (threads=%d)", threads), stats, elapsed)
}

// many-files: Create files of 1-10KB in nested dirs
func runManyFiles(dir string, threads int, deadline time.Time) {
	subdir := filepath.Join(dir, "many_files_bench")
	os.MkdirAll(subdir, 0755)
	defer os.RemoveAll(subdir)

	var totalOps int64
	var totalBytes int64
	var mu sync.Mutex
	var allLatencies []time.Duration
	var wg sync.WaitGroup
	var fileCounter int64

	start := time.Now()
	for t := 0; t < threads; t++ {
		wg.Add(1)
		go func(tid int) {
			defer wg.Done()
			rng := mrand.New(mrand.NewSource(time.Now().UnixNano()))
			var localLat []time.Duration
			var localOps, localBytes int64

			nestDir := filepath.Join(subdir, fmt.Sprintf("t%d", tid))
			os.MkdirAll(nestDir, 0755)

			for time.Now().Before(deadline) {
				idx := atomic.AddInt64(&fileCounter, 1)
				size := 1024 + rng.Intn(9*1024) // 1KB to 10KB
				buf := randBytes(size)
				fname := filepath.Join(nestDir, fmt.Sprintf("file_%d.dat", idx))

				t0 := time.Now()
				err := os.WriteFile(fname, buf, 0644)
				lat := time.Since(t0)
				if err != nil {
					continue
				}
				localLat = append(localLat, lat)
				localOps++
				localBytes += int64(size)
			}

			atomic.AddInt64(&totalOps, localOps)
			atomic.AddInt64(&totalBytes, localBytes)
			mu.Lock()
			allLatencies = append(allLatencies, localLat...)
			mu.Unlock()
		}(t)
	}
	wg.Wait()
	elapsed := time.Since(start)

	stats := &Stats{
		TotalOps:   atomic.LoadInt64(&totalOps),
		TotalBytes: atomic.LoadInt64(&totalBytes),
		Latencies:  allLatencies,
	}
	printStats(fmt.Sprintf("many-files (threads=%d)", threads), stats, elapsed)
}

// create-unlink: Create -> write 4KB -> unlink, repeat
func runCreateUnlink(dir string, threads int, deadline time.Time) {
	subdir := filepath.Join(dir, "create_unlink_bench")
	os.MkdirAll(subdir, 0755)
	defer os.RemoveAll(subdir)

	const writeSize = 4096
	buf := randBytes(writeSize)

	var totalOps int64
	var totalBytes int64
	var mu sync.Mutex
	var allLatencies []time.Duration
	var wg sync.WaitGroup
	var fileCounter int64

	start := time.Now()
	for t := 0; t < threads; t++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var localLat []time.Duration
			var localOps, localBytes int64

			for time.Now().Before(deadline) {
				idx := atomic.AddInt64(&fileCounter, 1)
				fname := filepath.Join(subdir, fmt.Sprintf("tmp_%d.dat", idx))

				t0 := time.Now()
				f, err := os.Create(fname)
				if err != nil {
					continue
				}
				_, err = f.Write(buf)
				f.Close()
				if err != nil {
					continue
				}
				os.Remove(fname)
				lat := time.Since(t0)

				localLat = append(localLat, lat)
				localOps++
				localBytes += writeSize
			}

			atomic.AddInt64(&totalOps, localOps)
			atomic.AddInt64(&totalBytes, localBytes)
			mu.Lock()
			allLatencies = append(allLatencies, localLat...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	stats := &Stats{
		TotalOps:   atomic.LoadInt64(&totalOps),
		TotalBytes: atomic.LoadInt64(&totalBytes),
		Latencies:  allLatencies,
	}
	printStats(fmt.Sprintf("create-unlink (threads=%d)", threads), stats, elapsed)
}

// mixed: 5 data files + 1 WAL + 1 temp thread concurrently (7 threads)
func runMixed(dir string, deadline time.Time) {
	subdir := filepath.Join(dir, "mixed_bench")
	os.MkdirAll(subdir, 0755)
	defer os.RemoveAll(subdir)

	var totalOps int64
	var totalBytes int64
	var mu sync.Mutex
	var allLatencies []time.Duration
	var wg sync.WaitGroup

	start := time.Now()

	// 5 data writer threads: 16KB sequential writes to separate files
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			fname := filepath.Join(subdir, fmt.Sprintf("data_%d.dat", idx))
			f, err := os.Create(fname)
			if err != nil {
				return
			}
			defer f.Close()

			const writeSize = 16 * 1024
			buf := randBytes(writeSize)
			var localLat []time.Duration
			var localOps, localBytes int64
			offset := int64(0)

			for time.Now().Before(deadline) {
				t0 := time.Now()
				_, err := f.WriteAt(buf, offset)
				lat := time.Since(t0)
				if err != nil {
					continue
				}
				localLat = append(localLat, lat)
				localOps++
				localBytes += writeSize
				offset += writeSize
			}

			atomic.AddInt64(&totalOps, localOps)
			atomic.AddInt64(&totalBytes, localBytes)
			mu.Lock()
			allLatencies = append(allLatencies, localLat...)
			mu.Unlock()
		}(i)
	}

	// 1 WAL writer thread: random 512B writes
	wg.Add(1)
	go func() {
		defer wg.Done()
		fname := filepath.Join(subdir, "wal.log")
		f, err := os.Create(fname)
		if err != nil {
			return
		}
		defer f.Close()
		f.Truncate(10 * 1024 * 1024) // 10MB

		rng := mrand.New(mrand.NewSource(time.Now().UnixNano()))
		const writeSize = 512
		buf := randBytes(writeSize)
		var localLat []time.Duration
		var localOps, localBytes int64

		for time.Now().Before(deadline) {
			off := int64(rng.Intn(10*1024*1024 - writeSize))
			t0 := time.Now()
			_, err := f.WriteAt(buf, off)
			lat := time.Since(t0)
			if err != nil {
				continue
			}
			localLat = append(localLat, lat)
			localOps++
			localBytes += writeSize
		}

		atomic.AddInt64(&totalOps, localOps)
		atomic.AddInt64(&totalBytes, localBytes)
		mu.Lock()
		allLatencies = append(allLatencies, localLat...)
		mu.Unlock()
	}()

	// 1 temp file thread: create+write+unlink cycle
	wg.Add(1)
	go func() {
		defer wg.Done()
		const writeSize = 4096
		buf := randBytes(writeSize)
		var localLat []time.Duration
		var localOps, localBytes int64
		counter := 0

		for time.Now().Before(deadline) {
			counter++
			fname := filepath.Join(subdir, fmt.Sprintf("tmp_%d.dat", counter))
			t0 := time.Now()
			f, err := os.Create(fname)
			if err != nil {
				continue
			}
			f.Write(buf)
			f.Close()
			os.Remove(fname)
			lat := time.Since(t0)
			localLat = append(localLat, lat)
			localOps++
			localBytes += writeSize
		}

		atomic.AddInt64(&totalOps, localOps)
		atomic.AddInt64(&totalBytes, localBytes)
		mu.Lock()
		allLatencies = append(allLatencies, localLat...)
		mu.Unlock()
	}()

	wg.Wait()
	elapsed := time.Since(start)

	stats := &Stats{
		TotalOps:   atomic.LoadInt64(&totalOps),
		TotalBytes: atomic.LoadInt64(&totalBytes),
		Latencies:  allLatencies,
	}
	printStats("mixed (7 threads)", stats, elapsed)
}
