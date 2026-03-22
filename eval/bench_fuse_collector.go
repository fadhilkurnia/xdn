// bench_fuse_collector.go - Statediff collector for FUSE filesystem benchmarking.
//
// Connects to a fuselog Unix socket, sends 'g' at configurable intervals,
// and measures capture latency, payload size, and action count.
//
// Supports both C++ and Rust fuselog socket protocols.
//
// Usage:
//   go run bench_fuse_collector.go --socket <path> --protocol cpp|rust \
//       --interval-ms <N> --duration <secs>

package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"time"
)

type CaptureResult struct {
	Latency    time.Duration
	PayloadSize int64
	NumActions  int64
}

func main() {
	socketPath := flag.String("socket", "/tmp/fuselog.sock", "Path to fuselog Unix socket")
	protocol := flag.String("protocol", "cpp", "Protocol: cpp or rust")
	intervalMs := flag.Int("interval-ms", 1000, "Interval between captures in milliseconds")
	duration := flag.Int("duration", 10, "Duration in seconds")
	flag.Parse()

	fmt.Printf("bench_fuse_collector: socket=%s protocol=%s interval=%dms duration=%ds\n",
		*socketPath, *protocol, *intervalMs, *duration)

	// Connect to the Unix socket
	conn, err := net.Dial("unix", *socketPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to socket: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	deadline := time.Now().Add(time.Duration(*duration) * time.Second)
	interval := time.Duration(*intervalMs) * time.Millisecond

	var results []CaptureResult

	for time.Now().Before(deadline) {
		t0 := time.Now()

		// Send 'g' command
		_, err := conn.Write([]byte("g"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error sending 'g': %v\n", err)
			break
		}

		// Read the response based on protocol
		var payloadSize int64
		var numActions int64

		switch *protocol {
		case "cpp":
			payloadSize, numActions, err = readCppResponse(conn)
		case "rust":
			payloadSize, numActions, err = readRustResponse(conn)
		default:
			fmt.Fprintf(os.Stderr, "Unknown protocol: %s\n", *protocol)
			os.Exit(1)
		}

		latency := time.Since(t0)

		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading response: %v\n", err)
			break
		}

		results = append(results, CaptureResult{
			Latency:     latency,
			PayloadSize: payloadSize,
			NumActions:  numActions,
		})

		// Wait for the next interval
		elapsed := time.Since(t0)
		if elapsed < interval {
			time.Sleep(interval - elapsed)
		}
	}

	// Print results
	printCaptureStats(results)
}

// readCppResponse reads the C++ fuselog binary protocol response.
// Format: [8-byte LE size][payload of that size]
// Within payload: [8:num_file][file mappings...][8:num_statediff][statediffs...]
func readCppResponse(conn net.Conn) (payloadSize int64, numActions int64, err error) {
	// Read 8-byte LE size header
	var sizeHeader [8]byte
	_, err = io.ReadFull(conn, sizeHeader[:])
	if err != nil {
		return 0, 0, fmt.Errorf("reading size header: %w", err)
	}
	totalSize := int64(binary.LittleEndian.Uint64(sizeHeader[:]))
	payloadSize = totalSize + 8 // include the size header itself

	// Read the rest of the payload
	remaining := totalSize
	buf := make([]byte, 65536)
	var totalRead int64

	// Read num_file (first 8 bytes of payload)
	var numFileBuf [8]byte
	n, err := io.ReadFull(conn, numFileBuf[:])
	if err != nil {
		return payloadSize, 0, fmt.Errorf("reading num_file: %w", err)
	}
	totalRead += int64(n)
	numFile := binary.LittleEndian.Uint64(numFileBuf[:])

	// Skip the file mappings: each is [8:fid][8:path_len][path_len:path]
	for i := uint64(0); i < numFile; i++ {
		// Read fid + path_len (16 bytes)
		var fidPathLen [16]byte
		n, err = io.ReadFull(conn, fidPathLen[:])
		if err != nil {
			return payloadSize, 0, fmt.Errorf("reading fid+path_len: %w", err)
		}
		totalRead += int64(n)
		pathLen := binary.LittleEndian.Uint64(fidPathLen[8:])

		// Read the path
		pathBuf := make([]byte, pathLen)
		n, err = io.ReadFull(conn, pathBuf)
		if err != nil {
			return payloadSize, 0, fmt.Errorf("reading path: %w", err)
		}
		totalRead += int64(n)
	}

	// Read num_statediff
	var numSdBuf [8]byte
	n, err = io.ReadFull(conn, numSdBuf[:])
	if err != nil {
		return payloadSize, 0, fmt.Errorf("reading num_statediff: %w", err)
	}
	totalRead += int64(n)
	numActions = int64(binary.LittleEndian.Uint64(numSdBuf[:]))

	// Drain remaining bytes
	remaining = totalSize - totalRead
	for remaining > 0 {
		toRead := remaining
		if toRead > int64(len(buf)) {
			toRead = int64(len(buf))
		}
		n, err = io.ReadFull(conn, buf[:toRead])
		if err != nil {
			return payloadSize, numActions, fmt.Errorf("draining payload: %w", err)
		}
		remaining -= int64(n)
	}

	return payloadSize, numActions, nil
}

// readRustResponse reads the Rust fuselog response.
// Format: [8-byte LE size][payload]
// Payload starts with compression mode byte: 'n' (raw), 'z' (zstd), 'd' (dict)
func readRustResponse(conn net.Conn) (payloadSize int64, numActions int64, err error) {
	// Read 8-byte LE size header
	var sizeHeader [8]byte
	_, err = io.ReadFull(conn, sizeHeader[:])
	if err != nil {
		return 0, 0, fmt.Errorf("reading size header: %w", err)
	}
	totalSize := int64(binary.LittleEndian.Uint64(sizeHeader[:]))
	payloadSize = totalSize + 8

	// Read and discard the entire payload (we don't decode bincode here)
	buf := make([]byte, 65536)
	remaining := totalSize
	for remaining > 0 {
		toRead := remaining
		if toRead > int64(len(buf)) {
			toRead = int64(len(buf))
		}
		n, err := io.ReadFull(conn, buf[:toRead])
		if err != nil {
			return payloadSize, 0, fmt.Errorf("draining payload: %w", err)
		}
		remaining -= int64(n)
	}

	// We can't easily decode bincode without a proper decoder,
	// so we return -1 for numActions in Rust mode
	numActions = -1
	return payloadSize, numActions, nil
}

func printCaptureStats(results []CaptureResult) {
	if len(results) == 0 {
		fmt.Println("No captures collected.")
		return
	}

	var totalPayload int64
	var totalActions int64
	latencies := make([]time.Duration, len(results))

	for i, r := range results {
		latencies[i] = r.Latency
		totalPayload += r.PayloadSize
		totalActions += r.NumActions
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	n := len(latencies)

	fmt.Printf("\n=== Capture Statistics (%d captures) ===\n", n)
	fmt.Printf("  Total payload:   %d bytes (%.2f MB)\n", totalPayload, float64(totalPayload)/(1024*1024))
	fmt.Printf("  Avg payload:     %d bytes\n", totalPayload/int64(n))
	fmt.Printf("  Total actions:   %d\n", totalActions)
	fmt.Printf("  Avg actions:     %.1f\n", float64(totalActions)/float64(n))
	fmt.Printf("  Capture lat p50: %v\n", latencies[n*50/100])
	fmt.Printf("  Capture lat p95: %v\n", latencies[n*95/100])
	fmt.Printf("  Capture lat p99: %v\n", latencies[n*99/100])
	fmt.Printf("  Capture lat max: %v\n", latencies[n-1])

	// Output CSV-like per-capture data
	fmt.Println("\ncapture_idx,latency_us,payload_bytes,num_actions")
	for i, r := range results {
		fmt.Printf("%d,%d,%d,%d\n", i, r.Latency.Microseconds(), r.PayloadSize, r.NumActions)
	}
}
