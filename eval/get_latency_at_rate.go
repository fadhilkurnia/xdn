// get_latency_at_rate sends HTTP requests to a URL at a target Poisson rate and
// reports latency percentiles plus success rate over a fixed duration.
// Usage (POST): go run get_latency_at_rate.go [-H "Key: Value"] [-X POST] <url> <json_payload> <duration_seconds> <target_rate>
// Usage (GET):  go run get_latency_at_rate.go [-H "Key: Value"] -X GET <url> <duration_seconds> <target_rate>
// Examples:
//   go run eval/get_latency_at_rate.go -X GET http://localhost:8000/key 30 200
//   go run eval/get_latency_at_rate.go -X GET http://localhost:8000/key "" 30 200
//   go run eval/get_latency_at_rate.go -X POST http://localhost:8000/key '{"k":"v"}' 30 200

package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type result struct {
	latency time.Duration
	success bool
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[len(sorted)-1]
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

type headerList []string

func (headers *headerList) String() string {
	return strings.Join(*headers, ",")
}

func (headers *headerList) Set(value string) error {
	*headers = append(*headers, value)
	return nil
}

func sanityCheck(client *http.Client, method string, url string, payload string, headers []struct {
	key   string
	value string
}) {
	var body io.Reader
	if method == http.MethodPost {
		body = bytes.NewBufferString(payload)
	}
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sanity check failed to create request: %v\n", err)
		os.Exit(1)
	}
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	for _, header := range headers {
		req.Header.Set(header.key, header.value)
	}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sanity check failed to reach endpoint: %v\n", err)
		os.Exit(1)
	}
	respBytes, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	if readErr != nil {
		fmt.Fprintf(os.Stderr, "sanity check failed to read response: %v\n", readErr)
		os.Exit(1)
	}
	responseBody := strings.TrimSpace(string(respBytes))
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		fmt.Fprintf(os.Stderr, "sanity check failed with status %s\nresponse_body: %s\n", resp.Status, responseBody)
		os.Exit(1)
	}
	fmt.Printf("example_response_status: %s\n", resp.Status)
	fmt.Printf("example_response_body: %s\n", responseBody)
}

func main() {
	var headerArgs headerList
	flag.Var(&headerArgs, "H", "Optional HTTP header in 'Key: Value' format (repeatable)")
	methodArg := flag.String("X", "POST", "HTTP method (POST or GET)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [options] <url> <json_payload> <duration_seconds> <target_rate>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "       %s [options] -X GET <url> <duration_seconds> <target_rate>\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	args := flag.Args()
	method := strings.ToUpper(strings.TrimSpace(*methodArg))
	if method != http.MethodPost && method != http.MethodGet {
		fmt.Fprintf(os.Stderr, "unsupported method: %s\n", method)
		os.Exit(1)
	}

	var (
		url     string
		payload string
	)
	if method == http.MethodGet {
		if len(args) != 3 && len(args) != 4 {
			flag.Usage()
			os.Exit(1)
		}
		url = args[0]
		if len(args) == 4 {
			payload = args[1]
		}
		if payload != "" {
			fmt.Fprintln(os.Stderr, "GET requests must use an empty payload")
			os.Exit(1)
		}
	} else {
		if len(args) != 4 {
			flag.Usage()
			os.Exit(1)
		}
		url = args[0]
		payload = args[1]
	}

	headers := make([]struct {
		key   string
		value string
	}, 0, len(headerArgs))
	for _, headerArg := range headerArgs {
		parts := strings.SplitN(headerArg, ":", 2)
		if len(parts) != 2 {
			fmt.Fprintf(os.Stderr, "invalid header: %s\n", headerArg)
			os.Exit(1)
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if key == "" {
			fmt.Fprintf(os.Stderr, "invalid header key: %s\n", headerArg)
			os.Exit(1)
		}
		headers = append(headers, struct {
			key   string
			value string
		}{key: key, value: value})
	}

	durationIndex := 2
	if method == http.MethodGet && len(args) == 3 {
		durationIndex = 1
	}
	durationSeconds, err := strconv.ParseFloat(args[durationIndex], 64)
	if err != nil || durationSeconds <= 0 {
		fmt.Fprintln(os.Stderr, "invalid duration_seconds")
		os.Exit(1)
	}
	targetRateIndex := durationIndex + 1
	targetRate, err := strconv.ParseFloat(args[targetRateIndex], 64)
	if err != nil || targetRate <= 0 {
		fmt.Fprintln(os.Stderr, "invalid target_rate")
		os.Exit(1)
	}

	transport := &http.Transport{
		MaxIdleConns:        1024,
		MaxIdleConnsPerHost: 1024,
	}
	client := &http.Client{Transport: transport}

	sanityCheck(client, method, url, payload, headers)

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	start := time.Now()
	end := start.Add(time.Duration(durationSeconds * float64(time.Second)))
	next := start

	results := make(chan result, 1024)
	var wg sync.WaitGroup
	totalSent := 0

	for !next.After(end) {
		now := time.Now()
		if now.Before(next) {
			time.Sleep(next.Sub(now))
		}

		totalSent++
		wg.Add(1)
		go func() {
			defer wg.Done()
			var body io.Reader
			if method == http.MethodPost {
				body = bytes.NewBufferString(payload)
			}
			req, reqErr := http.NewRequest(method, url, body)
			if reqErr != nil {
				results <- result{success: false}
				return
			}
			if method == http.MethodPost {
				req.Header.Set("Content-Type", "application/json")
			}
			for _, header := range headers {
				req.Header.Set(header.key, header.value)
			}
			t0 := time.Now()
			resp, respErr := client.Do(req)
			latency := time.Since(t0)
			if respErr != nil {
				results <- result{latency: latency, success: false}
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			success := resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices
			results <- result{latency: latency, success: success}
		}()

		interval := rng.ExpFloat64() / targetRate
		if interval < 0 {
			interval = 0
		}
		next = next.Add(time.Duration(interval * float64(time.Second)))
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	latencies := make([]float64, 0, totalSent)
	successCount := 0
	for res := range results {
		if res.success {
			successCount++
			latencies = append(latencies, float64(res.latency)/float64(time.Millisecond))
		}
	}

	elapsed := time.Since(start).Seconds()
	if elapsed <= 0 {
		elapsed = durationSeconds
	}
	actualRate := float64(totalSent) / elapsed
	throughput := float64(successCount) / elapsed

	minLatency := 0.0
	maxLatency := 0.0
	avgLatency := 0.0
	medianLatency := 0.0
	p90Latency := 0.0
	p95Latency := 0.0
	p99Latency := 0.0
	if len(latencies) > 0 {
		sort.Float64s(latencies)
		minLatency = latencies[0]
		maxLatency = latencies[len(latencies)-1]
		sum := 0.0
		for _, value := range latencies {
			sum += value
		}
		avgLatency = sum / float64(len(latencies))
		medianLatency = percentile(latencies, 0.5)
		p90Latency = percentile(latencies, 0.9)
		p95Latency = percentile(latencies, 0.95)
		p99Latency = percentile(latencies, 0.99)
	}

	fmt.Printf("total_requests_sent: %d\n", totalSent)
	fmt.Printf("total_successful_responses: %d\n", successCount)
	fmt.Printf("actual_achieved_rate_rps: %.2f\n", actualRate)
	fmt.Printf("actual_throughput_rps: %.2f\n", throughput)
	fmt.Printf("min_latency_ms: %.2f\n", minLatency)
	fmt.Printf("max_latency_ms: %.2f\n", maxLatency)
	fmt.Printf("average_latency_ms: %.2f\n", avgLatency)
	fmt.Printf("median_latency_ms: %.2f\n", medianLatency)
	fmt.Printf("p90_latency_ms: %.2f\n", p90Latency)
	fmt.Printf("p95_latency_ms: %.2f\n", p95Latency)
	fmt.Printf("p99_latency_ms: %.2f\n", p99Latency)
}
