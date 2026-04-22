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
	"net/http/cookiejar"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type result struct {
	latency    time.Duration
	success    bool
	statusCode int    // 0 means connection error
	errMsg     string // first few chars of error message (for diagnostics)
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

// hasHeader returns true if any -H arg has the given key (case-insensitive).
func hasHeader(headers []struct{ key, value string }, key string) bool {
	for _, h := range headers {
		if strings.EqualFold(h.key, key) {
			return true
		}
	}
	return false
}

func sanityCheck(client *http.Client, method string, url string, payload string, headers []struct {
	key   string
	value string
}) {
	maxRetries := 5
	for attempt := 1; attempt <= maxRetries; attempt++ {
		var body io.Reader
		if method == http.MethodPost || method == http.MethodPut {
			body = bytes.NewBufferString(payload)
		}
		req, err := http.NewRequest(method, url, body)
		if err != nil {
			fmt.Fprintf(os.Stderr, "sanity check failed to create request: %v\n", err)
			os.Exit(1)
		}
		if (method == http.MethodPost || method == http.MethodPut) && !hasHeader(headers, "Content-Type") {
			req.Header.Set("Content-Type", "application/json")
		}
		for _, header := range headers {
			req.Header.Set(header.key, header.value)
		}
		resp, err := client.Do(req)
		if err != nil {
			if attempt < maxRetries {
				fmt.Fprintf(os.Stderr, "sanity check attempt %d/%d failed: %v, retrying...\n", attempt, maxRetries, err)
				time.Sleep(2 * time.Second)
				continue
			}
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
		if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
			fmt.Printf("example_response_status: %s\n", resp.Status)
			fmt.Printf("example_response_body: %s\n", responseBody)
			return
		}
		if attempt < maxRetries {
			fmt.Fprintf(os.Stderr, "sanity check attempt %d/%d: status %s, retrying...\n", attempt, maxRetries, resp.Status)
			time.Sleep(2 * time.Second)
			continue
		}
		fmt.Fprintf(os.Stderr, "sanity check failed with status %s\nresponse_body: %s\n", resp.Status, responseBody)
		os.Exit(1)
	}
}

func raiseFileDescriptorLimit() {
	var rlimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlimit); err != nil {
		fmt.Fprintf(os.Stderr, "warning: failed to get rlimit: %v\n", err)
		return
	}
	if rlimit.Cur < rlimit.Max {
		rlimit.Cur = rlimit.Max
		if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rlimit); err != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to raise fd limit to %d: %v\n", rlimit.Max, err)
		}
	}
}

func main() {
	raiseFileDescriptorLimit()

	var headerArgs headerList
	flag.Var(&headerArgs, "H", "Optional HTTP header in 'Key: Value' format (repeatable)")
	methodArg := flag.String("X", "POST", "HTTP method (POST, PUT, or GET)")
	var payloadsFile string
	flag.StringVar(&payloadsFile, "payloads-file", "",
		"File with one POST body per line; payloads cycle round-robin across requests")
	var urlsFile string
	flag.StringVar(&urlsFile, "urls-file", "",
		"File with one URL per line; URLs cycle round-robin across requests")
	readRatio := flag.Float64("read-ratio", 0.0,
		"Fraction of requests that are GET reads (0.0=all writes, 1.0=all reads). Requires -read-url.")
	readURL := flag.String("read-url", "",
		"URL for GET (read) requests when using -read-ratio. Write URL is the positional arg.")
	enableCookies := flag.Bool("enable-cookies", false,
		"Enable cookie jar so the client sends back Set-Cookie values (e.g., XDN session timestamps).")
	perSecondOutput := flag.String("per-second-output", "",
		"Path to CSV file for per-second throughput (format: second,throughput_rps)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [options] <url> <json_payload> <duration_seconds> <target_rate>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "       %s [options] -X GET <url> <duration_seconds> <target_rate>\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	args := flag.Args()
	method := strings.ToUpper(strings.TrimSpace(*methodArg))
	if method != http.MethodPost && method != http.MethodGet && method != http.MethodPut {
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
	} else { // POST or PUT
		if len(args) != 4 {
			flag.Usage()
			os.Exit(1)
		}
		url = args[0]
		payload = args[1]
	}

	var payloads []string
	if payloadsFile != "" {
		data, err := os.ReadFile(payloadsFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cannot read -payloads-file: %v\n", err)
			os.Exit(1)
		}
		for _, line := range strings.Split(string(data), "\n") {
			if strings.TrimSpace(line) != "" {
				payloads = append(payloads, line)
			}
		}
		if len(payloads) == 0 {
			fmt.Fprintf(os.Stderr, "-payloads-file is empty\n")
			os.Exit(1)
		}
		payload = payloads[0] // use first entry for sanity check
	}

	var urls []string
	if urlsFile != "" {
		data, err := os.ReadFile(urlsFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cannot read -urls-file: %v\n", err)
			os.Exit(1)
		}
		for _, line := range strings.Split(string(data), "\n") {
			if strings.TrimSpace(line) != "" {
				urls = append(urls, strings.TrimSpace(line))
			}
		}
		if len(urls) == 0 {
			fmt.Fprintf(os.Stderr, "-urls-file is empty\n")
			os.Exit(1)
		}
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
		MaxIdleConns:        8192,
		MaxIdleConnsPerHost: 8192,
		MaxConnsPerHost:     8192,
	}
	// When cookies are disabled, all goroutines share a single client (no jar).
	// When cookies are enabled, each goroutine gets its own client with a
	// private cookie jar so that per-session timestamps (XDN-CC-TS-R/W) are
	// tracked independently per concurrent "session", avoiding cross-goroutine
	// timestamp leakage and cookie jar mutex contention.
	sharedClient := &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}
	newClient := func() *http.Client {
		if !*enableCookies {
			return sharedClient
		}
		j, _ := cookiejar.New(nil)
		return &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
			Jar:       j,
		}
	}

	sanityURL := url
	if len(urls) > 0 {
		sanityURL = urls[0]
	}
	sanityCheck(newClient(), method, sanityURL, payload, headers)

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	var payloadCounter int64
	var urlCounter int64

	start := time.Now()
	end := start.Add(time.Duration(durationSeconds * float64(time.Second)))
	next := start

	// Per-second success counters for optional CSV output.
	maxSeconds := int(durationSeconds) + 60
	perSecondSuccess := make([]int64, maxSeconds)

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
		// Each goroutine models an independent client. With cookies enabled,
		// each gets its own cookie jar (no shared state between clients).
		// Without cookies, all goroutines share the same http.Client.
		c := newClient()
		go func() {
			defer wg.Done()

			// Decide read vs write when -read-ratio is set
			thisMethod := method
			thisURL := url
			if *readRatio > 0 && *readURL != "" && rand.Float64() < *readRatio {
				thisMethod = http.MethodGet
				thisURL = *readURL
			}

			if len(urls) > 0 {
				idx := atomic.AddInt64(&urlCounter, 1) - 1
				thisURL = urls[idx%int64(len(urls))]
			}
			var body io.Reader
			if thisMethod == http.MethodPost || thisMethod == http.MethodPut {
				thisPayload := payload
				if len(payloads) > 0 {
					idx := atomic.AddInt64(&payloadCounter, 1) - 1
					thisPayload = payloads[idx%int64(len(payloads))]
				}
				body = bytes.NewBufferString(thisPayload)
			}
			req, reqErr := http.NewRequest(thisMethod, thisURL, body)
			if reqErr != nil {
				results <- result{success: false}
				return
			}
			if (thisMethod == http.MethodPost || thisMethod == http.MethodPut) && !hasHeader(headers, "Content-Type") {
				req.Header.Set("Content-Type", "application/json")
			}
			for _, header := range headers {
				req.Header.Set(header.key, header.value)
			}
			t0 := time.Now()
			resp, respErr := c.Do(req)
			latency := time.Since(t0)
			if respErr != nil {
				errStr := respErr.Error()
				if len(errStr) > 80 {
					errStr = errStr[:80]
				}
				results <- result{latency: latency, success: false, statusCode: 0, errMsg: errStr}
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			success := resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices
			if success {
				sec := int(time.Since(start).Seconds())
				if sec >= 0 && sec < len(perSecondSuccess) {
					atomic.AddInt64(&perSecondSuccess[sec], 1)
				}
			}
			results <- result{latency: latency, success: success, statusCode: resp.StatusCode}
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
	statusCounts := make(map[int]int)
	errMsgCounts := make(map[string]int)
	for res := range results {
		if res.success {
			successCount++
		}
		statusCounts[res.statusCode]++
		if res.errMsg != "" {
			errMsgCounts[res.errMsg]++
		}
		// Include all requests with a measured latency (success or error response)
		// so that survivorship bias does not hide queueing effects.
		if res.latency > 0 {
			latencies = append(latencies, float64(res.latency)/float64(time.Millisecond))
		}
	}

	elapsed := time.Since(start).Seconds()
	if elapsed <= 0 {
		elapsed = durationSeconds
	}
	// actualRate measures the send rate during the active send window,
	// not including the tail drain time for in-flight responses.
	actualRate := float64(totalSent) / durationSeconds
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

	// Print status code distribution
	fmt.Println("--- status_code_distribution ---")
	for code, count := range statusCounts {
		if code == 0 {
			fmt.Printf("  connection_error: %d\n", count)
		} else {
			fmt.Printf("  status_%d: %d\n", code, count)
		}
	}

	// Print error message distribution (top errors)
	if len(errMsgCounts) > 0 {
		fmt.Println("--- error_messages ---")
		for msg, count := range errMsgCounts {
			fmt.Printf("  [%d] %s\n", count, msg)
		}
	}

	// Write per-second throughput CSV if requested.
	if *perSecondOutput != "" {
		f, ferr := os.Create(*perSecondOutput)
		if ferr != nil {
			fmt.Fprintf(os.Stderr, "failed to create per-second output file: %v\n", ferr)
		} else {
			fmt.Fprintln(f, "second,throughput_rps")
			limit := int(durationSeconds) + 10
			if limit > len(perSecondSuccess) {
				limit = len(perSecondSuccess)
			}
			for sec := 0; sec < limit; sec++ {
				fmt.Fprintf(f, "%d,%.1f\n", sec, float64(perSecondSuccess[sec]))
			}
			f.Close()
			fmt.Fprintf(os.Stderr, "per-second throughput written to %s\n", *perSecondOutput)
		}
	}
}
