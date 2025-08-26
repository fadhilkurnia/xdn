package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Proxy is our proxy handler that holds shared config data.
type Proxy struct {
	serverLocations map[string]ServerLocation
	slowdownFactor  float64
	httpClient      *http.Client
}

// ServeHTTP implements the http.Handler interface, handling each request.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	processingStart := time.Now()

	// Check the X-Request-Delay header (in milliseconds).
	requestedDelayMs, _ := strconv.ParseUint(r.Header.Get("X-Request-Delay"), 10, 64)
	var requestDelayNs uint64 = requestedDelayMs * 1_000_000

	// If no X-Request-Delay, check X-Client-Location.
	if requestDelayNs == 0 && r.Header.Get("X-Client-Location") != "" {
		clientLoc := r.Header.Get("X-Client-Location") // e.g. "39.9526; -75.1652"
		parts := strings.Split(clientLoc, ";")
		if len(parts) != 2 {
			log.Printf("Error: invalid client location format %s", clientLoc)
		} else {
			latC, err1 := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
			lonC, err2 := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
			if err1 != nil {
				log.Printf("Error: invalid client latitude, %s", err1.Error())
			}
			if err2 != nil {
				log.Printf("Error: invalid client longitude, %s", err2.Error())
			}
			if err1 == nil && err2 == nil {
				// Find server location from the request's hostname.
				hostName := r.Host
				if u, err := url.Parse(r.RequestURI); err == nil && u.Hostname() != "" {
					hostName = u.Hostname()
				}
				hostPortParts := strings.Split(hostName, ":")
				hostIp := hostPortParts[0]
				if _, ok := p.serverLocations[hostIp]; !ok {
					log.Printf("Error: unknon data for host, %s", hostIp)
				}
				if srv, ok := p.serverLocations[hostIp]; ok {
					dist := calculateHaversineDistance(latC, lonC, srv.Latitude, srv.Longitude)
					emulatedLatencyMs := getEmulatedLatency(dist, p.slowdownFactor)
					emulatedRttLatencyMs := emulatedLatencyMs * 2.0             // double for rtt
					requestDelayNs = uint64(emulatedRttLatencyMs * 1_000_000.0) // convert ms -> ns
				}
			}
		}
	}

	delayCalcDone := time.Now()

	// Remove the special headers so they won't be forwarded upstream
	r.Header.Del("X-Request-Delay")
	r.Header.Del("X-Client-Location")

	// Emulate the delay with sleep.
	if requestDelayNs > 0 {
		log.Printf("Delaying request by %.2fms", float64(requestDelayNs)/1_000_000.0)
		delayDuration := time.Duration(requestDelayNs)
		time.Sleep(delayDuration)
	}
	delayDone := time.Now()

	// Forward the request. We have to create a new request object to send to the upstream server.
	//    The original r.URL may be relative; if so, we reconstruct the full URL from the Host and the original path.
	var upstreamURL string
	if strings.HasPrefix(r.RequestURI, "http") { // r.RequestURI is already absolute
		upstreamURL = r.RequestURI
	} else {
		// Rebuild an absolute URL from Host + scheme guess (http).
		// Adjust as needed if you handle https or custom logic.
		upstreamURL = fmt.Sprintf("http://%s%s", r.Host, r.RequestURI)
	}

	newReq, err := http.NewRequest(r.Method, upstreamURL, r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	// Copy headers (except the ones we removed).
	for k, vv := range r.Header {
		for _, v := range vv {
			newReq.Header.Add(k, v)
		}
	}

	// Do the request using our shared http.Client.
	resp, err := p.httpClient.Do(newReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	fwdDone := time.Now()

	// Copy upstream response headers & status to our response.
	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)

	// Stream the body to the client.
	if _, err := io.Copy(w, resp.Body); err != nil {
		log.Printf("Error copying upstream response body: %v", err)
	}

	totalDuration := time.Since(processingStart)
	log.Printf("Spent %v in proxy: pre=%v, dly=%v, fwd=%v",
		totalDuration,
		delayCalcDone.Sub(processingStart),
		delayDone.Sub(delayCalcDone),
		fwdDone.Sub(delayDone),
	)
}

func main() {
	// Simple CLI flag for config file
	flag.Usage = func() {
		fmt.Println("Usage: go run main.go -config=<path to config.json>")
		os.Exit(1)
	}
	configPath := flag.String("config", "", "Path to JSON config file")
	flag.Parse()
	if *configPath == "" {
		flag.Usage()
	}

	// Load config (server locations, slowdown factor, etc.).
	log.Printf("Reading server location from %s\n", *configPath)
	cfg, err := readConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	for _, serverLocation := range cfg.ServerLocations {
		log.Printf(" - parsed location %s: %s %f %f\n",
			serverLocation.Name,
			serverLocation.HostPort,
			serverLocation.Latitude,
			serverLocation.Longitude)
	}
	log.Printf(" Slowdown factor: %f\n", cfg.SlowdownFactor)

	// Set up a shared HTTP client.
	// Adjust Transport settings as needed for timeouts, keep‐alive, etc.
	client := &http.Client{}

	// Prepare our proxy and attach it to the default server mux.
	proxy := &Proxy{
		serverLocations: cfg.ServerLocations,
		slowdownFactor:  cfg.SlowdownFactor,
		httpClient:      client,
	}
	http.Handle("/", proxy)

	// Listen and serve on 127.0.0.1:8080
	addr := "127.0.0.1:8080"
	log.Printf("Latency proxy is listening on http://%s\n", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatal(err)
	}
}
