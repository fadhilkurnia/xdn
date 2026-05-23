// counter-app: a tiny HTTP service used as the migration test workload.
//
// Endpoints:
//   GET /inc    increments an atomic in-memory counter, returns the new value as text
//   GET /get    returns the current counter value (no increment)
//   GET /health returns 200 "ok"
//
// State lives only in memory, so any restart loses it — the whole point is
// that CRIU checkpoint/restore preserves it across hosts.
package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"sync/atomic"
)

func main() {
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	var counter atomic.Int64

	http.HandleFunc("/inc", func(w http.ResponseWriter, r *http.Request) {
		n := counter.Add(1)
		fmt.Fprintf(w, "%d", n)
	})
	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "%d", counter.Load())
	})
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	})

	log.Printf("counter-app listening on %s (pid=%d)", addr, os.Getpid())
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatal(err)
	}
}
