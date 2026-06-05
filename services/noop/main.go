package main

import (
	_ "embed"
	"fmt"
	"log"
	"net/http"
	"os"
)

//go:embed index.html
var indexHTML []byte

// noop is the smallest possible XDN service: it accepts any HTTP request and
// immediately returns a fixed 200 response, without touching disk or doing any
// work. It serves as a baseline to isolate XDN's coordination/replication
// overhead from application execution time.
//
// Paths "/" and "/view" serve a small browser UI; every other path (including
// the canonical benchmark endpoint "/ping") returns "ok" immediately.
func main() {
	port := "80"
	if p := os.Getenv("PORT"); p != "" {
		port = p
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ping", noop)
	mux.HandleFunc("/view", index)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" || r.URL.Path == "/view" {
			index(w, r)
			return
		}
		noop(w, r)
	})

	log.Printf("noop server running on :%s\n", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), mux))
}

func noop(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func index(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(indexHTML)
}
