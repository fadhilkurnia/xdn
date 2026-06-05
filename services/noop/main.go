package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
)

// noop is the smallest possible XDN service: it accepts any HTTP request and
// immediately returns a fixed 200 response, without touching disk or doing any
// work. It serves as a baseline to isolate XDN's coordination/replication
// overhead from application execution time.
func main() {
	port := "80"
	if p := os.Getenv("PORT"); p != "" {
		port = p
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	log.Printf("noop server running on :%s\n", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), handler))
}
