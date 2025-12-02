package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"bookcatalogmongo/src/routes"
	"github.com/gorilla/mux"
)

// loggingMiddleware logs HTTP request method, path, and latency.
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		duration := time.Since(start)
		log.Printf("%s %s [lat=%v]", r.Method, r.URL.Path, duration)
	})
}

func main() {
	port := "80"
	if p := os.Getenv("PORT"); p != "" {
		port = p
	}

	router := mux.NewRouter()
	routes.RegisterBookStoreRoutes(router)
	routes.RegisterViewRoutes(router)
	loggedRouter := loggingMiddleware(router)

	http.Handle("/", loggedRouter)

	log.Printf("Starting server on :%s\n", port)
	if err := http.ListenAndServe(fmt.Sprintf(":%s", port), loggedRouter); err != nil {
		log.Fatal(err)
	}
}
