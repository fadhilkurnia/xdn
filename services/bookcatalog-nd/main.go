package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/arpesam/go-book-api/src/routes"
	"github.com/gorilla/mux"
)

// loggingMiddleware is an HTTP middleware that logs the execution time of each request.
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Call the next handler in the chain
		next.ServeHTTP(w, r)

		// Calculate duration and log
		duration := time.Since(start)
		reqId := r.Header.Get("X-XDN-ReqId")
		if reqId == "" {
			reqId = "-"
		}
		log.Printf("%s %s [reqId=%s] [lat=%v]", r.Method, r.URL.Path, reqId, duration)
	})
}

func main() {
	port := "80"
	if p := os.Getenv("PORT"); p != "" {
		port = p
	}
	r := mux.NewRouter()

	// register routes
	routes.RegisterBookStoreRoutes(r)
	routes.RegisterViewRoutes(r)

	// wrap routes with logging middleware
	loggedRoutes := loggingMiddleware(r)

	// set the loggedRoutes as the default route
	http.Handle("/", loggedRoutes)

	log.Printf("Runnning server in :%s\n", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), loggedRoutes))
}
