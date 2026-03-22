package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/fadhilkurnia/xdn-todo-go/src/routes"
	"github.com/gorilla/mux"
)

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
	r := mux.NewRouter()

	routes.RegisterTodoRoutes(r)
	routes.RegisterViewRoutes(r)

	loggedRoutes := loggingMiddleware(r)

	http.Handle("/", loggedRoutes)

	log.Printf("Running server on :%s\n", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), loggedRoutes))
}
