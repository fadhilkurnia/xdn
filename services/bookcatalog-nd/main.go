package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/arpesam/go-book-api/src/routes"
	"github.com/gorilla/mux"
)

func main() {
	port := "80"
	if p := os.Getenv("PORT"); p != "" {
		port = p
	}
	r := mux.NewRouter()

	routes.RegisterBookStoreRoutes(r)
	routes.RegisterViewRoutes(r)
	http.Handle("/", r)
	fmt.Printf("Runnning server in :%s\n", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), r))
}
