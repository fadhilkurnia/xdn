package routes

import (
	"net/http"

	"bookcatalogmongo/src/controllers"
	"bookcatalogmongo/src/views"
	"github.com/gorilla/mux"
)

// RegisterBookStoreRoutes attaches API endpoints to the router.
var RegisterBookStoreRoutes = func(router *mux.Router) {
	router.HandleFunc("/api/books", controllers.CreateBook).Methods("POST")
	router.HandleFunc("/api/books", controllers.GetBook).Methods("GET")
	router.HandleFunc("/api/books/{bookId}", controllers.GetBookById).Methods("GET")
	router.HandleFunc("/api/books/{bookId}", controllers.UpdateBook).Methods("PUT")
	router.HandleFunc("/api/books/{bookId}", controllers.DeleteBook).Methods("DELETE")
}

// RegisterViewRoutes attaches view routes used by the demo UI.
var RegisterViewRoutes = func(router *mux.Router) {
	router.HandleFunc("/", RedirectToView).Methods("GET")
	router.HandleFunc("/view", views.RenderView).Methods("GET")
}

// RedirectToView redirects the root path to /view.
func RedirectToView(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/view", http.StatusPermanentRedirect)
}
