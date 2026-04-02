package routes

import (
	"net/http"

	"github.com/arpesam/go-book-api/src/controllers"
	"github.com/arpesam/go-book-api/src/views"
	"github.com/gorilla/mux"
)

var RegisterBookStoreRoutes = func(router *mux.Router) {
	router.HandleFunc("/api/books", controllers.CreateBook).Methods("POST")
	router.HandleFunc("/api/books", controllers.GetBook).Methods("GET")
	router.HandleFunc("/api/books/{bookId}", controllers.GetBookById).Methods("GET")
	router.HandleFunc("/api/books/{bookId}", controllers.UpdateBook).Methods("PUT")
	router.HandleFunc("/api/books/{bookId}", controllers.DeleteBook).Methods("DELETE")
}

var RegisterViewRoutes = func(router *mux.Router) {
	router.HandleFunc("/", RedirectToView).Methods("GET")
	router.HandleFunc("/view", views.RenderView).Methods("GET")
}

func RedirectToView(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/view", http.StatusPermanentRedirect)
}
