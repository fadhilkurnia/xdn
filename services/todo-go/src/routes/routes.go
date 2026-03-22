package routes

import (
	"net/http"

	"github.com/fadhilkurnia/xdn-todo-go/src/controllers"
	"github.com/fadhilkurnia/xdn-todo-go/src/views"
	"github.com/gorilla/mux"
)

var RegisterTodoRoutes = func(router *mux.Router) {
	router.HandleFunc("/api/todo/tasks", controllers.GetTasks).Methods("GET")
	router.HandleFunc("/api/todo/tasks/{item}", controllers.GetTask).Methods("GET")
	router.HandleFunc("/api/todo/tasks", controllers.AddTask).Methods("POST")
	router.HandleFunc("/api/todo/tasks", controllers.DeleteTask).Methods("DELETE")
}

var RegisterViewRoutes = func(router *mux.Router) {
	router.HandleFunc("/", RedirectToView).Methods("GET")
	router.HandleFunc("/view", views.RenderView).Methods("GET")
}

func RedirectToView(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/view", http.StatusFound)
}
