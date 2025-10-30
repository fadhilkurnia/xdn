package views

import (
	_ "embed"
	"net/http"
)

//go:embed index.html
var htmlPage string // htmlPage contains index.html, initialized at compile time

func RenderView(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(htmlPage))
}
