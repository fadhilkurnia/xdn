package controllers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/arpesam/go-book-api/src/models"
	"github.com/arpesam/go-book-api/src/utils"
	"github.com/gorilla/mux"
)

var NewBook models.Book

func CreateBook(w http.ResponseWriter, r *http.Request) {
	CreateBook := &models.Book{}
	utils.ParseBody(r, CreateBook)
	b, err := CreateBook.CreateBook()
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"error":"%s"}`, err.Error())))
		return
	}
	res, _ := json.Marshal(b)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(res)
}

func GetBook(w http.ResponseWriter, r *http.Request) {
	newBooks, err := models.GetAllBooks()
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"error":"%s"}`, err.Error())))
		return
	}
	res, _ := json.Marshal(newBooks)
	w.WriteHeader(http.StatusOK)
	w.Write(res)
}

func GetBookById(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bookId := vars["bookId"]
	ID, err := strconv.ParseInt(bookId, 0, 0)
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf(`{"error":"invalid book ID: %s"}`, err.Error())))
		return
	}
	bookDetails, err := models.GetBookById(ID)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(fmt.Sprintf(`{"error":"%s"}`, err.Error())))
		return
	}
	res, _ := json.Marshal(bookDetails)
	w.WriteHeader(http.StatusOK)
	w.Write(res)
}

func UpdateBook(w http.ResponseWriter, r *http.Request) {
	var updateBook = &models.Book{}
	utils.ParseBody(r, updateBook)
	vars := mux.Vars(r)
	bookId := vars["bookId"]
	ID, err := strconv.ParseInt(bookId, 0, 0)
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf(`{"error":"invalid book ID: %s"}`, err.Error())))
		return
	}
	bookDetails, err := models.GetBookById(ID)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(fmt.Sprintf(`{"error":"%s"}`, err.Error())))
		return
	}
	if updateBook.Title != "" {
		bookDetails.Title = updateBook.Title
	}
	if updateBook.Author != "" {
		bookDetails.Author = updateBook.Author
	}
	if err := bookDetails.Save(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"error":"%s"}`, err.Error())))
		return
	}
	res, _ := json.Marshal(bookDetails)
	w.WriteHeader(http.StatusOK)
	w.Write(res)
}

func DeleteBook(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bookId := vars["bookId"]
	ID, err := strconv.ParseInt(bookId, 0, 0)
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf(`{"error":"invalid book ID: %s"}`, err.Error())))
		return
	}
	if err := models.DeleteBook(ID); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"error":"%s"}`, err.Error())))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}`))
}
