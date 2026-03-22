package controllers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/arpesam/go-book-api/src/config"
	"github.com/arpesam/go-book-api/src/models"
	"github.com/arpesam/go-book-api/src/utils"
	"github.com/gorilla/mux"
)

func CreateBook(w http.ResponseWriter, r *http.Request) {
	newBook := &models.Book{}
	utils.ParseBody(r, newBook)
	b, err := newBook.CreateBook()
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"error":"%s"}`, err.Error())))
		return
	}
	res, _ := json.Marshal(b)
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

func WalCheckpoint(w http.ResponseWriter, r *http.Request) {
	sqlDB, err := config.GetSqlDB()
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"failed to get sql.DB"}`))
		return
	}
	_, err = sqlDB.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"checkpoint failed"}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}`))
}
