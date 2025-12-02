package controllers

import (
	"errors"
	"net/http"

	"bookcatalogmongo/src/models"
	"bookcatalogmongo/src/utils"
	"github.com/gorilla/mux"
)

// CreateBook handles POST /api/books.
func CreateBook(w http.ResponseWriter, r *http.Request) {
	var input models.BookInput
	if err := utils.ParseBody(r, &input); err != nil {
		utils.WriteError(w, http.StatusBadRequest, err)
		return
	}

	book, err := models.CreateBook(&input)
	if err != nil {
		utils.WriteError(w, http.StatusInternalServerError, err)
		return
	}

	utils.WriteJSON(w, http.StatusCreated, book)
}

// GetBook handles GET /api/books.
func GetBook(w http.ResponseWriter, r *http.Request) {
	books, err := models.GetAllBooks()
	if err != nil {
		utils.WriteError(w, http.StatusInternalServerError, err)
		return
	}

	utils.WriteJSON(w, http.StatusOK, books)
}

// GetBookById handles GET /api/books/{bookId}.
func GetBookById(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bookID := vars["bookId"]
	book, err := models.GetBookByID(bookID)
	if err != nil {
		if errors.Is(err, models.ErrBookNotFound) {
			utils.WriteError(w, http.StatusNotFound, err)
			return
		}
		utils.WriteError(w, http.StatusBadRequest, err)
		return
	}

	utils.WriteJSON(w, http.StatusOK, book)
}

// UpdateBook handles PUT /api/books/{bookId}.
func UpdateBook(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bookID := vars["bookId"]

	var update models.BookUpdate
	if err := utils.ParseBody(r, &update); err != nil {
		utils.WriteError(w, http.StatusBadRequest, err)
		return
	}

	book, err := models.UpdateBook(bookID, &update)
	if err != nil {
		switch {
		case errors.Is(err, models.ErrBookNotFound):
			utils.WriteError(w, http.StatusNotFound, err)
		default:
			utils.WriteError(w, http.StatusBadRequest, err)
		}
		return
	}

	utils.WriteJSON(w, http.StatusOK, book)
}

// DeleteBook handles DELETE /api/books/{bookId}.
func DeleteBook(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bookID := vars["bookId"]

	book, err := models.DeleteBook(bookID)
	if err != nil {
		switch {
		case errors.Is(err, models.ErrBookNotFound):
			utils.WriteError(w, http.StatusNotFound, err)
		default:
			utils.WriteError(w, http.StatusBadRequest, err)
		}
		return
	}

	utils.WriteJSON(w, http.StatusOK, book)
}
