package controllers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/arpesam/go-book-api/src/models"
	"github.com/arpesam/go-book-api/src/raftnode"
	"github.com/arpesam/go-book-api/src/utils"
	"github.com/gorilla/mux"
)

var node *raftnode.RaftNode

func SetRaftNode(rn *raftnode.RaftNode) {
	node = rn
}

func checkLeader(w http.ResponseWriter) bool {
	if node == nil || node.IsLeader() {
		return true
	}
	w.Header().Set("X-Raft-Leader", node.LeaderAddr())
	http.Error(w, "not the leader", http.StatusServiceUnavailable)
	return false
}

func CreateBook(w http.ResponseWriter, r *http.Request) {
	if !checkLeader(w) {
		return
	}

	book := &models.Book{}
	utils.ParseBody(r, book)

	cmd := raftnode.Command{
		Type:   raftnode.CmdCreateBook,
		Title:  book.Title,
		Author: book.Author,
	}

	resp, err := node.ApplyCommand(cmd, 5*time.Second)
	if err != nil {
		http.Error(w, fmt.Sprintf("raft apply failed: %v", err), http.StatusInternalServerError)
		return
	}
	if errResp, ok := resp.(error); ok {
		http.Error(w, fmt.Sprintf("create failed: %v", errResp), http.StatusInternalServerError)
		return
	}

	res, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(res)
}

func GetBook(w http.ResponseWriter, r *http.Request) {
	newBooks := models.GetAllBooks()
	res, _ := json.Marshal(newBooks)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(res)
}

func GetBookById(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bookId := vars["bookId"]
	ID, err := strconv.ParseInt(bookId, 0, 0)
	if err != nil {
		fmt.Println("Error while parsing")
	}
	bookDetails, _ := models.GetBookById(ID)
	res, _ := json.Marshal(bookDetails)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(res)
}

func UpdateBook(w http.ResponseWriter, r *http.Request) {
	if !checkLeader(w) {
		return
	}

	var updateBook = &models.Book{}
	utils.ParseBody(r, updateBook)
	vars := mux.Vars(r)
	bookId := vars["bookId"]
	ID, err := strconv.ParseInt(bookId, 0, 0)
	if err != nil {
		fmt.Println("Error while parsing")
	}

	cmd := raftnode.Command{
		Type:   raftnode.CmdUpdateBook,
		BookID: uint32(ID),
		Title:  updateBook.Title,
		Author: updateBook.Author,
	}

	resp, err := node.ApplyCommand(cmd, 5*time.Second)
	if err != nil {
		http.Error(w, fmt.Sprintf("raft apply failed: %v", err), http.StatusInternalServerError)
		return
	}
	if errResp, ok := resp.(error); ok {
		http.Error(w, fmt.Sprintf("update failed: %v", errResp), http.StatusInternalServerError)
		return
	}

	res, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(res)
}

func DeleteBook(w http.ResponseWriter, r *http.Request) {
	if !checkLeader(w) {
		return
	}

	vars := mux.Vars(r)
	bookId := vars["bookId"]
	ID, err := strconv.ParseInt(bookId, 0, 0)
	if err != nil {
		fmt.Println("Error while parsing")
	}

	cmd := raftnode.Command{
		Type:   raftnode.CmdDeleteBook,
		BookID: uint32(ID),
	}

	resp, err := node.ApplyCommand(cmd, 5*time.Second)
	if err != nil {
		http.Error(w, fmt.Sprintf("raft apply failed: %v", err), http.StatusInternalServerError)
		return
	}
	if errResp, ok := resp.(error); ok {
		http.Error(w, fmt.Sprintf("delete failed: %v", errResp), http.StatusInternalServerError)
		return
	}

	res, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(res)
}
