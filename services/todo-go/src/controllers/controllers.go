package controllers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/fadhilkurnia/xdn-todo-go/src/models"
	"github.com/fadhilkurnia/xdn-todo-go/src/utils"
	"github.com/gorilla/mux"
)

func GetTasks(w http.ResponseWriter, r *http.Request) {
	tasks := models.GetActiveTasks()
	if tasks == nil {
		tasks = []string{}
	}
	res, _ := json.Marshal(tasks)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(res)
}

func GetTask(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	item := vars["item"]
	task, err := models.GetTask(item)
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(fmt.Sprintf(`{"error":"task not found: %s"}`, item)))
		return
	}
	res, _ := json.Marshal(task)
	w.WriteHeader(http.StatusOK)
	w.Write(res)
}

func AddTask(w http.ResponseWriter, r *http.Request) {
	var task models.Task
	utils.ParseBody(r, &task)
	if err := models.AddTask(task.Item); err != nil {
		fmt.Println("Error adding task:", err)
		http.Error(w, "Error adding task", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Task added"))
}

func DeleteTask(w http.ResponseWriter, r *http.Request) {
	var task models.Task
	utils.ParseBody(r, &task)
	if err := models.RemoveTask(task.Item); err != nil {
		fmt.Println("Error removing task:", err)
		http.Error(w, "Error removing task", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Task removed"))
}
