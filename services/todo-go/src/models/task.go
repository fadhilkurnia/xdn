package models

import (
	"log"

	"github.com/fadhilkurnia/xdn-todo-go/src/config"
	"gorm.io/gorm"
)

var db *gorm.DB

type Task struct {
	Item    string `gorm:"primaryKey" json:"item"`
	Counter int    `json:"counter"`
}

func init() {
	config.Connect()
	db = config.GetDB()
	db.AutoMigrate(&Task{})
}

func GetActiveTasks() []string {
	var items []string
	result := db.Raw("SELECT item FROM tasks WHERE counter > 0 ORDER BY item ASC").Scan(&items)
	if result.Error != nil {
		log.Println("Error fetching tasks:", result.Error)
	}
	return items
}

func GetTask(item string) (*Task, error) {
	var task Task
	result := db.Where("item = ? AND counter > 0", item).First(&task)
	if result.Error != nil {
		return nil, result.Error
	}
	return &task, nil
}

func AddTask(item string) error {
	result := db.Exec(
		"INSERT INTO tasks (item, counter) VALUES (?, 1) ON CONFLICT(item) DO UPDATE SET counter = counter + 1",
		item,
	)
	return result.Error
}

func RemoveTask(item string) error {
	result := db.Exec(
		"INSERT INTO tasks (item, counter) VALUES (?, -1) ON CONFLICT(item) DO UPDATE SET counter = counter - 1",
		item,
	)
	return result.Error
}
