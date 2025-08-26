package models

import (
	"time"

	"github.com/arpesam/go-book-api/src/config"
	"gorm.io/gorm"
)

var db *gorm.DB

type Book struct {
	ID        uint           `gorm:"primary_key" json:"id"`
	Title     string         `gorm:"" json:"title"`
	Author    string         `json:"author"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
	DeletedAt gorm.DeletedAt `json:"deleted_at"`
}

func init() {
	config.Connect()
	db = config.GetDB()
	db.AutoMigrate(&Book{})
}

func (b *Book) CreateBook() *Book {
	db.Create(&b)
	return b
}

func (b *Book) Save() {
	db.Save(b)
}

func GetAllBooks() []Book {
	var Books []Book
	db.Find(&Books)
	return Books
}

func GetBookById(Id int64) (*Book, *gorm.DB) {
	var getBook Book
	db := db.First(&getBook, Id)
	return &getBook, db
}

func DeleteBook(ID int64) Book {
	var book Book
	db.Delete(&book, ID)
	return book
}
