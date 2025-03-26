package models

import (
	"github.com/arpesam/go-book-api/src/config"
	"gorm.io/gorm"
)

var db *gorm.DB

type Book struct {
	Id     uint32 `gorm:"primaryKey" json:"id"`
	Title  string `gorm:"" json:"title"`
	Author string `json:"author"`
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
