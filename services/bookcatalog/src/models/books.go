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

func (b *Book) CreateBook() (*Book, error) {
	if err := db.Create(&b).Error; err != nil {
		return nil, err
	}
	return b, nil
}

func (b *Book) Save() error {
	return db.Save(b).Error
}

func GetAllBooks() ([]Book, error) {
	var Books []Book
	if err := db.Find(&Books).Error; err != nil {
		return nil, err
	}
	return Books, nil
}

func GetBookById(Id int64) (*Book, error) {
	var getBook Book
	if err := db.First(&getBook, Id).Error; err != nil {
		return nil, err
	}
	return &getBook, nil
}

func DeleteBook(ID int64) error {
	return db.Delete(&Book{}, ID).Error
}
