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
