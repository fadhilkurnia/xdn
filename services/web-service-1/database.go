package main

import (
	"os"

	"github.com/jinzhu/gorm"
)

// InitDb is database initialization
func InitDb() *gorm.DB {
	// read the db path from env variable, else fallback to the default
	dbPath := "./data/data.db"
	if p := os.Getenv("SQLITE_PATH"); p != "" {
		dbPath = p
	}

	// Openning file
	db, err := gorm.Open("sqlite3", dbPath)
	// Display SQL queries if true
	db.LogMode(false)

	// Error
	if err != nil {
		panic(err)
	}
	// Creating the table
	if !db.HasTable(&Users{}) {
		db.CreateTable(&Users{})
		db.Set("gorm:table_options", "ENGINE=InnoDB").CreateTable(&Users{})
	}

	return db
}
