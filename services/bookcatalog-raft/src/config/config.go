package config

import (
	"log"
	"os"
	"path/filepath"
	"strings"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var db *gorm.DB

// DBFilePath is the full path to the SQLite database file, exported for use by FSM snapshots.
var DBFilePath string

func Connect() {
	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = filepath.Join(".", "data")
	}
	os.MkdirAll(dbPath, os.ModePerm)

	DBFilePath = filepath.Join(dbPath, "data.db")
	dsn := "file:" + DBFilePath
	isEnableWAL := os.Getenv("ENABLE_WAL")
	if isEnableWAL != "" && strings.ToLower(isEnableWAL) == "true" {
		dsn = dsn + "?_journal_mode=WAL"
	}

	gormConfig := &gorm.Config{
		SkipDefaultTransaction: true,
		PrepareStmt:            true,
	}

	log.Println("Using datastore: sqlite at", DBFilePath)

	d, err := gorm.Open(sqlite.Open(dsn), gormConfig)
	if err != nil {
		panic(err)
	}
	db = d
}

func GetDB() *gorm.DB {
	return db
}
