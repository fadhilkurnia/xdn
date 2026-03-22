package config

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"goki.dev/rqlite"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var db *gorm.DB

func Connect() {
	dbType := os.Getenv("DB_TYPE")
	if dbType == "" {
		dbType = "sqlite"
	}
	if dbType != "sqlite" && dbType != "rqlite" {
		panic("invalid DB_TYPE, options: sqlite, rqlite")
	}

	dbHost := os.Getenv("DB_HOST")
	if dbHost == "" {
		dbHost = "127.0.0.1"
	}

	gormConfig := &gorm.Config{
		SkipDefaultTransaction: true,
		PrepareStmt:            true,
	}
	isDisableTxn := os.Getenv("DISABLE_TXN")
	if isDisableTxn != "" && strings.ToLower(isDisableTxn) == "false" {
		gormConfig = &gorm.Config{}
	}

	log.Println("Using datastore:", dbType)

	switch dbType {
	case "sqlite":
		dataDir := filepath.Join(".", "data")
		os.MkdirAll(dataDir, os.ModePerm)
		dsn := "file:data/tasks.db"
		isEnableWAL := os.Getenv("ENABLE_WAL")
		if isEnableWAL != "" && strings.ToLower(isEnableWAL) == "true" {
			dsn = "file:data/tasks.db?_journal_mode=WAL&_synchronous=FULL"
		}
		d, err := gorm.Open(sqlite.Open(dsn), gormConfig)
		if err != nil {
			fmt.Println(err)
			panic(err)
		}
		db = d
	case "rqlite":
		dsn := normalizeRqliteURL(dbHost)
		d, err := gorm.Open(rqlite.Open(dsn), gormConfig)
		if err != nil {
			fmt.Println(err)
			panic(err)
		}
		db = d
	}
}

func normalizeRqliteURL(dbHost string) string {
	if strings.HasPrefix(dbHost, "http://") || strings.HasPrefix(dbHost, "https://") {
		return dbHost
	}
	if strings.Contains(dbHost, ":") {
		return fmt.Sprintf("http://%s", dbHost)
	}
	return fmt.Sprintf("http://%s:4001", dbHost)
}

func GetDB() *gorm.DB {
	return db
}
