package config

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/fadhilkurnia/xdn-todo-go/src/batcher"
	"goki.dev/rqlite"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var db *gorm.DB
var writeBatcher *batcher.Batcher

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

		// Initialize write batcher if enabled.
		if strings.ToLower(os.Getenv("ENABLE_RQLITE_BATCHING")) == "true" {
			maxSize := 128
			if v, err := strconv.Atoi(os.Getenv("RQLITE_BATCH_MAX_SIZE")); err == nil && v > 0 {
				maxSize = v
			}
			windowMs := 2
			if v, err := strconv.Atoi(os.Getenv("RQLITE_BATCH_WINDOW_MS")); err == nil && v > 0 {
				windowMs = v
			}
			b, err := batcher.New(dsn, maxSize, windowMs)
			if err != nil {
				log.Printf("WARNING: failed to create write batcher: %v", err)
			} else {
				b.Start()
				writeBatcher = b
			}
		}
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

// GetWriteBatcher returns the write batcher if rqlite batching is enabled,
// or nil otherwise.
func GetWriteBatcher() *batcher.Batcher {
	return writeBatcher
}
