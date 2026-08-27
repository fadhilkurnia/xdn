package config

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"goki.dev/rqlite"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var db *gorm.DB

func Connect() {
	dbType := os.Getenv("DB_TYPE")
	if dbType == "" {
		dbType = "sqlite"
	}
	if dbType != "mysql" && dbType != "postgres" && dbType != "sqlite" && dbType != "rqlite" {
		panic("invalid DB_TYPE, options: mysql, postgres, sqlite, rqlite")
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

	log.Println("Using datastore: ", dbType)

	connAttempt := 10
	waitTime := 500 * time.Millisecond

	switch dbType {
	case "mysql":
		dsn := "root:root@/books?charset=utf8&parseTime=True&loc=Local"
		isConnSuccess := false
		for connAttempt > 0 && !isConnSuccess {
			d, err := gorm.Open(mysql.Open(dsn), gormConfig)
			if err != nil {
				fmt.Println(err)
				fmt.Println("retrying to connect ...")
				time.Sleep(waitTime)
				connAttempt = connAttempt - 1
				waitTime = waitTime * 2
				continue
			}

			db = d
			isConnSuccess = true
		}
		if !isConnSuccess {
			panic("failed to connect to database")
		}
	case "postgres":
		dsn := fmt.Sprintf("host=%s user=postgres password=root dbname=books port=5432 sslmode=disable TimeZone=UTC", dbHost)
		isConnSuccess := false
		for connAttempt > 0 && !isConnSuccess {
			d, err := gorm.Open(postgres.Open(dsn), gormConfig)
			if err != nil {
				fmt.Println(err)
				fmt.Println("retrying to connect ...")
				time.Sleep(waitTime)
				connAttempt = connAttempt - 1
				waitTime = waitTime * 2
				continue
			}

			db = d
			isConnSuccess = true
		}
		if !isConnSuccess {
			panic("failed to connect to database")
		}
	case "sqlite":
		dataDir := filepath.Join(".", "data")
		os.MkdirAll(dataDir, os.ModePerm)
		// WAL + synchronous=FULL is the DEFAULT: durable (fsyncs the WAL per COMMIT)
		// and dramatically faster than the SQLite rollback-journal default, which
		// serializes/rewrites the whole journal per commit and is the write-path
		// bottleneck. Opt out to the old rollback-journal with ENABLE_WAL=false.
		// SQLITE_SYNC picks the WAL fsync level: FULL (default) or NORMAL. NORMAL
		// is safe under XDN replication (durability comes from the Paxos statediff
		// log, so the primary's local fsync is redundant) and ~2x faster on disk.
		// Accepts NORMAL/FULL case-insensitively; any other value falls back to FULL.
		syncLevel := strings.ToUpper(strings.TrimSpace(os.Getenv("SQLITE_SYNC")))
		if syncLevel != "NORMAL" && syncLevel != "FULL" {
			syncLevel = "FULL"
		}
		dsn := fmt.Sprintf("file:data/data.db?_journal_mode=WAL&_synchronous=%s", syncLevel)
		if strings.ToLower(os.Getenv("ENABLE_WAL")) == "false" {
			dsn = "file:data/data.db"
		}
		d, err := gorm.Open(sqlite.Open(dsn), gormConfig)
		if err != nil {
			fmt.Println(err)
			panic(err)
		}
		db = d
	case "rqlite":
		dsn := fmt.Sprintf("http://%s:4001", dbHost)
		d, err := gorm.Open(rqlite.Open(dsn), gormConfig)
		if err != nil {
			fmt.Println(err)
			panic(err)
		}
		db = d
	default:
		panic("invalid DB_TYPE, options: mysql, postgres, sqlite")
	}
}

func GetDB() *gorm.DB {
	return db
}
