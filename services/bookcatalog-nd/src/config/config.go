package config

import (
	"database/sql"
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
		dsn := fmt.Sprintf("root:root@tcp(%s:3306)/books?charset=utf8&parseTime=True&loc=Local", dbHost)
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
		// WAL + synchronous=FULL is the DEFAULT: it fsyncs the WAL on each COMMIT
		// (durable), and is dramatically faster than the SQLite rollback-journal
		// default under XDN -- the container's on-disk SQLite is the write-path
		// bottleneck, and journal mode serializes/rewrites the whole journal per
		// commit. Measured under XDN primary-backup: WAL cuts sequential p50 ~38%
		// and raises peak throughput ~2.4x (943 -> 2260 rps, and eliminates the
		// under-concurrency throughput collapse). WAL is XDN-capture-correct: the
		// state-diff recorder captures the -wal writes byte-exact and the replica
		// rebuilds -shm from the -wal on recovery (verified by the WAL-mode L5
		// fuselog fuzz). Opt out to the old rollback-journal with ENABLE_WAL=false.
		dsn := "file:data/data.db?_journal_mode=WAL&_synchronous=FULL"
		useWAL := strings.ToLower(os.Getenv("ENABLE_WAL")) != "false"
		if !useWAL {
			dsn = "file:data/data.db"
		}
		d, err := gorm.Open(sqlite.Open(dsn), gormConfig)
		if err != nil {
			fmt.Println(err)
			panic(err)
		}
		db = d
		disablePeriodicCheckpoint := os.Getenv("DISABLE_WAL_CHECKPOINT")
		if useWAL && (disablePeriodicCheckpoint == "" || strings.ToLower(disablePeriodicCheckpoint) != "true") {
			go periodicWalCheckpoint()
		}
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

func GetSqlDB() (*sql.DB, error) {
	return db.DB()
}

func periodicWalCheckpoint() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		sqlDB, err := db.DB()
		if err != nil {
			log.Printf("[wal-checkpoint] failed to get sql.DB: %v", err)
			continue
		}
		_, err = sqlDB.Exec("PRAGMA wal_checkpoint(PASSIVE)")
		if err != nil {
			log.Printf("[wal-checkpoint] checkpoint failed: %v", err)
		} else {
			log.Println("[wal-checkpoint] periodic passive checkpoint completed")
		}
	}
}
