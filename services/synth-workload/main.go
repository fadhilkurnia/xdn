package main

import (
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
)

var db *sql.DB

type WorkloadRequest struct {
	Txns       int  `json:"txns"`
	Ops        int  `json:"ops"`
	WriteSize  int  `json:"write_size"`
	AutoCommit bool `json:"autocommit"` // when true, each op is auto-committed (no explicit txn)
}

type WorkloadResponse struct {
	RequestID string `json:"request_id"`
	Txns      int    `json:"txns"`
	Ops       int    `json:"ops"`
	WriteSize int    `json:"write_size"`
	Rows      int    `json:"rows_inserted"`
	DurationMs float64 `json:"duration_ms"`
}

func initDB() {
	dbType := os.Getenv("DB_TYPE")
	if dbType == "" {
		dbType = "sqlite"
	}

	if dbType == "rqlite" {
		// For rqlite mode, we don't open a local DB — requests go directly
		// to the rqlite HTTP API. We still create the table via rqlite.
		rqliteURL := os.Getenv("RQLITE_URL")
		if rqliteURL == "" {
			rqliteURL = "http://localhost:4001"
		}
		createTable := `["CREATE TABLE IF NOT EXISTS workload_data (id INTEGER PRIMARY KEY AUTOINCREMENT, request_id TEXT NOT NULL, txn_idx INTEGER NOT NULL, op_idx INTEGER NOT NULL, payload BLOB, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"]`
		for attempt := 1; attempt <= 10; attempt++ {
			resp, err := http.Post(rqliteURL+"/db/execute", "application/json", strings.NewReader(createTable))
			if err == nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode < 300 {
					log.Printf("rqlite table created (attempt %d)", attempt)
					return
				}
			}
			log.Printf("rqlite not ready (attempt %d): %v", attempt, err)
			time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
		}
		log.Fatal("failed to create table on rqlite after 10 attempts")
	}

	// SQLite mode
	dsn := "/data/workload.db?_journal_mode=DELETE&_synchronous=FULL"
	enableWAL := os.Getenv("ENABLE_WAL")
	if strings.EqualFold(enableWAL, "true") {
		dsn = "/data/workload.db?_journal_mode=WAL&_synchronous=FULL"
	}

	var err error
	for attempt := 1; attempt <= 10; attempt++ {
		db, err = sql.Open("sqlite3", dsn)
		if err == nil {
			err = db.Ping()
		}
		if err == nil {
			break
		}
		log.Printf("sqlite open failed (attempt %d): %v", attempt, err)
		time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
	}
	if err != nil {
		log.Fatalf("failed to open sqlite after 10 attempts: %v", err)
	}

	db.SetMaxOpenConns(1) // SQLite doesn't support concurrent writes

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS workload_data (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		request_id TEXT NOT NULL,
		txn_idx INTEGER NOT NULL,
		op_idx INTEGER NOT NULL,
		payload BLOB,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`)
	if err != nil {
		log.Fatalf("failed to create table: %v", err)
	}
	log.Println("SQLite database initialized")
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func handleWorkload(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	var req WorkloadRequest
	if r.Body != nil {
		json.NewDecoder(r.Body).Decode(&req)
	}
	if req.Txns <= 0 {
		req.Txns = 1
	}
	if req.Ops <= 0 {
		req.Ops = 1
	}
	if req.WriteSize <= 0 {
		req.WriteSize = 100
	}

	requestID := uuid.New().String()
	totalRows := 0

	dbType := os.Getenv("DB_TYPE")
	if dbType == "rqlite" {
		totalRows = executeRqlite(requestID, req)
	} else {
		totalRows = executeSQLite(requestID, req)
	}

	resp := WorkloadResponse{
		RequestID:  requestID,
		Txns:       req.Txns,
		Ops:        req.Ops,
		WriteSize:  req.WriteSize,
		Rows:       totalRows,
		DurationMs: float64(time.Since(start).Microseconds()) / 1000.0,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func executeSQLite(requestID string, req WorkloadRequest) int {
	totalRows := 0
	payload := randomBytes(req.WriteSize)

	if req.AutoCommit {
		// Auto-commit mode: each INSERT is its own transaction.
		// Each auto-committed write triggers an independent fsync,
		// matching how most ORMs (GORM, wpdb, ActiveRecord) work.
		// On replicated block storage (e.g., OpenEBS), each fsync
		// is independently replicated — N ops = N coordination rounds.
		for i := 0; i < req.Txns; i++ {
			for j := 0; j < req.Ops; j++ {
				_, err := db.Exec(
					"INSERT INTO workload_data (request_id, txn_idx, op_idx, payload) VALUES (?, ?, ?, ?)",
					requestID, i, j, payload,
				)
				if err != nil {
					log.Printf("INSERT (autocommit) failed: %v", err)
					continue
				}
				totalRows++
			}
		}
	} else {
		// Explicit transaction mode: N ops wrapped in 1 txn = 1 fsync.
		for i := 0; i < req.Txns; i++ {
			tx, err := db.Begin()
			if err != nil {
				log.Printf("BEGIN failed: %v", err)
				continue
			}
			for j := 0; j < req.Ops; j++ {
				_, err := tx.Exec(
					"INSERT INTO workload_data (request_id, txn_idx, op_idx, payload) VALUES (?, ?, ?, ?)",
					requestID, i, j, payload,
				)
				if err != nil {
					log.Printf("INSERT failed: %v", err)
					continue
				}
				totalRows++
			}
			if err := tx.Commit(); err != nil {
				log.Printf("COMMIT failed: %v", err)
			}
		}
	}
	return totalRows
}

func executeRqlite(requestID string, req WorkloadRequest) int {
	rqliteURL := os.Getenv("RQLITE_URL")
	if rqliteURL == "" {
		rqliteURL = "http://localhost:4001"
	}

	totalRows := 0
	payload := fmt.Sprintf("%x", randomBytes(req.WriteSize))

	if req.AutoCommit {
		// Auto-commit mode: each INSERT is a separate rqlite request = separate Raft round.
		// N txns × N ops = N×N Raft coordination rounds.
		for i := 0; i < req.Txns; i++ {
			for j := 0; j < req.Ops; j++ {
				stmt := fmt.Sprintf(
					"INSERT INTO workload_data (request_id, txn_idx, op_idx, payload) VALUES ('%s', %d, %d, X'%s')",
					requestID, i, j, payload,
				)
				body, _ := json.Marshal([]string{stmt})
				resp, err := http.Post(rqliteURL+"/db/execute", "application/json", strings.NewReader(string(body)))
				if err != nil {
					log.Printf("rqlite execute (autocommit) failed: %v", err)
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode < 300 {
					totalRows++
				}
			}
		}
	} else {
		// Explicit transaction mode: each transaction = separate HTTP call to rqlite = separate Raft round
		for i := 0; i < req.Txns; i++ {
			stmts := make([]string, 0, req.Ops+2)
			stmts = append(stmts, "BEGIN")
			for j := 0; j < req.Ops; j++ {
				stmt := fmt.Sprintf(
					"INSERT INTO workload_data (request_id, txn_idx, op_idx, payload) VALUES ('%s', %d, %d, X'%s')",
					requestID, i, j, payload,
				)
				stmts = append(stmts, stmt)
			}
			stmts = append(stmts, "COMMIT")

			body, _ := json.Marshal(stmts)
			resp, err := http.Post(rqliteURL+"/db/execute?transaction", "application/json", strings.NewReader(string(body)))
			if err != nil {
				log.Printf("rqlite execute failed: %v", err)
				continue
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode < 300 {
				totalRows += req.Ops
			} else {
				log.Printf("rqlite returned HTTP %d for txn %d", resp.StatusCode, i)
			}
		}
	}
	return totalRows
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func handleStats(w http.ResponseWriter, r *http.Request) {
	dbType := os.Getenv("DB_TYPE")
	var count int

	if dbType == "rqlite" {
		rqliteURL := os.Getenv("RQLITE_URL")
		if rqliteURL == "" {
			rqliteURL = "http://localhost:4001"
		}
		resp, err := http.Get(rqliteURL + "/db/query?q=SELECT+COUNT(*)+FROM+workload_data")
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		defer resp.Body.Close()
		var result map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&result)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
		return
	}

	err := db.QueryRow("SELECT COUNT(*) FROM workload_data").Scan(&count)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int{"row_count": count})
}

func main() {
	port := "80"
	if p := os.Getenv("PORT"); p != "" {
		port = p
	}

	initDB()

	mux := http.NewServeMux()
	mux.HandleFunc("/workload", handleWorkload)
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/stats", handleStats)
	mux.HandleFunc("/", handleHealth)

	log.Printf("synth-workload listening on :%s (DB_TYPE=%s)", port, os.Getenv("DB_TYPE"))
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), mux))
}
