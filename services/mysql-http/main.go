// HTTP key-value frontend for the self-clustering MySQL (Group Replication)
// reference service. Runs as the entry sidecar in each replica's pod,
// sharing the cluster member's network namespace.
//
// Why this exists: XDN observes requests at its HTTP proxy to tell
// coordinated writes apart from uncoordinated reads; a raw MySQL wire
// connection is an opaque TCP pipe with no visible request boundaries, so
// every reference service gets a thin HTTP shim with the same surface:
//
//	GET  /            -> 200 once the backend is usable (readiness gate)
//	PUT  /kv/{key}    -> write (REPLACE INTO, routed to the GR primary)
//	GET  /kv/{key}    -> read  (SELECT on the local member)
//
// GR runs single-primary: the local member rejects writes unless it is the
// primary, so the frontend discovers the primary from the local member's
// performance_schema (MEMBER_HOST is the replica-N overlay alias, dialable
// through Docker's embedded DNS) and keeps a second connection there. On a
// write error the primary is re-discovered once and the write retried,
// which covers primary migration.
package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	rootPw    = os.Getenv("MYSQL_ROOT_PASSWORD")
	localDB   *sql.DB
	primaryMu sync.Mutex
	primaryDB *sql.DB
	primaryAt string
)

func dsn(host string) string {
	return fmt.Sprintf("root:%s@tcp(%s:3306)/?timeout=5s&readTimeout=10s&writeTimeout=10s", rootPw, host)
}

func discoverPrimary() (string, error) {
	var host string
	err := localDB.QueryRow(
		"SELECT MEMBER_HOST FROM performance_schema.replication_group_members" +
			" WHERE MEMBER_ROLE='PRIMARY' AND MEMBER_STATE='ONLINE'").Scan(&host)
	return host, err
}

// primary returns a DB handle to the current GR primary, re-resolving when
// forced or when no handle exists yet.
func primary(force bool) (*sql.DB, error) {
	primaryMu.Lock()
	defer primaryMu.Unlock()
	if primaryDB != nil && !force {
		return primaryDB, nil
	}
	host, err := discoverPrimary()
	if err != nil {
		return nil, fmt.Errorf("primary discovery: %w", err)
	}
	if primaryDB != nil && host == primaryAt {
		return primaryDB, nil
	}
	db, err := sql.Open("mysql", dsn(host))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	if primaryDB != nil {
		primaryDB.Close()
	}
	primaryDB, primaryAt = db, host
	log.Printf("primary is %s", host)
	return primaryDB, nil
}

func ensureSchema() error {
	db, err := primary(false)
	if err != nil {
		return err
	}
	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS bw"); err != nil {
		return err
	}
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS bw.kv (k VARCHAR(190) PRIMARY KEY, v BLOB)")
	return err
}

func main() {
	self := os.Getenv("XDN_CLUSTER_SELF")
	localDB, _ = sql.Open("mysql", dsn("127.0.0.1"))
	localDB.SetMaxOpenConns(8)

	ready := false
	var readyMu sync.Mutex
	go func() {
		for {
			if err := localDB.Ping(); err == nil {
				if err := ensureSchema(); err == nil {
					readyMu.Lock()
					ready = true
					readyMu.Unlock()
					log.Printf("%s: backend ready", self)
					return
				}
			}
			time.Sleep(2 * time.Second)
		}
	}()

	http.HandleFunc("/kv/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/kv/")
		switch r.Method {
		case http.MethodGet:
			var v []byte
			err := localDB.QueryRow("SELECT v FROM bw.kv WHERE k=?", key).Scan(&v)
			if err == sql.ErrNoRows {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Write(v)
		case http.MethodPut, http.MethodPost:
			body, _ := io.ReadAll(r.Body)
			db, err := primary(false)
			if err == nil {
				_, err = db.Exec("REPLACE INTO bw.kv VALUES (?, ?)", key, body)
			}
			if err != nil {
				// Primary may have migrated: re-discover once and retry.
				if db, err = primary(true); err == nil {
					_, err = db.Exec("REPLACE INTO bw.kv VALUES (?, ?)", key, body)
				}
			}
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			io.WriteString(w, "OK")
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		readyMu.Lock()
		ok := ready
		readyMu.Unlock()
		if !ok {
			http.Error(w, "warming up", http.StatusServiceUnavailable)
			return
		}
		io.WriteString(w, "ok mysql-http")
	})
	log.Fatal(http.ListenAndServe(":8080", nil))
}
