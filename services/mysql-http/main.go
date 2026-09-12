// HTTP key-value frontend for the self-clustering MySQL (Group
// Replication) reference service. Runs as the entry sidecar in each
// replica's pod, sharing the cluster member's network namespace.
//
// The shim is deliberately DUMB: it translates HTTP to SQL on the
// CO-LOCATED member (127.0.0.1) and nothing else — no primary discovery,
// no routing. mysqld never forwards writes, so in single-primary GR a PUT
// at a secondary returns the member's read-only error as a 500. The
// mysqlkv pod therefore runs the group in MULTI-PRIMARY mode
// (XDN_MYSQL_MULTI_PRIMARY=1 on the member), a first-class GR deployment
// shape where every member accepts writes under certification; that keeps
// the routing question inside the service where it belongs.
//
//	GET  /            -> 200 once the local member accepts writes
//	PUT  /kv/{key}    -> REPLACE INTO on the local member
//	GET  /kv/{key}    -> SELECT on the local member
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

func main() {
	self := os.Getenv("XDN_CLUSTER_SELF")
	dsn := fmt.Sprintf("root:%s@tcp(127.0.0.1:3306)/?timeout=5s&readTimeout=10s&writeTimeout=10s",
		os.Getenv("MYSQL_ROOT_PASSWORD"))
	local, _ := sql.Open("mysql", dsn)
	local.SetMaxOpenConns(8)

	ready := false
	var readyMu sync.Mutex
	go func() {
		// Ready = the replicated KV schema is visible on the local member.
		// STRICTLY read-only: the schema is created in-group by the member
		// entrypoint (ordinal 0, XDN_MYSQL_KV_TABLE=1) — a shim issuing DDL
		// here races group configuration and can permanently diverge a
		// joiner's GTID history (writable window before START
		// GROUP_REPLICATION engages super_read_only).
		for {
			var n int
			if err := local.QueryRow("SELECT COUNT(*) FROM bw.kv").Scan(&n); err == nil {
				readyMu.Lock()
				ready = true
				readyMu.Unlock()
				log.Printf("%s: backend ready (schema replicated)", self)
				return
			}
			time.Sleep(2 * time.Second)
		}
	}()

	http.HandleFunc("/kv/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/kv/")
		switch r.Method {
		case http.MethodGet:
			var v []byte
			err := local.QueryRow("SELECT v FROM bw.kv WHERE k=?", key).Scan(&v)
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
			if _, err := local.Exec("REPLACE INTO bw.kv VALUES (?, ?)", key, body); err != nil {
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
