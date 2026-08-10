// HTTP key-value frontend for the Cassandra (leaderless quorum) reference
// service (services/cassandra-cluster). Runs as the entry sidecar in each
// replica's pod. Same uniform surface as every XDN measurement shim:
//
//	GET  /            -> 200 once the local node coordinates queries
//	PUT  /kv/{key}    -> QUORUM write via the local node as coordinator
//	GET  /kv/{key}    -> QUORUM read via the local node as coordinator
//
// The session is pinned to 127.0.0.1 with host lookup disabled so the LOCAL
// member is always the coordinator; every request then fans out to the
// replica set from here, which is the signature leaderless shape.
package main

import (
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
)

func connect() *gocql.Session {
	// Replication factor defaults to the cluster size (full fan-out); set
	// XDN_CASS_RF for partial-quorum shapes (e.g. RF 3 on a 5-node ring).
	rf := os.Getenv("XDN_CASS_RF")
	if rf == "" {
		rf = os.Getenv("XDN_CLUSTER_SIZE")
	}
	if rf == "" {
		rf = "3"
	}
	for {
		c := gocql.NewCluster("127.0.0.1")
		c.Port = 9042
		c.Consistency = gocql.Quorum
		c.DisableInitialHostLookup = true
		c.Timeout = 10 * time.Second
		c.ConnectTimeout = 5 * time.Second
		s, err := c.CreateSession()
		if err == nil {
			if err = s.Query("CREATE KEYSPACE IF NOT EXISTS bw WITH replication =" +
				" {'class':'NetworkTopologyStrategy','dc1':" + rf + "}").Exec(); err == nil {
				if err = s.Query("CREATE TABLE IF NOT EXISTS bw.kv" +
					" (k text PRIMARY KEY, v blob)").Exec(); err == nil {
					return s
				}
			}
			s.Close()
		}
		log.Printf("waiting for cassandra: %v", err)
		time.Sleep(3 * time.Second)
	}
}

func main() {
	self := os.Getenv("XDN_CLUSTER_SELF")
	var mu sync.Mutex
	var session *gocql.Session
	go func() {
		s := connect()
		mu.Lock()
		session = s
		mu.Unlock()
		log.Printf("%s: backend ready", self)
	}()
	get := func() *gocql.Session {
		mu.Lock()
		defer mu.Unlock()
		return session
	}

	http.HandleFunc("/kv/", func(w http.ResponseWriter, r *http.Request) {
		s := get()
		if s == nil {
			http.Error(w, "warming up", http.StatusServiceUnavailable)
			return
		}
		key := strings.TrimPrefix(r.URL.Path, "/kv/")
		switch r.Method {
		case http.MethodGet:
			var v []byte
			err := s.Query("SELECT v FROM bw.kv WHERE k=?", key).Scan(&v)
			if err == gocql.ErrNotFound {
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
			if err := s.Query("INSERT INTO bw.kv (k, v) VALUES (?, ?)", key, body).Exec(); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			io.WriteString(w, "OK")
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if get() == nil {
			http.Error(w, "warming up", http.StatusServiceUnavailable)
			return
		}
		io.WriteString(w, "ok cassandra-http")
	})
	log.Fatal(http.ListenAndServe(":8080", nil))
}
