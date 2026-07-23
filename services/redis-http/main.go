// HTTP key-value frontend for the Redis sub-replica chain reference service
// (services/redis-chain). Runs as the entry sidecar in each replica's pod.
// Same uniform surface as every XDN measurement shim:
//
//	GET  /            -> 200 once the local redis answers
//	PUT  /kv/{key}    -> write (SET on the chain head, replica-0)
//	GET  /kv/{key}    -> read  (GET on the local member)
//
// Sub-replicas are read-only, so writes must go to the head. Forwarding
// them there from every frontend is the honest chain shape: the client
// write travels frontend -> head, then relays down the chain, mirroring
// how rqlite forwards to its Raft leader internally.
package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	port := os.Getenv("XDN_CLUSTER_PEER_PORT")
	if port == "" {
		port = "6379"
	}
	ordinal := os.Getenv("XDN_CLUSTER_ORDINAL")
	local := redis.NewClient(&redis.Options{Addr: "127.0.0.1:" + port})
	head := local
	if ordinal != "0" {
		head = redis.NewClient(&redis.Options{Addr: "replica-0:" + port})
	}

	http.HandleFunc("/kv/", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		key := strings.TrimPrefix(r.URL.Path, "/kv/")
		switch r.Method {
		case http.MethodGet:
			v, err := local.Get(ctx, key).Bytes()
			if err == redis.Nil {
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
			if err := head.Set(ctx, key, body, 0).Err(); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			io.WriteString(w, "OK")
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := local.Ping(ctx).Err(); err != nil {
			http.Error(w, "warming up", http.StatusServiceUnavailable)
			return
		}
		io.WriteString(w, "ok redis-http")
	})
	log.Fatal(http.ListenAndServe(":8080", nil))
}
