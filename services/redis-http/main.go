// HTTP key-value frontend for the Redis sub-replica chain reference service
// (services/redis-chain). Runs as the entry sidecar in each replica's pod.
//
// The shim is deliberately DUMB: it translates HTTP to redis commands on
// the CO-LOCATED member (127.0.0.1) and nothing else — no topology
// awareness, no routing. Whatever the local member answers is the answer.
// Redis sub-replicas are read-only and do NOT forward writes to their
// master, so a PUT at a non-head replica returns the member's -READONLY
// error as a 500; that rejection is the service's own behavior and is
// exactly what a blackbox client would observe. Writes succeed only at the
// chain head (replica-0).
//
//	GET  /            -> 200 once the local redis answers
//	PUT  /kv/{key}    -> SET on the local member
//	GET  /kv/{key}    -> GET on the local member
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
	local := redis.NewClient(&redis.Options{Addr: "127.0.0.1:" + port})

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
			if err := local.Set(ctx, key, body, 0).Err(); err != nil {
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
