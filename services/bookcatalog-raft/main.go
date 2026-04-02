package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/arpesam/go-book-api/src/config"
	"github.com/arpesam/go-book-api/src/controllers"
	"github.com/arpesam/go-book-api/src/models"
	"github.com/arpesam/go-book-api/src/raftnode"
	"github.com/arpesam/go-book-api/src/routes"
	"github.com/gorilla/mux"
)

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		duration := time.Since(start)
		log.Printf("%s %s [lat=%v]", r.Method, r.URL.Path, duration)
	})
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "80"
	}

	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		log.Fatal("NODE_ID is required")
	}

	raftAddr := os.Getenv("RAFT_ADDR")
	if raftAddr == "" {
		log.Fatal("RAFT_ADDR is required")
	}

	raftPeersStr := os.Getenv("RAFT_PEERS")
	if raftPeersStr == "" {
		log.Fatal("RAFT_PEERS is required (format: id1=addr1,id2=addr2,...)")
	}

	raftDir := os.Getenv("RAFT_DIR")
	if raftDir == "" {
		raftDir = "/app/raft-data"
	}

	// Parse peers: "node1=host1:7000,node2=host2:7000,..."
	var peers []raftnode.Peer
	for _, entry := range strings.Split(raftPeersStr, ",") {
		parts := strings.SplitN(strings.TrimSpace(entry), "=", 2)
		if len(parts) != 2 {
			log.Fatalf("invalid RAFT_PEERS entry: %q", entry)
		}
		peers = append(peers, raftnode.Peer{ID: parts[0], Address: parts[1]})
	}

	// 1. Init SQLite
	config.Connect()

	// 2. Init GORM models
	models.Init(config.GetDB())

	// 3. Init Raft cluster
	rn, err := raftnode.NewRaftNode(nodeID, raftAddr, raftDir, peers, config.GetDB(), config.DBFilePath)
	if err != nil {
		log.Fatalf("failed to start raft node: %v", err)
	}
	log.Printf("Raft node %s started, listening on %s", nodeID, raftAddr)

	// 4. Inject Raft into controllers
	controllers.SetRaftNode(rn)

	// 5. Start HTTP server
	r := mux.NewRouter()
	routes.RegisterBookStoreRoutes(r)
	routes.RegisterViewRoutes(r)
	loggedRoutes := loggingMiddleware(r)
	http.Handle("/", loggedRoutes)

	log.Printf("Running server on :%s", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), loggedRoutes))
}
