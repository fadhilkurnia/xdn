// HTTP key-value frontend for the MongoDB replica-set reference service
// (services/mongo-cluster). Runs as the entry sidecar in each replica's
// pod. Same uniform surface as every XDN measurement shim:
//
//	GET  /            -> 200 once the replica set answers
//	PUT  /kv/{key}    -> write, w:majority (driver routes to the primary)
//	GET  /kv/{key}    -> read, readPreference=nearest (the local member)
//
// The driver is seeded with the local member and discovers the replica set
// (member hosts are replica-N overlay aliases, dialable through embedded
// DNS), so writes reach the primary wherever it currently is, and reads
// stay local. That split is the signature to measure: per-write oplog +
// ack traffic on the overlay, wire-free reads.
package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func main() {
	port := os.Getenv("XDN_CLUSTER_PEER_PORT")
	if port == "" {
		port = "27017"
	}
	uri := "mongodb://127.0.0.1:" + port + "/?replicaSet=rs0&w=majority&readConcernLevel=majority"
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatal(err)
	}
	coll := client.Database("bw").Collection("kv")
	readColl := coll // reads pinned to the nearest (local) member
	if db := client.Database("bw", options.Database().SetReadPreference(readpref.Nearest())); db != nil {
		readColl = db.Collection("kv")
	}

	http.HandleFunc("/kv/", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		key := strings.TrimPrefix(r.URL.Path, "/kv/")
		switch r.Method {
		case http.MethodGet:
			var doc struct {
				V []byte `bson:"v"`
			}
			err := readColl.FindOne(ctx, bson.M{"_id": key}).Decode(&doc)
			if err == mongo.ErrNoDocuments {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Write(doc.V)
		case http.MethodPut, http.MethodPost:
			body, _ := io.ReadAll(r.Body)
			_, err := coll.ReplaceOne(ctx, bson.M{"_id": key},
				bson.M{"_id": key, "v": body}, options.Replace().SetUpsert(true))
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
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := client.Ping(ctx, readpref.Nearest()); err != nil {
			http.Error(w, "warming up", http.StatusServiceUnavailable)
			return
		}
		io.WriteString(w, "ok mongo-http")
	})
	log.Fatal(http.ListenAndServe(":8080", nil))
}
