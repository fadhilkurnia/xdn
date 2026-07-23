// HTTP key-value frontend for the MongoDB replica-set reference service
// (services/mongo-cluster). Runs as the entry sidecar in each replica's
// pod.
//
// The shim is deliberately DUMB: directConnection to the CO-LOCATED member
// (127.0.0.1) only — no replica-set discovery, no routing. MongoDB pushes
// routing to clients by design (mongod never forwards writes), so a PUT at
// a secondary returns the member's NotWritablePrimary error as a 500;
// writes succeed only at whichever member currently holds the primary
// role. Reads use readPreference=nearest so the local member serves them
// even while SECONDARY.
//
//	GET  /            -> 200 once the local member answers
//	PUT  /kv/{key}    -> write on the local member (w:majority; fails if SECONDARY)
//	GET  /kv/{key}    -> read on the local member
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
	uri := "mongodb://127.0.0.1:" + port + "/?directConnection=true&w=majority"
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatal(err)
	}
	db := client.Database("bw", options.Database().SetReadPreference(readpref.Nearest()))
	coll := db.Collection("kv")

	http.HandleFunc("/kv/", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		key := strings.TrimPrefix(r.URL.Path, "/kv/")
		switch r.Method {
		case http.MethodGet:
			var doc struct {
				V []byte `bson:"v"`
			}
			err := coll.FindOne(ctx, bson.M{"_id": key}).Decode(&doc)
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
		// Ready only when the local member belongs to an initialized replica
		// set (PRIMARY or SECONDARY) — a bare ping succeeds before
		// rs.initiate has reached this member, which is too early.
		var hello bson.M
		err := client.Database("admin").
			RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello)
		if err != nil || hello["setName"] == nil ||
			(hello["isWritablePrimary"] != true && hello["secondary"] != true) {
			http.Error(w, "warming up", http.StatusServiceUnavailable)
			return
		}
		io.WriteString(w, "ok mongo-http")
	})
	log.Fatal(http.ListenAndServe(":8080", nil))
}
