// A consolidated social-network service in the style of DeathStarBench's
// socialNetwork, deployed as a multi-container XDN service (this Go frontend
// plus a MongoDB backend declared in socialnetwork.yaml).
//
// All the original microservices are collapsed into this single HTTP server.
// Identifiers are assigned from a deterministic counter (not Mongo ObjectIDs)
// and no document stores a server-side wall-clock, so the replicated state is
// deterministic given the request order and the service can run under XDN with
// `--deterministic=true`.
package main

import (
	"context"
	_ "embed"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

//go:embed index.html
var indexHTML []byte

const dbName = "socialnetwork"

var (
	client *mongo.Client
	ctx    = context.Background()
)

func coll(name string) *mongo.Collection { return client.Database(dbName).Collection(name) }

func main() {
	port := "80"
	if p := os.Getenv("PORT"); p != "" {
		port = p
	}
	uri := os.Getenv("MONGO_URI")
	if uri == "" {
		uri = "mongodb://root:testing123@socialnetwork-db:27017"
	}

	log.Println("connecting to MongoDB…")
	var err error
	client, err = mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatalf("failed to connect to MongoDB: %v", err)
	}
	// Retry the initial ping so we tolerate the DB still starting up.
	for i := 0; i < 60; i++ {
		c, cancel := context.WithTimeout(ctx, 2*time.Second)
		err = client.Ping(c, nil)
		cancel()
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		log.Fatalf("MongoDB ping failed: %v", err)
	}
	log.Println("connected to MongoDB.")

	mux := http.NewServeMux()
	mux.HandleFunc("/", handleIndex)
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/init_db", handleInitDB)
	mux.HandleFunc("/register_user", handleRegisterUser)
	mux.HandleFunc("/compose_post", handleComposePost)
	mux.HandleFunc("/follow", handleFollow)
	mux.HandleFunc("/unfollow", handleUnfollow)
	mux.HandleFunc("/user_timeline", handleUserTimeline)
	mux.HandleFunc("/home_timeline", handleHomeTimeline)

	log.Printf("socialnetwork server running on :%s\n", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), mux))
}

// ----- helpers ---------------------------------------------------------------

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "use POST")
		return false
	}
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return false
	}
	return true
}

func hashPassword(p string) string {
	sum := sha256.Sum256([]byte(p))
	return hex.EncodeToString(sum[:])
}

// nextID returns a deterministic monotonically increasing id per kind, using an
// atomic $inc on a counter document (no Mongo ObjectIDs, no wall-clock). The
// default FindOneAndUpdate returns the pre-increment document, so the new id is
// (previous seq + 1).
func nextID(kind string) (int64, error) {
	var c struct {
		Seq int64 `bson:"seq"`
	}
	err := coll("counters").FindOneAndUpdate(ctx,
		bson.M{"_id": kind},
		bson.M{"$inc": bson.M{"seq": int64(1)}}).Decode(&c)
	if err == mongo.ErrNoDocuments {
		if _, ierr := coll("counters").InsertOne(ctx, bson.M{"_id": kind, "seq": int64(1)}); ierr != nil {
			return 0, ierr
		}
		return 1, nil
	}
	if err != nil {
		return 0, err
	}
	return c.Seq + 1, nil
}

func userExists(id int64) bool {
	err := coll("users").FindOne(ctx, bson.M{"_id": id}).Decode(&bson.M{})
	return err == nil
}

// ----- handlers --------------------------------------------------------------

func handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(indexHTML)
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	host, _ := os.Hostname()
	writeJSON(w, http.StatusOK, map[string]any{"health": true, "host": host})
}

// handleInitDB resets the dataset and creates users user1..userN (password
// "password"), seeding the id counters deterministically.
func handleInitDB(w http.ResponseWriter, r *http.Request) {
	var req struct {
		NumUsers int `json:"num_users"`
	}
	if r.Method == http.MethodPost {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	n := req.NumUsers
	if n <= 0 {
		n = 100
	}
	start := time.Now()
	for _, name := range []string{"users", "posts", "follows", "counters"} {
		if err := coll(name).Drop(ctx); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	docs := make([]any, 0, n)
	pw := hashPassword("password")
	for i := 1; i <= n; i++ {
		docs = append(docs, bson.M{"_id": int64(i), "username": fmt.Sprintf("user%d", i), "password": pw})
	}
	if n > 0 {
		if _, err := coll("users").InsertMany(ctx, docs); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	if _, err := coll("counters").InsertMany(ctx, []any{
		bson.M{"_id": "user", "seq": int64(n)},
		bson.M{"_id": "post", "seq": int64(0)},
	}); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"users": n, "took_ms": time.Since(start).Milliseconds()})
}

func handleRegisterUser(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if !decode(w, r, &req) {
		return
	}
	if req.Username == "" {
		writeErr(w, http.StatusBadRequest, "username required")
		return
	}
	if err := coll("users").FindOne(ctx, bson.M{"username": req.Username}).Decode(&bson.M{}); err == nil {
		writeErr(w, http.StatusConflict, "username taken")
		return
	}
	id, err := nextID("user")
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if _, err := coll("users").InsertOne(ctx, bson.M{"_id": id, "username": req.Username, "password": hashPassword(req.Password)}); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"user_id": id, "username": req.Username})
}

func handleComposePost(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UserID int64  `json:"user_id"`
		Text   string `json:"text"`
	}
	if !decode(w, r, &req) {
		return
	}
	if !userExists(req.UserID) {
		writeErr(w, http.StatusNotFound, "unknown user")
		return
	}
	id, err := nextID("post")
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if _, err := coll("posts").InsertOne(ctx, bson.M{"_id": id, "user_id": req.UserID, "text": req.Text}); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"post_id": id, "user_id": req.UserID})
}

func handleFollow(w http.ResponseWriter, r *http.Request) {
	var req struct {
		FollowerID int64 `json:"follower_id"`
		FolloweeID int64 `json:"followee_id"`
	}
	if !decode(w, r, &req) {
		return
	}
	if req.FollowerID == req.FolloweeID {
		writeErr(w, http.StatusBadRequest, "cannot follow yourself")
		return
	}
	if !userExists(req.FollowerID) || !userExists(req.FolloweeID) {
		writeErr(w, http.StatusNotFound, "unknown user")
		return
	}
	filter := bson.M{"follower_id": req.FollowerID, "followee_id": req.FolloweeID}
	if err := coll("follows").FindOne(ctx, filter).Decode(&bson.M{}); err == nil {
		writeJSON(w, http.StatusOK, map[string]any{"following": true, "already": true})
		return
	}
	if _, err := coll("follows").InsertOne(ctx, filter); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"following": true})
}

func handleUnfollow(w http.ResponseWriter, r *http.Request) {
	var req struct {
		FollowerID int64 `json:"follower_id"`
		FolloweeID int64 `json:"followee_id"`
	}
	if !decode(w, r, &req) {
		return
	}
	res, err := coll("follows").DeleteOne(ctx, bson.M{"follower_id": req.FollowerID, "followee_id": req.FolloweeID})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"following": false, "removed": res.DeletedCount})
}

func handleUserTimeline(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UserID int64 `json:"user_id"`
	}
	if !decode(w, r, &req) {
		return
	}
	writeJSON(w, http.StatusOK, timelineFor(w, bson.M{"user_id": req.UserID}))
}

func handleHomeTimeline(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UserID int64 `json:"user_id"`
	}
	if !decode(w, r, &req) {
		return
	}
	// Resolve the set of users this user follows.
	cur, err := coll("follows").Find(ctx, bson.M{"follower_id": req.UserID})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	var follows []struct {
		FolloweeID int64 `bson:"followee_id"`
	}
	if err := cur.All(ctx, &follows); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	ids := make([]int64, 0, len(follows))
	for _, f := range follows {
		ids = append(ids, f.FolloweeID)
	}
	writeJSON(w, http.StatusOK, timelineFor(w, bson.M{"user_id": bson.M{"$in": ids}}))
}

// timelineFor returns the posts matching filter, newest first (by descending id).
func timelineFor(w http.ResponseWriter, filter bson.M) map[string]any {
	opts := options.Find().SetSort(bson.D{{Key: "_id", Value: -1}}).SetLimit(50)
	cur, err := coll("posts").Find(ctx, filter, opts)
	if err != nil {
		return map[string]any{"posts": []any{}, "error": err.Error()}
	}
	var posts []struct {
		ID     int64  `bson:"_id"`
		UserID int64  `bson:"user_id"`
		Text   string `bson:"text"`
	}
	if err := cur.All(ctx, &posts); err != nil {
		return map[string]any{"posts": []any{}, "error": err.Error()}
	}
	out := make([]map[string]any, 0, len(posts))
	for _, p := range posts {
		out = append(out, map[string]any{"post_id": p.ID, "user_id": p.UserID, "text": p.Text})
	}
	return map[string]any{"posts": out, "count": len(out)}
}
