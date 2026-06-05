// A consolidated movie-review service in the style of DeathStarBench's
// media-microservices, deployed as a multi-container XDN service (this Go
// frontend plus a MongoDB backend declared in moviereview.yaml).
//
// All the original microservices are collapsed into this single HTTP server.
// Identifiers are assigned from a deterministic counter (not Mongo ObjectIDs)
// and no document stores a server-side wall-clock, so the replicated state is
// deterministic given the request order and the service can run under XDN with
// `--deterministic=true`.
package main

import (
	"context"
	"crypto/sha256"
	_ "embed"
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

const dbName = "moviereview"

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
		uri = "mongodb://root:testing123@moviereview-db:27017"
	}

	log.Println("connecting to MongoDB…")
	var err error
	client, err = mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatalf("failed to connect to MongoDB: %v", err)
	}
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
	mux.HandleFunc("/compose_review", handleComposeReview)
	mux.HandleFunc("/movie_reviews", handleMovieReviews)
	mux.HandleFunc("/user_reviews", handleUserReviews)
	mux.HandleFunc("/movie_info", handleMovieInfo)

	log.Printf("moviereview server running on :%s\n", port)
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

func exists(collection string, id int64) bool {
	err := coll(collection).FindOne(ctx, bson.M{"_id": id}).Decode(&bson.M{})
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

// handleInitDB resets the dataset and creates movies "Movie 1".."Movie M" and
// users user1..userN, seeding the id counters deterministically.
func handleInitDB(w http.ResponseWriter, r *http.Request) {
	var req struct {
		NumMovies int `json:"num_movies"`
		NumUsers  int `json:"num_users"`
	}
	if r.Method == http.MethodPost {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	m, n := req.NumMovies, req.NumUsers
	if m <= 0 {
		m = 50
	}
	if n <= 0 {
		n = 100
	}
	start := time.Now()
	for _, name := range []string{"movies", "users", "reviews", "counters"} {
		if err := coll(name).Drop(ctx); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	movieDocs := make([]any, 0, m)
	for i := 1; i <= m; i++ {
		movieDocs = append(movieDocs, bson.M{"_id": int64(i), "title": fmt.Sprintf("Movie %d", i)})
	}
	if _, err := coll("movies").InsertMany(ctx, movieDocs); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	userDocs := make([]any, 0, n)
	pw := hashPassword("password")
	for i := 1; i <= n; i++ {
		userDocs = append(userDocs, bson.M{"_id": int64(i), "username": fmt.Sprintf("user%d", i), "password": pw})
	}
	if _, err := coll("users").InsertMany(ctx, userDocs); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if _, err := coll("counters").InsertMany(ctx, []any{
		bson.M{"_id": "movie", "seq": int64(m)},
		bson.M{"_id": "user", "seq": int64(n)},
		bson.M{"_id": "review", "seq": int64(0)},
	}); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"movies": m, "users": n, "took_ms": time.Since(start).Milliseconds()})
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

// ComposeReview: a user rates a movie (1-5) with optional text.
func handleComposeReview(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UserID  int64  `json:"user_id"`
		MovieID int64  `json:"movie_id"`
		Rating  int    `json:"rating"`
		Text    string `json:"text"`
	}
	if !decode(w, r, &req) {
		return
	}
	if req.Rating < 1 || req.Rating > 5 {
		writeErr(w, http.StatusBadRequest, "rating must be 1-5")
		return
	}
	if !exists("users", req.UserID) {
		writeErr(w, http.StatusNotFound, "unknown user")
		return
	}
	if !exists("movies", req.MovieID) {
		writeErr(w, http.StatusNotFound, "unknown movie")
		return
	}
	id, err := nextID("review")
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if _, err := coll("reviews").InsertOne(ctx, bson.M{
		"_id": id, "user_id": req.UserID, "movie_id": req.MovieID, "rating": req.Rating, "text": req.Text,
	}); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"review_id": id, "movie_id": req.MovieID, "rating": req.Rating})
}

func handleMovieReviews(w http.ResponseWriter, r *http.Request) {
	var req struct {
		MovieID int64 `json:"movie_id"`
	}
	if !decode(w, r, &req) {
		return
	}
	writeJSON(w, http.StatusOK, reviewsFor(bson.M{"movie_id": req.MovieID}))
}

func handleUserReviews(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UserID int64 `json:"user_id"`
	}
	if !decode(w, r, &req) {
		return
	}
	writeJSON(w, http.StatusOK, reviewsFor(bson.M{"user_id": req.UserID}))
}

// MovieInfo (read-only): a movie's title plus its review count and average rating.
func handleMovieInfo(w http.ResponseWriter, r *http.Request) {
	var req struct {
		MovieID int64 `json:"movie_id"`
	}
	if !decode(w, r, &req) {
		return
	}
	var movie struct {
		ID    int64  `bson:"_id"`
		Title string `bson:"title"`
	}
	if err := coll("movies").FindOne(ctx, bson.M{"_id": req.MovieID}).Decode(&movie); err != nil {
		writeErr(w, http.StatusNotFound, "unknown movie")
		return
	}
	res := reviewsFor(bson.M{"movie_id": req.MovieID})
	avg := 0.0
	if n := res["count"].(int); n > 0 {
		sum := 0
		for _, rv := range res["reviews"].([]map[string]any) {
			sum += rv["rating"].(int)
		}
		avg = float64(sum) / float64(n)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"movie_id": movie.ID, "title": movie.Title,
		"num_reviews": res["count"], "avg_rating": avg,
	})
}

// reviewsFor returns the reviews matching filter, newest first (by descending id).
func reviewsFor(filter bson.M) map[string]any {
	opts := options.Find().SetSort(bson.D{{Key: "_id", Value: -1}}).SetLimit(50)
	cur, err := coll("reviews").Find(ctx, filter, opts)
	if err != nil {
		return map[string]any{"reviews": []map[string]any{}, "count": 0, "error": err.Error()}
	}
	var reviews []struct {
		ID      int64  `bson:"_id"`
		UserID  int64  `bson:"user_id"`
		MovieID int64  `bson:"movie_id"`
		Rating  int    `bson:"rating"`
		Text    string `bson:"text"`
	}
	if err := cur.All(ctx, &reviews); err != nil {
		return map[string]any{"reviews": []map[string]any{}, "count": 0, "error": err.Error()}
	}
	out := make([]map[string]any, 0, len(reviews))
	for _, rv := range reviews {
		out = append(out, map[string]any{
			"review_id": rv.ID, "user_id": rv.UserID, "movie_id": rv.MovieID, "rating": rv.Rating, "text": rv.Text,
		})
	}
	return map[string]any{"reviews": out, "count": len(out)}
}
