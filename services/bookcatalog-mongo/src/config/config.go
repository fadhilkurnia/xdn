package config

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var (
	client     *mongo.Client
	collection *mongo.Collection
	once       sync.Once
	connectErr error
)

// Connect initializes the MongoDB client and books collection.
func Connect() {
	once.Do(func() {
		uri := os.Getenv("MONGO_URI")
		if uri == "" {
			uri = "mongodb://localhost:27017/?directConnection=true"
		}
		dbName := os.Getenv("MONGO_DB")
		if dbName == "" {
			dbName = "books"
		}
		collectionName := os.Getenv("MONGO_COLLECTION")
		if collectionName == "" {
			collectionName = "books"
		}

		const maxAttempts = 5
		wait := 500 * time.Millisecond
		var lastErr error

		for attempt := 1; attempt <= maxAttempts; attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			cl, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
			if err == nil {
				err = cl.Ping(ctx, readpref.Primary())
			}
			cancel()

			if err != nil {
				lastErr = err
				log.Printf("MongoDB connection attempt %d/%d failed: %v", attempt, maxAttempts, err)
				time.Sleep(wait)
				wait *= 2
				continue
			}

			client = cl
			collection = client.Database(dbName).Collection(collectionName)
			log.Printf("Connected to MongoDB at %s (database=%s, collection=%s)", uri, dbName, collectionName)
			return
		}

		connectErr = fmt.Errorf("failed to connect to MongoDB after %d attempts: %w", maxAttempts, lastErr)
	})

	if connectErr != nil {
		panic(connectErr)
	}
}

// GetClient returns the MongoDB client instance.
func GetClient() *mongo.Client {
	if client == nil {
		Connect()
	}
	return client
}

// GetCollection returns the books collection.
func GetCollection() *mongo.Collection {
	if collection == nil {
		Connect()
	}
	return collection
}
