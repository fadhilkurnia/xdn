package models

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"bookcatalogmongo/src/config"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

var (
	// ErrBookNotFound is returned when a document lookup yields no results.
	ErrBookNotFound = errors.New("book not found")
)

const defaultTimeout = 5 * time.Second

var (
	booksCollection      *mongo.Collection
	supportsTransactions bool
)

func init() {
	config.Connect()
	booksCollection = config.GetCollection()
	supportsTransactions = detectTransactionSupport(config.GetClient())
	log.Printf("MongoDB transaction support: %v", supportsTransactions)
}

func detectTransactionSupport(client *mongo.Client) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var result bson.M
	if err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&result); err != nil {
		return false
	}

	if msg, ok := result["msg"].(string); ok && msg == "isdbgrid" {
		return true
	}
	if _, ok := result["setName"]; ok {
		return true
	}
	return false
}

func runInTransaction(ctx context.Context, fn func(context.Context) (interface{}, error)) (interface{}, error) {
	if !supportsTransactions {
		return fn(ctx)
	}

	client := config.GetClient()
	session, err := client.StartSession()
	if err != nil {
		return nil, err
	}
	defer session.EndSession(ctx)

	txnOpts := options.Transaction().
		SetWriteConcern(writeconcern.Journaled())
	result, err := session.WithTransaction(ctx, func(sc mongo.SessionContext) (interface{}, error) {
		return fn(sc)
	}, txnOpts)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// Book represents the MongoDB document schema.
type Book struct {
	ID     primitive.ObjectID `bson:"_id,omitempty"`
	Title  string             `bson:"title"`
	Author string             `bson:"author"`
}

// BookInput captures fields required to create a new book.
type BookInput struct {
	Title  string `json:"title"`
	Author string `json:"author"`
}

// BookUpdate captures fields that can be updated on an existing book.
type BookUpdate struct {
	Title  *string `json:"title,omitempty"`
	Author *string `json:"author,omitempty"`
}

// BookDTO represents the JSON payload returned by the API.
type BookDTO struct {
	ID     string `json:"id"`
	Title  string `json:"title"`
	Author string `json:"author"`
}

func contextWithTimeout() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), defaultTimeout)
}

func toDTO(book *Book) BookDTO {
	id := ""
	if book.ID != primitive.NilObjectID {
		id = book.ID.Hex()
	}
	return BookDTO{
		ID:     id,
		Title:  book.Title,
		Author: book.Author,
	}
}

func toDTOs(books []Book) []BookDTO {
	result := make([]BookDTO, 0, len(books))
	for i := range books {
		result = append(result, toDTO(&books[i]))
	}
	return result
}

// CreateBook inserts a new book document and returns the created record.
func CreateBook(input *BookInput) (*BookDTO, error) {
	if input == nil {
		return nil, errors.New("input cannot be nil")
	}

	doc := Book{
		Title:  input.Title,
		Author: input.Author,
	}

	ctx, cancel := contextWithTimeout()
	defer cancel()

	result, err := runInTransaction(ctx, func(txCtx context.Context) (interface{}, error) {
		res, err := booksCollection.InsertOne(txCtx, doc)
		if err != nil {
			return nil, err
		}
		return res.InsertedID, nil
	})
	if err != nil {
		return nil, err
	}

	insertedID, ok := result.(primitive.ObjectID)
	if !ok {
		return nil, fmt.Errorf("unexpected inserted ID type %T", result)
	}

	doc.ID = insertedID
	dto := toDTO(&doc)
	return &dto, nil
}

// GetAllBooks returns all books in the collection.
func GetAllBooks() ([]BookDTO, error) {
	ctx, cancel := contextWithTimeout()
	defer cancel()

	cursor, err := booksCollection.Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var books []Book
	if err = cursor.All(ctx, &books); err != nil {
		return nil, err
	}

	dtos := toDTOs(books)
	return dtos, nil
}

// GetBookByID returns a single book by its ObjectID hex string.
func GetBookByID(id string) (*BookDTO, error) {
	objectID, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, err
	}

	ctx, cancel := contextWithTimeout()
	defer cancel()

	var book Book
	err = booksCollection.FindOne(ctx, bson.M{"_id": objectID}).Decode(&book)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrBookNotFound
		}
		return nil, err
	}

	dto := toDTO(&book)
	return &dto, nil
}

// UpdateBook updates an existing book and returns the updated record.
func UpdateBook(id string, update *BookUpdate) (*BookDTO, error) {
	objectID, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, err
	}

	if update == nil {
		return GetBookByID(id)
	}

	set := bson.M{}
	if update.Title != nil {
		set["title"] = *update.Title
	}
	if update.Author != nil {
		set["author"] = *update.Author
	}

	if len(set) == 0 {
		return GetBookByID(id)
	}

	ctx, cancel := contextWithTimeout()
	defer cancel()

	result, err := runInTransaction(ctx, func(txCtx context.Context) (interface{}, error) {
		opts := options.FindOneAndUpdate().SetReturnDocument(options.After)
		updated := &Book{}
		if err := booksCollection.FindOneAndUpdate(txCtx, bson.M{"_id": objectID}, bson.M{"$set": set}, opts).Decode(updated); err != nil {
			return nil, err
		}
		return updated, nil
	})
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrBookNotFound
		}
		return nil, err
	}

	updated, ok := result.(*Book)
	if !ok || updated == nil {
		return nil, fmt.Errorf("unexpected update result type %T", result)
	}

	dto := toDTO(updated)
	return &dto, nil
}

// DeleteBook removes a book and returns the deleted record.
func DeleteBook(id string) (*BookDTO, error) {
	objectID, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, err
	}

	ctx, cancel := contextWithTimeout()
	defer cancel()

	result, err := runInTransaction(ctx, func(txCtx context.Context) (interface{}, error) {
		deleted := &Book{}
		if err := booksCollection.FindOneAndDelete(txCtx, bson.M{"_id": objectID}).Decode(deleted); err != nil {
			return nil, err
		}
		return deleted, nil
	})
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrBookNotFound
		}
		return nil, err
	}

	deleted, ok := result.(*Book)
	if !ok || deleted == nil {
		return nil, fmt.Errorf("unexpected delete result type %T", result)
	}

	dto := toDTO(deleted)
	return &dto, nil
}
