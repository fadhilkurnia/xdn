package data

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log"
	"strconv"

	"github.com/ThePlatypus-Person/hotelReservation/api/user"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func GenerateUsers(collection *mongo.Collection, ctx context.Context) (*mongo.InsertManyResult, error) {
    log.Println("Generating users")
    collection.DeleteMany(ctx, bson.D{})

    newUsers := getNewUsers()

    result, err := collection.InsertMany(ctx, newUsers)
	if err != nil {
        log.Panicf("Failed to generate users")
	}
    log.Println("Successfully generated users")

    return result, err
}

func getNewUsers() ([]interface{}) {
    newUsers := []interface{}{}

    for i := 0; i <= 500; i++ {
        suffix := strconv.Itoa(i)

        password := ""
        for j := 0; j < 10; j++ {
            password += suffix
        }
        sum := sha256.Sum256([]byte(password))

        newUsers = append(newUsers, user.User{
            Username: fmt.Sprintf("Cornell_%x", suffix),
            Password: fmt.Sprintf("%x", sum),
        })
    }

    sum := sha256.Sum256([]byte("123"))
    newUsers = append(newUsers, user.User{
        Username: "admin",
        Password: fmt.Sprintf("%x", sum),
    })

    return newUsers
}
