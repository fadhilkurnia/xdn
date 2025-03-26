package user

import (
	"context"
	"crypto/sha256"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type UserService struct {
    database *mongo.Database
    ctx context.Context
}

func NewUserService(database *mongo.Database, ctx context.Context) *UserService {
    return &UserService{
        database: database,
        ctx: ctx,
    }
}

func (s *UserService) CheckUser(username, password string) bool {
    // Verify Password
    sum := sha256.Sum256([]byte(password))
	pass := fmt.Sprintf("%x", sum)

    valid_password := false
    var user User
    filter := bson.D{{ "username", username }}
    err := s.database.Collection("user").FindOne(s.ctx, filter).Decode(&user)

    if err == nil {
        valid_password = (pass == user.Password)
    }
    
    return valid_password
}
