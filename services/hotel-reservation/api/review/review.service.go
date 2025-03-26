package review

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type ReviewService struct {
    database *mongo.Database
    ctx context.Context
}

func NewReviewSevice(database *mongo.Database, ctx context.Context) *ReviewService {
    return &ReviewService{
        database: database,
        ctx: ctx,
    }
}

func (s *ReviewService) GetReviews(hotelId string) ([]Review, error) {
    curr, err := s.database.Collection("reviews").Find(s.ctx, bson.M{"hotelId": hotelId})
    if err != nil {
        return nil, err
    }

    reviews := make([]Review, 0)
    err = curr.All(context.TODO(), &reviews)
    if err != nil {
        return nil, err
    }

	return reviews, nil
}
