package data

import (
	"context"
	"log"

	"github.com/ThePlatypus-Person/hotelReservation/api/review"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)


func GenerateReviews(collection *mongo.Collection, ctx context.Context) (*mongo.InsertManyResult, error) {
    log.Println("Generating reviews")
    collection.DeleteMany(ctx, bson.D{})

    newReviews := []interface{}{
        &review.Review{
            ReviewId: "1",
            HotelId: "1",
            Name: "Person 1",
            Rating: 3.4,
            Description: "A 6-minute walk from Union Square and 4 minutes from a Muni Metro station, this luxury hotel designed by Philippe Starck features an artsy furniture collection in the lobby, including work by Salvador Dali.",
            Image: &review.Image{
                Url: "some url",
                Default: false,
            },
        },
        &review.Review{
            ReviewId: "2",
            HotelId: "1",
            Name: "Person 2",
            Rating: 4.4,
            Description: "A 6-minute walk from Union Square and 4 minutes from a Muni Metro station, this luxury hotel designed by Philippe Starck features an artsy furniture collection in the lobby, including work by Salvador Dali.",
            Image: &review.Image{
                Url: "some url",
                Default: false,
            },
        },
        &review.Review{
            ReviewId: "3",
            HotelId: "1",
            Name: "Person 3",
            Rating: 4.2,
            Description: "A 6-minute walk from Union Square and 4 minutes from a Muni Metro station, this luxury hotel designed by Philippe Starck features an artsy furniture collection in the lobby, including work by Salvador Dali.",
            Image: &review.Image{
                Url: "some url",
                Default: false,
            },
        },
        &review.Review{
            ReviewId: "4",
            HotelId: "1",
            Name: "Person 4",
            Rating: 3.9,
            Description: "A 6-minute walk from Union Square and 4 minutes from a Muni Metro station, this luxury hotel designed by Philippe Starck features an artsy furniture collection in the lobby, including work by Salvador Dali.",
            Image: &review.Image{
                Url: "some url",
                Default: false,
            },
        },
        &review.Review{
            ReviewId: "5",
            HotelId: "2",
            Name: "Person 5",
            Rating: 4.2,
            Description: "A 6-minute walk from Union Square and 4 minutes from a Muni Metro station, this luxury hotel designed by Philippe Starck features an artsy furniture collection in the lobby, including work by Salvador Dali.",
            Image: &review.Image{
                Url: "some url",
                Default: false,
            },
        },
        &review.Review{
            ReviewId: "6",
            HotelId: "2",
            Name: "Person 6",
            Rating: 3.7,
            Description: "A 6-minute walk from Union Square and 4 minutes from a Muni Metro station, this luxury hotel designed by Philippe Starck features an artsy furniture collection in the lobby, including work by Salvador Dali.",
            Image: &review.Image{
                Url: "some url",
                Default: false,
            },
        },
    }

    result, err := collection.InsertMany(ctx, newReviews)

    if err != nil {
        log.Panicf("Failed to generate reviews")
    }
    log.Println("Successfully generated reviews")

    return result, err
}
