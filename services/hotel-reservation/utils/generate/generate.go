package generate

import (
	"context"
	"log"
	"sync"

	"github.com/ThePlatypus-Person/hotelReservation/utils/config"
	"github.com/ThePlatypus-Person/hotelReservation/utils/generate/data"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func GenerateDummyData(client *mongo.Client, ctx context.Context, config config.Config) error {
    log.Println("Generating dummy data...")

    var wg sync.WaitGroup
    errorCh := make(chan error, 10) // Buffer channel for errors

    // col = Collection
    // At = Attractions
    wg.Add(1)
    go func() {
        defer wg.Done()
        colAtHotels := client.Database(config.DbAttractions).Collection("hotels")
        data.GenerateHotels(colAtHotels, ctx)
    }()

    wg.Add(1)
    go func() {
        defer wg.Done()
        colAtRestaurants := client.Database(config.DbAttractions).Collection("restaurants")
        data.GenerateRestaurants(colAtRestaurants, ctx)
    }()

    wg.Add(1)
    go func() {
        defer wg.Done()
        colAtMuseums := client.Database(config.DbAttractions).Collection("museums")
        data.GenerateMuseums(colAtMuseums, ctx)
    }()

    // geo
    wg.Add(1)
    go func() {
        defer wg.Done()
        colGeo := client.Database(config.DbGeo).Collection("geo")
        data.GenerateGeos(colGeo, ctx)
    }()

    // Pro = Profile
    wg.Add(1)
    go func() {
        defer wg.Done()
        colProHotels := client.Database(config.DbProfile).Collection("hotels")
        data.GenerateProfileHotels(colProHotels, ctx)
    }()

    // Rate
    wg.Add(1)
    go func() {
        defer wg.Done()
        colRateInventory := client.Database(config.DbRate).Collection("inventory")
        data.GenerateInventory(colRateInventory, ctx)
    }()

    // Rec = Recommendation
    wg.Add(1)
    go func() {
        defer wg.Done()
        colRec := client.Database(config.DbRecommendation).Collection("recommendation")
        data.GenerateRecommendations(colRec, ctx)
    }()

    // Res = Reservation
    wg.Add(1)
    go func() {
        defer wg.Done()
        colResReservation := client.Database(config.DbReservation).Collection("reservation")
        data.GenerateReservations(colResReservation, ctx)
    }()

    wg.Add(1)
    go func() {
        defer wg.Done()
        colResNumber := client.Database(config.DbReservation).Collection("number")
        data.GenerateReservationNumbers(colResNumber, ctx)
    }()

    // Rev = Review
    wg.Add(1)
    go func() {
        defer wg.Done()
        colRev := client.Database(config.DbReview).Collection("reviews")
        data.GenerateReviews(colRev, ctx)
    }()

    // User
    wg.Add(1)
    go func() {
        defer wg.Done()
        colUser := client.Database(config.DbUser).Collection("user")
        data.GenerateUsers(colUser, ctx)
    }()

    // Wait for all goroutines to finish
    wg.Wait()

    // Close the error channel after all goroutines finish
    close(errorCh)

    // Handle any errors
    for err := range errorCh {
        if err != nil {
            log.Println("Error generating data:", err)
            return err;
        }
    }

    log.Println("Dummy data generation completed.")
    return nil
}

func ClearDatabaseAll(client *mongo.Client, ctx context.Context, config config.Config) error {
    log.Println("Clearing database...")

    var wg sync.WaitGroup
    errorCh := make(chan error, 10) // Buffer channel for errors

    // col = Collection
    // At = Attractions
    wg.Add(1)
    go func() {
        defer wg.Done()
        colAtHotels := client.Database(config.DbAttractions).Collection("hotels")
        ClearDatabase(colAtHotels, ctx)
    }()

    wg.Add(1)
    go func() {
        defer wg.Done()
        colAtRestaurants := client.Database(config.DbAttractions).Collection("restaurants")
        ClearDatabase(colAtRestaurants, ctx)
    }()

    wg.Add(1)
    go func() {
        defer wg.Done()
        colAtMuseums := client.Database(config.DbAttractions).Collection("museums")
        ClearDatabase(colAtMuseums, ctx)
    }()

    // geo
    wg.Add(1)
    go func() {
        defer wg.Done()
        colGeo := client.Database(config.DbGeo).Collection("geo")
        ClearDatabase(colGeo, ctx)
    }()

    // Pro = Profile
    wg.Add(1)
    go func() {
        defer wg.Done()
        colProHotels := client.Database(config.DbProfile).Collection("hotels")
        ClearDatabase(colProHotels, ctx)
    }()

    // Rate
    wg.Add(1)
    go func() {
        defer wg.Done()
        colRateInventory := client.Database(config.DbRate).Collection("inventory")
        ClearDatabase(colRateInventory, ctx)
    }()

    // Rec = Recommendation
    wg.Add(1)
    go func() {
        defer wg.Done()
        colRec := client.Database(config.DbRecommendation).Collection("recommendation")
        ClearDatabase(colRec, ctx)
    }()

    // Res = Reservation
    wg.Add(1)
    go func() {
        defer wg.Done()
        colResReservation := client.Database(config.DbReservation).Collection("reservation")
        ClearDatabase(colResReservation, ctx)
    }()

    wg.Add(1)
    go func() {
        defer wg.Done()
        colResNumber := client.Database(config.DbReservation).Collection("number")
        ClearDatabase(colResNumber, ctx)
    }()

    // Rev = Review
    wg.Add(1)
    go func() {
        defer wg.Done()
        colRev := client.Database(config.DbReview).Collection("reviews")
        ClearDatabase(colRev, ctx)
    }()

    // User
    wg.Add(1)
    go func() {
        defer wg.Done()
        colUser := client.Database(config.DbUser).Collection("user")
        ClearDatabase(colUser, ctx)
    }()

    // Wait for all goroutines to finish
    wg.Wait()

    // Close the error channel after all goroutines finish
    close(errorCh)

    // Handle any errors
    for err := range errorCh {
        if err != nil {
            log.Println("Error clearing databases:", err)
            return err;
        }
    }

    log.Println("Database clearing process completed.")
    return nil
}


func ClearDatabase(collection *mongo.Collection, ctx context.Context) (*mongo.DeleteResult, error) {
    log.Printf("Clearing %v\n", collection.Name())
    return collection.DeleteMany(ctx, bson.D{})
}

