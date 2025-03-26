package data

import (
	"context"
	"log"

	"github.com/ThePlatypus-Person/hotelReservation/api/attractions"
	"github.com/ThePlatypus-Person/hotelReservation/api/geo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func GenerateHotels(collection *mongo.Collection, ctx context.Context) (*mongo.InsertManyResult, error) {
    log.Println("Generating hotels")
    collection.DeleteMany(ctx, bson.D{})

    newPoints := []interface{}{
        geo.Point{Pid: "1", Plat: 37.7867, Plon: -122.4112},
        geo.Point{Pid: "2", Plat: 37.7854, Plon: -122.4005},
        geo.Point{Pid: "3", Plat: 37.7854, Plon: -122.4071},
        geo.Point{Pid: "4", Plat: 37.7936, Plon: -122.3930},
        geo.Point{Pid: "5", Plat: 37.7831, Plon: -122.4181},
        geo.Point{Pid: "6", Plat: 37.7863, Plon: -122.4015},
    }

    result, err := collection.InsertMany(ctx, newPoints)
	if err != nil {
        log.Panicf("Failed to generate hotels")
	}
    log.Println("Successfully generated hotels")

    return result, err
}

func GenerateRestaurants(collection *mongo.Collection, ctx context.Context) (*mongo.InsertManyResult, error) {
    log.Println("Generating restaurants")
    collection.DeleteMany(ctx, bson.D{})

    newRestaurants := []interface{}{
        &attractions.Restaurant{RestaurantId: "1", RLat: 37.7867, RLon: -122.4112, RestaurantName: "R1", Rating: 3.5, Type: "fusion"},
        &attractions.Restaurant{RestaurantId: "2", RLat: 37.7857, RLon: -122.4012, RestaurantName: "R2", Rating: 3.9, Type: "italian"},
        &attractions.Restaurant{RestaurantId: "3", RLat: 37.7847, RLon: -122.3912, RestaurantName: "R3", Rating: 4.5, Type: "sushi"},
        &attractions.Restaurant{RestaurantId: "4", RLat: 37.7862, RLon: -122.4212, RestaurantName: "R4", Rating: 3.2, Type: "sushi"},
        &attractions.Restaurant{RestaurantId: "5", RLat: 37.7839, RLon: -122.4052, RestaurantName: "R5", Rating: 4.9, Type: "fusion"},
        &attractions.Restaurant{RestaurantId: "6", RLat: 37.7831, RLon: -122.3812, RestaurantName: "R6", Rating: 4.1, Type: "american"},
    }

    result, err := collection.InsertMany(ctx, newRestaurants)
	if err != nil {
        log.Panicf("Failed to generate restaurants")
	}
    log.Println("Successfully generated restaurants")

    return result, err
}

func GenerateMuseums(collection *mongo.Collection, ctx context.Context) (*mongo.InsertManyResult, error) {
    log.Println("Generating museums")
    collection.DeleteMany(ctx, bson.D{})

    newMuseums := []interface{}{
        &attractions.Museum{MuseumId: "1", MLat: 35.7867, MLon: -122.4112, MuseumName: "M1", Type: "history"},
        &attractions.Museum{MuseumId: "2", MLat: 36.7867, MLon: -122.5112, MuseumName: "M2", Type: "history"},
        &attractions.Museum{MuseumId: "3", MLat: 38.7867, MLon: -122.4612, MuseumName: "M3", Type: "nature"},
        &attractions.Museum{MuseumId: "4", MLat: 37.7867, MLon: -122.4912, MuseumName: "M4", Type: "nature"},
        &attractions.Museum{MuseumId: "5", MLat: 36.9867, MLon: -122.4212, MuseumName: "M5", Type: "nature"},
        &attractions.Museum{MuseumId: "6", MLat: 37.3867, MLon: -122.5012, MuseumName: "M6", Type: "technology"},
    }

    result, err := collection.InsertMany(ctx, newMuseums)
	if err != nil {
        log.Panicf("Failed to generate museums")
	}
    log.Println("Successfully generated museums")

    return result, err
}
