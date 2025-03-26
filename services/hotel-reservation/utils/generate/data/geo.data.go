package data

import (
	"context"
	"log"

	"github.com/ThePlatypus-Person/hotelReservation/api/geo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)


func GenerateGeos(collection *mongo.Collection, ctx context.Context) (*mongo.InsertManyResult, error) {
    log.Println("Generating geos")
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
        log.Panicf("Failed to generate geos")
	}
    log.Println("Successfully generated geos")

    return result, err
}
