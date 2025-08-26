package data

import (
	"context"
	"log"
	"strconv"

	"github.com/ThePlatypus-Person/hotelReservation/api/reservation"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func GenerateReservations(collection *mongo.Collection, ctx context.Context) (*mongo.InsertManyResult, error) {
    log.Println("Generating reservations")
    collection.DeleteMany(ctx, bson.D{})

    newReservations := []interface{}{
        reservation.Reservation{HotelId: "4", CustomerName: "Alice", InDate: "2015-04-09", OutDate: "2015-04-10", Number: 1},
    }

    result, err := collection.InsertMany(ctx, newReservations)
	if err != nil {
        log.Panicf("Failed to generate reservations")
	}
    log.Println("Successfully generated reservations")

    return result, err
}

func GenerateReservationNumbers(collection *mongo.Collection, ctx context.Context) (*mongo.InsertManyResult, error) {
    log.Println("Generating reservation numbers")
    collection.DeleteMany(ctx, bson.D{})

    newNumbers := getReservationNumbers()

    result, err := collection.InsertMany(ctx, newNumbers)
	if err != nil {
        log.Panicf("Failed to generate reservation numbers")
	}
    log.Println("Successfully generated reservation numbers")

    return result, err
}

func getReservationNumbers() ([]interface{}) {
    newNumbers := []interface{}{
        reservation.Number{HotelId: "1", Number: 200},
        reservation.Number{HotelId: "2", Number: 200},
        reservation.Number{HotelId: "3", Number: 200},
        reservation.Number{HotelId: "4", Number: 200},
        reservation.Number{HotelId: "5", Number: 200},
        reservation.Number{HotelId: "6", Number: 200},
    }

    for i := 7; i <= 80; i++ {
        hotelID := strconv.Itoa(i)

        roomNumber := 200
        if i%3 == 1 {
            roomNumber = 300
        } else if i%3 == 2 {
            roomNumber = 250
        }

        newNumbers = append(newNumbers, reservation.Number{HotelId: hotelID, Number: roomNumber})
    }

    return newNumbers
}
