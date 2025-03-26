package data

import (
	"strconv"
	"context"
	"log"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"github.com/ThePlatypus-Person/hotelReservation/api/hotels"
)

func GenerateRecommendations(collection *mongo.Collection, ctx context.Context) (*mongo.InsertManyResult, error) {
    log.Println("Generating recommendations")
    collection.DeleteMany(ctx, bson.D{})

    newHotels := getNewHotels()

    result, err := collection.InsertMany(ctx, newHotels)
	if err != nil {
        log.Panicf("Failed to generate recommendations")
	}
    log.Println("Successfully generated recommendations")

    return result, err
}

func getNewHotels() ([]interface{}) {
    newHotels := []interface{}{
        hotels.HotelData{HId: "1", HLat: 37.7867, HLon: -122.4112, HRate: 109.00, HPrice: 150.00},
        hotels.HotelData{HId: "2", HLat: 37.7854, HLon: -122.4005, HRate: 139.00, HPrice: 120.00},
        hotels.HotelData{HId: "3", HLat: 37.7834, HLon: -122.4071, HRate: 109.00, HPrice: 190.00},
        hotels.HotelData{HId: "4", HLat: 37.7936, HLon: -122.3930, HRate: 129.00, HPrice: 160.00},
        hotels.HotelData{HId: "5", HLat: 37.7831, HLon: -122.4181, HRate: 119.00, HPrice: 140.00},
        hotels.HotelData{HId: "6", HLat: 37.7863, HLon: -122.4015, HRate: 149.00, HPrice: 200.00},
    }

    for i := 7; i <= 80; i++ {
        rate := 135.00
        rateInc := 179.00
        hotelID := strconv.Itoa(i)
        lat := 37.7835 + float64(i)/500.0*3
        lon := -122.41 + float64(i)/500.0*4

        if i%3 == 0 {
            switch i % 5 {
            case 1:
                rate = 120.00
                rateInc = 140.00
            case 2:
                rate = 124.00
                rateInc = 144.00
            case 3:
                rate = 132.00
                rateInc = 158.00
            case 4:
                rate = 232.00
                rateInc = 258.00
            default:
                rate = 109.00
                rateInc = 123.17
            }
        }

        newHotels = append(
            newHotels,
            hotels.HotelData{HId: hotelID, HLat: lat, HLon: lon, HRate: rate, HPrice: rateInc},
        )
    }

    return newHotels
}
