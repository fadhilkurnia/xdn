package data

import (
	"fmt"
	"strconv"
	"context"
	"log"

	"github.com/ThePlatypus-Person/hotelReservation/api/rate"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func GenerateInventory(collection *mongo.Collection, ctx context.Context) (*mongo.InsertManyResult, error) {
    log.Println("Generating inventory")
    collection.DeleteMany(ctx, bson.D{})

    newRatePlans := getNewRatePlans()

    result, err := collection.InsertMany(ctx, newRatePlans)
	if err != nil {
        log.Panicf("Failed to generate inventory")
	}
    log.Println("Successfully generated inventory")

    return result, err
}

func getNewRatePlans() ([]interface{}) {
    newRatePlans := []interface{}{
        rate.RatePlan{
            HotelId: "1",
            Code: "RACK",
            InDate: "2015-04-09",
            OutDate: "2015-04-10",
            RoomType: &rate.RoomType{
                BookableRate: 109.00,
                Code: "KNG",
                RoomDescription: "King sized bed",
                TotalRate: 109.00,
                TotalRateInclusive: 123.17,
            },
        },
        rate.RatePlan{
            HotelId: "2",
            Code: "RACK",
            InDate: "2015-04-09",
            OutDate: "2015-04-10",
            RoomType: &rate.RoomType{
                BookableRate: 139.00,
                Code: "QN",
                RoomDescription: "Queen sized bed",
                TotalRate: 139.00,
                TotalRateInclusive: 153.09,
            },
        },
        rate.RatePlan{
            HotelId: "3",
            Code: "RACK",
            InDate: "2015-04-09",
            OutDate: "2015-04-10",
            RoomType: &rate.RoomType{
                BookableRate: 109.00,
                Code: "KNG",
                RoomDescription: "King sized bed",
                TotalRate: 109.00,
                TotalRateInclusive: 123.17,
            },
        },
    }


    for i := 7; i <= 80; i++ {
        if i%3 != 0 {
            continue
        }

        hotelID := strconv.Itoa(i)

        endDate := "2015-04-"
        if i%2 == 0 {
            endDate = fmt.Sprintf("%s17", endDate)
        } else {
            endDate = fmt.Sprintf("%s24", endDate)
        }

        tmpRate := 109.00
        rateInc := 123.17
        if i%5 == 1 {
            tmpRate = 120.00
            rateInc = 140.00
        } else if i%5 == 2 {
            tmpRate = 124.00
            rateInc = 144.00
        } else if i%5 == 3 {
            tmpRate = 132.00
            rateInc = 158.00
        } else if i%5 == 4 {
            tmpRate = 232.00
            rateInc = 258.00
        }

        newRatePlans = append(
            newRatePlans,
            rate.RatePlan{
                HotelId: hotelID,
                Code: "RACK",
                InDate: "2015-04-09",
                OutDate: endDate,
                RoomType: &rate.RoomType{
                    BookableRate: tmpRate,
                    Code: "KNG",
                    RoomDescription: "King sized bed",
                    TotalRate: tmpRate,
                    TotalRateInclusive: rateInc,
                },
            },
        )
    }

    return newRatePlans
}
