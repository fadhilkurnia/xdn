package recommendations

import (
	"context"
	//"log"
	"math"

	"github.com/ThePlatypus-Person/hotelReservation/api/hotels"
	"github.com/hailocab/go-geoindex"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type RecommendationService struct {
    database *mongo.Database
    ctx context.Context
}

func NewRecommendationService(database *mongo.Database, ctx context.Context) *RecommendationService {
    return &RecommendationService{
        database: database,
        ctx: ctx,
    }
}


func (s *RecommendationService) GetRecommendations(require string, lat, lon float32) []string {
    // Getting hotel recommendations from mongodb
    curr, _ := s.database.Collection("recommendation").Find(s.ctx, bson.D{})
    /*
    if err != nil {
        log.Panicf("Failed to get recommendation data: %v\n", err)
    }
    */

    var hotelsList []hotels.HotelData
    curr.All(s.ctx, &hotelsList)

    // Processing request
    var hotelIds []string
    switch require {
    case "dis":
        hotelIds = getClosestHotels(hotelsList, float64(lat), float64(lon))
    case "rate":
        hotelIds = getHighestRatedHotels(hotelsList)
    case "price":
        hotelIds = getCheapestHotels(hotelsList)
    }

    return hotelIds
}

func getClosestHotels(hotels []hotels.HotelData, lat, lon float64) ([]string) {
    p1 := &geoindex.GeoPoint{
        Pid:  "",
        Plat: lat,
        Plon: lon,
    }

    min := math.MaxFloat64
    hotelIds := make([]string, 0)

    for _, hotel := range hotels {
        tmp := float64(geoindex.Distance(p1, &geoindex.GeoPoint{
            Pid:  "",
            Plat: hotel.HLat,
            Plon: hotel.HLon,
        })) / 1000
        if tmp < min {
            min = tmp
        }
    }
    for _, hotel := range hotels {
        tmp := float64(geoindex.Distance(p1, &geoindex.GeoPoint{
            Pid:  "",
            Plat: hotel.HLat,
            Plon: hotel.HLon,
        })) / 1000
        if tmp == min {
            hotelIds = append(hotelIds, hotel.HId)
        }
    }

    return hotelIds
}

func getHighestRatedHotels(hotels []hotels.HotelData) ([]string) {
    max := 0.0
    hotelIds := make([]string, 0)

    for _, hotel := range hotels {
        if hotel.HRate > max {
            max = hotel.HRate
        }
    }
    for _, hotel := range hotels {
        if hotel.HRate == max {
            hotelIds = append(hotelIds, hotel.HId)
        }
    }

    return hotelIds
}

func getCheapestHotels(hotels []hotels.HotelData) ([]string) {
    min := math.MaxFloat64
    hotelIds := make([]string, 0)

    for _, hotel := range hotels {
        if hotel.HPrice < min {
            min = hotel.HPrice
        }
    }
    for _, hotel := range hotels {
        if hotel.HPrice == min {
            hotelIds = append(hotelIds, hotel.HId)
        }
    }

    return hotelIds
}
