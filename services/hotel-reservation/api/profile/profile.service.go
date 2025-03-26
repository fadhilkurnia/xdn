package profile

import (
	"context"
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type ProfileService struct {
    database *mongo.Database
    ctx context.Context
}

func NewProfileService(database *mongo.Database, ctx context.Context) *ProfileService {
    return &ProfileService{
        database: database,
        ctx: ctx,
    }
}

func (s *ProfileService) GetProfiles(hotelIds []string) ([]HotelDesc, error) {
    var wg sync.WaitGroup
    var mutex sync.Mutex

    hotelsList := make([]HotelDesc, 0)

    wg.Add(len(hotelIds))
    for _, hotelId := range hotelIds {
        go func(hotelId string) {
            defer wg.Done()

            var hotelProfile HotelDesc
            filter := bson.D{{ "id", hotelId }}
            err := s.database.Collection("hotels").FindOne(s.ctx, filter).Decode(&hotelProfile)
            if err != nil {
                return
            }

            mutex.Lock()
            hotelsList = append(hotelsList, hotelProfile)
            mutex.Unlock()
        }(hotelId)
    }
    wg.Wait()

    return hotelsList, nil
}


func (s *ProfileService) GetProfile(hotelId string) (HotelDesc, error) {
    hotelProfile := HotelDesc{}
    filter := bson.D{{ "id", hotelId }}
    err := s.database.Collection("hotels").FindOne(s.ctx, filter).Decode(&hotelProfile)
    if err != nil {
        return hotelProfile, err
    }

    return hotelProfile, nil
}
