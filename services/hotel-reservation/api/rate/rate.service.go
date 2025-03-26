package rate

import (
	"context"
	"log"
	"sort"
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// RatePlans Array
type RatePlans []RatePlan

func (r RatePlans) Len() int {
    return len(r)
}

func (r RatePlans) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r RatePlans) Less(i, j int) bool {
	return r[i].RoomType.TotalRate > r[j].RoomType.TotalRate
}

type RateService struct {
    database *mongo.Database
    ctx context.Context
}

func NewRateService(database *mongo.Database, ctx context.Context) *RateService {
    return &RateService{
        database: database,
        ctx: ctx,
    }
}

func (s *RateService) GetRates(hotelIds []string, inDate string, outDate string) (RatePlans, error) {
    ratePlans := make(RatePlans, 0)

    var wg sync.WaitGroup
    var mutex sync.Mutex

    wg.Add(len(hotelIds))
    for _, hotelId := range hotelIds {
        go func(id string) {
            curr, err := s.database.Collection("inventory").Find(s.ctx, bson.D{})
            if err != nil {
                log.Fatalf("ERROR: Failed to get inventory data. %v\n", err)
            }

            tmpRatePlans := make(RatePlans, 0)
            curr.All(context.TODO(), &tmpRatePlans)
            for _, r := range tmpRatePlans {
                mutex.Lock()
                ratePlans = append(ratePlans, r)
                mutex.Unlock()
            }

            defer wg.Done()
        }(hotelId)
    }

    wg.Wait()

    sort.Sort(ratePlans)
    return ratePlans, nil
}
