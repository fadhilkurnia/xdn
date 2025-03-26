package geo

import (
	"context"
	"log"

	"github.com/hailocab/go-geoindex"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)
const (
	maxSearchRadius float64 = 10
	maxSearchResults int = 5
)

type GeoService struct {
    database *mongo.Database
    ctx context.Context
}

func NewGeoService(database *mongo.Database, ctx context.Context) *GeoService {
    return &GeoService{
        database: database,
        ctx: ctx,
    }
}

func (s *GeoService) Nearby(lat float64, lon float64) ([]string, error) {
    var points []geoindex.Point = s.getNearbyPoints(lat, lon)

    var hotelIds []string
    for _, p := range points {
        hotelIds = append(hotelIds, p.Id())
    }

    return hotelIds, nil
}

func (s *GeoService) getNearbyPoints(lat float64, lon float64) []geoindex.Point {
    curr, err := s.database.Collection("geo").Find(context.TODO(), bson.D{})

    var points []*Point
    err = curr.All(s.ctx, &points)
    if err != nil {
        log.Fatalf("ERROR: Failed to get Geo data. %v", err)
    }

    index := geoindex.NewClusteringIndex()
    for _, point := range points {
        index.Add(point)
    }

    center := &geoindex.GeoPoint{
        Pid: "",
        Plat: lat,
        Plon: lon,
    }

    return index.KNearest(
        center,
        maxSearchResults,
        geoindex.Km(maxSearchRadius),
        func(p geoindex.Point) bool {
            return true
        },
    )
}
