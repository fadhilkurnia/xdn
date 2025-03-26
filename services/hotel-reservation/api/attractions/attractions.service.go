package attractions

import (
	"context"
	"log"

	"github.com/ThePlatypus-Person/hotelReservation/api/geo"
	"github.com/hailocab/go-geoindex"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

const (
	maxSearchRadius  = 10
	maxSearchResults = 5
)

type AttractionService struct {
    database *mongo.Database
    ctx context.Context
}

func NewAttractionService(database *mongo.Database, ctx context.Context) *AttractionService {
    return &AttractionService{
        database: database,
        ctx: ctx,
    }
}

func (s *AttractionService) NearbyRestaurants(hotelId string) ([]string, error) {
    var hotel geo.Point
    filter := bson.D{{ "hotelId", hotelId }}
    err := s.database.Collection("hotels").FindOne(s.ctx, filter).Decode(&hotel)
    if err != nil {
        log.Fatalf("ERROR: %v\n", err)
    }
    
    restaurantList := s.getNearbyRestaurants(hotel.Plat, hotel.Plon)
    var restaurantIds []string
    for _, restaurant := range restaurantList {
        restaurantIds = append(restaurantIds, restaurant.Id())
    }
    
    return restaurantIds, nil
}

func (s *AttractionService) NearbyMuseums(hotelId string) ([]string, error) {
    var hotel geo.Point
    filter := bson.D{{ "hotelId", hotelId }}
    err := s.database.Collection("hotels").FindOne(s.ctx, filter).Decode(&hotel)
    if err != nil {
        log.Fatalf("ERROR: %v\n", err)
    }
    
    museumList := s.getNearbyMuseums(hotel.Plat, hotel.Plon)
    var museumIds []string
    for _, museum := range museumList {
        museumIds = append(museumIds, museum.Id())
    }
    
    return museumIds, nil
}

func (s *AttractionService) NearbyCinemas(hotelId string) ([]string, error) {
    var hotel geo.Point
    filter := bson.D{{ "hotelId", hotelId }}
    err := s.database.Collection("hotels").FindOne(s.ctx, filter).Decode(&hotel)
    if err != nil {
        log.Fatalf("ERROR: %v\n", err)
    }
    
    // Cinema table doesn't exist. The original authors of DeathStarBench used hotels
    cinemaList := s.getNearbyHotels(hotel.Plat, hotel.Plon)
    var cinemaIds []string
    for _, cinema := range cinemaList {
        cinemaIds = append(cinemaIds, cinema.Id())
    }
    
    return cinemaIds, nil
}

func (s *AttractionService) getNearbyRestaurants(lat, lon float64) []geoindex.Point {
    // Fetch all restaurants
    curr, err := s.database.Collection("restaurants").Find(s.ctx, bson.D{})

    var restaurantList []*Restaurant
    err = curr.All(s.ctx, &restaurantList)
    if err != nil {
        log.Fatalf("ERROR: %v", err)
    }

    restaurantIndex := geoindex.NewClusteringIndex()
    for _, restaurant := range restaurantList {
        restaurantIndex.Add(restaurant)
    }

    // Calculate nearest restaurants
    center := &geoindex.GeoPoint{
        Pid:  "",
        Plat: lat,
        Plon: lon,
    }

    return restaurantIndex.KNearest(
        center,
        maxSearchResults,
        geoindex.Km(maxSearchRadius), func(p geoindex.Point) bool {
            return true
        },
    )
}

func (s *AttractionService) getNearbyMuseums(lat, lon float64) []geoindex.Point {
    // Fetch all museums
    curr, err := s.database.Collection("museums").Find(s.ctx, bson.D{})

    var museumList []*Museum
    err = curr.All(s.ctx, &museumList)
    if err != nil {
        log.Fatalf("ERROR: %v", err)
    }

    museumIndex := geoindex.NewClusteringIndex()
    for _, museum := range museumList {
        museumIndex.Add(museum)
    }

    // Calculate nearest restaurants
    center := &geoindex.GeoPoint{
        Pid:  "",
        Plat: lat,
        Plon: lon,
    }

    return museumIndex.KNearest(
        center,
        maxSearchResults,
        geoindex.Km(maxSearchRadius), func(p geoindex.Point) bool {
            return true
        },
    )
}

func (s *AttractionService) getNearbyHotels(lat, lon float64) []geoindex.Point {
    // Fetch all cinemas
    curr, err := s.database.Collection("hotels").Find(s.ctx, bson.D{})

    var hotelList []*geo.Point
    err = curr.All(s.ctx, &hotelList)
    if err != nil {
        log.Fatalf("ERROR: %v", err)
    }

    hotelIndex := geoindex.NewClusteringIndex()
    for _, hotel := range hotelList {
        hotelIndex.Add(hotel)
    }

    // Calculate nearest restaurants
    center := &geoindex.GeoPoint{
        Pid:  "",
        Plat: lat,
        Plon: lon,
    }

    return hotelIndex.KNearest(
        center,
        maxSearchResults,
        geoindex.Km(maxSearchRadius), func(p geoindex.Point) bool {
            return true
        },
    )
}
