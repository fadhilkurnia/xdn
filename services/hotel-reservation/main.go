package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/ThePlatypus-Person/hotelReservation/api/cinema"
	"github.com/ThePlatypus-Person/hotelReservation/api/hotels"
	museum "github.com/ThePlatypus-Person/hotelReservation/api/museums"
	"github.com/ThePlatypus-Person/hotelReservation/api/profile"
	"github.com/ThePlatypus-Person/hotelReservation/api/recommendations"
	"github.com/ThePlatypus-Person/hotelReservation/api/reservation"
	"github.com/ThePlatypus-Person/hotelReservation/api/restaurants"
	"github.com/ThePlatypus-Person/hotelReservation/api/review"
	"github.com/ThePlatypus-Person/hotelReservation/api/user"
	"github.com/ThePlatypus-Person/hotelReservation/utils/config"
	"github.com/ThePlatypus-Person/hotelReservation/utils/generate"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func main() {
    r := chi.NewRouter()
    r.Use(middleware.Logger)

    // Load config.env
    config, err := config.LoadConfig()
    if err != nil {
        log.Fatalf("Failed to load config.env: %v", err)
    }

    // Connect to MongoDB
    log.Println("Connecting to MongoDB...")
    /*
    ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
    defer cancel()
    */
    ctx := context.TODO()

    client, err := mongo.Connect(options.Client().ApplyURI(config.MongoURI))
    if err != nil {
        log.Fatalf("Failed to connect to MongoDB: %v", err)
    }

    err = client.Ping(ctx, nil)
    if err != nil {
        log.Fatalf("MongoDB ping failed: %v", err)
    }

    log.Println("Connected to MongoDB.")

    // Static File Server
    workDir, err := os.Getwd()
    if err != nil {
        log.Fatalf("Workdir is empty.")
		return
    }

    fs := http.FileServer(http.Dir(filepath.Join(workDir, "static/")))
    r.Handle("/*", http.StripPrefix("/", fs))

    // Controllers
    userController := user.NewUserController(client, ctx, config)
    recomController := recommendations.NewRecommendationController(client, ctx, config)
    reviewController := review.NewReviewController(client, ctx, config)
    restaurantController := restaurants.NewRestaurantController(client, ctx, config)
    museumController := museum.NewMuseumController(client, ctx, config)
    cinemaController := cinema.NewCinemaController(client, ctx, config)
    reservController := reservation.NewReservationController(client, ctx, config)
    hotelController := hotels.NewHotelController(client, ctx, config)
    profileController := profile.NewProfileController(client, ctx, config)

    // Routers
    r.Route("/user", func(r chi.Router) {
        r.Post("/", userController.PostHandler)
    })

    r.Route("/recommendations", func(r chi.Router) {
        r.Get("/", recomController.GetHandler)
    })

    r.Route("/review", func(r chi.Router) {
        r.Get("/", reviewController.GetHandler)
    })

    r.Route("/restaurants", func(r chi.Router) {
        r.Get("/", restaurantController.GetHandler)
    })

    r.Route("/museums", func(r chi.Router) {
        r.Get("/", museumController.GetHandler)
    })

    r.Route("/cinema", func(r chi.Router) {
        r.Get("/", cinemaController.GetHandler)
    })

    r.Route("/reservation", func(r chi.Router) {
        r.Post("/", reservController.PostHandler)
    })

    r.Route("/hotels", func(r chi.Router) {
        r.Get("/", hotelController.GetHandler)
    })

    r.Route("/profile", func(r chi.Router) {
        r.Get("/", profileController.GetHandler)
    })

    r.Post("/secret/generate", func(w http.ResponseWriter, req *http.Request) {
        // Generate Dummy Data
        err := generate.GenerateDummyData(client, ctx, config)
        if err != nil {
            log.Fatalf("ERROR: %v\n", err)
        }

        w.Header().Set("Content-Type", "application/json")
        fmt.Fprintf(w, "Dummy data generated.");
    })

    r.Post("/secret/clear", func(w http.ResponseWriter, req *http.Request) {
        err := generate.ClearDatabaseAll(client, ctx, config)
        if err != nil {
            log.Fatalf("ERROR: %v\n", err)
        }

        w.Header().Set("Content-Type", "application/json")
        fmt.Fprintf(w, "Database cleared.");
    })



    log.Printf("Server listening to port %d...\n", config.Port)
    strPort := fmt.Sprintf(":%d", config.Port)
    err = http.ListenAndServe(strPort, r)
    if err != nil {
        log.Fatalf("Server failed to listen on port %d.\n", config.Port)
        return
    }
}
