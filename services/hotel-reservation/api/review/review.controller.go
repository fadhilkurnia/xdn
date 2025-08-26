package review

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/ThePlatypus-Person/hotelReservation/api/user"
	"github.com/ThePlatypus-Person/hotelReservation/utils/config"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type ReviewController struct {
    userService *user.UserService
    reviewService *ReviewService
}

func NewReviewController(client *mongo.Client, ctx context.Context, config config.Config) *ReviewController {
    userService := user.NewUserService(client.Database(config.DbUser), ctx)
    reviewService := NewReviewSevice(client.Database(config.DbReview), ctx)

    return &ReviewController{
        userService: userService,
        reviewService: reviewService,
    }
}


func (c *ReviewController) GetHandler(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Access-Control-Allow-Origin", "*")
    // Read params
	username := r.URL.Query().Get("username")
    password := r.URL.Query().Get("password")
    hotelId := r.URL.Query().Get("hotelId")

    if username == "" || password == "" {
        http.Error(w, "Please specify username and password", http.StatusBadRequest)
        return
    }

    if hotelId == "" {
        http.Error(w, "Please specify hotelId", http.StatusBadRequest)
        return
    }

    // Verify credentials
    valid_password := c.userService.CheckUser(username, password)

    if !valid_password {
        http.Error(w, "Please specify username and password", http.StatusUnauthorized)
        return
    }

    // Get reviews
    reviews, err := c.reviewService.GetReviews(hotelId)
    if err != nil {
        log.Fatalf("ERROR: %v", err)
    }

    // Convert interface{} into JSON
    jsonBytes, err := json.Marshal(reviews)
    if err != nil {
        http.Error(w, "Internal Error", http.StatusInternalServerError)
        return
    }

    // Send response
    w.Header().Set("Content-Type", "application/json")
    _, err = w.Write(jsonBytes)
    if err != nil {
        http.Error(w, "Internal Error", http.StatusInternalServerError)
        return
    }
}
