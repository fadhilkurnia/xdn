package profile

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/ThePlatypus-Person/hotelReservation/api/user"
	"github.com/ThePlatypus-Person/hotelReservation/utils/config"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type ProfileController struct {
    profileService *ProfileService
    userService *user.UserService
}

func NewProfileController(client *mongo.Client, ctx context.Context, config config.Config) *ProfileController {
    profileService := NewProfileService(client.Database(config.DbProfile), ctx)
    userService := user.NewUserService(client.Database(config.DbUser), ctx)

    return &ProfileController{
        profileService: profileService,
        userService: userService,
    }
}

func (c *ProfileController) GetHandler(w http.ResponseWriter, r *http.Request) {
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

    // Get nearby museums
    hotelDesc, _ := c.profileService.GetProfile(hotelId)

    // Convert interface{} into JSON
    jsonBytes, err := json.Marshal(hotelDesc)
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
