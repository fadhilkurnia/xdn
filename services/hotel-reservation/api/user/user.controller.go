package user

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/ThePlatypus-Person/hotelReservation/utils/config"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type UserController struct {
    service *UserService
}


func NewUserController(client *mongo.Client, ctx context.Context, config config.Config) *UserController {
    service := NewUserService(client.Database(config.DbUser), ctx)
    return &UserController{
        service: service,
    }
}

func (c *UserController) PostHandler(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Access-Control-Allow-Origin", "*")
    // Read params
	username := r.URL.Query().Get("username")
    password := r.URL.Query().Get("password")

    if username == "" || password == "" {
        http.Error(w, "Please specify username and password", http.StatusBadRequest)
        return
    }

    // Verify Password
    valid_password := c.service.CheckUser(username, password)
    
    // Create response
    str := "Login successfully!"
    if !valid_password {
        str = "Failed. Please check your username and password."
    }

    res := map[string]interface{}{
        "message": str,
    }

    jsonBytes, err := json.Marshal(res)
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
