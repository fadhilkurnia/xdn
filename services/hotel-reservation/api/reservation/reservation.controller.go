package reservation

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"github.com/ThePlatypus-Person/hotelReservation/api/user"
	"github.com/ThePlatypus-Person/hotelReservation/utils/config"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type ReservationController struct {
    reservService *ReservationService
    userService *user.UserService
}

func NewReservationController(client *mongo.Client, ctx context.Context, config config.Config) *ReservationController {
    reservService := NewReservationService(client.Database(config.DbReservation), ctx)
    userService := user.NewUserService(client.Database(config.DbUser), ctx)

    return &ReservationController{
        reservService: reservService,
        userService: userService,
    }
}

func (c *ReservationController) PostHandler(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Access-Control-Allow-Origin", "*")
    // Read params
    inDate := r.URL.Query().Get("inDate")
    outDate := r.URL.Query().Get("outDate")
    hotelId := r.URL.Query().Get("hotelId")
	customerName := r.URL.Query().Get("customerName")
	username := r.URL.Query().Get("username")
    password := r.URL.Query().Get("password")
    number := r.URL.Query().Get("number")

	if inDate == "" || outDate == "" {
		http.Error(w, "Please specify inDate/outDate params", http.StatusBadRequest)
		return
	}

    if hotelId == "" {
        http.Error(w, "Please specify hotelId", http.StatusBadRequest)
        return
    }

    if customerName == "" {
        http.Error(w, "Please specify customerName", http.StatusBadRequest)
        return
    }

    if username == "" || password == "" {
        http.Error(w, "Please specify username and password", http.StatusBadRequest)
        return
    }

    if number == "" {
        http.Error(w, "Please specify number", http.StatusBadRequest)
        return
    }
    numberOfRoom, err := strconv.Atoi(number)
    if err != nil {
        http.Error(w, "number param must be of type integer", http.StatusBadRequest)
        return
    }

    // Make Reservation
    hotelIds, err := c.reservService.MakeReservation(customerName, hotelId, inDate, outDate, numberOfRoom) 
    if err != nil {
        log.Fatalf("Error: Failed making reservation. %v\n", err)
    }

    // Convert interface{} into JSON
    jsonBytes, err := json.Marshal(hotelIds)
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
