package recommendations

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/ThePlatypus-Person/hotelReservation/api/profile"
	"github.com/ThePlatypus-Person/hotelReservation/utils/config"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type RecommendationController struct {
    recomService *RecommendationService
    profileService *profile.ProfileService
}

func NewRecommendationController(client *mongo.Client, ctx context.Context, config config.Config) *RecommendationController {
    recomService := NewRecommendationService(client.Database(config.DbRecommendation), ctx)
    profileService := profile.NewProfileService(client.Database(config.DbProfile), ctx)

    return &RecommendationController{
        recomService: recomService,
        profileService: profileService,
    }
}

func (c *RecommendationController) GetHandler(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Access-Control-Allow-Origin", "*")
    // Read params
	require := r.URL.Query().Get("require")
    strLat := r.URL.Query().Get("lat")
    strLon := r.URL.Query().Get("lon")

	if require == "" {
		http.Error(w, "Please specify require params", http.StatusBadRequest)
		return
	}

	if strLat == "" || strLon == "" {
		http.Error(w, "Please specify latitude/longitude params", http.StatusBadRequest)
		return
	}

    tempLat, _ := strconv.ParseFloat(strLat, 32)
	tempLon, _ := strconv.ParseFloat(strLon, 32)
    lat := float32(tempLat)
	lon := float32(tempLon)

    // Get Recommendations
    var hotelIds []string = c.recomService.GetRecommendations(require, lat, lon)

    // Get Profiles
    hotelDescriptions, err := c.profileService.GetProfiles(hotelIds)

    // Convert interface{} into JSON
    jsonBytes, err := json.Marshal(hotelDescriptions)
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
