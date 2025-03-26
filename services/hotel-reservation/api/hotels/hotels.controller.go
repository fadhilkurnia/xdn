package hotels

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"github.com/ThePlatypus-Person/hotelReservation/api/geo"
	"github.com/ThePlatypus-Person/hotelReservation/api/profile"
	"github.com/ThePlatypus-Person/hotelReservation/api/rate"
	"github.com/ThePlatypus-Person/hotelReservation/api/reservation"
	"github.com/ThePlatypus-Person/hotelReservation/utils/config"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type HotelController struct {
    geoService *geo.GeoService
    rateService *rate.RateService
    profileService *profile.ProfileService
    reservService *reservation.ReservationService
}

func NewHotelController(client *mongo.Client, ctx context.Context, config config.Config) *HotelController {
    geoService := geo.NewGeoService(client.Database(config.DbGeo), ctx)
    rateService := rate.NewRateService(client.Database(config.DbRate), ctx)
    profileService := profile.NewProfileService(client.Database(config.DbProfile), ctx)
    reservService := reservation.NewReservationService(client.Database(config.DbReservation), ctx)

    return &HotelController{
        geoService: geoService,
        rateService: rateService,
        profileService: profileService,
        reservService: reservService,
    }
}

func (c *HotelController) GetHandler(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Access-Control-Allow-Origin", "*")
    // Read params
    inDate := r.URL.Query().Get("inDate")
    outDate := r.URL.Query().Get("outDate")
    strLat := r.URL.Query().Get("lat")
    strLon := r.URL.Query().Get("lon")
    locale := r.URL.Query().Get("locale")

	if inDate == "" || outDate == "" {
		http.Error(w, "Please specify inDate/outDate params", http.StatusBadRequest)
		return
	}

	if strLat == "" || strLon == "" {
		http.Error(w, "Please specify latitude/longitude params", http.StatusBadRequest)
		return
	}

    if locale == "" {
        locale = "en"
    }

	tempLat, _ := strconv.ParseFloat(strLat, 32)
	tempLon, _ := strconv.ParseFloat(strLon, 32)
    lat := float32(tempLat)
	lon := float32(tempLon)

    // Search nearby hotels (geographically)
    nearbyHotels, _ := c.geoService.Nearby(float64(lat), float64(lon))

    // Get ratings of nearby hotels
    ratings, _ := c.rateService.GetRates(nearbyHotels, inDate, outDate)

    // Get all hotels with ratings
    ratedHotelIds := make([]string, 0)
    for _, rating := range ratings {
        ratedHotelIds = append(ratedHotelIds, rating.HotelId)
    }

    // Check Availability
    availableHotels, err := c.reservService.CheckAvailability("", ratedHotelIds, inDate, outDate, 1)
    if err != nil {
        log.Fatalf("ERROR: Failed to find available hotels. %v\n", err)
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    
    hotelsList, err := c.profileService.GetProfiles(availableHotels)
    if err != nil {
        log.Fatalf("ERROR: Failed to find profiles for hotels. %v\n", err)
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    json.NewEncoder(w).Encode(geoJSONResponse(hotelsList))
}

func geoJSONResponse(hs []profile.HotelDesc) map[string]interface{} {
	fs := []interface{}{}

	for _, h := range hs {
		fs = append(fs, map[string]interface{}{
			"type": "Feature",
			"id":   h.Id,
			"properties": map[string]string{
				"name":         h.Name,
				"phone_number": h.PhoneNumber,
			},
			"geometry": map[string]interface{}{
				"type": "Point",
				"coordinates": []float32{
					h.Address.Lon,
					h.Address.Lat,
				},
			},
		})
	}

	return map[string]interface{}{
		"type":     "FeatureCollection",
		"features": fs,
	}
}
