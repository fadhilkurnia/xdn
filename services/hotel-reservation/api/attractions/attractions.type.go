package attractions

type Cinema struct {
	CinemaId   string  `bson:"cinemaId"`
	CLat       float64 `bson:"lat"`
	CLon       float64 `bson:"lon"`
	CinemaName string  `bson:"cinemaName"`
	Type       string  `bson:"type"`
}

func (c *Cinema) Lat() float64 { return c.CLat }
func (c *Cinema) Lon() float64 { return c.CLon }
func (c *Cinema) Id() string   { return c.CinemaId }


type Museum struct {
	MuseumId   string  `bson:"museumId"`
	MLat       float64 `bson:"lat"`
	MLon       float64 `bson:"lon"`
	MuseumName string  `bson:"museumName"`
	Type       string  `bson:"type"`
}

func (m *Museum) Lat() float64 { return m.MLat }
func (m *Museum) Lon() float64 { return m.MLon }
func (m *Museum) Id() string   { return m.MuseumId }


type Restaurant struct {
	RestaurantId   string  `bson:"restaurantId"`
	RLat           float64 `bson:"lat"`
	RLon           float64 `bson:"lon"`
	RestaurantName string  `bson:"restaurantName"`
	Rating         float32 `bson:"rating"`
	Type           string  `bson:"type"`
}

func (r *Restaurant) Lat() float64 { return r.RLat }
func (r *Restaurant) Lon() float64 { return r.RLon }
func (r *Restaurant) Id() string   { return r.RestaurantId }
