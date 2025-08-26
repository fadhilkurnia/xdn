package rate

type RoomType struct {
	BookableRate       float64 `bson:"bookableRate"`
	Code               string  `bson:"code"`
	RoomDescription    string  `bson:"roomDescription"`
	TotalRate          float64 `bson:"totalRate"`
	TotalRateInclusive float64 `bson:"totalRateInclusive"`
}

type RatePlan struct {
	HotelId  string    `bson:"hotelId"`
	Code     string    `bson:"code"`
	InDate   string    `bson:"inDate"`
	OutDate  string    `bson:"outDate"`
	RoomType *RoomType `bson:"roomType"`
}
