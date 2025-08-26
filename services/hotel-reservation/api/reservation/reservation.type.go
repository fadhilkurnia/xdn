package reservation

type Reservation struct {
	HotelId      string `bson:"hotelId"`
	CustomerName string `bson:"customerName"`
	InDate       string `bson:"inDate"`
	OutDate      string `bson:"outDate"`
	Number       int    `bson:"number"`
}

type Number struct {
	HotelId string `bson:"hotelId"`
	Number  int    `bson:"numberOfRoom"`
}
