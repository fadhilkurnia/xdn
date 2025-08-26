package review

type Review struct {
	ReviewId    string  `bson:"reviewId"`
	HotelId     string  `bson:"hotelId"`
	Name        string  `bson:"name"`
	Rating      float32 `bson:"rating"`
	Description string  `bson:"description"`
	Image       *Image  `bson:"images"`
}

type Image struct {
	Url     string `bson:"url"`
	Default bool   `bson:"default"`
}
