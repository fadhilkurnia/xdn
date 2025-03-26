package data

import (
	"fmt"
	"strconv"
	"context"
	"log"

	"github.com/ThePlatypus-Person/hotelReservation/api/hotels"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func GenerateProfileHotels(collection *mongo.Collection, ctx context.Context) (*mongo.InsertManyResult, error) {
    log.Println("Generating profile hotels")
    collection.DeleteMany(ctx, bson.D{})

    newProfiles := getNewProfiles()

    result, err := collection.InsertMany(ctx, newProfiles)
	if err != nil {
        log.Panicf("Failed to generate profile hotels")
	}
    log.Println("Successfully generated profile hotels")

    return result, err
}

func getNewProfiles() ([]interface{}) {
    newProfiles := []interface{}{
        hotels.HotelDesc{
            Id: "1",
            Name: "Clift Hotel",
            PhoneNumber: "(415) 775-4700",
            Description: "A 6-minute walk from Union Square and 4 minutes from a Muni Metro station, this luxury hotel designed by Philippe Starck features an artsy furniture collection in the lobby, including work by Salvador Dali.",
            Address: &hotels.Address{
                StreetNumber: "495",
                StreetName: "Geary St",
                City: "San Francisco",
                State: "CA",
                Country: "United States",
                PostalCode: "94102",
                Lat: 37.7867,
                Lon: -122.4112,
            },
        },
        hotels.HotelDesc{
            Id: "2",
            Name: "W San Francisco",
            PhoneNumber: "(415) 777-5300",
            Description: "Less than a block from the Yerba Buena Center for the Arts, this trendy hotel is a 12-minute walk from Union Square.",
            Address: &hotels.Address{
                StreetNumber: "181",
                StreetName: "3rd St",
                City: "San Francisco",
                State: "CA",
                Country: "United States",
                PostalCode: "94103",
                Lat: 37.7854,
                Lon: -122.4005,
            },
        },
        hotels.HotelDesc{
            Id: "3",
            Name: "Hotel Zetta",
            PhoneNumber: "(415) 543-8555",
            Description: "A 3-minute walk from the Powell Street cable-car turnaround and BART rail station, this hip hotel 9 minutes from Union Square combines high-tech lodging with artsy touches.",
            Address: &hotels.Address{
                StreetNumber: "55",
                StreetName: "5th St",
                City: "San Francisco",
                State: "CA",
                Country: "United States",
                PostalCode: "94103",
                Lat: 37.7834,
                Lon: -122.4071,
            },
        },
        hotels.HotelDesc{
            Id: "4",
            Name: "Hotel Vitale",
            PhoneNumber: "(415) 278-3700",
            Description: "This waterfront hotel with Bay Bridge views is 3 blocks from the Financial District and a 4-minute walk from the Ferry Building.",
            Address: &hotels.Address{
                StreetNumber: "8",
                StreetName: "Mission St",
                City: "San Francisco",
                State: "CA",
                Country: "United States",
                PostalCode: "94105",
                Lat: 37.7936,
                Lon: -122.3930,
            },
        },
        hotels.HotelDesc{
            Id: "5",
            Name: "Phoenix Hotel",
            PhoneNumber: "(415) 776-1380",
            Description: "Located in the Tenderloin neighborhood, a 10-minute walk from a BART rail station, this retro motor lodge has hosted many rock musicians and other celebrities since the 1950s. It’s a 4-minute walk from the historic Great American Music Hall nightclub.",
            Address: &hotels.Address{
                StreetNumber: "601",
                StreetName: "Eddy St",
                City: "San Francisco",
                State: "CA",
                Country: "United States",
                PostalCode: "94109",
                Lat: 37.7831,
                Lon: -122.4181,
            },
        },
        hotels.HotelDesc{
            Id: "6",
            Name: "St. Regis San Francisco",
            PhoneNumber: "(415) 284-4000",
            Description: "St. Regis Museum Tower is a 42-story, 484 ft skyscraper in the South of Market district of San Francisco, California, adjacent to Yerba Buena Gardens, Moscone Center, PacBell Building and the San Francisco Museum of Modern Art.",
            Address: &hotels.Address{
                StreetNumber: "125",
                StreetName: "3rd St",
                City: "San Francisco",
                State: "CA",
                Country: "United States",
                PostalCode: "94109",
                Lat: 37.7863,
                Lon: -122.4015,
            },
        },
    }


    for i := 7; i <= 80; i++ {
        hotelID := strconv.Itoa(i)
        phoneNumber := fmt.Sprintf("(415) 284-40%s", hotelID)

        lat := 37.7835 + float32(i)/500.0*3
        lon := -122.41 + float32(i)/500.0*4

        newProfiles = append(
            newProfiles,
            hotels.HotelDesc{
                Id: hotelID,
                Name: "St. Regis San Francisco",
                PhoneNumber: phoneNumber,
                Description: "St. Regis Museum Tower is a 42-story, 484 ft skyscraper in the South of Market district of San Francisco, California, adjacent to Yerba Buena Gardens, Moscone Center, PacBell Building and the San Francisco Museum of Modern Art.",
                Address: &hotels.Address{
                    StreetNumber: "125",
                    StreetName: "3rd St",
                    City: "San Francisco",
                    State: "CA",
                    Country: "United States",
                    PostalCode: "94109",
                    Lat: lat,
                    Lon: lon,
                },
            },
        )
    }

    return newProfiles
}
