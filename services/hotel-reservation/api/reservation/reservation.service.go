package reservation

import (
	"context"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type ReservationService struct {
    database *mongo.Database
    ctx context.Context
}

func NewReservationService(database *mongo.Database, ctx context.Context) *ReservationService {
    return &ReservationService{
        database: database,
        ctx: ctx,
    }
}

func (s *ReservationService) MakeReservation(customerName, hotelId string, InDate, OutDate string, roomNumber int) ([]string, error) {
    hotelIds := make([]string, 0)

    resCollection := s.database.Collection("reservation")
    numCollection := s.database.Collection("number")

    inDate, _ := time.Parse(time.RFC3339, InDate+"T12:00:00+00:00")
    outDate, _ := time.Parse(time.RFC3339, OutDate+"T12:00:00+00:00")
    // hotelId := hotelIds[0]

    indate := inDate.String()[0:10]

    // Check if the hotel still has room
    for inDate.Before(outDate) {
        count := 0
        inDate = inDate.AddDate(0, 0, 1)
        outdate := inDate.String()[0:10]

        // Find all reservations
        filter := bson.D{{"hotelId", hotelId}, {"inDate", indate}, {"outDate", outdate}}
        curr, err := resCollection.Find(s.ctx, filter)
        if err != nil {
            return nil, err
        }

        // Count number of reservation in that hotel
        var reserve []Reservation
        curr.All(s.ctx, &reserve)

        for _, r := range reserve {
            count += r.Number
        }

        // Find total number of rooms in that hotel
        var num Number
        err = numCollection.FindOne(s.ctx, &bson.D{{"hotelId", hotelId}}).Decode(&num)
        if err != nil {
            return nil, err
        }
        hotel_cap := int(num.Number)

        if count+roomNumber > hotel_cap {
            return hotelIds, nil
        }
        indate = outdate
    }


    inDate, _ = time.Parse(time.RFC3339, InDate+"T12:00:00+00:00")
    indate = inDate.String()[0:10]

    // Make reservation
    for inDate.Before(outDate) {
        inDate = inDate.AddDate(0, 0, 1)
        outdate := inDate.String()[0:10]

        _, err := resCollection.InsertOne(
            s.ctx,
            Reservation{
                HotelId:      hotelId,
                CustomerName: customerName,
                InDate:       indate,
                OutDate:      outdate,
                Number:       roomNumber,
            },
        )
        if err != nil {
            return nil, err
        }
        indate = outdate
    }

    hotelIds = append(hotelIds, hotelId)
    return hotelIds, nil
}


func (s *ReservationService) CheckAvailability(customerName string, hotelIds []string, InDate, OutDate string, roomNumber int) ([]string, error) {
    availableHotels := make([]string, 0)
    availabilityMap := make(map[string]bool)
    for _, hotelId := range hotelIds {
        availabilityMap[hotelId] = true
    }

    // Get all hotel room count
    var nums []Number
    curr, err := s.database.Collection("number").Find(s.ctx, bson.D{{ "hotelId", bson.D{{"$in", hotelIds}} }})
    if err != nil {
        return nil, err
    }
    curr.All(s.ctx, &nums)

    cacheCap := make(map[string]int)
    for _, num := range nums {
        cacheCap[num.HotelId] = num.Number
    }

    queryMap := make(map[string]map[string]string)
    for _, hotelId := range hotelIds {
        inDate, _ := time.Parse(time.RFC3339, InDate+"T12:00:00+00:00")
        outDate, _ := time.Parse(time.RFC3339, OutDate+"T12:00:00+00:00")

        for inDate.Before(outDate) {
            indate := inDate.String()[:10]
            inDate = inDate.AddDate(0, 0, 1)
            outDate := inDate.String()[:10]
            memcKey := hotelId + "_" + outDate + "_" + outDate
            queryMap[memcKey] = map[string]string{
                "hotelId":   hotelId,
                "startDate": indate,
                "endDate":   outDate,
            }
        }
    }

    ch := make(chan struct {
        hotelId  string
        checkRes bool
    })

    var wg sync.WaitGroup
    wg.Add(len(queryMap))
    go func() {
        wg.Wait()
        close(ch)
    }()

    for command := range queryMap {
        go func(comm string) {
            defer wg.Done()

            var reserve []Reservation
            queryItem := queryMap[comm]
            filter := bson.D{{"hotelId", queryItem["hotelId"]}, {"inDate", queryItem["startDate"]}, {"outDate", queryItem["endDate"]}}
            
            curr, err := s.database.Collection("reservation").Find(s.ctx, filter)
            if err != nil {
                return
            }
            curr.All(s.ctx, &reserve)
            
            var count int
            for _, r := range reserve {
                count += r.Number
            }
            
            var res bool
            if count+roomNumber <= cacheCap[queryItem["hotelId"]] {
                res = true
            }
            ch <- struct {
                hotelId  string
                checkRes bool
            }{
                hotelId:  queryItem["hotelId"],
                checkRes: res,
            }
        }(command)
    }

    for task := range ch {
        if !task.checkRes {
            availabilityMap[task.hotelId] = false
        }
    }

    for k, v := range availabilityMap {
        if v {
            availableHotels = append(availableHotels, k)
        }
    }

    return availableHotels, nil
}
