# seats - SEATS airline-ticketing benchmark web service

A single-container implementation of the
[SEATS](https://hstore.cs.brown.edu/projects/seats/) (Stonebraker Electronic
Airline Ticketing System) OLTP benchmark, exposed over HTTP and backed by
SQLite. It models airports, flights, customers, and seat reservations.

All transactions are **deterministic** given the request (the dataset is
generated deterministically and nothing reads a server-side clock or random
source), so the service can be replicated under XDN with `--deterministic=true`.

Tech: Go, SQLite.

## Endpoints

| Path | Method | Body | Description |
|------|--------|------|-------------|
| `/health` | GET | — | liveness check |
| `/init_db` | POST | `{"num_airports":10,"num_customers":1000,"seats_per_flight":100}` | (re)create + populate (optional body) |
| `/find_flights` | POST | `{"depart":"AP1","arrive":"AP2"}` | read-only: flights between two airports (optional `date`) |
| `/find_open_seats` | POST | `{"flight_id":1}` | read-only: unreserved seat numbers on a flight |
| `/new_reservation` | POST | `{"customer_id":1,"flight_id":1,"seat":5}` | book a seat (rejects if taken or full) |
| `/update_reservation` | POST | `{"reservation_id":1,"seat":10}` | move a reservation to a different seat |
| `/delete_reservation` | POST | `{"reservation_id":1}` | cancel a reservation, free the seat |
| `/update_customer` | POST | `{"customer_id":1,"name":"...","balance":123}` | update customer attributes |

`/init_db` creates airports `AP1`..`APa`, one flight for each ordered pair of
distinct airports, and customers `cust1`..`custc`.

## Run locally

```bash
docker run -p 80:80 fadhilkurnia/xdn-seats
curl -X POST localhost/init_db -d '{"num_airports":5,"num_customers":100}'
curl -X POST localhost/find_flights -d '{"depart":"AP1","arrive":"AP2"}'
curl -X POST localhost/new_reservation -d '{"customer_id":1,"flight_id":1,"seat":5}'
```

State (the SQLite database) is stored under `/app/data/`.

## Build a container image

```bash
docker build --tag=xdn-seats .
```

To build for multiple platforms:

```bash
docker buildx build --platform linux/amd64,linux/arm64 --tag=xdn-seats .
```
