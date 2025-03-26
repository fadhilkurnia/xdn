# Hotel Reservation Service
A simplified version of the DeathStarBench hotel reservation app. All the microservices in the original DeathStarBench has been consolidated into a single server, the use of memcached has been removed, and all the different MongoDB instances are also combined.

## Technology
- Go
- MongoDB

## Deployment
### Build Docker Image
```
docker build -t hotel-reservation:1 .
```

### Deploy Locally using Docker Compose
```
docker compose up -d
docker compose down
```

### Deploy on XDN 
#### Deterministic Deployment
```
xdn launch hotel-reservation --file=hotel-reservation.yaml
```
#### Non-Deterministic Deployment
```
xdn launch hotel-reservation-nd --file=hotel-reservation-nd.yaml
```
