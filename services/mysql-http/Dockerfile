# HTTP KV frontend for the MySQL GR cluster reference service (see main.go).
# Build: docker build -t xdn-mysql-http:test services/mysql-http/
FROM golang:1.22 AS build
WORKDIR /src
COPY go.mod main.go ./
RUN go mod tidy && CGO_ENABLED=0 go build -o /frontend .

FROM alpine:3.20
COPY --from=build /frontend /frontend
ENTRYPOINT ["/frontend"]
