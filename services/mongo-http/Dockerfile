# HTTP KV frontend for the mongo cluster reference service (see main.go).
# Build: docker build -t xdn-mongo-http:test services/mongo-http/
FROM golang:1.22 AS build
WORKDIR /src
COPY go.mod main.go ./
RUN go mod tidy && CGO_ENABLED=0 go build -o /frontend .

FROM alpine:3.20
COPY --from=build /frontend /frontend
ENTRYPOINT ["/frontend"]
