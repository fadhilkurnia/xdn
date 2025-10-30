# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM golang:1.21 AS build-stage
ARG TARGETOS
ARG TARGETARCH

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -buildvcs=false -trimpath -ldflags "-s -w" -o bookcatalog-mongo .

FROM scratch
COPY --from=build-stage /app/bookcatalog-mongo /app/bookcatalog-mongo
WORKDIR /app
EXPOSE 80
ENTRYPOINT ["/app/bookcatalog-mongo"]

