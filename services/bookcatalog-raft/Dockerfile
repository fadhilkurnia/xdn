# syntax=docker/dockerfile:1

# Bookcatalog with embedded HashiCorp Raft for application-level replication.
# Build: docker build -t bookcatalog-raft .

# ===== build stage =========================================

FROM golang:1.21 AS build-stage

WORKDIR /app
ADD src ./src
COPY main.go ./
COPY go.mod go.sum ./

# Install build dependencies
RUN apt-get update && apt-get install -y gcc sqlite3 libsqlite3-dev

RUN go mod download

# Compile
RUN CGO_ENABLED=1 \
    go build -buildvcs=false -trimpath \
    -ldflags '-w -s' -o main .

# ===== release stage =========================================

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build-stage /app/main /app/main
WORKDIR /app
RUN mkdir -p /app/data /app/raft-data
EXPOSE 80 7000

ENTRYPOINT [ "/app/main" ]
