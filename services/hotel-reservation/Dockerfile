FROM golang:1.24.0-bullseye AS build

# Add non root user
RUN useradd -u 1001 nonroot

WORKDIR /app 

COPY go.mod go.sum ./

# Use cache mount to speed up install of existing dependencies
RUN --mount=type=cache,target=/go/pkg/mod \
  --mount=type=cache,target=/root/.cache/go-build \
  go mod download

COPY . .

RUN go build \
  -ldflags="-linkmode external -extldflags -static" \
  -tags netgo \
  -o hotel-reservation


###
FROM ubuntu:22.04

WORKDIR /

# Copy the passwd file
COPY --from=build /etc/passwd /etc/passwd

# Copy the binary from the build stage
COPY --from=build /app/hotel-reservation hotel-reservation

# Copy static file
COPY --from=build /app/static /static

# Copy config.env file
COPY --from=build /app/config.env .

# Use nonroot user
USER nonroot

# Indicate expected port
EXPOSE 5000

CMD ["/hotel-reservation"]
