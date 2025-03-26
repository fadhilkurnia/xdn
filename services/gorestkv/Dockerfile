# syntax=docker/dockerfile:1

# This Dockerfile creates the service image using the default
# sqlite datastore. We use multi stage building so the final
# image only contains the compiled binary (~20MB), excluding 
# the source code.

# To build the image: 
# docker build -t restkv .

# ===== build stage =========================================

# setup the base image and working directory
FROM golang:1.21 AS build-stage
WORKDIR /app

# import the codebase and its dependencies
COPY main.go ./
COPY go.mod go.sum ./
RUN go mod download

# compile the codebase
RUN CGO_ENABLED=1 GOOS=linux go build -o restkv -a -ldflags \
    '-linkmode external -extldflags "-static"' .

# Note:
# `CGO_ENABLED=1 GOOS=linux` are important because
# the release stage below will use Alpine Linux that
# does not use glibc.
# The `ldflags` is important so Go builds purely static binary
# that includes the CGO library.
# Reference: 
# https://awstip.com/containerize-go-sqlite-with-docker-6d7fbecd14f0

# ===== release stage =========================================

FROM scratch AS build-release-stage

# Set the working directory inside the container
WORKDIR /app

# Copy the compiled Go binary from the build stage
COPY --from=build-stage /app/restkv /app/restkv

# Expose the port that the app runs on
EXPOSE 8080

# Command to run the binary
CMD ["/app/restkv"]
