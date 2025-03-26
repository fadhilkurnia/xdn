# syntax=docker/dockerfile:1

# This Dockerfile creates the service image using the default
# sqlite datastore. We use multi stage building so the final
# image only contains the compiled binary (~20MB), excluding 
# the source code.

# To build the image: 
# docker build --tag=bookcatalog-nd .

# ===== build stage ===========================================

# setup the base image and working directory
FROM golang:1.21 AS build-stage
WORKDIR /app

# import the codebase
ADD src ./src
COPY main.go ./

# prepare dependencies
COPY go.mod go.sum ./
RUN go mod download

# compile the codebase
RUN CGO_ENABLED=1 go build -o main -a -ldflags \
    '-linkmode external -extldflags "-static"' .

# CMD ["./main"]

# Note:
# `CGO_ENABLED=1` is important because
# the release stage below will use Alpine Linux that
# does not use glibc.
# The `ldflags` is important so Go builds purely static binary
# that includes the CGO library.
# Reference: 
# https://awstip.com/containerize-go-sqlite-with-docker-6d7fbecd14f0

# ===== release stage =========================================

FROM scratch
COPY --from=build-stage /app/main /app/main
WORKDIR /app
EXPOSE 80

# command to run the service
CMD ["./main"]
