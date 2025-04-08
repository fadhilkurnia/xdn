# syntax=docker/dockerfile:1

# To build the image: 
# docker build --target=build-release-stage --tag=rsmbench .

# ===== build stage =======
FROM golang:1.21 AS build-stage
WORKDIR /app
COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -o rsmbench main.go

# Note:
# `CGO_ENABLED=0 GOOS=linux` are important because
# the release stage below will use Alpine Linux that
# does not use glibc.

# ==== release stage =====
FROM alpine:latest AS build-release-stage
WORKDIR /app
COPY --from=build-stage /app/rsmbench /app/rsmbench
EXPOSE 80
CMD ["/app/rsmbench"]
