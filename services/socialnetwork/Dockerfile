# syntax=docker/dockerfile:1

# Social-network frontend for the multi-container XDN service (Go + MongoDB).
# The Mongo driver is pure Go, so this builds with CGO disabled and cross-
# compiles cleanly to any architecture without an emulator or cross toolchain.
#
# Deployed together with a MongoDB backend via socialnetwork.yaml.
#
# To build the image:
#   docker build --tag=xdn-socialnetwork .
# To build it for multiple architectures:
#   docker buildx build --platform linux/amd64,linux/arm64 --tag=xdn-socialnetwork .

# ===== build stage ============================================================
FROM --platform=$BUILDPLATFORM golang:1.24-bookworm AS build-stage
ARG TARGETOS
ARG TARGETARCH

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY main.go index.html ./
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags '-w -s' -o socialnetwork .

# ===== release stage ==========================================================
FROM scratch
COPY --from=build-stage /app/socialnetwork /app/socialnetwork
WORKDIR /app
EXPOSE 80

ENTRYPOINT ["/app/socialnetwork"]
