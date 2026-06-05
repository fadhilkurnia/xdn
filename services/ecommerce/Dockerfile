# syntax=docker/dockerfile:1

# E-commerce webshop as a single-container XDN service (Go + SQLite).
# Multi-stage build; the final image is just the static binary on scratch.
#
# To build the image:
#   docker build --tag=xdn-ecommerce .
# To build it for multiple architectures:
#   docker buildx build --platform linux/amd64,linux/arm64 --tag=xdn-ecommerce .

# ===== build stage ============================================================
FROM --platform=$BUILDPLATFORM golang:1.21 AS build-stage
ARG TARGETOS
ARG TARGETARCH

WORKDIR /app

# Install the cross-compiler matching the *target* arch. The build stage runs
# on $BUILDPLATFORM (the host); for an arm64 target on an amd64 host we need the
# aarch64 cross toolchain, since the cgo sqlite driver compiles C.
RUN apt-get update && \
    if [ "$TARGETARCH" = "amd64" ]; then \
        apt-get install -y gcc-x86-64-linux-gnu; \
    else \
        apt-get install -y gcc-aarch64-linux-gnu; \
    fi

COPY go.mod go.sum ./
RUN go mod download
COPY main.go index.html ./

RUN if [ "$TARGETARCH" = "amd64" ]; then CC=x86_64-linux-gnu-gcc; else CC=aarch64-linux-gnu-gcc; fi && \
    CC=$CC \
    CGO_ENABLED=1 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    CGO_CFLAGS="-O2" \
    CGO_LDFLAGS="-O2" \
    go build -buildvcs=false -trimpath \
    -ldflags '-w -s -extldflags "-static"' -a -o ecommerce .

# ===== release stage ==========================================================
FROM scratch
COPY --from=build-stage /app/ecommerce /app/ecommerce
WORKDIR /app
EXPOSE 80

ENTRYPOINT ["/app/ecommerce"]
