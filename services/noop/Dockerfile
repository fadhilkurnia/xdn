# syntax=docker/dockerfile:1

# NoOp: the smallest XDN service. It returns an immediate 200 response with no
# state and no work, serving as a baseline for XDN coordination overhead.
#
# Pure Go with CGO disabled, so it cross-compiles cleanly to any architecture
# without an emulator or a cross toolchain.
#
# To build the image:
#   docker build --tag=xdn-noop .
# To build it for multiple architectures:
#   docker buildx build --platform linux/amd64,linux/arm64 --tag=xdn-noop .

# ===== build stage ============================================================
FROM --platform=$BUILDPLATFORM golang:1.21 AS build-stage
ARG TARGETOS
ARG TARGETARCH

WORKDIR /app
COPY go.mod ./
COPY main.go ./
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -buildvcs=false -trimpath -ldflags '-w -s' -o noop .

# ===== release stage ==========================================================
FROM scratch
COPY --from=build-stage /app/noop /app/noop
WORKDIR /app
EXPOSE 80

ENTRYPOINT ["/app/noop"]
