# syntax=docker/dockerfile:1

# randstate: a non-deterministic XDN service that writes RANDOM state on first bootstrap, to
# demonstrate (and stress) primary-backup non-deterministic initial-state synchronization. See
# main.go. Pure Go with CGO disabled, so it cross-compiles cleanly to any architecture without an
# emulator or a cross toolchain.
#
# Build:
#   docker build --tag=xdn-randstate .
#   docker buildx build --platform linux/amd64,linux/arm64 --tag=xdn-randstate .

# ===== build stage ============================================================
FROM --platform=$BUILDPLATFORM golang:1.21 AS build-stage
ARG TARGETOS
ARG TARGETARCH

WORKDIR /app
COPY go.mod ./
COPY main.go ./
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -buildvcs=false -trimpath -ldflags '-w -s' -o randstate .

# ===== release stage ==========================================================
FROM scratch
COPY --from=build-stage /app/randstate /app/randstate
WORKDIR /app
EXPOSE 80

ENTRYPOINT ["/app/randstate"]
