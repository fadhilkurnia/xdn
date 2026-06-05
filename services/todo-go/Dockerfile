# syntax=docker/dockerfile:1

# Multi-stage build for the Go todo service.
# Uses CGO for sqlite support, static linking, scratch base.

# ===== build stage =========================================

FROM --platform=$BUILDPLATFORM golang:1.21 AS build-stage
ARG TARGETOS
ARG TARGETARCH
ARG TARGETPLATFORM

WORKDIR /app
ADD src ./src
COPY main.go ./

# Install the cross-compiler matching the *target* arch. The build stage runs
# on $BUILDPLATFORM (the host), so for an arm64 target on an amd64 host we need
# the aarch64 cross toolchain — plain `gcc` would try to assemble arm64 asm with
# the host assembler and fail.
RUN apt-get update && \
    if [ "$TARGETARCH" = "amd64" ]; then \
        apt-get install -y gcc-x86-64-linux-gnu; \
    else \
        apt-get install -y gcc-aarch64-linux-gnu; \
    fi
COPY go.mod go.sum ./
RUN go mod download

RUN if [ "$TARGETARCH" = "amd64" ]; then CC=x86_64-linux-gnu-gcc; else CC=aarch64-linux-gnu-gcc; fi && \
    CC=$CC \
    CGO_ENABLED=1 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    CGO_CFLAGS="-O2" \
    CGO_LDFLAGS="-O2" \
    go build -buildvcs=false -trimpath \
    -ldflags '-w -s -extldflags "-static"' -a -o main .

# ===== release stage =========================================

FROM scratch
COPY --from=build-stage /app/main /app/main
WORKDIR /app
EXPOSE 80

ENTRYPOINT [ "/app/main" ]
