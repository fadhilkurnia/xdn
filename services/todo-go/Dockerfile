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

RUN apt-get update && \
    if [ "$TARGETPLATFORM" = "linux/amd64" ]; then \
        apt-get install -y gcc-x86-64-linux-gnu sqlite3 libsqlite3-dev; \
    else \
        apt-get install -y gcc sqlite3 libsqlite3-dev; \
    fi
COPY go.mod go.sum ./
RUN go mod download

RUN if [ "$TARGETPLATFORM" = "linux/amd64" ]; then \
        CC=x86_64-linux-gnu-gcc \
        CGO_ENABLED=1 GOOS=$TARGETOS GOARCH=$TARGETARCH \
        CGO_CFLAGS="-O2" \
        CGO_LDFLAGS="-O2" \
        go build -buildvcs=false -trimpath \
        -ldflags '-w -s -extldflags "-static"' -a -o main . ; \
    else \
        CGO_ENABLED=1 GOOS=$TARGETOS GOARCH=$TARGETARCH \
        go build -buildvcs=false -trimpath \
        -ldflags '-w -s -extldflags "-static"' -a -o main . ; \
    fi

# ===== release stage =========================================

FROM scratch
COPY --from=build-stage /app/main /app/main
WORKDIR /app
EXPOSE 80

ENTRYPOINT [ "/app/main" ]
