# syntax=docker/dockerfile:1

# This Dockerfile creates the service image using the default
# sqlite datastore. We use multi stage building so the final
# image only contains the compiled binary (~30MB), excluding 
# the source code.

# To build the image: 
# docker build --tag=bookcatalog .
# docker buildx build --platform linux/amd64,linux/arm64 --tag=bookcatalog .

# ===== build stage =========================================

# setup the base image and working directory
FROM --platform=$BUILDPLATFORM golang:1.21 AS build-stage
ARG TARGETOS
ARG TARGETARCH
ARG TARGETPLATFORM

# import the codebase
WORKDIR /app
ADD src ./src
COPY main.go ./

# prepare dependencies, including gcc
RUN apt-get update && \
    if [ "$TARGETPLATFORM" = "linux/amd64" ]; then \
        apt-get install -y gcc-x86-64-linux-gnu sqlite3 libsqlite3-dev; \
    else \
        apt-get install -y gcc sqlite3 libsqlite3-dev; \
    fi
COPY go.mod go.sum ./
RUN go mod download

# compile the source code
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

# command to run the service
ENTRYPOINT [ "/app/main" ]

