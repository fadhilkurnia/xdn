# syntax=docker/dockerfile:1

# This Dockerfile creates the service image. We use multi stage building so the
# final image only contains the compiled binary (~65MB), excluding the source code.
#
# To build the image: 
# docker build --tag=webkv .
#
# To build it for multiple architectures:
# docker buildx build --platform linux/amd64,linux/arm64 --tag=webkv .

# ===== build stage ============================================================
FROM --platform=$BUILDPLATFORM tonistiigi/xx:1.4.0 AS xx

FROM --platform=$BUILDPLATFORM rust:1.83-bullseye AS builder
WORKDIR /app
COPY --from=xx / /
ARG TARGETPLATFORM
ENV PKG_CONFIG_ALLOW_CROSS=1
ENV PKG_CONFIG=xx-pkg-config

## Prepare host dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    clang \
    lld \
    libclang-dev \
    pkg-config \
    libssl-dev \
    cmake \
    perl \
    && rm -rf /var/lib/apt/lists/*

# Target toolchain for cross-compilation
RUN xx-apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libc6-dev

# Set up cross-compilation for C/C++ code (used by rocksdb, openssl-src, etc.)
RUN xx-clang --setup-target-triple

## Copy the source code
COPY Cargo.lock .
COPY Cargo.toml .
COPY src ./src
COPY static ./static
RUN mkdir -p /app/data

RUN xx-cargo build --release --locked
RUN mkdir -p /out \
    && find target -name webkv -path '*/release/webkv' -type f -exec cp {} /out/webkv \;

# ===== release stage ==========================================================
FROM --platform=$TARGETPLATFORM debian:bullseye-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libstdc++6 \
    libgcc-s1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/webkv .
COPY --from=builder /app/static ./static
COPY --from=builder /app/data ./data
EXPOSE 80
CMD ["./webkv"]
