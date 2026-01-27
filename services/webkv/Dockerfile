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
FROM rust:1.83-bullseye AS builder
WORKDIR /app

## Copy the source code
COPY Cargo.lock .
COPY Cargo.toml .
COPY src ./src
COPY static ./static
RUN mkdir -p /app/data

## Prepare dependencies
RUN apt-get update && apt-get install -y \
    clang \
    libclang-dev \
    pkg-config \
    libssl-dev

ENV OPENSSL_STATIC=1
ENV PKG_CONFIG_ALL_STATIC=1
RUN cargo build --release --locked \
    && strip target/release/webkv

# ===== release stage ==========================================================
FROM gcr.io/distroless/base-debian12
WORKDIR /app
COPY --from=builder /app/target/release/webkv .
COPY --from=builder /app/static ./static
COPY --from=builder /app/data ./data
# Distroless doesn't ship OpenSSL 1.1 runtime libs; copy only what we need.
COPY --from=builder /usr/lib/*-linux-gnu/libssl.so.1.1 /usr/lib/
COPY --from=builder /usr/lib/*-linux-gnu/libcrypto.so.1.1 /usr/lib/
COPY --from=builder /usr/lib/*-linux-gnu/libstdc++.so.6 /usr/lib/
COPY --from=builder /lib/*-linux-gnu/libgcc_s.so.1 /lib/
EXPOSE 80
CMD ["./webkv"]
