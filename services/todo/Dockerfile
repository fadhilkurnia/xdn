# syntax=docker/dockerfile:1

# Working, small-ish image for the TODO service.
# - Single-arch build:
#     docker build -t todo-simple .
# - Multi-arch build (requires buildx + QEMU):
#     docker buildx build --platform linux/amd64,linux/arm64 -t todo-simple .

ARG RUST_VERSION=1.78

# ===== build stage ============================================================
# Build for the target platform; buildx will run this stage per-arch.
FROM rust:${RUST_VERSION}-alpine AS builder
WORKDIR /app

# Build deps (musl + sqlite bundled build requirements).
RUN apk add --no-cache build-base musl-dev pkgconfig ca-certificates
RUN rustup target add x86_64-unknown-linux-musl aarch64-unknown-linux-musl

# Build the binary (static via musl).
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY static ./static
ARG TARGETARCH
RUN case "$TARGETARCH" in \
      amd64) TARGET_TRIPLE="x86_64-unknown-linux-musl" ;; \
      arm64) TARGET_TRIPLE="aarch64-unknown-linux-musl" ;; \
      *) echo "unsupported TARGETARCH=$TARGETARCH" && exit 1 ;; \
    esac && \
    cargo build --release --target "${TARGET_TRIPLE}" && \
    mkdir -p /app/out && \
    cp "/app/target/${TARGET_TRIPLE}/release/todo-simple" /app/out/todo-simple

# ===== runtime stage (scratch) ================================================
FROM scratch AS runtime
WORKDIR /app

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

COPY --from=builder /app/out/todo-simple /app/todo-simple
COPY --from=builder /app/static /app/static

ENV PORT=80
EXPOSE 80
CMD ["/app/todo-simple"]
