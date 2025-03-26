# syntax=docker/dockerfile:1

# This Dockerfile creates the service image. We use multi stage building so the
# final image only contains the compiled binary (~65MB), excluding the source code.
#
# To build the image: 
# docker build --tag=restkv .
#
# To build it for multiple architectures:
# docker buildx build --platform linux/amd64,linux/arm64 --tag=restkv .

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
    libclang-dev

RUN cargo build --release

# ===== release stage ==========================================================
FROM gcr.io/distroless/cc
WORKDIR /app
COPY --from=builder /app/target/release/restkv .
COPY --from=builder /app/static ./static
COPY --from=builder /app/data ./data
EXPOSE 80
CMD ["./restkv"]
