# syntax=docker/dockerfile:1

# This Dockerfile creates the service image statically, including the sqlite
# library. We use multi stage building so the final image only contains the 
# compiled binary (~10MB), excluding the source code.

# To build the image: 
# docker build --tag=todo-simple .

# ===== build stage ============================================================
FROM rust:1.81-alpine AS builder
WORKDIR /app

# Copy the source code
COPY Cargo.lock .
COPY Cargo.toml .
COPY src ./src
COPY static ./static
RUN mkdir -p /app/data

# Prepare dependencies
RUN apk add --no-cache musl-dev sqlite-static openssl-dev \
    openssl-libs-static pkgconf

# Set `SYSROOT` to a dummy path (default is /usr) because pkg-config-rs *always*
# links those located in that path dynamically but we want static linking, c.f.
# https://github.com/rust-lang/pkg-config-rs/blob/54325785816695df031cef3b26b6a9a203bbc01b/src/lib.rs#L613
ENV SYSROOT=/dummy

# The env vars tell libsqlite3-sys to statically link libsqlite3.
ENV SQLITE3_STATIC=1 SQLITE3_LIB_DIR=/usr/lib/

RUN cargo build --bins --release


# ===== release stage ==========================================================
FROM scratch
WORKDIR /app
COPY --from=builder /app/target/release/todo-simple .
COPY --from=builder /app/static ./static
COPY --from=builder /app/data ./data
EXPOSE 80
CMD ["./todo-simple"]
