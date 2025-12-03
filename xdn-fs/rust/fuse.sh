#!/bin/bash 

MOUNT_DIR="/tmp/fuse/"
mkdir -p "$MOUNT_DIR"

sudo ADAPTIVE_DEV_MODE=false \
     FUSELOG_COMPRESSION=false \
     FUSELOG_PRUNE=false \
     WRITE_COALESCING=false \
     ADAPTIVE_COMPRESSION=false \
     RUST_LOG=info \
     ./fuse_rust/target/release/fuselog_core "$MOUNT_DIR"
