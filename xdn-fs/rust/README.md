# FuseLog (Rust)

## Components
- `fuselog_core`: A FUSE filesystem that records file operations into a StateDiffLog and exposes them over a Unix socket.
- `fuselog_apply`: A daemon that connects to the socket, decompresses and decodes the diffs, and applies them to a target directory.

## Build
```bash
cargo build --workspace --release
```

## Environment flags
- `FUSELOG_SOCKET_FILE` (default `/tmp/fuselog.sock`)
- `FUSELOG_COMPRESSION` (default `false`): enable standard zstd compression for diff payloads.
- `ADAPTIVE_COMPRESSION` (default `false`): train and reuse a zstd dictionary for better compression.
- `ADAPTIVE_DEV_MODE` (default `false`): reduce sample size thresholds for dictionary training for quick local testing.
- `WRITE_COALESCING` (default `false`): merge sequential writes before logging to reduce action count.
- `FUSELOG_PRUNE` (default `false`): drop redundant actions.
- `FUSELOG_DAEMON_LOGS` (default `false`): redirect stdout/stderr to `/tmp/fuselog.out` and `/tmp/fuselog.err`.
- `RUST_LOG` (`info`, `debug`): standard env_logger filter.
