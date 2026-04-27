#!/bin/bash
# build_xdn_fuselog.sh - a script to compile custom FUSE-based
# filesystem for xdn's primary backup in the Active Replica machines.
# Specifically, the binary of 'fuselog' and 'fuselog-apply'.
#
# Usage:
#   ./bin/build_xdn_fuselog.sh           # Build both C++ and Rust
#   ./bin/build_xdn_fuselog.sh cpp       # Build C++ only
#   ./bin/build_xdn_fuselog.sh rust      # Build Rust only
#   ./bin/build_xdn_fuselog.sh install   # Synonym for `all` (kept for back-compat)
#
# Every target installs /usr/local/bin/ symlinks for whatever was built.
# The Java state-diff recorders (FuselogStateDiffRecorder, FuseRustStateDiffRecorder)
# look up these binaries by absolute path under /usr/local/bin/.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CPP_DIR="$PROJECT_ROOT/xdn-fs/cpp"
RUST_DIR="$PROJECT_ROOT/xdn-fs/rust"
BIN_DIR="$PROJECT_ROOT/bin"

function main() {
  # validate the program name, which must be 'build_xdn_fuselog.sh'.
  local PROGRAM_NAME=$(basename "$0")
  if [[ "$PROGRAM_NAME" != "build_xdn_fuselog.sh" ]]; then
    echo "Invalid program name. Must be 'build_xdn_fuselog.sh'."
    exit 1
  fi

  # validate that you use Linux to compile the filesystem
  if [[ "$(uname)" != "Linux" ]]; then
    echo "Sorry, currently we can only compile fuselog on Linux."
    echo "That is the machine that we use for edge server."
    exit 1
  fi

  local TARGET="${1:-all}"

  case "$TARGET" in
    cpp)
      build_cpp
      ;;
    rust)
      build_rust
      ;;
    all|install)
      build_cpp
      build_rust
      ;;
    *)
      echo "Unknown target: $TARGET"
      echo "Usage: $0 [cpp|rust|install|all]"
      exit 1
      ;;
  esac

  stage_project_binaries
  install_symlinks
  echo "Build complete."
}

function build_cpp() {
  echo "=== Building C++ fuselog ==="

  # check for libfuse3
  if ! pkg-config --exists fuse3 2>/dev/null; then
    echo "Error: libfuse3 not found. Install with: apt install pkg-config libfuse3-dev"
    exit 1
  fi

  # check for libzstd
  if ! pkg-config --exists libzstd 2>/dev/null; then
    echo "Error: libzstd-dev not found. Install with: apt install libzstd-dev"
    exit 1
  fi

  echo "  Compiling fuselogv2.cpp -> fuselog ..."
  g++ -Wall "$CPP_DIR/fuselogv2.cpp" -o "$CPP_DIR/fuselog" \
    -D_FILE_OFFSET_BITS=64 \
    $(pkg-config fuse3 --cflags --libs) \
    $(pkg-config libzstd --cflags --libs) \
    -pthread -O3 -std=c++11

  echo "  Compiling fuselog-apply.cpp -> fuselog-apply ..."
  g++ -Wall "$CPP_DIR/fuselog-apply.cpp" -o "$CPP_DIR/fuselog-apply" \
    $(pkg-config libzstd --cflags --libs) \
    -O3 -std=c++20

  echo "  C++ binaries built in $CPP_DIR/"
  ls -lh "$CPP_DIR/fuselog" "$CPP_DIR/fuselog-apply"
}

function build_rust() {
  echo "=== Building Rust fuselog ==="

  if ! command -v cargo &>/dev/null; then
    echo "Error: cargo not found. Install Rust from https://rustup.rs"
    exit 1
  fi

  echo "  Building fuselog_core (release) ..."
  cd "$RUST_DIR" && cargo build --release

  echo "  Rust binaries built:"
  ls -lh "$RUST_DIR/target/release/fuselog_core" 2>/dev/null || true
  ls -lh "$RUST_DIR/target/release/fuselog_apply" 2>/dev/null || true
}

function stage_project_binaries() {
  echo "=== Staging binaries into $BIN_DIR/ ==="
  mkdir -p "$BIN_DIR"

  if [[ -f "$CPP_DIR/fuselog" ]]; then
    cp "$CPP_DIR/fuselog" "$BIN_DIR/fuselog"
    echo "  Staged fuselog"
  fi
  if [[ -f "$CPP_DIR/fuselog-apply" ]]; then
    cp "$CPP_DIR/fuselog-apply" "$BIN_DIR/fuselog-apply"
    echo "  Staged fuselog-apply"
  fi
  if [[ -f "$RUST_DIR/target/release/fuselog_core" ]]; then
    cp "$RUST_DIR/target/release/fuselog_core" "$BIN_DIR/fuserust"
    echo "  Staged fuserust"
  fi
  if [[ -f "$RUST_DIR/target/release/fuselog_apply" ]]; then
    cp "$RUST_DIR/target/release/fuselog_apply" "$BIN_DIR/fuserust-apply"
    echo "  Staged fuserust-apply"
  fi
}

function install_symlinks() {
  echo "=== Installing symlinks at /usr/local/bin/ ==="

  local INSTALL_DIR="/usr/local/bin"
  local names=(fuselog fuselog-apply fuserust fuserust-apply)
  local name src dst

  for name in "${names[@]}"; do
    src="$BIN_DIR/$name"
    dst="$INSTALL_DIR/$name"

    # Only symlink binaries that this invocation actually built+staged.
    # `cpp` runs leave fuserust* missing; `rust` runs leave fuselog* missing.
    [[ -f "$src" ]] || continue

    # Try without sudo first (works on CI runners and on macOS where
    # /usr/local/bin is user-writable); fall back to sudo on Linux dev machines.
    if ln -sf "$src" "$dst" 2>/dev/null; then
      echo "  Symlinked: $dst -> $src"
    elif command -v sudo &>/dev/null && sudo ln -sf "$src" "$dst"; then
      echo "  Symlinked (with sudo): $dst -> $src"
    else
      echo "  Warning: could not create $dst. To install manually:"
      echo "    sudo ln -sf $src $dst"
    fi
  done
}

main "$@"
