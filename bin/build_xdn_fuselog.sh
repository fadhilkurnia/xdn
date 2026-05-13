#!/bin/bash
# build_xdn_fuselog.sh - a script to compile custom FUSE-based
# filesystem for xdn's primary backup in the Active Replica machines.
# Specifically, the binary of 'fuselog' and 'fuselog-apply'.
#
# Usage:
#   ./bin/build_xdn_fuselog.sh           # Build both C++ and Rust
#   ./bin/build_xdn_fuselog.sh cpp       # Build C++ only
#   ./bin/build_xdn_fuselog.sh rust      # Build Rust only
#   ./bin/build_xdn_fuselog.sh test      # Build and run C++ unit tests (GoogleTest)
#   ./bin/build_xdn_fuselog.sh install   # Build both and install to /usr/local/bin

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
    test)
      build_cpp_tests
      echo "Build complete."
      exit 0
      ;;
    install)
      build_cpp
      build_rust
      install_binaries
      ;;
    all)
      build_cpp
      build_rust
      ;;
    *)
      echo "Unknown target: $TARGET"
      echo "Usage: $0 [cpp|rust|test|install|all]"
      exit 1
      ;;
  esac

  stage_project_binaries
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

function build_cpp_tests() {
  echo "=== Building and running C++ unit tests ==="

  # GoogleTest sources/libs. Most distros split between -lgtest (compiled
  # libs from libgtest-dev's prebuilt package) and the source tree in
  # /usr/src/googletest. Prefer prebuilt libs; if missing, compile from src.
  local GTEST_CXXFLAGS=""
  local GTEST_LDFLAGS=""
  if pkg-config --exists gtest_main 2>/dev/null; then
    GTEST_CXXFLAGS=$(pkg-config gtest_main --cflags)
    GTEST_LDFLAGS=$(pkg-config gtest_main --libs)
  elif [[ -f /usr/lib/x86_64-linux-gnu/libgtest_main.a ]] || \
       [[ -f /usr/lib/libgtest_main.a ]]; then
    GTEST_LDFLAGS="-lgtest_main -lgtest -pthread"
  else
    echo "Error: GoogleTest not found."
    echo "Install with: apt install libgtest-dev"
    echo "(On Debian/Ubuntu you may also need: apt install googletest && "
    echo " cd /usr/src/googletest && cmake . && make && cp lib/*.a /usr/lib/)"
    exit 1
  fi

  echo "  Compiling test_fuselog.cpp -> test_fuselog ..."
  g++ -Wall -O2 -g -std=c++17 \
    "$CPP_DIR/test_fuselog.cpp" -o "$CPP_DIR/test_fuselog" \
    $GTEST_CXXFLAGS $GTEST_LDFLAGS -pthread

  echo "  Running tests ..."
  "$CPP_DIR/test_fuselog"
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

function install_binaries() {
  echo "=== Installing binaries to /usr/local/bin/ ==="

  local INSTALL_DIR="/usr/local/bin"

  # C++ binaries
  if [[ -f "$CPP_DIR/fuselog" ]]; then
    cp "$CPP_DIR/fuselog" "$INSTALL_DIR/fuselog"
    echo "  Installed fuselog"
  fi
  if [[ -f "$CPP_DIR/fuselog-apply" ]]; then
    cp "$CPP_DIR/fuselog-apply" "$INSTALL_DIR/fuselog-apply"
    echo "  Installed fuselog-apply"
  fi

  # Rust binaries
  if [[ -f "$RUST_DIR/target/release/fuselog_core" ]]; then
    cp "$RUST_DIR/target/release/fuselog_core" "$INSTALL_DIR/fuserust"
    echo "  Installed fuserust"
  fi
  if [[ -f "$RUST_DIR/target/release/fuselog_apply" ]]; then
    cp "$RUST_DIR/target/release/fuselog_apply" "$INSTALL_DIR/fuserust-apply"
    echo "  Installed fuserust-apply"
  fi

  echo "  Installation complete."
}

main "$@"
