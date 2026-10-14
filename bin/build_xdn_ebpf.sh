#!/usr/bin/env bash

# Build the eBPF state diff recorder and install the binaries expected by XDN.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
EBPF_DIR="$PROJECT_ROOT/experiments/eBPF"
BIN_DIR="$PROJECT_ROOT/bin"
INSTALL_DIR="/usr/local/bin"
BINARIES=(statediff_vfs statediff_apply_vfs)

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "Error: the eBPF state diff recorder can only be built on Linux." >&2
  exit 1
fi

for command_name in make clang cc git; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "Error: required command '$command_name' was not found." >&2
    exit 1
  fi
done

if [[ ! -r /sys/kernel/btf/vmlinux ]]; then
  echo "Error: kernel BTF is unavailable at /sys/kernel/btf/vmlinux." >&2
  exit 1
fi

echo "Building eBPF state diff recorder binaries ..."
make -C "$EBPF_DIR" all

echo "Staging binaries in $BIN_DIR ..."
for binary in "${BINARIES[@]}"; do
  install -m 0755 "$EBPF_DIR/$binary" "$BIN_DIR/$binary"
done

echo "Installing symlinks in $INSTALL_DIR ..."
for binary in "${BINARIES[@]}"; do
  source_path="$BIN_DIR/$binary"
  target_path="$INSTALL_DIR/$binary"

  if ln -sfn "$source_path" "$target_path" 2>/dev/null; then
    echo "  $target_path -> $source_path"
  elif command -v sudo >/dev/null 2>&1 && sudo ln -sfn "$source_path" "$target_path"; then
    echo "  $target_path -> $source_path"
  else
    echo "Error: failed to install $target_path." >&2
    exit 1
  fi
done

echo "eBPF state diff recorder build complete."
