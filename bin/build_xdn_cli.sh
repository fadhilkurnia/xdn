#!/bin/bash
# build_xdn_cli.sh - a script to compile 'xdn' cli.

function main() {
  # validate the program name, which must be 'build_xdn_cli.sh'.
  local PROGRAM_NAME=$(basename "$0")
  if [[ "$PROGRAM_NAME" != "build_xdn_cli.sh" ]]; then
    echo "Invalid program name. Must be 'build_xdn_cli.sh'."
    exit 1
  fi

  # validate that you have go installed already
  if ! command -v go &> /dev/null; then
      echo "'go' could not be found in your machine, please install it first."
      exit 1
  fi

  # change directory to the cli source code, given this script location
  cd "$(dirname "$0")"
  cd ../
  local XDN_ROOT="$PWD"
  cd ./xdn-cli/

  local BIN_DIR="${XDN_ROOT}/bin"
  local LINUX_AMD64_BIN="${BIN_DIR}/xdn-linux-amd64"
  local DARWIN_ARM64_BIN="${BIN_DIR}/xdn-darwin-arm64"
  local HOST_OS
  local HOST_ARCH

  # build the xdn cli for linux/amd64 and darwin/arm64
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o "${LINUX_AMD64_BIN}" .
  CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -o "${DARWIN_ARM64_BIN}" .

  HOST_OS="$(uname -s)"
  HOST_ARCH="$(uname -m)"

  local HOST_BIN=""
  case "${HOST_OS}-${HOST_ARCH}" in
    Linux-x86_64)
      HOST_BIN="${LINUX_AMD64_BIN}"
      ;;
    Darwin-arm64)
      HOST_BIN="${DARWIN_ARM64_BIN}"
      ;;
    *)
      echo "Warning: Unsupported host (${HOST_OS}/${HOST_ARCH})."
      echo "Built binaries:"
      echo "  ${LINUX_AMD64_BIN}"
      echo "  ${DARWIN_ARM64_BIN}"
      echo "Create a suitable alias manually if needed."
      ;;
  esac

  local SYSTEM_LINK="/usr/local/bin/xdn"
  if [[ -n "${HOST_BIN}" ]]; then
    ln -sf "${HOST_BIN}" "${BIN_DIR}/xdn"

    # Also expose `xdn` system-wide so it resolves on $PATH without manual setup.
    # Try without sudo first (works on CI runners and on macOS where
    # /usr/local/bin is user-writable); fall back to sudo on Linux dev machines.
    if ln -sf "${HOST_BIN}" "${SYSTEM_LINK}" 2>/dev/null; then
      echo "Symlink created: ${SYSTEM_LINK} -> ${HOST_BIN}"
    elif command -v sudo &> /dev/null && sudo ln -sf "${HOST_BIN}" "${SYSTEM_LINK}"; then
      echo "Symlink created (with sudo): ${SYSTEM_LINK} -> ${HOST_BIN}"
    else
      echo "Warning: could not create ${SYSTEM_LINK}. To install manually:"
      echo "  sudo ln -sf ${HOST_BIN} ${SYSTEM_LINK}"
    fi
  fi

  echo "Success! binaries generated:"
  echo "  ${LINUX_AMD64_BIN}"
  echo "  ${DARWIN_ARM64_BIN}"
  echo "Alias (if supported) at ${BIN_DIR}/xdn and ${SYSTEM_LINK}"
}

main "$@"
