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

  case "${HOST_OS}-${HOST_ARCH}" in
    Linux-x86_64)
      ln -sf "${LINUX_AMD64_BIN}" "${BIN_DIR}/xdn"
      ;;
    Darwin-arm64)
      ln -sf "${DARWIN_ARM64_BIN}" "${BIN_DIR}/xdn"
      ;;
    *)
      echo "Warning: Unsupported host (${HOST_OS}/${HOST_ARCH})."
      echo "Built binaries:"
      echo "  ${LINUX_AMD64_BIN}"
      echo "  ${DARWIN_ARM64_BIN}"
      echo "Create a suitable alias manually if needed."
      ;;
  esac

  if [[ -f "${BIN_DIR}/xdn" ]]; then
    local INSTALL_DIR="/usr/local/bin"
    local SUDO=""
    if [[ ! -w "${INSTALL_DIR}" ]]; then
      if command -v sudo &> /dev/null; then
        SUDO="sudo"
      else
        echo "Warning: ${INSTALL_DIR} is not writable and sudo is unavailable; skipping system-wide symlink."
        INSTALL_DIR=""
      fi
    fi
    if [[ -n "${INSTALL_DIR}" ]]; then
      $SUDO ln -sf "${BIN_DIR}/xdn" "${INSTALL_DIR}/xdn"
      echo "Symlinked ${INSTALL_DIR}/xdn -> ${BIN_DIR}/xdn"
    fi
  fi

  echo "Success! binaries generated:"
  echo "  ${LINUX_AMD64_BIN}"
  echo "  ${DARWIN_ARM64_BIN}"
  echo "Alias (if supported) at ${BIN_DIR}/xdn"
  echo ""
  echo "To run 'xdn' from anywhere in your machine, add XDN's binary directory into your \$PATH:"
  echo "  export PATH=$XDN_ROOT/bin/:\$PATH"
  echo ""
  echo "assuming \$XDN_ROOT is $XDN_ROOT"
}

main "$@"
