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
  cd ./cli/

  # actually build the xdn cli
  go build -o ../bin/xdn -v .

  echo "Success! the CLI is accessible at \$XDN_ROOT/bin/xdn"
  echo ""
  echo "To run 'xdn' from anywhere in your machine, add XDN's binary directory into your \$PATH:"
  echo "  export PATH=$XDN_ROOT/bin/:\$PATH"
  echo ""
  echo "assuming \$XDN_ROOT is $XDN_ROOT"
}

main "$@"