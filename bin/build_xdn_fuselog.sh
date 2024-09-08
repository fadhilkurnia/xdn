#!/bin/bash
# build_xdn_fuselog.sh - a script to compile custom FUSE-based
# filesystem for xdn's primary backup in the Active Replica machines.
# Specifically, the binary of 'fuselog' and 'fuselog-apply'.

# TODO: complete this script!

function main() {
  # validate the program name, which must be 'build_xdn_fuselog.sh'.
  local PROGRAM_NAME=$(basename "$0")
  if [[ "$PROGRAM_NAME" != "build_xdn_fuselog.sh" ]]; then
    echo "Invalid program name. Must be 'build_xdn_fuselog.sh'."
    exit 1
  fi

  # validate that you use Linux to compile the filesystem
  local OS_NAME=$(uname)
  if ! [[ "$OSTYPE" == "linux-gnu-*" ]]; then
    echo "Sorry, currently we can only compile fuselog in Linux machine."
    echo "That is the machine that we use for edge server."
    exit 1
  fi

  echo "unimplemented :("
}

main "$@"