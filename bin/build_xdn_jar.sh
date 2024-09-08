#!/bin/bash
# build_xdn_jar.sh - a script to compile 'xdn' jar runnable in
# the Reconfigurator or Active Replica machines.

function main() {
  # validate the program name, which must be 'build_xdn_jar.sh'.
  local PROGRAM_NAME=$(basename "$0")
  if [[ "$PROGRAM_NAME" != "build_xdn_jar.sh" ]]; then
    echo "Invalid program name. Must be 'build_xdn_jar.sh'."
    exit 1
  fi

  # validate that you have ant and java installed already
  if ! command -v ant &> /dev/null; then
      echo "'ant' could not be found in your machine, please install it first."
      exit 1
  fi
  if ! command -v java &> /dev/null; then
      echo "'java' could not be found in your machine, please install it first."
      exit 1
  fi

  # change directory to the xdn source code, given this script location
  cd "$(dirname "$0")"
  cd ../
  local XDN_ROOT="$PWD"

  # actually build the xdn jar
  ant clean
  ant jar

  echo ""
  echo "Success! the XDN jars are accessible at \$XDN_ROOT/jars"
  echo ""
  echo "Check https://xdn.cs.umass.edu/ to see how to run the jar"
  echo "in either control plane machines (Reconfigurator/RC) or"
  echo "edge server deployment machines (ActiveReplica/AR)"
  echo "with an appropriate config."
}

main "$@"