#!/usr/bin/env bash

set -euo pipefail

VERSION="${GOOGLE_JAVA_FORMAT_VERSION:-1.17.0}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIRS=(
  "${REPO_ROOT}/src/edu/umass/cs/xdn"
  "${REPO_ROOT}/test"
)
JAR_PATH="${REPO_ROOT}/bin/google-java-format-${VERSION}-all-deps.jar"
JAR_URL="https://github.com/google/google-java-format/releases/download/v${VERSION}/google-java-format-${VERSION}-all-deps.jar"
JAVA_FILES=()

usage() {
  cat <<'EOF'
Usage: bin/google-java-format.sh [--check]

Formats XDN Java sources (including tests) with google-java-format.

  --check   Run formatter in dry-run mode and fail on changes.
  -h|--help Show this help text.
EOF
}

download_jar_if_needed() {
  if [[ -f "${JAR_PATH}" ]]; then
    return
  fi

  echo "Downloading google-java-format ${VERSION} ..."
  mkdir -p "$(dirname "${JAR_PATH}")"
  curl -fL -o "${JAR_PATH}" "${JAR_URL}"
}

collect_java_files() {
  JAVA_FILES=()
  while IFS= read -r -d '' file; do
    JAVA_FILES+=("$file")
  done < <(find "${TARGET_DIRS[@]}" -type f -name '*.java' -print0 2>/dev/null || true)
}

run_formatter() {
  local mode="$1"
  download_jar_if_needed
  collect_java_files

  if (( ${#JAVA_FILES[@]} == 0 )); then
    echo "No Java files found under: ${TARGET_DIRS[*]}"
    return 0
  fi

  if [[ "${mode}" == "check" ]]; then
    java -jar "${JAR_PATH}" --dry-run --set-exit-if-changed "${JAVA_FILES[@]}"
  else
    java -jar "${JAR_PATH}" -i "${JAVA_FILES[@]}"
  fi
}

main() {
  local mode="format"

  case "${1:-}" in
    --check)
      mode="check"
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    "")
      ;;
    *)
      echo "Unknown option: ${1}" >&2
      usage
      exit 1
      ;;
  esac

  run_formatter "${mode}"
}

main "$@"
