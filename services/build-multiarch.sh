#!/usr/bin/env bash
#
# Build and push multi-architecture (linux/amd64 + linux/arm64) images for the
# XDN service images that were previously published amd64-only. Run from the
# repo's services/ directory after `docker login -u fadhilkurnia`.
#
# Requires a docker-container buildx builder and QEMU binfmt for arm64:
#   docker run --privileged --rm tonistiigi/binfmt --install arm64
#   docker buildx create --name xdn-multiarch --driver docker-container --bootstrap --use
#
# Usage: ./build-multiarch.sh [--push]   (omit --push to validate only)

set -uo pipefail

BUILDER="xdn-multiarch"
PLATFORMS="linux/amd64,linux/arm64"
PUSH_FLAG="${1:-}"
OUT="${PUSH_FLAG:+--push}"
[ -z "$OUT" ] && OUT="" && echo "[mode] validate-only (no --push); pass --push to publish"

# image repo (under fadhilkurnia/) -> source context directory
declare -A IMAGES=(
  ["xdn-rsmbench"]="rsmbench"
  ["xdn-webkv"]="webkv"
  ["xdn-tpcc"]="tpcc"
  ["xdn-tpcc-java"]="tpcc-java"
  ["xdn-todo"]="todo-go"
  ["xdn-bookcatalog"]="bookcatalog"
  ["xdn-bookcatalog-nd"]="bookcatalog-nd"
  ["xdn-bookcatalog-mongo"]="bookcatalog-mongo"
  ["xdn-noop"]="noop"
  ["xdn-smallbank"]="smallbank"
  ["xdn-seats"]="seats"
  ["xdn-ecommerce"]="ecommerce"
  ["xdn-socialnetwork"]="socialnetwork"
  ["xdn-moviereview"]="moviereview"
)
REPOS="xdn-rsmbench xdn-webkv xdn-tpcc xdn-tpcc-java xdn-todo \
       xdn-bookcatalog xdn-bookcatalog-nd xdn-bookcatalog-mongo \
       xdn-noop xdn-smallbank xdn-seats xdn-ecommerce \
       xdn-socialnetwork xdn-moviereview"

cd "$(dirname "$0")"  # services/
declare -A RESULT
for repo in $REPOS; do
  ctx="${IMAGES[$repo]}"
  tag="fadhilkurnia/${repo}:latest"
  echo "======================================================================"
  echo "[build] $tag  <-  services/$ctx   ($PLATFORMS)"
  echo "======================================================================"
  if docker buildx build --builder "$BUILDER" --platform "$PLATFORMS" \
        -t "$tag" $OUT "$ctx"; then
    RESULT[$repo]="OK"
  else
    RESULT[$repo]="FAILED"
  fi
done

echo
echo "================== SUMMARY =================="
for repo in $REPOS; do
  printf "  %-22s %s\n" "$repo" "${RESULT[$repo]}"
done
