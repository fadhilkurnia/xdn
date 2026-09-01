#!/usr/bin/env bash
#
# benchmark.sh — measure ICMP ping RTT and HTTP request RTT (via curl,
# keep-alive) between this machine and a target ip:port, producing one raw
# sample file per stage.
#
# Usage:
#   ./benchmark.sh --ip <target_ip> --port <target_port> --path <http_path> \
#                   [--count 10000] [--interval 0.01]
#
# Outputs (written to the current directory):
#   ping_raw.txt  — raw `ping -D` output, one line per ICMP echo
#   http_raw.txt  — "time_total=<s> time_connect=<s>" per HTTP request
#
# Notes:
#   - The ping stage requires root when --interval is below 0.2s (200ms),
#     because sub-200ms send intervals need CAP_NET_RAW. With the default
#     interval of 0.01s, this stage runs under sudo. You'll be prompted
#     for your password when the stage starts.
#   - The curl stage reuses a single TCP connection across all requests
#     (HTTP/1.1 keep-alive, default curl behavior for multiple URLs in one
#     invocation to the same host:port). Only the first request will show
#     a non-zero time_connect; that's expected, not a bug.
#   - All samples are kept, including the first of each stage. The first
#     sample can be skewed by ARP cache population (ping) or the TCP
#     handshake cost folded into curl's first keep-alive request — see
#     analyze.py, which reports percentiles both with and without it.

set -euo pipefail

COUNT=10000
INTERVAL=0.01
IP=""
PORT=""
HTTP_PATH=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ip) IP="$2"; shift 2 ;;
    --port) PORT="$2"; shift 2 ;;
    --path) HTTP_PATH="$2"; shift 2 ;;
    --count) COUNT="$2"; shift 2 ;;
    --interval) INTERVAL="$2"; shift 2 ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$IP" || -z "$PORT" || -z "$HTTP_PATH" ]]; then
  echo "Usage: $0 --ip <ip> --port <port> --path <http_path> [--count N] [--interval SEC]" >&2
  exit 1
fi

NEEDS_ROOT_FOR_INTERVAL=$(awk -v i="$INTERVAL" 'BEGIN { print (i < 0.2) ? "1" : "0" }')

echo "== Config =="
echo "target:   $IP:$PORT$HTTP_PATH"
echo "count:    $COUNT"
echo "interval: ${INTERVAL}s"
echo

# ---------------------------------------------------------------------------
# Stage 1: ICMP ping
# ---------------------------------------------------------------------------
echo "[1/2] ICMP ping ($COUNT samples, ${INTERVAL}s interval) -> ping_raw.txt"
if [[ "$NEEDS_ROOT_FOR_INTERVAL" == "1" ]]; then
  sudo ping -D -i "$INTERVAL" -c "$COUNT" "$IP" > ping_raw.txt
else
  ping -D -i "$INTERVAL" -c "$COUNT" "$IP" > ping_raw.txt
fi
echo "done."
echo

# ---------------------------------------------------------------------------
# Stage 2: HTTP RTT via curl, single reused keep-alive connection
# ---------------------------------------------------------------------------
echo "[2/2] HTTP RTT ($COUNT requests, keep-alive) -> http_raw.txt"

CURL_CONFIG=$(mktemp)
trap 'rm -f "$CURL_CONFIG"' EXIT

{
  echo "silent"
  echo "show-error"
  echo 'write-out = "time_total=%{time_total} time_connect=%{time_connect}\n"'
  echo "output = /dev/null"
  for ((i = 0; i < COUNT; i++)); do
    printf 'url = "http://%s:%s%s"\n' "$IP" "$PORT" "$HTTP_PATH"
  done
} > "$CURL_CONFIG"

curl -K "$CURL_CONFIG" > http_raw.txt
echo "done."
echo

echo "All stages complete: ping_raw.txt, http_raw.txt"
