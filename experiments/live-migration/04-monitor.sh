#!/usr/bin/env bash
# Continuously hits the replica's service to measure downtime DURING a migration.
# Run it on EITHER host (both are on 10.10.1.0/24) while you kick off ./03-migrate.sh
# from the other terminal. Ctrl-C to stop and print a summary.
#
#   ./04-monitor.sh           # ~20 req/s
#   INTERVAL=0.02 ./04-monitor.sh
set -euo pipefail
source "$(dirname "$0")/config.sh"

INTERVAL="${INTERVAL:-0.05}"     # seconds between probes
TIMEOUT="${TIMEOUT:-2}"          # per-request timeout

ok=0; fail=0; first_fail=""; last_fail=""; gap_start=""; max_gap=0
trap 'summary' INT TERM
summary() {
  echo
  say "summary"
  printf '  ok=%d  fail=%d  (%.2f%% success)\n' "$ok" "$fail" \
    "$(echo "scale=2; ($ok*100)/($ok+$fail+0.0001)" | bc)"
  if [ "$fail" -gt 0 ]; then
    printf '  longest continuous outage (downtime): ~%.2f s\n' "$max_gap"
    printf '  first failure: %s   last failure: %s\n' "$first_fail" "$last_fail"
  else
    echo "  no failed requests observed — zero measured downtime"
  fi
  exit 0
}

say "probing ${SERVICE_URL} every ${INTERVAL}s (Ctrl-C for summary)"
while true; do
  now="$(date +%s.%N)"
  if curl -fsS --max-time "$TIMEOUT" "$SERVICE_URL" >/dev/null 2>&1; then
    ok=$((ok+1))
    if [ -n "$gap_start" ]; then
      gap="$(echo "$now - $gap_start" | bc)"
      awk "BEGIN{exit !($gap>$max_gap)}" && max_gap="$gap"
      printf '\033[1;32m  recovered\033[0m after ~%.2fs outage\n' "$gap"
      gap_start=""
    fi
    printf '.'
  else
    fail=$((fail+1))
    [ -z "$gap_start" ] && gap_start="$now"
    ts="$(date +%H:%M:%S.%2N)"
    [ -z "$first_fail" ] && first_fail="$ts"
    last_fail="$ts"
    printf '\033[1;31mX\033[0m'
  fi
  sleep "$INTERVAL"
done
