#!/bin/bash
# probe — bang on /inc against the src host, fail over to the dst host once the
# src starts erroring, log every request line with epoch-ns timestamps. Writes
# the log to stdout (so caller can redirect). Run it in the background, then
# kick off the migration in the foreground.
#
# Usage:
#   probe.sh <src-host> <dst-host> <port> <total-seconds>
set -uo pipefail   # NOT -e — we expect curl to fail during the migration window
SRC=$1
DST=$2
PORT=$3
DURATION=${4:-30}
END=$(( $(date +%s) + DURATION ))

# Stay on SRC until we get a connection error or timeout, then immediately
# start probing DST. We don't switch back; once on DST we stay there.
ON=SRC
while [ "$(date +%s)" -lt "$END" ]; do
  HOST=$SRC; [ "$ON" = DST ] && HOST=$DST
  TS=$(date +%s.%N)
  # short timeout so a host that's down doesn't cost us probe granularity.
  BODY=$(curl --max-time 0.3 --connect-timeout 0.2 -sS "http://${HOST}:${PORT}/inc" 2>&1)
  EC=$?
  printf '%s\t%s\t%d\t%s\n' "$TS" "$HOST" "$EC" "$BODY"
  if [ $EC -ne 0 ] && [ "$ON" = SRC ]; then
    ON=DST
  fi
done
