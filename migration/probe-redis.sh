#!/bin/bash
# probe-redis — like probe.sh but speaks redis. PING continuously against SRC,
# fail over to DST on first error, log epoch.ns + host + exit + body.
set -uo pipefail
SRC=$1
DST=$2
PORT=${3:-6379}
DURATION=${4:-30}
END=$(( $(date +%s) + DURATION ))
ON=SRC
while [ "$(date +%s)" -lt "$END" ]; do
  HOST=$SRC; [ "$ON" = DST ] && HOST=$DST
  TS=$(date +%s.%N)
  R=$(timeout 0.4 redis-cli -h "$HOST" -p "$PORT" PING 2>&1)
  EC=$?
  printf '%s\t%s\t%d\t%s\n' "$TS" "$HOST" "$EC" "$R"
  if [ $EC -ne 0 ] && [ "$ON" = SRC ]; then
    ON=DST
  fi
done
