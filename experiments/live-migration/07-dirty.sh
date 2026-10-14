#!/usr/bin/env bash
# Start/stop a RAM-dirtying workload INSIDE the guest, to exercise live-migration
# convergence (and to make XBZRLE matter). Run from either host.
#   ./07-dirty.sh start [ws_MB] [rate_MBps]   # default 256 MiB @ 40 MB/s
#   ./07-dirty.sh stop
#   ./07-dirty.sh status
set -euo pipefail
source "$(dirname "$0")/config.sh"

G="sudo ssh -i ${MIG_KEY} -o StrictHostKeyChecking=accept-new -o ConnectTimeout=8 ubuntu@${VM_IP}"

case "${1:-status}" in
  start)
    WS="${2:-256}"; RATE="${3:-40}"
    $G "cat > /tmp/dirty.py" < "$(dirname "$0")/dirty.py"
    $G "pkill -f dirty.py 2>/dev/null || true; nohup python3 /tmp/dirty.py ${WS} ${RATE} >/tmp/dirty.log 2>&1 & sleep 1; pgrep -af dirty.py" \
      && say "dirtying ${WS} MiB @ ${RATE} MB/s inside the guest"
    ;;
  stop)
    $G "pkill -f dirty.py 2>/dev/null; echo stopped" ;;
  status)
    $G "pgrep -af dirty.py || echo 'not running'" ;;
  *) die "usage: $0 {start|stop|status} [ws_MB] [rate_MBps]" ;;
esac
