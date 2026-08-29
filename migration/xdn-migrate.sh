#!/bin/bash
# xdn-migrate — naive single-shot container migration (no pre-copy).
#
# Usage:
#   xdn-migrate.sh <src-user@host> <dst-user@host> <container-name>
#
# Prints a JSON report on stdout with checkpoint/scp/restore timings and the
# wall-clock window from "source paused" to "destination running".
set -euo pipefail

SRC=$1
DST=$2
CONTAINER=$3
WORK_PREFIX=${WORK_PREFIX:-/tmp/xdn-migrate}
ARCHIVE_NAME="${CONTAINER}-cp.tar.gz"

ts() { date +%s.%N; }

T_START=$(ts)

# 1. checkpoint on source. --tcp-established lets CRIU dump open TCP sockets
#    too (required if external clients have keep-alive connections at the
#    moment of checkpoint). --leave-stopped (default) pauses+exits the source
#    container, which is the moment external requests start failing.
ssh -o BatchMode=yes "$SRC" "
  set -e
  sudo mkdir -p $WORK_PREFIX
  sudo podman --runtime runc container checkpoint --tcp-established $CONTAINER \
       --export=$WORK_PREFIX/$ARCHIVE_NAME
  sudo chmod a+r $WORK_PREFIX/$ARCHIVE_NAME
" >/dev/null
T_CHECKPOINT_DONE=$(ts)

# 2. ship archive directly from source to destination. Ensure the work dir
#    exists on dst before scp so scp's destination is a real path (scp doesn't
#    create directories).
ssh -o BatchMode=yes "$DST" "sudo mkdir -p $WORK_PREFIX && sudo chmod 0777 $WORK_PREFIX" >/dev/null
ssh -o BatchMode=yes "$SRC" "scp -o BatchMode=yes $WORK_PREFIX/$ARCHIVE_NAME ${DST}:$WORK_PREFIX/$ARCHIVE_NAME" >/dev/null
T_SCP_DONE=$(ts)

# 3. restore on destination. podman re-creates the container with the same
#    name from the embedded spec inside the archive. --tcp-established matches
#    the checkpoint side.
ssh -o BatchMode=yes "$DST" "
  set -e
  sudo mkdir -p $WORK_PREFIX
  sudo podman --runtime runc rm -f $CONTAINER 2>/dev/null || true
  sudo podman --runtime runc container restore --tcp-established \
       --import=$WORK_PREFIX/$ARCHIVE_NAME
" >/dev/null
T_RESTORE_DONE=$(ts)

# 4. cleanup: tear down the (now-stopped) source container and remove archives.
ssh -o BatchMode=yes "$SRC" "
  sudo podman --runtime runc rm -f $CONTAINER 2>/dev/null || true
  sudo rm -f $WORK_PREFIX/$ARCHIVE_NAME
" >/dev/null
ssh -o BatchMode=yes "$DST" "sudo rm -f $WORK_PREFIX/$ARCHIVE_NAME" >/dev/null

T_END=$(ts)

# archive size from the source's filesystem before we deleted it would be ideal,
# but easier to just snapshot it before scp; redo with `du`.
cat <<EOF
{
  "container": "$CONTAINER",
  "src": "$SRC",
  "dst": "$DST",
  "checkpoint_sec": $(echo "$T_CHECKPOINT_DONE - $T_START" | bc),
  "scp_sec":        $(echo "$T_SCP_DONE - $T_CHECKPOINT_DONE" | bc),
  "restore_sec":    $(echo "$T_RESTORE_DONE - $T_SCP_DONE" | bc),
  "cleanup_sec":    $(echo "$T_END - $T_RESTORE_DONE" | bc),
  "downtime_sec":   $(echo "$T_RESTORE_DONE - $T_START" | bc),
  "total_sec":      $(echo "$T_END - $T_START" | bc)
}
EOF
