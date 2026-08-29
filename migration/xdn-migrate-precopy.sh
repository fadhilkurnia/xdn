#!/bin/bash
# xdn-migrate-precopy — iterative pre-copy migration via CRIU pre-dumps.
#
# Pipeline:
#   round 1..N-1:   pre-checkpoint -P [--with-previous]   ──scp──▶ dst
#                   (container keeps running on src — NOT downtime)
#   final round:    checkpoint --with-previous            ──scp──▶ dst
#                   (container paused; this IS the downtime window)
#   restore:        --import=final --import-previous=pre-N    on dst
#
# Pre-dumps only ship dirty pages from the previous round (CRIU tracks via
# soft-dirty pte bit), so downtime-critical scp shrinks with each round
# until the dirty-page rate reaches steady state.
#
# Usage:
#   xdn-migrate-precopy.sh <src> <dst> <container> [rounds]   (rounds default 2)
set -euo pipefail
SRC=$1
DST=$2
CONTAINER=$3
ROUNDS=${4:-2}
WORK=/tmp/xdn-migrate
ts() { date +%s.%N; }

ssh -o BatchMode=yes "$DST" "sudo mkdir -p $WORK && sudo chmod 0777 $WORK" >/dev/null
ssh -o BatchMode=yes "$SRC" "sudo rm -rf $WORK && sudo mkdir -p $WORK && sudo chmod 0777 $WORK" >/dev/null

T_TOTAL_START=$(ts)

# ───── Pre-copy phase (container keeps running) ─────────────────────────
# NOTE: podman 3.4.4 rejects `--with-previous` together with `--pre-checkpoint`,
# so the iterative chain documented in the plan collapses to a SINGLE pre-dump
# round in this PoC. Multiple chained pre-dumps need newer podman (≥ 4.x) or
# bypassing podman and calling criu directly.
if [ "$ROUNDS" -gt 1 ]; then
  echo "warning: rounds>1 not supported by podman 3.4.4; clamping to 1" >&2
fi
PRE_SIZES=()
T0=$(ts)
ssh -o BatchMode=yes "$SRC" "
  set -e
  sudo podman --runtime runc container checkpoint -P $CONTAINER \
       --export=$WORK/pre-1.tar.gz >/dev/null
  sudo chmod a+r $WORK/pre-1.tar.gz
"
ssh -o BatchMode=yes "$SRC" "scp -o BatchMode=yes $WORK/pre-1.tar.gz ${DST}:$WORK/pre-1.tar.gz" >/dev/null
T1=$(ts)
SZ=$(ssh -o BatchMode=yes "$SRC" "stat -c%s $WORK/pre-1.tar.gz")
PRE_SIZES+=("$SZ")
printf 'pre-round 1: %d bytes, %.3fs (container still running)\n' "$SZ" "$(echo "$T1 - $T0" | bc)" >&2

# ───── Final dump + ship + restore (DOWNTIME WINDOW) ─────────────────────
T_DOWNTIME_START=$(ts)
ssh -o BatchMode=yes "$SRC" "
  set -e
  sudo podman --runtime runc container checkpoint --tcp-established --with-previous \
       $CONTAINER --export=$WORK/final.tar.gz >/dev/null
  sudo chmod a+r $WORK/final.tar.gz
"
T_FINAL_CP=$(ts)
ssh -o BatchMode=yes "$SRC" "scp -o BatchMode=yes $WORK/final.tar.gz ${DST}:$WORK/final.tar.gz" >/dev/null
T_FINAL_SCP=$(ts)
ssh -o BatchMode=yes "$DST" "
  sudo podman --runtime runc rm -f $CONTAINER 2>/dev/null || true
  sudo podman --runtime runc container restore --tcp-established \
       --import=$WORK/final.tar.gz \
       --import-previous=$WORK/pre-1.tar.gz >/dev/null
"
T_RESTORE=$(ts)
DOWNTIME=$(echo "$T_RESTORE - $T_DOWNTIME_START" | bc)
FINAL_SZ=$(ssh -o BatchMode=yes "$DST" "stat -c%s $WORK/final.tar.gz")

# ───── Cleanup ────────────────────────────────────────────────────────────
ssh -o BatchMode=yes "$SRC" "sudo podman --runtime runc rm -f $CONTAINER 2>/dev/null || true; sudo rm -rf $WORK" >/dev/null
ssh -o BatchMode=yes "$DST" "sudo rm -rf $WORK" >/dev/null
T_TOTAL=$(ts)

cat <<EOF
{
  "container":       "$CONTAINER",
  "src":             "$SRC",
  "dst":             "$DST",
  "rounds":          $ROUNDS,
  "pre_sizes_bytes": [$(IFS=,; echo "${PRE_SIZES[*]}")],
  "final_bytes":     $FINAL_SZ,
  "final_cp_sec":    $(echo "$T_FINAL_CP - $T_DOWNTIME_START" | bc),
  "final_scp_sec":   $(echo "$T_FINAL_SCP - $T_FINAL_CP" | bc),
  "restore_sec":     $(echo "$T_RESTORE - $T_FINAL_SCP" | bc),
  "downtime_sec":    $DOWNTIME,
  "total_sec":       $(echo "$T_TOTAL - $T_TOTAL_START" | bc)
}
EOF
