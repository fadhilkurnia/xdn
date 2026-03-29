#!/usr/bin/env bash
# profile_jvm_during_load.sh — Profile JVM thread CPU usage during load tests.
#
# Captures periodic thread-level CPU snapshots via `top -H` and an optional
# Java Flight Recorder (JFR) recording from the XDN JVM on the primary node.
#
# Usage:
#   ./profile_jvm_during_load.sh [host] [duration_sec] [output_dir]
#
# Examples:
#   ./profile_jvm_during_load.sh 10.10.1.3 30 results/profile_run1
#   ./profile_jvm_during_load.sh   # defaults: 10.10.1.3, 60s, results/profile_<ts>

set -euo pipefail

HOST="${1:-10.10.1.3}"
DURATION="${2:-60}"
OUTPUT_DIR="${3:-results/profile_$(date +%Y%m%d_%H%M%S)}"
INTERVAL=2  # seconds between top -H snapshots

mkdir -p "$OUTPUT_DIR"

echo "============================================================"
echo "JVM Thread Profiler"
echo "  Host     : $HOST"
echo "  Duration : ${DURATION}s (sampling every ${INTERVAL}s)"
echo "  Output   : $OUTPUT_DIR"
echo "============================================================"

# Find JVM PID
echo "Finding JVM PID on $HOST ..."
JVM_PID=$(ssh -o StrictHostKeyChecking=no "$HOST" \
    "ps -ef | grep 'java.*ReconfigurableNode' | grep -v grep | grep -v sudo | awk '{print \$2}' | head -1" 2>/dev/null)

if [ -z "$JVM_PID" ]; then
    echo "ERROR: No ReconfigurableNode JVM found on $HOST"
    exit 1
fi
echo "JVM PID: $JVM_PID"

# Start JFR recording (non-blocking)
echo "Starting Java Flight Recorder for ${DURATION}s ..."
JFR_FILE="/tmp/xdn_profile_${JVM_PID}.jfr"
ssh -o StrictHostKeyChecking=no "$HOST" \
    "sudo jcmd $JVM_PID JFR.start name=xdn_profile duration=${DURATION}s filename=$JFR_FILE settings=profile 2>&1 || echo 'JFR_FAILED'" \
    > "$OUTPUT_DIR/jfr_start.log" 2>&1 &
JFR_PID=$!

# Collect top -H snapshots
echo "Collecting top -H snapshots every ${INTERVAL}s for ${DURATION}s ..."
TOP_FILE="$OUTPUT_DIR/top_threads.log"
> "$TOP_FILE"

END_TIME=$((SECONDS + DURATION))
SAMPLE=0
while [ $SECONDS -lt $END_TIME ]; do
    SAMPLE=$((SAMPLE + 1))
    echo "=== Sample $SAMPLE at $(date +%H:%M:%S) ===" >> "$TOP_FILE"
    ssh -o StrictHostKeyChecking=no "$HOST" \
        "top -H -p $JVM_PID -b -n 1 2>/dev/null | head -50" >> "$TOP_FILE" 2>/dev/null
    echo "" >> "$TOP_FILE"
    sleep "$INTERVAL"
done

echo "Collected $SAMPLE top snapshots → $TOP_FILE"

# Wait for JFR to finish and download
wait $JFR_PID 2>/dev/null || true
sleep 2

echo "Downloading JFR recording ..."
scp -o StrictHostKeyChecking=no "$HOST:$JFR_FILE" "$OUTPUT_DIR/profile.jfr" 2>/dev/null && {
    echo "JFR saved → $OUTPUT_DIR/profile.jfr"
    ssh -o StrictHostKeyChecking=no "$HOST" "rm -f $JFR_FILE" 2>/dev/null
} || echo "JFR download failed (may not be available)"

# Parse top snapshots: extract hot threads summary
echo ""
echo "============================================================"
echo "Hot Thread Summary (threads with >10% CPU in any sample)"
echo "============================================================"
grep -E "^\s+[0-9]+" "$TOP_FILE" | \
    awk '$9 > 10.0 {printf "%-8s %6.1f%% CPU  %s\n", $1, $9, $NF}' | \
    sort -t'%' -k1 -rn | \
    sort -u -k3 | \
    sort -t'%' -k1 -rn | head -20

# Generate per-thread CPU timeline
TIMELINE_FILE="$OUTPUT_DIR/thread_cpu_timeline.txt"
echo "Thread CPU Timeline (sampled every ${INTERVAL}s)" > "$TIMELINE_FILE"
echo "=================================================" >> "$TIMELINE_FILE"

# Extract unique hot thread names
HOT_THREADS=$(grep -E "^\s+[0-9]+" "$TOP_FILE" | \
    awk '$9 > 5.0 {print $NF}' | sort -u)

for thread in $HOT_THREADS; do
    echo "" >> "$TIMELINE_FILE"
    echo "--- $thread ---" >> "$TIMELINE_FILE"
    grep -A0 "=== Sample" "$TOP_FILE" | grep "Sample" | \
        while read -r sample_line; do
            sample_num=$(echo "$sample_line" | grep -oP 'Sample \K[0-9]+')
            # Find the CPU% for this thread in this sample's block
            cpu=$(awk -v s="$sample_line" -v t="$thread" '
                $0 == s {found=1; next}
                found && /^=== Sample/ {exit}
                found && $NF == t {print $9; exit}
            ' "$TOP_FILE")
            printf "  sample %3s: %s%%\n" "$sample_num" "${cpu:-0.0}" >> "$TIMELINE_FILE"
        done
done

echo "Thread timeline → $TIMELINE_FILE"
echo ""
echo "[Done] Results in $OUTPUT_DIR/"
echo "  top_threads.log         — raw top -H snapshots"
echo "  thread_cpu_timeline.txt — per-thread CPU over time"
echo "  profile.jfr             — Java Flight Recorder (open with JDK Mission Control)"
