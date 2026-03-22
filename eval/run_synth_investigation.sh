#!/bin/bash
# run_synth_investigation.sh — Master runner for synth-workload benchmarks.
#
# Runs all three benchmark variants (PB, OpenEBS, rqlite) sequentially,
# then generates comparison plots.
#
# Usage:
#   ./run_synth_investigation.sh [--experiments 1,2,3,4]
#
# Individual variants can be run separately:
#   python3 run_load_pb_synth_reflex.py
#   python3 run_load_pb_synth_openebs.py
#   python3 run_load_pb_synth_rqlite.py

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

EXPERIMENTS="${1:---experiments 1,2,3,4}"
LOGFILE="run_synth_investigation_$(date +%Y%m%d_%H%M%S).log"

echo "=================================================================="
echo "Synth-Workload Sync-Granularity Investigation"
echo "Log: $LOGFILE"
echo "=================================================================="

# Phase 1: XDN Primary-Backup
echo ""
echo "[1/4] Running XDN PB benchmark ..."
python3 run_load_pb_synth_reflex.py $EXPERIMENTS 2>&1 | tee -a "$LOGFILE"
echo ""
echo "[1/4] XDN PB benchmark complete."

# Phase 2: OpenEBS
echo ""
echo "[2/4] Running OpenEBS benchmark ..."
python3 run_load_pb_synth_openebs.py $EXPERIMENTS 2>&1 | tee -a "$LOGFILE"
echo ""
echo "[2/4] OpenEBS benchmark complete."

# Phase 3: rqlite
echo ""
echo "[3/4] Running rqlite benchmark ..."
python3 run_load_pb_synth_rqlite.py $EXPERIMENTS 2>&1 | tee -a "$LOGFILE"
echo ""
echo "[3/4] rqlite benchmark complete."

# Phase 4: Generate comparison plots
echo ""
echo "[4/4] Generating comparison plots ..."
python3 plot_synth_comparison.py 2>&1 | tee -a "$LOGFILE"

echo ""
echo "=================================================================="
echo "All done! Results in eval/results/"
echo "  load_pb_synth_reflex/       — XDN Primary-Backup results"
echo "  synth_openebs/  — OpenEBS results"
echo "  synth_rqlite/   — rqlite results"
echo "  synth_exp*.png  — Comparison plots"
echo "  synth_final_report.txt — Summary report"
echo "=================================================================="
