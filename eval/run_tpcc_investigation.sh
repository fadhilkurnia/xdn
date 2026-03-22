#!/bin/bash
# run_tpcc_investigation.sh — Sequential orchestrator for TPC-C PB investigation.
#
# Runs all 4 experiment scripts in order, then generates the comparison plot.
# Each experiment takes ~25-30 minutes; total runtime ~2 hours.
#
# Usage (from eval/ directory):
#   bash run_tpcc_investigation.sh
#   bash run_tpcc_investigation.sh --skip-finesweep   # skip experiment 1
#   bash run_tpcc_investigation.sh --only-plot         # only generate plots

set -e
cd "$(dirname "$0")"

LOG_DIR="/users/fadhil/xdn/screen_logs"
mkdir -p "$LOG_DIR"

SKIP_FINESWEEP=false
SKIP_UNREPLICATED=false
SKIP_RSYNC=false
SKIP_BATCHED=false
ONLY_PLOT=false

for arg in "$@"; do
    case "$arg" in
        --skip-finesweep)   SKIP_FINESWEEP=true ;;
        --skip-unreplicated) SKIP_UNREPLICATED=true ;;
        --skip-rsync)       SKIP_RSYNC=true ;;
        --skip-batched)     SKIP_BATCHED=true ;;
        --only-plot)        ONLY_PLOT=true ;;
        *)
            echo "Unknown option: $arg"
            echo "Usage: $0 [--skip-finesweep] [--skip-unreplicated] [--skip-rsync] [--skip-batched] [--only-plot]"
            exit 1
            ;;
    esac
done

echo "============================================================"
echo "TPC-C PB Investigation — Sequential Orchestrator"
echo "Started: $(date)"
echo "============================================================"

if [ "$ONLY_PLOT" = false ]; then

    # Experiment 1: Fine-grained rate sweep
    if [ "$SKIP_FINESWEEP" = false ]; then
        echo ""
        echo "[$(date)] Starting Experiment 1: Fine-grained rate sweep ..."
        python3 -u investigate_tpcc_pb_finesweep.py 2>&1 | tee "$LOG_DIR/tpcc_pb_finesweep.log"
        echo "[$(date)] Experiment 1 done."
    else
        echo "[$(date)] Skipping Experiment 1 (--skip-finesweep)"
    fi

    # Experiment 2: Unreplicated baseline
    if [ "$SKIP_UNREPLICATED" = false ]; then
        echo ""
        echo "[$(date)] Starting Experiment 2: Unreplicated baseline ..."
        python3 -u investigate_tpcc_pb_unreplicated.py 2>&1 | tee "$LOG_DIR/tpcc_pb_unreplicated.log"
        echo "[$(date)] Experiment 2 done."
    else
        echo "[$(date)] Skipping Experiment 2 (--skip-unreplicated)"
    fi

    # Experiment 3: RSYNC recorder
    if [ "$SKIP_RSYNC" = false ]; then
        echo ""
        echo "[$(date)] Starting Experiment 3: RSYNC recorder ..."
        python3 -u investigate_tpcc_pb_rsync.py 2>&1 | tee "$LOG_DIR/tpcc_pb_rsync.log"
        echo "[$(date)] Experiment 3 done."
    else
        echo "[$(date)] Skipping Experiment 3 (--skip-rsync)"
    fi

    # Experiment 4: Batch accumulation (includes ant compile)
    if [ "$SKIP_BATCHED" = false ]; then
        echo ""
        echo "[$(date)] Starting Experiment 4: Batch accumulation (10ms) ..."
        python3 -u investigate_tpcc_pb_batched.py 2>&1 | tee "$LOG_DIR/tpcc_pb_batched.log"
        echo "[$(date)] Experiment 4 done."
    else
        echo "[$(date)] Skipping Experiment 4 (--skip-batched)"
    fi

fi

# Experiment 5: Comparison plot
echo ""
echo "[$(date)] Generating comparison plots ..."
python3 -u plot_tpcc_pb_investigation.py 2>&1 | tee "$LOG_DIR/tpcc_pb_investigation_plot.log"

echo ""
echo "============================================================"
echo "ALL DONE: $(date)"
echo "Results: eval/results/load_pb_tpcc_reflex_investigation_*.png"
echo "Report:  eval/results/load_pb_tpcc_reflex_investigation_report.txt"
echo "============================================================"
