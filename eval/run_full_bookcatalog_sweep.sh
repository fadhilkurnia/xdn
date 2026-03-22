#!/usr/bin/env bash
# run_full_bookcatalog_sweep.sh — Run all 4 bookcatalog-nd systems with dense rate sweep.
#
# Rates: 100, 200, 300, ..., 3000 (by 100), auto-stops at saturation.
# Each system is run sequentially; PB uses fresh cluster per rate.
#
# Usage:
#   cd eval && bash run_full_bookcatalog_sweep.sh [system ...]
#   Systems: pb, rqlite, openebs, criu  (default: all four)

set -euo pipefail
cd "$(dirname "$0")"

RATES="100,200,300,400,500,600,700,800,900,1000,1100,1200,1300,1400,1500,1600,1700,1800,1900,2000,2100,2200,2300,2400,2500,2600,2700,2800,2900,3000"

# Determine which systems to run
if [ $# -gt 0 ]; then
    SYSTEMS=("$@")
else
    SYSTEMS=(pb rqlite openebs criu)
fi

echo "============================================================"
echo "Full BookCatalog-ND Sweep"
echo "  Systems: ${SYSTEMS[*]}"
echo "  Rates:   100..3000 by 100"
echo "============================================================"

for sys in "${SYSTEMS[@]}"; do
    echo ""
    echo "============================================================"
    echo "  Starting: $sys"
    echo "  $(date)"
    echo "============================================================"

    case "$sys" in
        pb)
            python3 -u run_load_pb_bookcatalog_reflex.py --rates "$RATES" 2>&1 | tee "run_sweep_pb_$(date +%Y%m%d_%H%M%S).log"
            # Update symlink to latest results
            LATEST=$(ls -dt results/load_pb_bookcatalog_reflex_20* | head -1)
            if [ -n "$LATEST" ]; then
                ln -sfn "$(basename "$LATEST")" results/load_pb_bookcatalog_reflex
                echo "Symlink updated: results/load_pb_bookcatalog_reflex -> $(basename "$LATEST")"
            fi
            ;;
        rqlite)
            python3 -u run_load_pb_bookcatalog_rqlite.py --rates "$RATES" 2>&1 | tee "run_sweep_rqlite_$(date +%Y%m%d_%H%M%S).log"
            LATEST=$(ls -dt results/load_pb_bookcatalog_rqlite_20* | head -1)
            if [ -n "$LATEST" ]; then
                ln -sfn "$(basename "$LATEST")" results/load_pb_bookcatalog_rqlite
                echo "Symlink updated: results/load_pb_bookcatalog_rqlite -> $(basename "$LATEST")"
            fi
            ;;
        openebs)
            python3 -u run_load_pb_bookcatalog_openebs.py --rates "$RATES" 2>&1 | tee "run_sweep_openebs_$(date +%Y%m%d_%H%M%S).log"
            LATEST=$(ls -dt results/load_pb_bookcatalog_openebs_20* | head -1)
            if [ -n "$LATEST" ]; then
                ln -sfn "$(basename "$LATEST")" results/load_pb_bookcatalog_openebs
                echo "Symlink updated: results/load_pb_bookcatalog_openebs -> $(basename "$LATEST")"
            fi
            ;;
        criu)
            python3 -u run_load_pb_bookcatalog_criu.py --rates "$RATES" 2>&1 | tee "run_sweep_criu_$(date +%Y%m%d_%H%M%S).log"
            LATEST=$(ls -dt results/load_pb_bookcatalog_criu_20* | head -1)
            if [ -n "$LATEST" ]; then
                ln -sfn "$(basename "$LATEST")" results/load_pb_bookcatalog_criu
                echo "Symlink updated: results/load_pb_bookcatalog_criu -> $(basename "$LATEST")"
            fi
            ;;
        *)
            echo "Unknown system: $sys (valid: pb, rqlite, openebs, criu)"
            exit 1
            ;;
    esac

    echo ""
    echo "  Finished: $sys at $(date)"
done

echo ""
echo "============================================================"
echo "All sweeps done. Regenerating comparison plot..."
echo "============================================================"
python3 -u plot_paper_comparison.py
echo "Done at $(date)"
