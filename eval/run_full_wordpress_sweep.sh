#!/usr/bin/env bash
# run_full_wordpress_sweep.sh — Run all 3 WordPress systems with dense rate sweep.
#
# Rates: 100, 200, 300, ..., 3000 (by 100), auto-stops at saturation.
# Each system is run sequentially.
#
# Usage:
#   cd eval && bash run_full_wordpress_sweep.sh [system ...]
#   Systems: pb, openebs, semisync  (default: all three)

set -euo pipefail
cd "$(dirname "$0")"

RATES="100,200,300,400,500,600,700,800,900,1000,1100,1200,1300,1400,1500,1600,1700,1800,1900,2000,2100,2200,2300,2400,2500,2600,2700,2800,2900,3000"

# Determine which systems to run
if [ $# -gt 0 ]; then
    SYSTEMS=("$@")
else
    SYSTEMS=(pb openebs semisync)
fi

echo "============================================================"
echo "Full WordPress Sweep"
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
            python3 -u run_load_pb_wordpress_reflex.py --rates "$RATES" 2>&1 | tee "run_sweep_wp_pb_$(date +%Y%m%d_%H%M%S).log"
            # Update symlink to latest results
            LATEST=$(ls -dt results/load_pb_wordpress_reflex_20* 2>/dev/null | head -1)
            if [ -n "$LATEST" ]; then
                ln -sfn "$(basename "$LATEST")" results/load_pb_wordpress_reflex
                echo "Symlink updated: results/load_pb_wordpress_reflex -> $(basename "$LATEST")"
            fi
            ;;
        openebs)
            python3 -u run_load_pb_wordpress_openebs.py --rates "$RATES" 2>&1 | tee "run_sweep_wp_openebs_$(date +%Y%m%d_%H%M%S).log"
            LATEST=$(ls -dt results/load_pb_wordpress_openebs_20* 2>/dev/null | head -1)
            if [ -n "$LATEST" ]; then
                ln -sfn "$(basename "$LATEST")" results/load_pb_wordpress_openebs
                echo "Symlink updated: results/load_pb_wordpress_openebs -> $(basename "$LATEST")"
            fi
            ;;
        semisync)
            python3 -u run_load_pb_wordpress_semisync.py --rates "$RATES" 2>&1 | tee "run_sweep_wp_semisync_$(date +%Y%m%d_%H%M%S).log"
            LATEST=$(ls -dt results/load_pb_wordpress_semisync_20* 2>/dev/null | head -1)
            if [ -n "$LATEST" ]; then
                ln -sfn "$(basename "$LATEST")" results/load_pb_wordpress_semisync
                echo "Symlink updated: results/load_pb_wordpress_semisync -> $(basename "$LATEST")"
            fi
            ;;
        *)
            echo "Unknown system: $sys (valid: pb, openebs, semisync)"
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

echo ""
echo "Generating WordPress CSV comparison files..."
python3 -u generate_wordpress_csv.py

echo "Done at $(date)"
