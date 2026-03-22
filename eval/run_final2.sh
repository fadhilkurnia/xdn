#!/bin/bash
set -e
cd /users/fadhil/xdn/eval

echo "[$(date)] Starting XDN PB benchmark..."
python3 -u run_load_pb_wordpress_reflex.py 2>&1 | tee /users/fadhil/xdn/screen_logs/final2_pb.log

echo "[$(date)] PB done. Starting Semi-sync benchmark..."
python3 -u run_load_pb_wordpress_semisync.py 2>&1 | tee /users/fadhil/xdn/screen_logs/final2_semisync.log

echo "[$(date)] Semi-sync done. Starting OpenEBS benchmark..."
python3 -u run_load_pb_wordpress_openebs.py 2>&1 | tee /users/fadhil/xdn/screen_logs/final2_openebs.log

echo "[$(date)] OpenEBS done. Generating plots..."
python3 -u plot_triple_comparison.py 2>&1 | tee /users/fadhil/xdn/screen_logs/final2_plot.log

echo "[$(date)] ALL DONE."
