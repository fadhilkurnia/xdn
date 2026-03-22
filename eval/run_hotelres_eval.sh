#!/bin/bash
set -e
cd /users/fadhil/xdn/eval

echo "[$(date)] Starting Hotel-Reservation XDN PB benchmark..."
python3 -u run_load_pb_hotelres_reflex.py 2>&1 | tee /users/fadhil/xdn/screen_logs/hotelres_pb.log

echo "[$(date)] PB done. Starting Replicated MongoDB benchmark..."
python3 -u run_load_pb_hotelres_replmongo.py 2>&1 | tee /users/fadhil/xdn/screen_logs/hotelres_replmongo.log

echo "[$(date)] Repl. MongoDB done. Starting OpenEBS benchmark..."
python3 -u run_load_pb_hotelres_openebs.py 2>&1 | tee /users/fadhil/xdn/screen_logs/hotelres_openebs.log

echo "[$(date)] OpenEBS done. Generating comparison plots..."
python3 -u plot_hotelres_comparison.py 2>&1 | tee /users/fadhil/xdn/screen_logs/hotelres_plot.log

echo "[$(date)] ALL DONE."
