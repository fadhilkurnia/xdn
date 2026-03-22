#!/bin/bash
set -e
cd /users/fadhil/xdn/eval

echo "[$(date)] Starting TPC-C XDN PB benchmark..."
python3 -u run_load_pb_tpcc_reflex.py 2>&1 | tee /users/fadhil/xdn/screen_logs/tpcc_pb.log

echo "[$(date)] PB done. Starting PG Sync-Rep benchmark..."
python3 -u run_load_pb_tpcc_pgsync.py 2>&1 | tee /users/fadhil/xdn/screen_logs/tpcc_pgsync.log

echo "[$(date)] Sync-rep done. Starting OpenEBS benchmark..."
python3 -u run_load_pb_tpcc_openebs.py 2>&1 | tee /users/fadhil/xdn/screen_logs/tpcc_openebs.log

echo "[$(date)] OpenEBS done. Generating comparison plots..."
python3 -u plot_tpcc_comparison.py 2>&1 | tee /users/fadhil/xdn/screen_logs/tpcc_plot.log

echo "[$(date)] ALL DONE."
