#!/bin/bash
set -e
cd /users/fadhil/xdn/eval

mkdir -p results /users/fadhil/xdn/screen_logs

echo "============================================================"
echo "[$(date)] === WORDPRESS EVALUATION ==="
echo "============================================================"
bash run_final2.sh

echo ""
echo "============================================================"
echo "[$(date)] === TPC-C EVALUATION ==="
echo "============================================================"
bash run_tpcc_eval.sh

echo ""
echo "============================================================"
echo "[$(date)] === HOTEL-RESERVATION EVALUATION ==="
echo "============================================================"
bash run_hotelres_eval.sh

echo ""
echo "============================================================"
echo "[$(date)] === EXTRACTING MAX THROUGHPUT ==="
echo "============================================================"
for dir in results/{final7_pb,final2_semisync,final2_openebs,tpcc_pb,tpcc_pgsync,tpcc_openebs,hotelres_pb,hotelres_replmongo,hotelres_openebs}; do
  best=$(grep -h actual_throughput_rps "$dir"/rate*.txt 2>/dev/null | sed 's/.*= *//' | sort -n | tail -1)
  printf "%-25s %s req/s\n" "$(basename $dir)" "${best:-N/A}"
done

echo ""
echo "[$(date)] ALL EVALUATIONS COMPLETE."
