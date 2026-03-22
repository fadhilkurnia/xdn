#!/usr/bin/env bash
# run_load_ar_eval.sh — Run Active Replication (Paxos RSM) benchmarks for
# bookcatalog, webkv, and todo against four baselines (XDN, DistDB, OpenEBS,
# CRIU), then organize results for the plotting infrastructure.
#
# Usage:
#   ./run_load_ar_eval.sh                     # run all benchmarks
#   ./run_load_ar_eval.sh --step reflex       # run only XDN active replication
#   ./run_load_ar_eval.sh --step distdb       # run only distributed DB baselines
#   ./run_load_ar_eval.sh --step oebs         # run only OpenEBS baselines
#   ./run_load_ar_eval.sh --step criu         # run only CRIU baselines
#   ./run_load_ar_eval.sh --step collect      # only collect/organize existing results
#   ./run_load_ar_eval.sh --step plot         # only generate plots

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CONFIG="$REPO_ROOT/conf/gigapaxos.xdn.3way.cloudlab.properties"
RESULTS_DIR="$SCRIPT_DIR/results"
RATES="${RATES:-100,200,300,400,500,800,1000}"
DURATION="${DURATION:-30s}"
APPS=("fadhilkurnia/xdn-bookcatalog" "fadhilkurnia/xdn-webkv" "fadhilkurnia/xdn-todo")
APP_NAMES=("bookcatalog" "webkv" "todo")

# Parse arguments
STEP="${1:-all}"
if [[ "$1" == "--step" ]] 2>/dev/null; then
    STEP="${2:-all}"
fi

log() { echo "$(date '+%H:%M:%S') [ar-eval] $*"; }

mkdir -p "$RESULTS_DIR"

# ── Step 1: XDN Active Replication (Paxos RSM) ──────────────────────────────
run_reflex() {
    log "=== Running XDN Active Replication benchmarks ==="
    for i in "${!APPS[@]}"; do
        local image="${APPS[$i]}"
        local app="${APP_NAMES[$i]}"
        log "--- reflex: $app ($image) ---"
        python3 "$SCRIPT_DIR/run_load_ar_reflex_app.py" \
            "$CONFIG" "$image" \
            --rates "$RATES" \
            --duration "$DURATION" \
            --load-generator go \
            2>&1 | tee "$RESULTS_DIR/reflex_${app}_run.log"
        log "reflex $app done"
    done
}

# ── Step 2: Distributed DB Baselines ────────────────────────────────────────
run_distdb() {
    log "=== Running Distributed DB baselines ==="
    for i in "${!APPS[@]}"; do
        local image="${APPS[$i]}"
        local app="${APP_NAMES[$i]}"
        log "--- distdb: $app ($image) ---"
        python3 "$SCRIPT_DIR/run_load_ar_distdb_app.py" \
            "$CONFIG" "$image" \
            --rates "$RATES" \
            --duration "$DURATION" \
            --load-generator go \
            2>&1 | tee "$RESULTS_DIR/distdb_${app}_run.log"
        log "distdb $app done"
    done
}

# ── Step 3: OpenEBS Baselines ───────────────────────────────────────────────
run_oebs() {
    log "=== Running OpenEBS baselines ==="
    for i in "${!APPS[@]}"; do
        local image="${APPS[$i]}"
        local app="${APP_NAMES[$i]}"
        log "--- oebs: $app ($image) ---"
        python3 "$SCRIPT_DIR/run_load_ar_oebs_app.py" \
            "$image" \
            --rates "$RATES" \
            --duration "$DURATION" \
            --load-generator go \
            2>&1 | tee "$RESULTS_DIR/oebs_${app}_run.log"
        log "oebs $app done"
    done
}

# ── Step 4: CRIU Baselines ──────────────────────────────────────────────────
run_criu() {
    log "=== Running CRIU baselines ==="
    for i in "${!APPS[@]}"; do
        local image="${APPS[$i]}"
        local app="${APP_NAMES[$i]}"
        log "--- criu: $app ($image) ---"
        python3 "$SCRIPT_DIR/run_load_ar_criu_app.py" \
            "$CONFIG" "$image" \
            --rates "$RATES" \
            --duration "$DURATION" \
            --load-generator go \
            2>&1 | tee "$RESULTS_DIR/criu_${app}_run.log"
        log "criu $app done"
    done
}

# ── Step 5: Collect and organize results ────────────────────────────────────
collect_results() {
    log "=== Collecting and organizing results ==="

    for app in "${APP_NAMES[@]}"; do
        # Reflex results: reflex_go_latency_rate_{app}_{rate}.txt -> reflex_{app}/rate{rate}.txt
        local reflex_dir="$RESULTS_DIR/reflex_${app}"
        mkdir -p "$reflex_dir"
        for src in "$RESULTS_DIR"/reflex_go_latency_rate_${app}_*.txt; do
            [ -f "$src" ] || continue
            # Extract rate from filename: reflex_go_latency_rate_{app}_{rate}.txt
            local rate
            rate=$(basename "$src" .txt | sed "s/reflex_go_latency_rate_${app}_//")
            cp "$src" "$reflex_dir/rate${rate}.txt"
            log "  $src -> reflex_${app}/rate${rate}.txt"
        done

        # DistDB results: {backend}_go_latency_rate_{app}_{rate}.txt -> distdb_{app}/rate{rate}.txt
        local distdb_dir="$RESULTS_DIR/distdb_${app}"
        mkdir -p "$distdb_dir"
        for backend in rqlite tikv; do
            for src in "$RESULTS_DIR"/${backend}_go_latency_rate_${app}_*.txt; do
                [ -f "$src" ] || continue
                local rate
                rate=$(basename "$src" .txt | sed "s/${backend}_go_latency_rate_${app}_//")
                cp "$src" "$distdb_dir/rate${rate}.txt"
                log "  $src -> distdb_${app}/rate${rate}.txt"
            done
        done

        # OpenEBS results: oebs-xdn-{app}_{rate}rps_latency.txt -> oebs_{app}/rate{rate}.txt
        local oebs_dir="$RESULTS_DIR/oebs_${app}"
        mkdir -p "$oebs_dir"
        for src in "$RESULTS_DIR"/oebs-xdn-${app}_*rps_latency.txt; do
            [ -f "$src" ] || continue
            local rate
            rate=$(basename "$src" .txt | sed 's/oebs-xdn-'"${app}"'_//' | sed 's/rps_latency//')
            cp "$src" "$oebs_dir/rate${rate}.txt"
            log "  $src -> oebs_${app}/rate${rate}.txt"
        done

        # CRIU results: criu_go_latency_rate{rate}.txt -> criu_{app}/rate{rate}.txt
        local criu_dir="$RESULTS_DIR/criu_${app}"
        mkdir -p "$criu_dir"
        for src in "$RESULTS_DIR"/criu_go_latency_rate*.txt; do
            [ -f "$src" ] || continue
            local rate
            rate=$(basename "$src" .txt | sed 's/criu_go_latency_rate//')
            cp "$src" "$criu_dir/rate${rate}.txt"
            log "  $src -> criu_${app}/rate${rate}.txt"
        done
    done

    log "Results organized into per-app directories."
}

# ── Step 6: Generate plots ──────────────────────────────────────────────────
generate_plots() {
    log "=== Generating paper-ready comparison plots ==="
    python3 "$SCRIPT_DIR/plot_paper_comparison.py" \
        --results-base "$RESULTS_DIR" \
        --out-dir "$REPO_ROOT/reflex-paper/figures"
    log "Plots generated."
}

# ── Main ────────────────────────────────────────────────────────────────────
case "$STEP" in
    reflex)   run_reflex; collect_results ;;
    distdb)   run_distdb; collect_results ;;
    oebs)     run_oebs; collect_results ;;
    criu)     run_criu; collect_results ;;
    collect)  collect_results ;;
    plot)     generate_plots ;;
    all)
        run_reflex
        run_distdb
        run_oebs
        run_criu
        collect_results
        generate_plots
        ;;
    *)
        echo "Usage: $0 [--step reflex|distdb|oebs|criu|collect|plot|all]"
        exit 1
        ;;
esac

log "Done."
