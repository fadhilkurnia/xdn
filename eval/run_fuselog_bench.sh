#!/bin/bash
# run_fuselog_bench.sh - Run C++ vs Rust fuselog performance comparison
#
# This script runs workload benchmarks against both fuselog implementations
# and collects results for comparison.

set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CPP_BIN="$PROJECT_ROOT/xdn-fs/cpp/fuselog"
RUST_BIN="$PROJECT_ROOT/xdn-fs/rust/target/release/fuselog_core"
EVAL_DIR="$PROJECT_ROOT/eval"

BENCH_DIR="/tmp/xdn/bench"
MOUNT_DIR="$BENCH_DIR/mount"
SOCKET_PATH="$BENCH_DIR/fuselog.sock"
RESULTS_DIR="$EVAL_DIR/results/fuselog_bench"

DURATION=${DURATION:-10}
REPS=${REPS:-3}

mkdir -p "$MOUNT_DIR" "$RESULTS_DIR"

cleanup() {
  fusermount3 -u "$MOUNT_DIR" 2>/dev/null || true
  rm -f "$SOCKET_PATH"
  sleep 1
}

mount_cpp() {
  cleanup
  FUSELOG_SOCKET_FILE="$SOCKET_PATH" "$CPP_BIN" -o allow_other -o default_permissions "$MOUNT_DIR" &
  sleep 2
  if ! mount | grep -q "$MOUNT_DIR"; then
    echo "ERROR: C++ mount failed"
    return 1
  fi
}

mount_rust_st() {
  cleanup
  FUSELOG_SOCKET_FILE="$SOCKET_PATH" "$RUST_BIN" "$MOUNT_DIR" &
  sleep 2
  if ! mount | grep -q "$MOUNT_DIR"; then
    echo "ERROR: Rust ST mount failed"
    return 1
  fi
}

mount_rust_mt() {
  cleanup
  FUSELOG_SOCKET_FILE="$SOCKET_PATH" "$RUST_BIN" -f --multi-threaded "$MOUNT_DIR" &
  sleep 2
  if ! mount | grep -q "$MOUNT_DIR"; then
    echo "ERROR: Rust MT mount failed"
    return 1
  fi
}

run_writer() {
  local profile=$1
  local threads=$2
  local duration=$3
  cd "$EVAL_DIR" && go run bench_fuse_writer.go \
    --dir "$MOUNT_DIR" --profile "$profile" \
    --threads "$threads" --duration "$duration" 2>&1
}

run_bench_config() {
  local impl_name=$1
  local profile=$2
  local threads=$3
  local rep=$4
  local outfile="$RESULTS_DIR/${impl_name}_${profile}_t${threads}_r${rep}.txt"

  echo "  [$impl_name] $profile threads=$threads rep=$rep"
  run_writer "$profile" "$threads" "$DURATION" > "$outfile"

  # Extract key metrics
  local mbps=$(grep "Throughput:" "$outfile" | awk '{print $2}')
  local ops=$(grep "Ops/sec:" "$outfile" | awk '{print $2}')
  local p50=$(grep "Latency p50:" "$outfile" | awk '{print $2}')
  local p95=$(grep "Latency p95:" "$outfile" | awk '{print $2}')
  local p99=$(grep "Latency p99:" "$outfile" | awk '{print $2}')
  echo "    MB/s=$mbps ops/s=$ops p50=$p50 p95=$p95 p99=$p99"
  echo "$impl_name,$profile,$threads,$rep,$mbps,$ops,$p50,$p95,$p99" >> "$RESULTS_DIR/all_results.csv"
}

echo "========================================"
echo "  Fuselog Benchmark: C++ vs Rust"
echo "========================================"
echo "Duration per run: ${DURATION}s"
echo "Repetitions: ${REPS}"
echo ""

# CSV header
echo "impl,profile,threads,rep,throughput_mbps,ops_per_sec,lat_p50,lat_p95,lat_p99" > "$RESULTS_DIR/all_results.csv"

# === Phase 1: Single-threaded baseline (all profiles, threads=1) ===
echo ""
echo "=== Phase 1: Single-threaded baseline ==="
PROFILES="seq-large rand-small many-files create-unlink mixed"

for profile in $PROFILES; do
  threads=1
  if [ "$profile" = "mixed" ]; then
    threads=1  # mixed uses fixed 7 internal threads
  fi

  for rep in $(seq 1 $REPS); do
    # C++
    mount_cpp
    run_bench_config "cpp" "$profile" "$threads" "$rep"
    cleanup

    # Rust single-threaded
    mount_rust_st
    run_bench_config "rust-st" "$profile" "$threads" "$rep"
    cleanup

    # Rust multi-threaded
    mount_rust_mt
    run_bench_config "rust-mt" "$profile" "$threads" "$rep"
    cleanup
  done
done

# === Phase 2: Multi-threaded scaling (seq-large and rand-small) ===
echo ""
echo "=== Phase 2: Multi-threaded scaling ==="
SCALING_PROFILES="seq-large rand-small"
THREAD_COUNTS="1 2 4 8"

for profile in $SCALING_PROFILES; do
  for threads in $THREAD_COUNTS; do
    # Skip threads=1, already done in Phase 1
    if [ "$threads" = "1" ]; then
      continue
    fi

    for rep in $(seq 1 $REPS); do
      mount_cpp
      run_bench_config "cpp" "$profile" "$threads" "$rep"
      cleanup

      mount_rust_st
      run_bench_config "rust-st" "$profile" "$threads" "$rep"
      cleanup

      mount_rust_mt
      run_bench_config "rust-mt" "$profile" "$threads" "$rep"
      cleanup
    done
  done
done

# === Phase 3: Metadata ops scaling (create-unlink, many-files) ===
echo ""
echo "=== Phase 3: Metadata ops scaling ==="
META_PROFILES="create-unlink many-files"

for profile in $META_PROFILES; do
  for threads in 2 4 8; do
    for rep in $(seq 1 $REPS); do
      mount_cpp
      run_bench_config "cpp" "$profile" "$threads" "$rep"
      cleanup

      mount_rust_st
      run_bench_config "rust-st" "$profile" "$threads" "$rep"
      cleanup

      mount_rust_mt
      run_bench_config "rust-mt" "$profile" "$threads" "$rep"
      cleanup
    done
  done
done

echo ""
echo "========================================"
echo "  Benchmark complete!"
echo "  Results in: $RESULTS_DIR/"
echo "========================================"
cat "$RESULTS_DIR/all_results.csv"
