#!/usr/bin/env python3
"""
capture_bench_concurrent.py - Fires genuinely CONCURRENT write requests
against the same app+DB(+fuselog) instance, to test whether racing DB
execution alone (independent of XDN's own capture-orchestration code) can
produce a corrupted stateDiff sequence.

Design, and why it's built this way:

  - Writes are organized into "rounds": every thread in a round waits on a
    shared Barrier before issuing its write, so every round has genuine,
    verified-after-the-fact overlap -- not just accidental scheduling luck
    on the first request. check_round_overlap() confirms this after the
    fact from each write's [start,end] HTTP window.
  - --capture-strategy controls how/whether fuselog is asked for a diff:
      per-write (default): only the fuselog CAPTURE call is serialized (via
        a lock around the 'g' -> size-header -> payload socket protocol).
        The HTTP write requests themselves are allowed to race completely
        freely, matching how PrimaryBackupManager.handleClientRequest's
        execute() call has zero serialization today. This isolates: is
        concurrent DB execution alone enough to produce corruption, even
        with the capture step made maximally safe?
      end-only: no capture happens during the write phase at all -- every
        thread finishes all its rounds first, then ONE capture happens
        after every thread has joined. This asks a narrower question: if
        capture never has to interleave with anything, does concurrent
        execution alone still leave fuselog with a consistent final state?
  - --mode controls whether fuselog is involved at all: 'fuselog' (default)
    mounts fuselog and (per --capture-strategy) captures diffs; 'none' skips
    mounting/capture entirely and only measures write-race HTTP latency and
    overlap, e.g. for baseline comparison.
  - Diff numbering (per-write strategy only) uses a lock-protected shared
    counter, since concurrent completion order is not the same as
    submission order.
  - Every write's start/end timestamps are logged to results/timeline.csv
    specifically so a "clean" result can be checked for genuine overlap,
    rather than trusting that concurrency happened just because threads
    were used.

Reuses capture_bench.py's plumbing (mount/compose/health-wait/save_diff/
gap-check) directly via `import capture_bench as seq` -- that's shared
infra, not part of what this script is testing.

Usage:
  python3 capture_bench_concurrent.py --app bookcatalog-nd-mysql \
      --concurrency 8 --rounds 1250 --out-dir runs/concurrent_run1
  (produces 8 * 1250 = 10000 writes)

  python3 capture_bench_concurrent.py --app bookcatalog-nd-mysql \
      --concurrency 8 --rounds 1250 --capture-strategy end-only \
      --out-dir runs/concurrent_run2
"""

import argparse
import csv
import json
import statistics
import sys
import threading
import time
from pathlib import Path

import requests as http

import bench_common as bc
import capture_bench as seq

SCRIPT_DIR = Path(__file__).resolve().parent


class AtomicCounter:
    """Thread-safe monotonic counter. Always used for write payload content
    (so writes stay individually identifiable regardless of capture
    strategy); also used for diff filenames when capture_strategy is
    per-write, since concurrent writers complete in unpredictable order and
    a plain incrementing variable would risk two threads grabbing the same
    count."""

    def __init__(self, start=0):
        self._value = start
        self._lock = threading.Lock()

    def next(self):
        with self._lock:
            self._value += 1
            return self._value


def percentiles(values):
    if not values:
        return {"p50": None, "p95": None, "p99": None, "avg": None, "count": 0}
    s = sorted(values)
    n = len(s)
    return {
        "p50": s[int(n * 0.50) if n * 0.50 < n else n - 1],
        "p95": s[int(n * 0.95) if n * 0.95 < n else n - 1],
        "p99": s[int(n * 0.99) if n * 0.99 < n else n - 1],
        "avg": statistics.mean(s),
        "count": n,
    }


def writer_thread(
    thread_id, rounds, barrier, counter, capture_per_write,
    capture_lock, fs_sock, base_url, write_endpoint, write_method,
    write_payload_tmpl, write_headers, diff_dir, app_key,
    timeline_rows, timeline_lock, t_origin, log_fh, log_lock, abort_flag,
):
    for round_idx in range(rounds):
        if abort_flag.is_set():
            return

        # Every thread in this round must arrive here before any of them
        # proceeds -- this is what turns "threads happen to overlap
        # sometimes" into "this round is guaranteed to have N-way overlap".
        barrier.wait()

        count = counter.next()
        payload = write_payload_tmpl.replace("{counter}", str(count))

        t_write_start = time.perf_counter() - t_origin
        try:
            resp = http.request(
                write_method, base_url + write_endpoint,
                data=payload, headers=write_headers, timeout=30,
            )
            status = resp.status_code
        except Exception as e:
            status = -1
            with log_lock:
                seq.log(log_fh, f"thread={thread_id} round={round_idx} "
                                 f"write failed: {e}")
        t_write_end = time.perf_counter() - t_origin

        t_capture_start = ""
        t_capture_end = ""
        capture_ok = ""
        if capture_per_write:
            with capture_lock:
                t_capture_start = time.perf_counter() - t_origin
                capture_ok = True
                try:
                    diff_bytes = bc.capture_state_diff(fs_sock)
                    seq.save_diff(diff_dir, app_key, count, diff_bytes)
                except bc.StateDiffDesyncError as e:
                    capture_ok = False
                    with log_lock:
                        seq.log(log_fh, f"FATAL: stateDiff desync at count={count} "
                                         f"(thread={thread_id} round={round_idx}): {e}")
                    abort_flag.set()
                except RuntimeError as e:
                    capture_ok = False
                    with log_lock:
                        seq.log(log_fh, f"FATAL: capture failed at count={count} "
                                         f"(thread={thread_id} round={round_idx}): {e}")
                    abort_flag.set()
                t_capture_end = time.perf_counter() - t_origin

        with timeline_lock:
            timeline_rows.append([
                round_idx, thread_id, count, status,
                f"{t_write_start:.6f}", f"{t_write_end:.6f}",
                f"{t_capture_start:.6f}" if t_capture_start != "" else "",
                f"{t_capture_end:.6f}" if t_capture_end != "" else "",
                capture_ok,
            ])

        if abort_flag.is_set():
            return


def check_round_overlap(timeline_rows):
    """Post-hoc verification: for each round, did the writes' [start,end]
    HTTP windows actually overlap in wall-clock time? If not, the
    concurrency we intended never really happened, and a clean result
    would be meaningless."""
    by_round = {}
    for row in timeline_rows:
        round_idx = row[0]
        by_round.setdefault(round_idx, []).append(row)

    overlapping_rounds = 0
    total_rounds = len(by_round)
    for round_idx, rows in by_round.items():
        if len(rows) < 2:
            continue
        intervals = [(float(r[4]), float(r[5])) for r in rows]  # write start/end
        intervals.sort()
        has_overlap = False
        for i in range(len(intervals) - 1):
            if intervals[i + 1][0] < intervals[i][1]:
                has_overlap = True
                break
        if has_overlap:
            overlapping_rounds += 1

    return overlapping_rounds, total_rounds


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--app", required=True, help="App key in bench_endpoints.yaml")
    ap.add_argument("--mode", default="fuselog", choices=["fuselog", "none"],
                     help="Default fuselog; use 'none' for a no-fuselog write-race baseline")
    ap.add_argument("--capture-strategy", default="per-write", choices=["per-write", "end-only"],
                     help="per-write: capture (serialized) after every write (default). "
                          "end-only: no capture during writes, exactly one after all threads join. "
                          "Ignored entirely when --mode none.")
    ap.add_argument("--concurrency", type=int, default=2,
                    help="Number of writer threads racing each other per round")
    ap.add_argument("--rounds", type=int, required=True,
                    help="Number of rounds; total writes = concurrency * rounds")
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--endpoints-config", type=Path,
                    default=SCRIPT_DIR / "bench_endpoints.yaml")
    ap.add_argument("--fuselog-bin", default="fuselog")
    ap.add_argument("--health-timeout", type=int, default=120)
    ap.add_argument("--disable-coalescing", action="store_true",
                     help="Set WRITE_COALESCING=false for the fuselog process "
                          "(fuselogv2.cpp reads this via getenv_bool at startup, "
                          "default true -- no rebuild needed to toggle it)")
    ap.add_argument("--disable-prune", action="store_true",
                     help="Set FUSELOG_PRUNE=false for the fuselog process "
                          "(same mechanism as --disable-coalescing, default true)")
    args = ap.parse_args()
    args.out_dir = args.out_dir.resolve()

    entry, descriptor = bc.load_app_config(args.endpoints_config, args.app)

    run_id = args.out_dir.name
    live_dir = args.out_dir / "live"
    diff_dir = args.out_dir / "diff"
    results_dir = args.out_dir / "results"
    for d in (live_dir, diff_dir, results_dir):
        d.mkdir(parents=True, exist_ok=True)

    log_fh = open(results_dir / "run.log", "w")
    log_lock = threading.Lock()
    seq.log(log_fh, f"app={args.app} mode={args.mode} capture_strategy={args.capture_strategy} "
                     f"concurrency={args.concurrency} rounds={args.rounds} "
                     f"total_writes={args.concurrency * args.rounds} "
                     f"out_dir={args.out_dir} "
                     f"disable_coalescing={args.disable_coalescing} "
                     f"disable_prune={args.disable_prune}")

    sock_path = args.out_dir / "fuselog.sock"
    compose_path = args.out_dir / "docker-compose.yml"
    host_port = entry["capture_host_port"]
    # Fixed path, always used when mode=fuselog -- no CLI flag needed.
    trace_file = results_dir / "fuselog_trace.log"

    entry_name, entry_spec = bc.find_entry_component(descriptor)
    state_component, _ = bc.parse_state(descriptor)
    entry_container = f"{run_id}-{entry_name}"
    state_container = f"{run_id}-{state_component}"

    fs_sock = None
    aborted = False
    final_capture_ms = None
    capture_per_write = args.mode == "fuselog" and args.capture_strategy == "per-write"

    try:
        if args.mode == "fuselog":
            seq.mount_fuselog(live_dir, sock_path, args.fuselog_bin, log_fh,
                              disable_coalescing=args.disable_coalescing,
                              disable_prune=args.disable_prune,
                              trace_file=trace_file)
            fs_sock = bc.connect_fuselog_socket(sock_path)
        else:
            live_dir.mkdir(parents=True, exist_ok=True)

        bc.generate_capture_compose(descriptor, live_dir, run_id, host_port, compose_path)
        seq.log(log_fh, f"generated {compose_path}")

        seq.compose_up(compose_path, log_fh)
        state_healthy = seq.wait_for_healthy(state_container, args.health_timeout, log_fh)
        if not state_healthy:
            raise RuntimeError(f"{state_container} never became healthy; aborting")

        hc_path = entry_spec.get("healthcheck", {}).get("path", "/")
        app_url = f"http://localhost:{host_port}{hc_path}"
        healthy = seq.wait_for_http_healthy(app_url, args.health_timeout, log_fh)
        if not healthy:
            raise RuntimeError(f"{app_url} never responded; aborting")

        # NOTE: no baseline diff #0 capture here in either strategy --
        # per-write captures alongside each write starting at count=1;
        # end-only captures nothing until every thread has joined.

        base_url = f"http://localhost:{host_port}"
        write_endpoint = entry["write_endpoint"]
        write_method = entry.get("write_method", "POST")
        write_payload_tmpl = entry["write_payload"]
        write_headers = entry.get("write_headers", {})

        counter = AtomicCounter(0)
        capture_lock = threading.Lock() if capture_per_write else None
        barrier = threading.Barrier(args.concurrency)
        timeline_rows = []
        timeline_lock = threading.Lock()
        abort_flag = threading.Event()
        t_origin = time.perf_counter()
        wall_origin = time.time()  # anchor: lets timeline.csv's relative
        # perf_counter times be converted back to
        # wall-clock, to compare against
        # fuselog_trace.log's wall-clock timestamps

        seq.log(log_fh, f"wall_origin={wall_origin:.6f} "
                         f"(perf_counter t_origin corresponds to this "
                         f"wall-clock instant)")

        threads = []
        for tid in range(args.concurrency):
            t = threading.Thread(
                target=writer_thread,
                args=(
                    tid, args.rounds, barrier, counter, capture_per_write,
                    capture_lock, fs_sock, base_url, write_endpoint,
                    write_method, write_payload_tmpl, write_headers,
                    diff_dir, args.app, timeline_rows, timeline_lock,
                    t_origin, log_fh, log_lock, abort_flag,
                ),
            )
            threads.append(t)

        seq.log(log_fh, f"starting {args.concurrency} writer threads, "
                         f"{args.rounds} rounds each "
                         f"(capture_per_write={capture_per_write})")
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        aborted = abort_flag.is_set()

        seq.log(log_fh, "all writer threads joined")

        timeline_rows.sort(key=lambda r: (r[0], r[1]))
        timeline_csv = results_dir / "timeline.csv"
        with open(timeline_csv, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "round", "thread_id", "count", "http_status",
                "write_start_s", "write_end_s",
                "capture_start_s", "capture_end_s", "capture_ok",
            ])
            writer.writerows(timeline_rows)
        seq.log(log_fh, f"wrote {timeline_csv}")

        overlapping_rounds, total_rounds = check_round_overlap(timeline_rows)
        seq.log(log_fh, f"overlap check: {overlapping_rounds}/{total_rounds} rounds "
                         f"had genuinely overlapping write windows")
        if overlapping_rounds == 0 and total_rounds > 0:
            seq.log(log_fh, "WARNING: zero rounds showed real overlap -- "
                             "a clean corruption result would NOT be meaningful. "
                             "Check system load / GIL contention / barrier usage.")

        # end-only strategy's single capture, now that every thread has joined.
        if args.mode == "fuselog" and args.capture_strategy == "end-only" and not aborted:
            seq.log(log_fh, "capturing the single end-of-run stateDiff")
            t_cap0 = time.perf_counter()
            try:
                diff_bytes = bc.capture_state_diff(fs_sock)
                fname = seq.save_diff(diff_dir, args.app, 0, diff_bytes)
                final_capture_ms = (time.perf_counter() - t_cap0) * 1000
                seq.log(log_fh, f"captured end-of-run diff -> {fname} "
                                 f"({len(diff_bytes)} bytes, {final_capture_ms:.3f}ms)")
            except bc.StateDiffDesyncError as e:
                seq.log(log_fh, f"FATAL: stateDiff desync on end-of-run capture: {e}")
                aborted = True
            except RuntimeError as e:
                seq.log(log_fh, f"FATAL: end-of-run capture failed: {e}")
                aborted = True

        gaps = set()
        if capture_per_write:
            write_counter_final = counter._value
            gaps = seq.check_diff_sequence_gaps(diff_dir, args.app, write_counter_final, log_fh)

        write_latencies_ms = [
            (float(r[5]) - float(r[4])) * 1000 for r in timeline_rows
        ]
        capture_latencies_ms = [
            (float(r[7]) - float(r[6])) * 1000 for r in timeline_rows
            if capture_per_write and r[8] == True
        ]

        summary = {
            "app": args.app,
            "mode": args.mode,
            "capture_strategy": args.capture_strategy,
            "concurrency": args.concurrency,
            "rounds": args.rounds,
            "total_writes_attempted": args.concurrency * args.rounds,
            "total_writes_completed": len(timeline_rows),
            "aborted": aborted,
            "overlapping_rounds": overlapping_rounds,
            "total_rounds_recorded": total_rounds,
            "write_latency_ms": percentiles(write_latencies_ms),
        }
        if capture_per_write:
            summary["capture_latency_ms"] = percentiles(capture_latencies_ms)
            summary["diff_sequence_gaps"] = sorted(gaps) if gaps else []
        elif args.mode == "fuselog" and args.capture_strategy == "end-only":
            summary["final_capture_ms"] = final_capture_ms

        (results_dir / "summary.json").write_text(json.dumps(summary, indent=2))
        seq.log(log_fh, f"summary written to {results_dir / 'summary.json'}")
        print(json.dumps(summary, indent=2))

    finally:
        if fs_sock:
            try:
                fs_sock.close()
            except OSError:
                pass
        seq.compose_down(compose_path, log_fh)
        if args.mode == "fuselog":
            seq.unmount_fuselog(live_dir, log_fh)
        seq.log(log_fh, "teardown complete")
        log_fh.close()

    if aborted:
        sys.exit(1)


if __name__ == "__main__":
    main()
