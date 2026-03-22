"""
investigate_parse_pb_timing.py — Parse XDN screen log and produce a timing breakdown report.

Reads the INFO-level timing lines emitted by PrimaryBackupManager and
FuselogStateDiffRecorder, then correlates them with the per-rate go-client
output files from run_load_pb_wordpress_reflex.py, run_load_pb_tpcc_reflex.py, or
run_load_pb_hotelres_reflex.py.

Usage (run from eval/ directory):
    python3 investigate_parse_pb_timing.py [--log PATH] [--results-dir PATH] [--app NAME]

Outputs (in results-dir):
    pb_timing_breakdown.png     — stacked bar chart
    statediff_distribution.png  — boxplot per rate
    batch_distribution.png      — batch size histogram
    report.txt                  — human-readable analysis
    Timing table printed to stdout.

Log line patterns expected (emitted at INFO level; ensure logging.properties
sets PrimaryBackupManager and FuselogStateDiffRecorder to INFO):
    <node>:PrimaryBackupManager - executing batch-of-batches (N batches, M reqs) within X.XXXXXX ms
    <node>:PrimaryBackupManager - capturing stateDiff within X.XXXXXX ms size=N bytes
    <node>:PrimaryBackupManager - batch-of-batches (N batches) committed within X ms
    <node>:FuselogStateDiffRecorder - capturing stateDiff within X.XXXXXX ms
"""

import argparse
import datetime
import math
import re
import sys
from pathlib import Path

# matplotlib is optional — gracefully degrade if not installed
try:
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

# ── Defaults ──────────────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_LOG = SCRIPT_DIR / "results" / "bottleneck" / "screen.log"
DEFAULT_RESULTS_DIR = SCRIPT_DIR / "results" / "bottleneck"
LOAD_DURATION_SEC = 60      # must match run_load_pb_wordpress_reflex.py

# ── Regex patterns ─────────────────────────────────────────────────────────────

# Log line timestamp prefix: "2026-02-24 23:17:18.123456789 [WARNING] ..."
RE_TIMESTAMP = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")

# PrimaryBackupManager timing lines (batch-of-batches format)
RE_EXECUTE = re.compile(
    r"PrimaryBackupManager\s*-\s*executing batch-of-batches\s*\((\d+)\s+batches,\s*(\d+)\s+reqs\)\s+within\s+([\d.]+)\s*ms"
)
RE_CAPTURE_PBM = re.compile(
    r"PrimaryBackupManager\s*-\s*capturing stateDiff within\s+([\d.]+)\s*ms"
    r"(?:\s+size=(\d+)\s+bytes)?"
)
RE_COMMIT = re.compile(
    r"PrimaryBackupManager\s*-\s*batch-of-batches\s*\((\d+)\s+batches\)\s+committed\s+within\s+([\d.]+)\s*ms"
)
# FuselogStateDiffRecorder timing line (inner recorder timing) — now at WARNING too
RE_CAPTURE_FUSE = re.compile(
    r"FuselogStateDiffRecorder\s*-\s*capturing stateDiff within\s+([\d.]+)\s*ms"
)

# New capture thread format (Phase 1 instrumentation, promoted to INFO):
#   capture thread: stateDiff in X.XXX ms, size=Y bytes, covering Z completed batches (W requests)
#   capture thread: cycle=X.XXXms propose=Y.YYYms drained=Z requests=W
RE_CAPTURE_THREAD_SD = re.compile(
    r"PrimaryBackupManager\s*-\s*capture thread:\s*stateDiff in\s+([\d.]+)\s*ms,\s*size=(\d+)\s*bytes,\s*"
    r"covering\s+(\d+)\s+completed batches\s+\((\d+)\s+requests\)"
)
RE_CAPTURE_THREAD_CYCLE = re.compile(
    r"PrimaryBackupManager\s*-\s*capture thread:\s*cycle=([\d.]+)ms\s+propose=([\d.]+)ms\s+drained=(\d+)\s+requests=(\d+)"
)


# ── Rate discovery ─────────────────────────────────────────────────────────────

def discover_rates(results_dir: Path) -> list:
    """Auto-discover rate points from rate*.txt files in results_dir."""
    rates = []
    for p in sorted(results_dir.glob("rate*.txt")):
        try:
            rates.append(int(p.stem.replace("rate", "")))
        except ValueError:
            pass
    return sorted(rates)


# ── Parsing ───────────────────────────────────────────────────────────────────

def parse_log(log_path: Path):
    """Extract timing samples from the screen log.

    Returns eight values:
        execute_ms             — list of float (ms), one per batch cycle
        execute_ts             — list of datetime | None, one per batch cycle
        execute_n_reqs         — list of int, inner-request count per batch cycle
        capture_pbm_ms         — list of float (ms)
        capture_pbm_size_bytes — list of int | None (bytes, None if not present)
        commit_ms              — list of float (ms)
        commit_n_batches       — list of int, outer-batch count per commit
        capture_fuse_ms        — list of float (ms)
    """
    execute_ms = []
    execute_ts = []
    execute_n_reqs = []
    capture_pbm_ms = []
    capture_pbm_size_bytes = []
    commit_ms = []
    commit_n_batches = []
    capture_fuse_ms = []
    # New capture thread metrics
    capture_thread_sd_ms = []
    capture_thread_sd_size = []
    capture_thread_sd_requests = []
    capture_thread_cycle_ms = []
    capture_thread_propose_ms = []
    capture_thread_cycle_requests = []

    if not log_path.exists():
        print(f"ERROR: log file not found: {log_path}")
        print("  Run run_load_pb_wordpress_reflex.py first to generate the log.")
        sys.exit(1)

    current_ts = None
    with open(log_path, errors="replace") as f:
        for line in f:
            ts_m = RE_TIMESTAMP.match(line)
            if ts_m:
                try:
                    current_ts = datetime.datetime.strptime(
                        ts_m.group(1), "%Y-%m-%d %H:%M:%S"
                    )
                except ValueError:
                    pass
            m = RE_EXECUTE.search(line)
            if m:
                # groups: (n_batches, n_reqs, exec_ms)
                execute_ms.append(float(m.group(3)))
                execute_ts.append(current_ts)
                execute_n_reqs.append(int(m.group(2)))
                continue
            m = RE_CAPTURE_PBM.search(line)
            if m:
                capture_pbm_ms.append(float(m.group(1)))
                capture_pbm_size_bytes.append(int(m.group(2)) if m.group(2) else None)
                continue
            m = RE_COMMIT.search(line)
            if m:
                # groups: (n_batches, commit_ms)
                commit_n_batches.append(int(m.group(1)))
                commit_ms.append(float(m.group(2)))
                continue
            m = RE_CAPTURE_FUSE.search(line)
            if m:
                capture_fuse_ms.append(float(m.group(1)))
                continue
            # New capture thread formats (Phase 1 instrumentation)
            m = RE_CAPTURE_THREAD_SD.search(line)
            if m:
                # groups: (sd_ms, size_bytes, n_batches, n_requests)
                sd_ms = float(m.group(1))
                sd_size = int(m.group(2))
                sd_reqs = int(m.group(4))
                capture_thread_sd_ms.append(sd_ms)
                capture_thread_sd_size.append(sd_size)
                capture_thread_sd_requests.append(sd_reqs)
                # Also populate the legacy arrays for compatibility
                capture_pbm_ms.append(sd_ms)
                capture_pbm_size_bytes.append(sd_size)
                continue
            m = RE_CAPTURE_THREAD_CYCLE.search(line)
            if m:
                # groups: (cycle_ms, propose_ms, n_drained, n_requests)
                cycle_ms = float(m.group(1))
                propose_ms = float(m.group(2))
                n_requests = int(m.group(4))
                capture_thread_cycle_ms.append(cycle_ms)
                capture_thread_propose_ms.append(propose_ms)
                capture_thread_cycle_requests.append(n_requests)
                continue

    return (execute_ms, execute_ts, execute_n_reqs,
            capture_pbm_ms, capture_pbm_size_bytes,
            commit_ms, commit_n_batches, capture_fuse_ms)


def _parse_go_output(path: Path):
    metrics = {}
    if not path.exists():
        return metrics
    for line in open(path, errors="replace"):
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        try:
            metrics[k.strip()] = float(v.strip())
        except ValueError:
            pass
    return {
        "throughput_rps": metrics.get("actual_throughput_rps", float("nan")),
        "avg_ms":         metrics.get("average_latency_ms", float("nan")),
        "p50_ms":         metrics.get("median_latency_ms", float("nan")),
    }


def _avg(lst):
    return sum(lst) / len(lst) if lst else float("nan")


def _percentile(lst, p):
    if not lst:
        return float("nan")
    s = sorted(lst)
    k = (len(s) - 1) * p / 100.0
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def _partition_by_timestamps(execute_ms, execute_ts, capture_pbm_ms,
                              capture_pbm_size_bytes, commit_ms,
                              results_dir, rates):
    """Partition timing samples using the modification times of rate*.txt files.

    Returns (segments_execute, segs_cap, segs_size, segs_com) or None if
    any rate*.txt file is missing.
    """
    rate_end_times = {}
    for rate in rates:
        p = results_dir / f"rate{rate}.txt"
        if not p.exists():
            return None
        rate_end_times[rate] = datetime.datetime.fromtimestamp(p.stat().st_mtime)

    starts = []
    for i, rate in enumerate(rates):
        if i == 0:
            starts.append(
                rate_end_times[rate]
                - datetime.timedelta(seconds=LOAD_DURATION_SEC - 2)
            )
        else:
            starts.append(rate_end_times[rates[i - 1]] + datetime.timedelta(seconds=1))

    ends = []
    for i, rate in enumerate(rates):
        if i < len(rates) - 1:
            ends.append(starts[i + 1] - datetime.timedelta(seconds=1))
        else:
            ends.append(rate_end_times[rate] + datetime.timedelta(seconds=60))

    rate_windows = {rate: (starts[i], ends[i]) for i, rate in enumerate(rates)}

    phase_indices = {rate: [] for rate in rates}
    for j, ts in enumerate(execute_ts):
        if ts is None:
            continue
        for rate in rates:
            start, end = rate_windows[rate]
            if start <= ts <= end:
                phase_indices[rate].append(j)
                break

    segments_execute = [[execute_ms[j] for j in phase_indices[r]] for r in rates]
    segs_cap = [
        [capture_pbm_ms[j] for j in phase_indices[r] if j < len(capture_pbm_ms)]
        for r in rates
    ]
    segs_size = [
        [capture_pbm_size_bytes[j]
         for j in phase_indices[r]
         if j < len(capture_pbm_size_bytes) and capture_pbm_size_bytes[j] is not None]
        for r in rates
    ]
    segs_com = [
        [commit_ms[j] for j in phase_indices[r] if j < len(commit_ms)]
        for r in rates
    ]
    return segments_execute, segs_cap, segs_size, segs_com


def _partition_by_largest_gaps(execute_ms, execute_ts, capture_pbm_ms,
                                capture_pbm_size_bytes, commit_ms, rates):
    """Fallback: find phase boundaries using the (n_phases) largest inter-arrival gaps."""
    valid = [(i, ts) for i, ts in enumerate(execute_ts) if ts is not None]
    if len(valid) < 2:
        n = len(rates)
        chunk = max(1, len(execute_ms) // n)
        segs_e, segs_c, segs_sz, segs_co = [], [], [], []
        for i in range(n):
            s = i * chunk
            e = s + chunk if i < n - 1 else len(execute_ms)
            segs_e.append(execute_ms[s:e])
            segs_c.append(capture_pbm_ms[s:min(e, len(capture_pbm_ms))])
            segs_sz.append([v for v in capture_pbm_size_bytes[s:min(e, len(capture_pbm_size_bytes))]
                            if v is not None])
            segs_co.append(commit_ms[s:min(e, len(commit_ms))])
        return segs_e, segs_c, segs_sz, segs_co

    gaps = []
    for k in range(1, len(valid)):
        i_prev, ts_prev = valid[k - 1]
        i_curr, ts_curr = valid[k]
        dt = (ts_curr - ts_prev).total_seconds()
        gaps.append((dt, i_curr))

    n_phases = len(rates)
    sorted_gaps = sorted(gaps, key=lambda x: -x[0])
    candidate_boundaries = sorted([idx for _, idx in sorted_gaps[:n_phases]])
    if candidate_boundaries and candidate_boundaries[0] < 5:
        candidate_boundaries = candidate_boundaries[1:]
    boundaries = candidate_boundaries[-(n_phases - 1):]

    seg_ends = boundaries + [len(execute_ms)]
    seg_starts = [0] + boundaries
    segs_e  = [execute_ms[s:e]     for s, e in zip(seg_starts, seg_ends)]
    segs_c  = [capture_pbm_ms[s:min(e, len(capture_pbm_ms))]
               for s, e in zip(seg_starts, seg_ends)]
    segs_sz = [
        [v for v in capture_pbm_size_bytes[s:min(e, len(capture_pbm_size_bytes))]
         if v is not None]
        for s, e in zip(seg_starts, seg_ends)
    ]
    segs_co = [commit_ms[s:min(e, len(commit_ms))]
               for s, e in zip(seg_starts, seg_ends)]
    return segs_e, segs_c, segs_sz, segs_co


def _partition_by_rate(execute_ms, execute_ts, capture_pbm_ms, capture_pbm_size_bytes,
                       commit_ms, results_dir, rates):
    """Partition timing samples into per-rate segments.

    Returns ((exec_segs, cap_segs, size_segs, com_segs), partition_method).
    """
    result = _partition_by_timestamps(
        execute_ms, execute_ts, capture_pbm_ms, capture_pbm_size_bytes,
        commit_ms, results_dir, rates
    )
    if result is not None:
        return result, "mtime"

    result = _partition_by_largest_gaps(
        execute_ms, execute_ts, capture_pbm_ms, capture_pbm_size_bytes,
        commit_ms, rates
    )
    return result, "gap-detect"


# ── Reporting ─────────────────────────────────────────────────────────────────

def print_report(rates, results_dir, execute_segs, capture_segs, commit_segs,
                 partition_method="mtime"):
    print()
    print(f"Timing breakdown (primary node — PrimaryBackupManager)  [partition={partition_method}]:")
    print(
        f"  {'rate':>6}  {'execute':>10}  {'capture':>10}  {'paxos':>10}  "
        f"{'cycle':>10}  {'pred_max':>10}  {'actual':>8}"
    )
    print("  " + "-" * 74)

    for i, rate in enumerate(rates):
        exec_avg = _avg(execute_segs[i])
        cap_avg = _avg(capture_segs[i])
        com_avg = _avg(commit_segs[i])

        cycle_avg = exec_avg + cap_avg

        if cycle_avg > 0 and cycle_avg == cycle_avg:
            pred_max = 1000.0 / cycle_avg
        else:
            pred_max = float("nan")

        go_out = _parse_go_output(results_dir / f"rate{rate}.txt")
        actual = go_out.get("throughput_rps", float("nan"))

        n_exec = len(execute_segs[i])
        n_cap  = len(capture_segs[i])
        n_com  = len(commit_segs[i])

        # Skip rows with no log data (stale files from previous runs)
        if n_exec == 0:
            continue

        def fmt(v):
            return f"{v:.1f}ms" if v == v else "   n/a"

        print(
            f"  {rate:>5.0f}  "
            f"{fmt(exec_avg):>10}  "
            f"{fmt(cap_avg):>10}  "
            f"{fmt(com_avg):>10}  "
            f"{fmt(cycle_avg):>10}  "
            f"{'%.1f rps' % pred_max if pred_max==pred_max else '   n/a':>10}  "
            f"{'%.2f rps' % actual if actual==actual else '   n/a':>8}"
            f"  (n={n_exec}/{n_cap}/{n_com})"
        )

    print()
    print("  cycle_ms = execute_ms + capture_ms (time held inside synchronized block)")
    print("  pred_max = 1000 / cycle_ms  (theoretical ceiling if cycle is the bottleneck)")
    print()


def generate_report(rates, results_dir, exec_segs, cap_segs, com_segs, size_segs,
                    execute_n_reqs, app_name: str = "XDN"):
    """Write human-readable analysis to report.txt and print to stdout."""
    lines = []
    lines.append(f"=== {app_name} Primary-Backup Throughput Report ===")
    lines.append("")

    # Per-rate table
    header = (
        f"{'Rate':>6} | {'Tput':>7} | {'Avg lat':>8} | "
        f"{'Exec avg':>9} | {'Cap avg':>9} | {'Cap p90':>9} | {'Cap p99':>9} | "
        f"{'Size med':>9} | {'Size p90':>9}"
    )
    sep = "-" * len(header)
    lines.append(header)
    lines.append(sep)

    for i, rate in enumerate(rates):
        exec_avg  = _avg(exec_segs[i])
        cap_avg   = _avg(cap_segs[i])
        cap_p90   = _percentile(cap_segs[i], 90)
        cap_p99   = _percentile(cap_segs[i], 99)

        size_med  = _percentile(size_segs[i], 50) if size_segs[i] else float("nan")
        size_p90  = _percentile(size_segs[i], 90) if size_segs[i] else float("nan")

        go_out = _parse_go_output(results_dir / f"rate{rate}.txt")
        tput   = go_out.get("throughput_rps", float("nan"))
        avg_lat = go_out.get("avg_ms", float("nan"))

        def fms(v):
            return f"{v:.1f}ms" if v == v else "  n/a"

        def fbytes(v):
            if v != v:
                return "  n/a"
            if v >= 1024 * 1024:
                return f"{v/1024/1024:.1f} MB"
            if v >= 1024:
                return f"{v/1024:.1f} KB"
            return f"{v:.0f} B"

        # Skip rows with no log data (stale files from previous runs)
        if not exec_segs[i] and tput != tput:
            continue

        tput_str = f"{tput:>6.2f}r" if tput == tput else "   n/a "
        lines.append(
            f"{rate:>6} | {tput_str} | {fms(avg_lat):>8} | "
            f"{fms(exec_avg):>9} | {fms(cap_avg):>9} | {fms(cap_p90):>9} | {fms(cap_p99):>9} | "
            f"{fbytes(size_med):>9} | {fbytes(size_p90):>9}"
        )

    lines.append("")
    lines.append("Analysis:")

    # Find peak rate (highest rate with tput >= 80% of offered)
    peak_rate = None
    peak_idx  = None
    for i, rate in enumerate(rates):
        go_out = _parse_go_output(results_dir / f"rate{rate}.txt")
        tput = go_out.get("throughput_rps", float("nan"))
        if tput == tput and tput >= 0.8 * rate:
            peak_rate = rate
            peak_idx  = i

    if peak_rate is not None and peak_idx is not None:
        exec_avg = _avg(exec_segs[peak_idx])
        cap_avg  = _avg(cap_segs[peak_idx])
        cycle    = exec_avg + cap_avg
        if cycle > 0 and cycle == cycle:
            cap_pct  = 100.0 * cap_avg / cycle
            pred_max = 1000.0 / cycle
            go_out   = _parse_go_output(results_dir / f"rate{peak_rate}.txt")
            actual   = go_out.get("throughput_rps", float("nan"))

            size_med_low  = _percentile(size_segs[0], 50)  if size_segs else float("nan")
            size_med_peak = _percentile(size_segs[peak_idx], 50) if size_segs else float("nan")

            lines.append(
                f"- At rate={peak_rate} rps (peak), statediff capture accounts for "
                f"{cap_pct:.0f}% of synchronized block time"
            )
            def _fbytes(v):
                if v != v:
                    return "n/a"
                if v >= 1024 * 1024:
                    return f"{v/1024/1024:.1f} MB"
                if v >= 1024:
                    return f"{v/1024:.1f} KB"
                return f"{v:.0f} B"

            lines.append(
                f"- Median statediff size: {_fbytes(size_med_low)} at rate={rates[0]}, "
                f"{_fbytes(size_med_peak)} at rate={peak_rate}"
            )
            lines.append(
                f"- pred_max = {pred_max:.1f} rps, actual = {actual:.2f} rps at rate={peak_rate} "
                f"→ {'capture is confirmed bottleneck' if abs(pred_max - actual) / max(pred_max, 0.01) < 0.3 else 'other bottleneck present'}"
            )

    if execute_n_reqs:
        med_batch = _percentile(execute_n_reqs, 50)
        lines.append(
            f"- Batch sizes: median={med_batch:.0f} reqs/cycle across all rate points"
        )

    report_text = "\n".join(lines) + "\n"
    report_path = results_dir / "report.txt"
    report_path.write_text(report_text)
    print(report_text)
    print(f"   Report saved → {report_path}")


def plot_chart(rates, execute_avgs, capture_avgs, commit_avgs, output_path: Path,
               app_name: str = "XDN"):
    if not HAS_MATPLOTLIB:
        print("WARNING: matplotlib not available — skipping chart generation.")
        print("         Install with: pip install matplotlib")
        return

    import numpy as np
    x = np.arange(len(rates))
    width = 0.5

    fig, ax = plt.subplots(figsize=(8, 5))
    b1 = ax.bar(x, execute_avgs, width, label="execute (HTTP fwd)", color="#4C72B0")
    b2 = ax.bar(x, capture_avgs, width, bottom=execute_avgs,
                label="capture stateDiff (fuselog)", color="#DD8452")
    b3 = ax.bar(x, commit_avgs, width,
                bottom=[e + c for e, c in zip(execute_avgs, capture_avgs)],
                label="stateDiff commit (Paxos)", color="#55A868", alpha=0.7)

    for i, (rate, ex, cap) in enumerate(zip(rates, execute_avgs, capture_avgs)):
        cycle = ex + cap
        if cycle > 0 and cycle == cycle:
            ceiling = 1000.0 / cycle
            ax.annotate(
                f"⌈{ceiling:.1f} rps",
                xy=(x[i], ex + cap + commit_avgs[i] + 5),
                ha="center", va="bottom", fontsize=9, color="#2d2d2d"
            )

    ax.set_xticks(x)
    ax.set_xticklabels([f"{r} rps" for r in rates])
    ax.set_xlabel("Offered Load", fontsize=12)
    ax.set_ylabel("Time (ms)", fontsize=12)
    ax.set_title(
        f"{app_name} PB: Time per request (stacked)\n"
        "synchronized block = execute + capture  →  ceiling = 1/cycle_ms",
        fontsize=11,
    )
    ax.legend(fontsize=9)
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=150)
    print(f"   Chart saved → {output_path}")


def plot_statediff_distribution(rates, size_segs, output_path: Path,
                                app_name: str = "XDN"):
    """Boxplot of statediff sizes per rate point."""
    if not HAS_MATPLOTLIB:
        print("WARNING: matplotlib not available — skipping statediff distribution plot.")
        return

    # Filter to rates that have data
    valid_rates = []
    valid_data  = []
    for r, seg in zip(rates, size_segs):
        if seg:
            valid_rates.append(r)
            valid_data.append(seg)

    if not valid_rates:
        print("   No statediff size data available — skipping statediff_distribution.png")
        return

    fig, ax = plt.subplots(figsize=(9, 5))
    bp = ax.boxplot(valid_data, patch_artist=True, showfliers=True)

    # Color boxes
    colors = ["#4C72B0", "#DD8452", "#55A868", "#C44E52", "#8172B2", "#937860"]
    for patch, color in zip(bp["boxes"], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.6)

    # Annotate median
    for i, (seg, pos) in enumerate(zip(valid_data, range(1, len(valid_data) + 1))):
        med = _percentile(seg, 50)
        if med >= 1024 * 1024:
            label = f"{med/1024/1024:.1f}MB"
        elif med >= 1024:
            label = f"{med/1024:.0f}KB"
        else:
            label = f"{med:.0f}B"
        ax.text(pos, med * 1.05, label, ha="center", va="bottom", fontsize=8)

    ax.set_xticks(range(1, len(valid_rates) + 1))
    ax.set_xticklabels([f"{r} rps" for r in valid_rates])
    ax.set_xlabel("Offered Load", fontsize=12)
    ax.set_ylabel("StateDiff Size (bytes)", fontsize=12)
    ax.set_yscale("log")
    ax.grid(axis="y", alpha=0.3, linestyle="--")
    ax.set_title(
        f"{app_name} PB: StateDiff Size Distribution per Rate\n"
        "Each box = IQR of statediff sizes during 60 s measurement window",
        fontsize=11,
    )
    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=150)
    plt.close(fig)
    print(f"   Chart saved → {output_path}")


def plot_batch_distribution(execute_n_reqs, output_path: Path,
                            app_name: str = "XDN"):
    """Bar chart of batch sizes (inner requests per execution cycle)."""
    if not HAS_MATPLOTLIB:
        print("WARNING: matplotlib not available — skipping batch distribution plot.")
        return

    if not execute_n_reqs:
        print("   No batch size data available — skipping batch_distribution.png")
        return

    from collections import Counter
    counts = Counter(execute_n_reqs)
    batch_sizes = sorted(counts.keys())
    batch_counts = [counts[n] for n in batch_sizes]
    med_batch = _percentile(execute_n_reqs, 50)

    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.bar(batch_sizes, batch_counts, color="#4C72B0", alpha=0.8, width=0.7)

    # Annotate median
    ax.axvline(med_batch, color="#C44E52", linestyle="--", linewidth=1.5,
               label=f"median = {med_batch:.0f} reqs/cycle")

    ax.set_xlabel("Batch Size (inner requests per cycle)", fontsize=12)
    ax.set_ylabel("Count (cycles)", fontsize=12)
    ax.set_title(
        f"{app_name} PB: Batch Size Distribution\n"
        "Distribution of inner HTTP requests combined per execution cycle",
        fontsize=11,
    )
    ax.legend(fontsize=10)
    ax.grid(axis="y", alpha=0.3, linestyle="--")

    # x-ticks: show every batch size if few, else thin out
    if len(batch_sizes) <= 20:
        ax.set_xticks(batch_sizes)
    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=150)
    plt.close(fig)
    print(f"   Chart saved → {output_path}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def _infer_app_name(results_dir: Path) -> str:
    """Infer application name from results directory path."""
    name = results_dir.name
    if "hotelres" in name:
        return "Hotel-Res"
    if "tpcc" in name:
        return "TPC-C"
    if "wordpress" in name or "wp" in name or "bottleneck" in name or "final" in name:
        return "WordPress"
    return "XDN"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--log", type=Path, default=DEFAULT_LOG,
        help=f"Path to screen log (default: {DEFAULT_LOG})"
    )
    parser.add_argument(
        "--results-dir", type=Path, default=DEFAULT_RESULTS_DIR,
        help=f"Directory with rate*.txt go-client output files (default: {DEFAULT_RESULTS_DIR})"
    )
    parser.add_argument(
        "--app", type=str, default=None,
        help="Application name for chart titles (auto-detected from results-dir if omitted)"
    )
    args = parser.parse_args()
    app_name = args.app or _infer_app_name(args.results_dir)

    # Auto-discover rates from results directory
    rates = discover_rates(args.results_dir)
    if not rates:
        print(f"ERROR: no rate*.txt files found in {args.results_dir}")
        print("       Run run_load_pb_wordpress_reflex.py first.")
        sys.exit(1)
    print(f"Discovered rate points: {rates}")

    print(f"Parsing log: {args.log}")
    (execute_ms, execute_ts, execute_n_reqs,
     capture_pbm_ms, capture_pbm_size_bytes,
     commit_ms, commit_n_batches, capture_fuse_ms) = parse_log(args.log)

    print(f"  Found {len(execute_ms)} execute samples")
    print(f"  Found {len(capture_pbm_ms)} capture (PBM) samples")
    size_present = sum(1 for v in capture_pbm_size_bytes if v is not None)
    print(f"  Found {size_present}/{len(capture_pbm_size_bytes)} capture samples with size")
    print(f"  Found {len(commit_ms)} commit samples")
    print(f"  Found {len(capture_fuse_ms)} capture (fuselog) samples")
    ts_valid = sum(1 for t in execute_ts if t is not None)
    print(f"  Timestamps parsed: {ts_valid}/{len(execute_ts)} execute samples have timestamps")

    if not execute_ms:
        print(
            "\nERROR: No 'executing batch-of-batches' lines found in log.\n"
            "  Ensure the JAR was rebuilt after enabling executeXdnBatchOfBatches\n"
            "  and deployed to all nodes before starting the cluster."
        )
        sys.exit(1)

    # Batch-size distribution (inner requests per cycle)
    if execute_n_reqs:
        from collections import Counter
        counts = Counter(execute_n_reqs)
        total_cycles = len(execute_n_reqs)
        total_reqs = sum(execute_n_reqs)
        print(f"\nBatch-size distribution ({total_cycles} cycles, {total_reqs} reqs total):")
        print(f"  {'reqs':>5}  {'count':>6}  {'pct':>6}  {'avg_exec_ms':>12}")
        print("  " + "-" * 38)
        for n in sorted(counts):
            idxs = [i for i, r in enumerate(execute_n_reqs) if r == n]
            avg_e = sum(execute_ms[i] for i in idxs) / len(idxs)
            pct = 100.0 * counts[n] / total_cycles
            print(f"  {n:>5}  {counts[n]:>6}  {pct:>5.1f}%  {avg_e:>10.1f}ms")
        print()

    # Partition samples into per-rate windows
    (exec_segs, cap_segs, size_segs, com_segs), partition_method = _partition_by_rate(
        execute_ms, execute_ts, capture_pbm_ms, capture_pbm_size_bytes,
        commit_ms, args.results_dir, rates
    )

    print_report(rates, args.results_dir, exec_segs, cap_segs, com_segs, partition_method)

    # Compute averages for stacked bar chart
    execute_avgs = [_avg(s) for s in exec_segs]
    capture_avgs = [_avg(s) for s in cap_segs]
    commit_avgs  = [_avg(s) for s in com_segs]

    chart_path = args.results_dir / "pb_timing_breakdown.png"
    plot_chart(rates, execute_avgs, capture_avgs, commit_avgs, chart_path, app_name)

    # Statediff size distribution
    plot_statediff_distribution(
        rates, size_segs,
        args.results_dir / "statediff_distribution.png",
        app_name,
    )

    # Batch size histogram
    plot_batch_distribution(
        execute_n_reqs,
        args.results_dir / "batch_distribution.png",
        app_name,
    )

    # Human-readable report
    generate_report(rates, args.results_dir, exec_segs, cap_segs, com_segs,
                    size_segs, execute_n_reqs, app_name)

    # Print interpretation
    print("Interpretation:")
    print("  If pred_max ≈ actual at high rates, the synchronized(currentEpoch)")
    print("  block in PrimaryBackupManager.java is the confirmed bottleneck.")
    print("  At low rates pred_max >> actual because the system is under-loaded.")
    print()


if __name__ == "__main__":
    main()
