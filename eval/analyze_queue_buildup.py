#!/usr/bin/env python3
"""
analyze_queue_buildup.py — Parse instrumentation logs to identify queue buildup.

Reads the screen log and produces per-second time series for all 5 queues.

Usage:
    python3 analyze_queue_buildup.py --log <screen_rate600.log>
"""

import argparse
import re
import sys
from collections import defaultdict
from datetime import datetime


def parse_timestamp(line):
    """Extract datetime from log line like '2026-03-10 06:46:23.543343667 [INFO] ...'"""
    m = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\.\d+', line)
    if m:
        return datetime.strptime(m.group(1), '%Y-%m-%d %H:%M:%S')
    return None


def parse_java_int(s):
    """Parse Java MessageFormat integer like '4,269' -> 4269"""
    return int(s.replace(',', ''))


def analyze_log(log_path):
    # Per-second buckets
    dispatch_data = defaultdict(list)   # sec -> list of (size, oldestWaitMs, newestWaitMs, batchingQD, submissionQD)
    callback_data = defaultdict(list)   # sec -> list of (size, pipelineMs, oldestTotalMs, completionQD)
    worker_data = defaultdict(list)     # sec -> list of (batchSize, queueRemaining)
    capture_data = defaultdict(list)    # sec -> list of (drained, doneQueueRemaining)
    writepool_data = defaultdict(list)  # sec -> list of (inFlight,)

    # Regex patterns
    dispatch_re = re.compile(
        r'batcher dispatch: size=(\d+) oldestWaitMs=([\d,]+) newestWaitMs=([\d,]+)'
        r' batchingQueueDepth=([\d,]+) submissionQueueDepth=([\d,]+)')
    callback_re = re.compile(
        r'batcher callback: size=(\d+) pipelineMs=([\d,]+) oldestTotalMs=([\d,]+)'
        r' completionQueueDepth=([\d,]+)')
    worker_re = re.compile(
        r'worker pickup: batchSize=(\d+) queueRemaining=(\d+)')
    capture_re = re.compile(
        r'capture thread: drained=(\d+) doneQueueRemaining=(\d+)')
    writepool_re = re.compile(
        r'writePool near saturation: inFlight=(\d+)')

    min_ts = None
    with open(log_path) as f:
        for line in f:
            ts = parse_timestamp(line)
            if ts is None:
                continue
            if min_ts is None:
                min_ts = ts
            sec = int((ts - min_ts).total_seconds())

            m = dispatch_re.search(line)
            if m:
                dispatch_data[sec].append((
                    int(m.group(1)),
                    parse_java_int(m.group(2)),
                    parse_java_int(m.group(3)),
                    parse_java_int(m.group(4)),
                    parse_java_int(m.group(5)),
                ))
                continue

            m = callback_re.search(line)
            if m:
                callback_data[sec].append((
                    int(m.group(1)),
                    parse_java_int(m.group(2)),
                    parse_java_int(m.group(3)),
                    parse_java_int(m.group(4)),
                ))
                continue

            m = worker_re.search(line)
            if m:
                worker_data[sec].append((int(m.group(1)), int(m.group(2))))
                continue

            m = capture_re.search(line)
            if m:
                capture_data[sec].append((int(m.group(1)), int(m.group(2))))
                continue

            m = writepool_re.search(line)
            if m:
                writepool_data[sec].append((int(m.group(1)),))

    if min_ts is None:
        print("ERROR: No timestamped lines found in log.")
        sys.exit(1)

    # Determine time range
    all_secs = sorted(set(
        list(dispatch_data.keys()) + list(callback_data.keys()) +
        list(worker_data.keys()) + list(capture_data.keys())
    ))
    if not all_secs:
        print("ERROR: No instrumentation data found.")
        sys.exit(1)

    max_sec = all_secs[-1]

    # Print header
    print(f"Log: {log_path}")
    print(f"Time range: {min_ts} to {max_sec}s later")
    print(f"Total dispatch events: {sum(len(v) for v in dispatch_data.values())}")
    print(f"Total callback events: {sum(len(v) for v in callback_data.values())}")
    print(f"Total worker pickup events: {sum(len(v) for v in worker_data.values())}")
    print(f"Total capture thread events: {sum(len(v) for v in capture_data.values())}")
    print(f"Total writePool saturation events: {sum(len(v) for v in writepool_data.values())}")
    print()

    # ── Per-second batcher dispatch summary ──
    print("=" * 100)
    print("BATCHER DISPATCH (Q1+Q2 wait time)")
    print("=" * 100)
    print(f"{'sec':>4}  {'dispatches':>10}  {'totalReqs':>9}  {'avgBatchSz':>10}  "
          f"{'maxOldWait':>10}  {'avgOldWait':>10}  {'maxBatchQD':>10}  {'maxSubQD':>8}")
    print("-" * 100)
    for sec in range(0, max_sec + 1):
        entries = dispatch_data.get(sec, [])
        if not entries:
            continue
        n = len(entries)
        total_reqs = sum(e[0] for e in entries)
        avg_batch = total_reqs / n
        max_oldest_wait = max(e[1] for e in entries)
        avg_oldest_wait = sum(e[1] for e in entries) / n
        max_batching_qd = max(e[3] for e in entries)
        max_sub_qd = max(e[4] for e in entries)
        print(f"{sec:>4}  {n:>10}  {total_reqs:>9}  {avg_batch:>10.1f}  "
              f"{max_oldest_wait:>10}  {avg_oldest_wait:>10.1f}  {max_batching_qd:>10}  {max_sub_qd:>8}")

    print()

    # ── Per-second batcher callback summary ──
    print("=" * 100)
    print("BATCHER CALLBACK (pipeline time = Q3 + exec + Q4 + capture + propose)")
    print("=" * 100)
    print(f"{'sec':>4}  {'callbacks':>9}  {'totalReqs':>9}  {'maxPipeMs':>9}  "
          f"{'avgPipeMs':>9}  {'maxTotalMs':>10}  {'maxComplQD':>10}")
    print("-" * 100)
    for sec in range(0, max_sec + 1):
        entries = callback_data.get(sec, [])
        if not entries:
            continue
        n = len(entries)
        total_reqs = sum(e[0] for e in entries)
        max_pipe = max(e[1] for e in entries)
        avg_pipe = sum(e[1] for e in entries) / n
        max_total = max(e[2] for e in entries)
        max_compl_qd = max(e[3] for e in entries)
        print(f"{sec:>4}  {n:>9}  {total_reqs:>9}  {max_pipe:>9}  "
              f"{avg_pipe:>9.1f}  {max_total:>10}  {max_compl_qd:>10}")

    print()

    # ── Per-second PBM worker pickup summary ──
    print("=" * 80)
    print("PBM WORKER PICKUP (Q3 = serviceBatchQueue)")
    print("=" * 80)
    print(f"{'sec':>4}  {'pickups':>8}  {'totalReqs':>9}  {'avgBatchSz':>10}  {'maxQRemain':>10}")
    print("-" * 80)
    for sec in range(0, max_sec + 1):
        entries = worker_data.get(sec, [])
        if not entries:
            continue
        n = len(entries)
        total_reqs = sum(e[0] for e in entries)
        avg_batch = total_reqs / n
        max_q_remain = max(e[1] for e in entries)
        print(f"{sec:>4}  {n:>8}  {total_reqs:>9}  {avg_batch:>10.1f}  {max_q_remain:>10}")

    print()

    # ── Per-second capture thread summary ──
    print("=" * 80)
    print("CAPTURE THREAD (Q4 = serviceDoneQueue)")
    print("=" * 80)
    print(f"{'sec':>4}  {'captures':>8}  {'totalDrained':>12}  {'maxDoneQRemain':>14}")
    print("-" * 80)
    for sec in range(0, max_sec + 1):
        entries = capture_data.get(sec, [])
        if not entries:
            continue
        n = len(entries)
        total_drained = sum(e[0] for e in entries)
        max_done_remain = max(e[1] for e in entries)
        print(f"{sec:>4}  {n:>8}  {total_drained:>12}  {max_done_remain:>14}")

    print()

    # ── writePool saturation ──
    if writepool_data:
        print("=" * 60)
        print("WRITEPOOL SATURATION EVENTS")
        print("=" * 60)
        for sec in sorted(writepool_data.keys()):
            for entry in writepool_data[sec]:
                print(f"  sec={sec}  inFlight={entry[0]}")
    else:
        print("No writePool saturation events (inFlight never exceeded 30).")

    print()

    # ── Condensed timeline with key indicators ──
    print("=" * 120)
    print("CONDENSED TIMELINE (key indicators per second)")
    print("=" * 120)
    print(f"{'sec':>4}  {'dispReqs':>8}  {'callReqs':>8}  {'maxOldWt':>8}  "
          f"{'maxPipe':>7}  {'maxTotal':>8}  {'batchQD':>7}  "
          f"{'pbmQRem':>7}  {'doneQRem':>8}  {'complQD':>7}")
    print("-" * 120)
    for sec in range(0, max_sec + 1):
        d_entries = dispatch_data.get(sec, [])
        c_entries = callback_data.get(sec, [])
        w_entries = worker_data.get(sec, [])
        cap_entries = capture_data.get(sec, [])

        if not d_entries and not c_entries and not w_entries and not cap_entries:
            continue

        disp_reqs = sum(e[0] for e in d_entries) if d_entries else 0
        call_reqs = sum(e[0] for e in c_entries) if c_entries else 0
        max_old_wait = max((e[1] for e in d_entries), default=0)
        max_pipe = max((e[1] for e in c_entries), default=0)
        max_total = max((e[2] for e in c_entries), default=0)
        max_batch_qd = max((e[3] for e in d_entries), default=0)
        max_pbm_q_remain = max((e[1] for e in w_entries), default=0)
        max_done_q_remain = max((e[1] for e in cap_entries), default=0)
        max_compl_qd = max((e[3] for e in c_entries), default=0)

        print(f"{sec:>4}  {disp_reqs:>8}  {call_reqs:>8}  {max_old_wait:>8}  "
              f"{max_pipe:>7}  {max_total:>8}  {max_batch_qd:>7}  "
              f"{max_pbm_q_remain:>7}  {max_done_q_remain:>8}  {max_compl_qd:>7}")


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--log', required=True, help='Path to screen log file')
    args = p.parse_args()
    analyze_log(args.log)
