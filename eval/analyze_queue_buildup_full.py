#!/usr/bin/env python3
"""
analyze_queue_buildup_full.py — Full pipeline timing analysis combining
screen log (queue instrumentation) and node log (PBM execute/capture/commit).

Usage:
    python3 analyze_queue_buildup_full.py \
        --screen <screen_rate600.log> \
        --node <node_ar2.log>
"""

import argparse
import re
import sys
from collections import defaultdict
from datetime import datetime


def parse_timestamp(line):
    m = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\.\d+', line)
    if m:
        return datetime.strptime(m.group(1), '%Y-%m-%d %H:%M:%S')
    return None


def parse_java_int(s):
    return int(s.replace(',', ''))


def analyze(screen_path, node_path):
    # ── Parse screen log (queue instrumentation) ──
    dispatch_data = defaultdict(list)
    callback_data = defaultdict(list)

    dispatch_re = re.compile(
        r'batcher dispatch: size=(\d+) oldestWaitMs=([\d,]+) newestWaitMs=([\d,]+)'
        r' batchingQueueDepth=([\d,]+) submissionQueueDepth=([\d,]+)')
    callback_re = re.compile(
        r'batcher callback: size=(\d+) pipelineMs=([\d,]+) oldestTotalMs=([\d,]+)'
        r' completionQueueDepth=([\d,]+)')

    min_ts = None
    with open(screen_path) as f:
        for line in f:
            ts = parse_timestamp(line)
            if ts is None:
                continue
            if min_ts is None:
                min_ts = ts
            sec = int((ts - min_ts).total_seconds())

            m = dispatch_re.search(line)
            if m:
                dispatch_data[sec].append({
                    'size': int(m.group(1)),
                    'oldestWaitMs': parse_java_int(m.group(2)),
                    'batchingQD': parse_java_int(m.group(4)),
                    'submissionQD': parse_java_int(m.group(5)),
                })
                continue

            m = callback_re.search(line)
            if m:
                callback_data[sec].append({
                    'size': int(m.group(1)),
                    'pipelineMs': parse_java_int(m.group(2)),
                    'oldestTotalMs': parse_java_int(m.group(3)),
                    'completionQD': parse_java_int(m.group(4)),
                })

    # ── Parse node log (PBM internals) ──
    exec_data = defaultdict(list)      # sec -> list of (nBatches, nReqs, execMs)
    capture_data = defaultdict(list)    # sec -> list of (captureMs, sizeBytes, nBatches)
    commit_data = defaultdict(list)     # sec -> list of (nReqs, commitMs)
    worker_data = defaultdict(list)     # sec -> list of (batchSize, queueRemaining)
    done_data = defaultdict(list)       # sec -> list of (drained, doneQRemaining)

    exec_re = re.compile(
        r'executing batch-of-batches \((\d+) batches, (\d+) reqs\) in parallel within ([\d.]+) ms')
    capture_re = re.compile(
        r'capture thread: stateDiff in ([\d.]+) ms, size=(\d+) bytes, covering (\d+)')
    commit_re = re.compile(
        r'capture thread: (\d+) requests committed in (\d+) ms')
    worker_re = re.compile(
        r'worker pickup: batchSize=(\d+) queueRemaining=(\d+)')
    done_re = re.compile(
        r'capture thread: drained=(\d+) doneQueueRemaining=(\d+)')

    node_min_ts = None
    with open(node_path) as f:
        for line in f:
            ts = parse_timestamp(line)
            if ts is None:
                continue
            if node_min_ts is None:
                node_min_ts = ts
            # Use the same min_ts as screen log for alignment
            if min_ts:
                sec = int((ts - min_ts).total_seconds())
            else:
                sec = int((ts - node_min_ts).total_seconds())

            m = exec_re.search(line)
            if m:
                exec_data[sec].append({
                    'nBatches': int(m.group(1)),
                    'nReqs': int(m.group(2)),
                    'execMs': float(m.group(3)),
                })
                continue

            m = capture_re.search(line)
            if m:
                capture_data[sec].append({
                    'captureMs': float(m.group(1)),
                    'sizeBytes': int(m.group(2)),
                    'nBatches': int(m.group(3)),
                })
                continue

            m = commit_re.search(line)
            if m:
                commit_data[sec].append({
                    'nReqs': int(m.group(1)),
                    'commitMs': int(m.group(2)),
                })
                continue

            m = worker_re.search(line)
            if m:
                worker_data[sec].append({
                    'batchSize': int(m.group(1)),
                    'queueRemaining': int(m.group(2)),
                })
                continue

            m = done_re.search(line)
            if m:
                done_data[sec].append({
                    'drained': int(m.group(1)),
                    'doneQRemaining': int(m.group(2)),
                })

    # ── Determine time range ──
    all_secs = sorted(set(
        list(dispatch_data.keys()) + list(exec_data.keys()) + list(callback_data.keys())
    ))
    if not all_secs:
        print("ERROR: No data found.")
        sys.exit(1)
    max_sec = all_secs[-1]

    # ── Print combined per-second timeline ──
    print("=" * 160)
    print("FULL PIPELINE TIMELINE (per second)")
    print("=" * 160)
    print(f"{'sec':>4}  {'dispRq':>6}  {'callRq':>6}  "
          f"{'batchSz':>7}  {'maxOldW':>7}  {'maxBQD':>6}  "
          f"{'maxPipe':>7}  {'maxTotal':>8}  {'maxCQD':>6}  "
          f"{'execCnt':>7}  {'avgExMs':>7}  {'maxExMs':>7}  "
          f"{'capCnt':>6}  {'avgCapMs':>8}  {'avgCapSz':>8}  "
          f"{'comCnt':>6}  {'avgComMs':>8}  "
          f"{'pbmQR':>5}  {'doneQR':>6}")
    print("-" * 160)

    for sec in range(0, max_sec + 1):
        d = dispatch_data.get(sec, [])
        cb = callback_data.get(sec, [])
        ex = exec_data.get(sec, [])
        cap = capture_data.get(sec, [])
        com = commit_data.get(sec, [])
        w = worker_data.get(sec, [])
        dn = done_data.get(sec, [])

        if not d and not cb and not ex and not cap and not com:
            continue

        disp_reqs = sum(e['size'] for e in d)
        call_reqs = sum(e['size'] for e in cb)
        avg_batch_sz = disp_reqs / len(d) if d else 0
        max_old_wait = max((e['oldestWaitMs'] for e in d), default=0)
        max_bqd = max((e['batchingQD'] for e in d), default=0)
        max_pipe = max((e['pipelineMs'] for e in cb), default=0)
        max_total = max((e['oldestTotalMs'] for e in cb), default=0)
        max_cqd = max((e['completionQD'] for e in cb), default=0)

        exec_count = len(ex)
        avg_exec_ms = sum(e['execMs'] for e in ex) / len(ex) if ex else 0
        max_exec_ms = max((e['execMs'] for e in ex), default=0)

        cap_count = len(cap)
        avg_cap_ms = sum(e['captureMs'] for e in cap) / len(cap) if cap else 0
        avg_cap_sz = sum(e['sizeBytes'] for e in cap) / len(cap) if cap else 0

        com_count = len(com)
        avg_com_ms = sum(e['commitMs'] for e in com) / len(com) if com else 0

        max_pbm_qr = max((e['queueRemaining'] for e in w), default=0)
        max_done_qr = max((e['doneQRemaining'] for e in dn), default=0)

        print(f"{sec:>4}  {disp_reqs:>6}  {call_reqs:>6}  "
              f"{avg_batch_sz:>7.1f}  {max_old_wait:>7}  {max_bqd:>6}  "
              f"{max_pipe:>7}  {max_total:>8}  {max_cqd:>6}  "
              f"{exec_count:>7}  {avg_exec_ms:>7.1f}  {max_exec_ms:>7.1f}  "
              f"{cap_count:>6}  {avg_cap_ms:>8.2f}  {avg_cap_sz:>8.0f}  "
              f"{com_count:>6}  {avg_com_ms:>8.1f}  "
              f"{max_pbm_qr:>5}  {max_done_qr:>6}")

    # ── Summary: pre-transition vs post-transition ──
    # Find the transition point where batch size jumps
    transition_sec = None
    for sec in sorted(dispatch_data.keys()):
        entries = dispatch_data[sec]
        total_reqs = sum(e['size'] for e in entries)
        avg_bs = total_reqs / len(entries) if entries else 0
        if avg_bs > 5 and total_reqs > 100:
            transition_sec = sec
            break

    if transition_sec:
        print(f"\n{'='*80}")
        print(f"PHASE COMPARISON (transition at sec {transition_sec})")
        print(f"{'='*80}")

        for phase_name, sec_range in [
            ("STABLE", range(max(0, transition_sec - 30), transition_sec)),
            ("DEGRADED", range(transition_sec, max_sec + 1)),
        ]:
            phase_dispatch = [e for s in sec_range for e in dispatch_data.get(s, [])]
            phase_callback = [e for s in sec_range for e in callback_data.get(s, [])]
            phase_exec = [e for s in sec_range for e in exec_data.get(s, [])]
            phase_capture = [e for s in sec_range for e in capture_data.get(s, [])]
            phase_commit = [e for s in sec_range for e in commit_data.get(s, [])]

            if not phase_dispatch:
                continue

            total_reqs = sum(e['size'] for e in phase_dispatch)
            n_dispatches = len(phase_dispatch)
            duration = len([s for s in sec_range if dispatch_data.get(s)])

            print(f"\n  {phase_name} phase ({duration}s):")
            print(f"    Total requests dispatched: {total_reqs}")
            print(f"    Throughput: {total_reqs/duration:.0f} reqs/sec")
            print(f"    Dispatches/sec: {n_dispatches/duration:.1f}")
            print(f"    Avg batch size: {total_reqs/n_dispatches:.1f}")
            print(f"    Max oldest wait (Q1+Q2): {max(e['oldestWaitMs'] for e in phase_dispatch)} ms")
            print(f"    Max batchingQueueDepth: {max(e['batchingQD'] for e in phase_dispatch)}")

            if phase_callback:
                pipes = [e['pipelineMs'] for e in phase_callback]
                totals = [e['oldestTotalMs'] for e in phase_callback]
                print(f"    Pipeline time (dispatch→callback): "
                      f"avg={sum(pipes)/len(pipes):.1f} max={max(pipes)} ms")
                print(f"    Total time (submit→callback): "
                      f"avg={sum(totals)/len(totals):.1f} max={max(totals)} ms")

            if phase_exec:
                exec_ms_list = [e['execMs'] for e in phase_exec]
                total_exec_reqs = sum(e['nReqs'] for e in phase_exec)
                print(f"    HTTP execute: "
                      f"avg={sum(exec_ms_list)/len(exec_ms_list):.1f} "
                      f"max={max(exec_ms_list):.1f} ms, "
                      f"total reqs={total_exec_reqs}")

            if phase_capture:
                cap_ms_list = [e['captureMs'] for e in phase_capture]
                cap_sz_list = [e['sizeBytes'] for e in phase_capture]
                print(f"    StateDiff capture: "
                      f"avg={sum(cap_ms_list)/len(cap_ms_list):.2f} "
                      f"max={max(cap_ms_list):.2f} ms, "
                      f"avg size={sum(cap_sz_list)/len(cap_sz_list):.0f} bytes")

            if phase_commit:
                com_ms_list = [e['commitMs'] for e in phase_commit]
                print(f"    Paxos commit: "
                      f"avg={sum(com_ms_list)/len(com_ms_list):.1f} "
                      f"max={max(com_ms_list)} ms")

    # ── Key finding: where does pipeline time come from? ──
    print(f"\n{'='*80}")
    print("PIPELINE BREAKDOWN DURING DEGRADED PHASE")
    print("(pipeline = HTTP execute + capture + Paxos commit)")
    print(f"{'='*80}")
    if transition_sec:
        for sec in range(transition_sec, min(transition_sec + 10, max_sec + 1)):
            ex = exec_data.get(sec, [])
            cap = capture_data.get(sec, [])
            com = commit_data.get(sec, [])
            cb = callback_data.get(sec, [])

            if not ex:
                continue

            avg_exec = sum(e['execMs'] for e in ex) / len(ex)
            max_exec = max(e['execMs'] for e in ex)
            avg_cap = sum(e['captureMs'] for e in cap) / len(cap) if cap else 0
            avg_com = sum(e['commitMs'] for e in com) / len(com) if com else 0
            avg_pipe = sum(e['pipelineMs'] for e in cb) / len(cb) if cb else 0
            unaccounted = avg_pipe - avg_exec - avg_cap - avg_com

            print(f"  sec {sec}: pipeline={avg_pipe:.0f}ms = "
                  f"exec={avg_exec:.1f}ms + capture={avg_cap:.1f}ms + "
                  f"commit={avg_com:.0f}ms + unaccounted={unaccounted:.0f}ms  "
                  f"(maxExec={max_exec:.0f}ms, nExec={len(ex)})")


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--screen', required=True)
    p.add_argument('--node', required=True)
    args = p.parse_args()
    analyze(args.screen, args.node)
