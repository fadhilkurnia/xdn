"""
investigate_parse_paxos_proposal_size.py — Parse PaxosProposalSize log lines from XDN screen logs.

Reports the distribution of proposal sizes (reqValLen and totalEstimate) for
Active Replication vs Primary-Backup comparisons.

Usage:
    python3 investigate_parse_paxos_proposal_size.py --log <screen.log> [--group <name>]

Log line format (emitted by PaxosInstanceStateMachine):
    PaxosProposalSize - group=<name> slot=<N> proposals=<M> reqValLen=<L> totalEstimate=<T>
"""

import argparse
import re
import sys
from pathlib import Path

RE_PROPOSAL_SIZE = re.compile(
    r"PaxosProposalSize - group=(\S+) slot=(\d+) proposals=(\d+) reqValLen=(\d+) totalEstimate=(\d+)"
)


def parse_log(log_path, group_filter=None):
    entries = []
    with open(log_path, errors="replace") as f:
        for line in f:
            m = RE_PROPOSAL_SIZE.search(line)
            if m:
                group = m.group(1)
                if group_filter and group != group_filter:
                    continue
                entries.append({
                    "group": group,
                    "slot": int(m.group(2)),
                    "proposals": int(m.group(3)),
                    "reqValLen": int(m.group(4)),
                    "totalEstimate": int(m.group(5)),
                })
    return entries


def print_stats(entries, label=""):
    if not entries:
        print(f"  No PaxosProposalSize entries found{' for ' + label if label else ''}.")
        return

    req_vals = [e["reqValLen"] for e in entries]
    totals = [e["totalEstimate"] for e in entries]

    req_vals.sort()
    totals.sort()

    n = len(req_vals)

    def percentile(data, p):
        idx = int(p / 100 * len(data))
        idx = min(idx, len(data) - 1)
        return data[idx]

    header = f"Proposal size distribution"
    if label:
        header += f" ({label})"
    print(f"\n{header}:")
    print(f"  Entries: {n}")
    print()
    print(f"  {'metric':>20}  {'min':>10}  {'p25':>10}  {'p50':>10}  {'p75':>10}  {'p90':>10}  {'p99':>10}  {'max':>10}  {'avg':>10}")
    print("  " + "-" * 100)

    for name, data in [("reqValLen (chars)", req_vals), ("totalEstimate (bytes)", totals)]:
        mn = min(data)
        mx = max(data)
        avg = sum(data) / len(data)
        p25 = percentile(data, 25)
        p50 = percentile(data, 50)
        p75 = percentile(data, 75)
        p90 = percentile(data, 90)
        p99 = percentile(data, 99)
        print(f"  {name:>20}  {mn:>10,}  {p25:>10,}  {p50:>10,}  {p75:>10,}  {p90:>10,}  {p99:>10,}  {mx:>10,}  {avg:>10,.1f}")

    # Bucket distribution for reqValLen
    print(f"\n  reqValLen bucket distribution:")
    buckets = [
        (0, 100, "0-100B"),
        (100, 500, "100-500B"),
        (500, 1000, "500B-1KB"),
        (1000, 5000, "1-5KB"),
        (5000, 10000, "5-10KB"),
        (10000, 50000, "10-50KB"),
        (50000, 100000, "50-100KB"),
        (100000, 500000, "100-500KB"),
        (500000, 1000000, "500KB-1MB"),
        (1000000, float("inf"), "1MB+"),
    ]
    print(f"  {'bucket':>15}  {'count':>8}  {'pct':>7}")
    print("  " + "-" * 35)
    for lo, hi, label_b in buckets:
        count = sum(1 for v in req_vals if lo <= v < hi)
        if count > 0:
            pct = 100.0 * count / n
            print(f"  {label_b:>15}  {count:>8}  {pct:>6.1f}%")
    print()


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--log", type=Path, required=True, help="Path to XDN screen log")
    p.add_argument("--group", type=str, default=None, help="Filter by paxos group name")
    args = p.parse_args()

    if not args.log.exists():
        print(f"ERROR: log file not found: {args.log}")
        sys.exit(1)

    entries = parse_log(args.log, args.group)
    print(f"Parsed {len(entries)} PaxosProposalSize entries from {args.log}")

    if args.group:
        print_stats(entries, label=args.group)
    else:
        groups = sorted(set(e["group"] for e in entries))
        for group in groups:
            group_entries = [e for e in entries if e["group"] == group]
            print_stats(group_entries, label=group)


if __name__ == "__main__":
    main()
