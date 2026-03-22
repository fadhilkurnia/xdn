"""
investigate_parse_paxos_batch_size.py — Parse PaxosSlotBatch log lines from an XDN screen log.

Reports the distribution of proposals-per-slot (Paxos-level batch size).
Works for both Active Replication and Primary-Backup services, since both
go through PaxosInstanceStateMachine.extractExecuteAndCheckpoint().

Usage:
    python3 investigate_parse_paxos_batch_size.py --log <screen.log> [--group <paxos-group-name>]

Log line format (emitted at INFO by PaxosInstanceStateMachine):
    PaxosSlotBatch - group=<name> slot=<N> proposals=<M>
"""

import argparse
import re
import sys
from collections import Counter
from pathlib import Path

RE_PAXOS_BATCH = re.compile(
    r"PaxosSlotBatch - group=(\S+) slot=(\d+) proposals=(\d+)"
)


def parse_log(log_path, group_filter=None):
    """Parse PaxosSlotBatch lines, return list of (group, slot, proposals)."""
    entries = []
    with open(log_path, errors="replace") as f:
        for line in f:
            m = RE_PAXOS_BATCH.search(line)
            if m:
                group = m.group(1)
                slot = int(m.group(2))
                proposals = int(m.group(3))
                if group_filter and group != group_filter:
                    continue
                entries.append((group, slot, proposals))
    return entries


def print_distribution(entries, label=""):
    """Print batch size distribution table."""
    if not entries:
        print(f"  No PaxosSlotBatch entries found{' for ' + label if label else ''}.")
        return

    proposals_list = [p for _, _, p in entries]
    counts = Counter(proposals_list)
    total_slots = len(proposals_list)
    total_proposals = sum(proposals_list)
    avg_batch = total_proposals / total_slots if total_slots > 0 else 0

    header = f"Paxos batch-size distribution"
    if label:
        header += f" ({label})"
    print(f"\n{header}:")
    print(f"  Total slots: {total_slots}, total proposals: {total_proposals}, "
          f"avg proposals/slot: {avg_batch:.2f}")
    print(f"  {'proposals':>10}  {'slots':>8}  {'pct':>7}")
    print("  " + "-" * 30)
    for n in sorted(counts):
        pct = 100.0 * counts[n] / total_slots
        print(f"  {n:>10}  {counts[n]:>8}  {pct:>6.1f}%")
    print()


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--log", type=Path, required=True, help="Path to XDN screen log")
    p.add_argument("--group", type=str, default=None,
                   help="Filter by paxos group name (e.g., 'svc')")
    args = p.parse_args()

    if not args.log.exists():
        print(f"ERROR: log file not found: {args.log}")
        sys.exit(1)

    entries = parse_log(args.log, args.group)
    print(f"Parsed {len(entries)} PaxosSlotBatch entries from {args.log}")

    if args.group:
        print_distribution(entries, label=args.group)
    else:
        # Print per-group distributions
        groups = sorted(set(g for g, _, _ in entries))
        for group in groups:
            group_entries = [(g, s, p) for g, s, p in entries if g == group]
            print_distribution(group_entries, label=group)

        # Print overall
        if len(groups) > 1:
            print_distribution(entries, label="ALL GROUPS")


if __name__ == "__main__":
    main()
