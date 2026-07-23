#!/usr/bin/env python3
"""
verify_inversion.py

Checks a capture_bench_concurrent.py results/timeline.csv for count/capture
inversions: sorts rows by <count> (the ticket number assigned when a write
is submitted -- see AtomicCounter in capture_bench_concurrent.py) and walks
them in that order, flagging any row whose capture_start_s is EARLIER than
a preceding row's -- i.e. a case where ticket order and true capture order
disagree. This is exactly the condition replay_checkpoints.py's
--timeline-csv option exists to route around (see its "Replay ORDER"
docstring section): if inversions exist here, a plain count-order replay of
the same diff/ directory is applying diffs in a different order than
fuselog actually produced them.

Usage:
  python3 verify_inversion.py --timeline-csv runs/case3.t5.2/results/timeline.csv

  # Check several runs in one go
  python3 verify_inversion.py --timeline-csv runs/case3.t5.2/results/timeline.csv \\
      runs/case3.t5.4/results/timeline.csv runs/case3.t5.8/results/timeline.csv

Exit code is non-zero if any file has at least one inversion, so this can be
used as a pass/fail gate in a larger script (e.g. run capture_bench_concurrent.py,
then verify_inversion.py, then only bother with replay_checkpoints.py's
--timeline-csv comparison if inversions were actually found).
"""

import argparse
import csv
import sys
from pathlib import Path


def check_file(timeline_csv: Path):
    """Returns (inversions, total_rows) for one timeline.csv."""
    with open(timeline_csv, newline="") as f:
        rows = sorted(csv.DictReader(f), key=lambda r: int(r["count"]))

    prev_capture_start = -1
    inversions = 0
    for r in rows:
        cs = float(r["capture_start_s"])
        if cs < prev_capture_start:
            inversions += 1
            print(f"  INVERSION: count={r['count']} capture_start={cs} "
                  f"< previous count's capture_start={prev_capture_start}")
        prev_capture_start = cs

    return inversions, len(rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--timeline-csv", required=True, type=Path, nargs="+",
                     help="One or more capture_bench_concurrent.py results/timeline.csv "
                          "files to check")
    args = ap.parse_args()

    total_inversions = 0
    for path in args.timeline_csv:
        if not path.is_file():
            print(f"[error] not a file: {path}", file=sys.stderr)
            sys.exit(1)

        if len(args.timeline_csv) > 1:
            print(f"=== {path} ===")
        inversions, total_rows = check_file(path)
        total_inversions += inversions
        suffix = f"  ({path})" if len(args.timeline_csv) > 1 else ""
        print(f"total inversions: {inversions} / {total_rows}{suffix}")

    if len(args.timeline_csv) > 1:
        print(f"\ngrand total inversions across {len(args.timeline_csv)} files: "
              f"{total_inversions}")

    if total_inversions > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
