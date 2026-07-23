#!/usr/bin/env python3
"""
replay_checkpoints.py

Replays fuselog diff/ files (p<pepoch>:<app_key>:<count>.diff), starting from
an empty directory (or an optional --snp-base) -- exactly matching what
snpDiffApplyThread does in PrimaryBackupManager (fuselog-apply <target_dir>
--statediff=<diff_file>, one diff at a time, no batching, no skipping).

Since there's no baseline to fall back on by default, the diff sequence is
required to be complete and gap-free starting at count=0 -- validated
automatically; the script errors out (rather than silently replaying a
partial sequence) if count=0 is missing or a gap exists.

At each requested "checkpoint" count, the current state of the working
directory is snapshotted into its own directory. A docker-compose.yml is
then generated with one service per checkpoint -- ONLY the app's stateful
component (no network, no entry/app container needed), reusing the image
and environment straight from the app's XDN descriptor, bind-mounted to
each checkpoint's snapshot. Bring up checkpoints one at a time and inspect
manually.

Replay ORDER -- count-order (default) vs capture-time-order:
  Every diff's <count> is a ticket number assigned when a write is *submitted*
  (see AtomicCounter in capture_bench_concurrent.py) -- under
  --concurrency > 1, tickets are handed out before the write/capture race
  resolves, so ticket order is NOT necessarily the order fuselog actually
  harvested/captured each diff in. Replaying strictly by count (the default,
  and what production's snpDiffApplyThread does) assumes ticket order and
  true capture order always agree. --timeline-csv lets you test that
  assumption directly: point it at a capture_bench_concurrent.py
  results/timeline.csv and diffs are re-sorted by each row's
  capture_start_s (the moment a thread actually acquired the capture lock
  and began harvesting) instead of by count, before being applied in that
  order. Comparing a count-order replay against a capture-time-order replay
  of the *same* diff/ directory is how you check whether ticket-order
  replay is silently masking a corruption that only shows up when diffs are
  applied in the order fuselog actually produced them.

Usage:
  # Basic: sequential capture_bench.py output, default count-order replay.
  python3 replay_checkpoints.py \
      --app bookcatalog-nd-mysql \
      --cmtdiff-dir runs/run1/diff \
      --checkpoints 577,980,1200,1308,1399 \
      --out-dir replays/run1

  # Concurrent capture_bench_concurrent.py output, still count-order
  # (the default -- matches what production actually does today).
  python3 replay_checkpoints.py \
      --app bookcatalog-nd-mysql \
      --cmtdiff-dir runs/concurrent_run1/diff \
      --checkpoints 500,1000,1500,2000 \
      --out-dir replays/concurrent_run1_count_order

  # Same diff/ directory, but replayed in TRUE capture order instead, via
  # the timeline.csv that capture_bench_concurrent.py wrote alongside it.
  # Compare this run's checkpoints against the count-order run above --
  # a difference in which checkpoints boot cleanly (or where fuselog-apply
  # itself fails) means count-order replay was hiding an ordering-dependent
  # corruption that only true capture order exposes.
  python3 replay_checkpoints.py \
      --app bookcatalog-nd-mysql \
      --cmtdiff-dir runs/concurrent_run1/diff \
      --timeline-csv runs/concurrent_run1/results/timeline.csv \
      --checkpoints 500,1000,1500,2000 \
      --out-dir replays/concurrent_run1_capture_order

  # Real XDN diffs (not from this bench suite) -- <primary> is a node ID
  # like "west1b", not an app key, so the default app-key filter would
  # reject every file. --skip-app-filter accepts any file matching the
  # p<epoch>:<primary>:<count>.diff pattern regardless of <primary>.
  python3 replay_checkpoints.py \
      --app bookcatalog-nd-mysql \
      --cmtdiff-dir /tmp/xdn2/state/east1a/bookcatalog-nd/e0/cmtDiff \
      --skip-app-filter \
      --checkpoints 577,980,1200 \
      --out-dir replays/xdn_prod_repro

Assumptions (flagging explicitly):
  - fuselog-apply is invoked as: fuselog-apply <target_dir> --statediff=<diff_file>
  - "checkpoint N" means "state after diff N has been applied" (matching
    snpDiffCount = nextCount being set *after* a successful apply).
  - If multiple files share the same <count>, this script warns and picks one
    arbitrarily (same non-determinism as the original Java glob-scan).
  - --timeline-csv assumes the capture_bench_concurrent.py schema: a `count`
    column matching diff filenames' <count>, and a `capture_start_s` column.
    count=0 (the pre-workload bootstrap diff, captured before any writer
    thread starts) never appears as a row in timeline.csv -- it's exempted
    from the mapping check and always sorted first, since every other diff
    is a delta on top of it regardless of replay order.
"""

import argparse
import csv
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

import bench_common as bc


def discover_diffs(cmtdiff_dir: Path, app_key: str = None):
    """Returns a list of (count, path) sorted ascending by count. If app_key
    is given, filters to diffs whose <primary> field matches it (the
    convention capture_bench_concurrent.py/capture_bench.py use). If app_key
    is None, accepts every file matching the count pattern regardless of the
    <primary> field -- use this for real XDN diffs, where <primary> is a
    node ID rather than an app key. Warns (does not fail) on duplicate
    counts."""
    by_count = {}
    for entry in sorted(cmtdiff_dir.iterdir()):
        if not entry.is_file():
            continue
        m = bc.DIFF_NAME_RE.match(entry.name)
        if not m:
            print(f"[warn] skipping unrecognized file in diff/: {entry.name}", file=sys.stderr)
            continue
        if app_key is not None and m.group("primary") != app_key:
            continue
        count = int(m.group("count"))
        if app_key is not None and m.group("primary") != app_key:
            continue
        count = int(m.group("count"))
        if count in by_count:
            print(f"[warn] duplicate diff for count={count}: "
                f"{by_count[count].name} vs {entry.name} -- keeping the first, "
                f"matching the non-deterministic glob-scan behavior of the "
                f"original Java code. Check whether this indicates a stale "
                f"epoch leftover.", file=sys.stderr)
            continue
        by_count[count] = entry
    return sorted(by_count.items(), key=lambda kv: kv[0])


def load_capture_order(timeline_csv: Path):
    """Returns a dict mapping count -> capture_start_s, read from a
    capture_bench_concurrent.py timeline.csv. capture_start_s is recorded
    right after a thread acquires capture_lock, so it reflects the true
    serialized harvest order (no two rows can overlap), unlike the diff's
    filename count which is assigned as a ticket before the write/capture
    race resolves."""
    order = {}
    with open(timeline_csv, newline="") as f:
        for row in csv.DictReader(f):
            order[int(row["count"])] = float(row["capture_start_s"])
    return order


def validate_complete_sequence(diffs):
    """Diffs are assumed complete and gap-free starting at count=0, since
    there's no separate snapshot baseline to fall back on. Exits if that
    assumption doesn't hold, rather than silently replaying a partial or
    gapped sequence."""
    counts = [c for c, _ in diffs]
    if counts[0] != 0:
        print(f"[fatal] lowest diff count is {counts[0]}, not 0 -- there is "
              f"missing state before this point (no --snp-base baseline was "
              f"provided to fall back on, so replay must start at count=0). "
              f"Cannot proceed.", file=sys.stderr)
        sys.exit(1)
    gaps = []
    for prev, nxt in zip(counts, counts[1:]):
        if nxt != prev + 1:
            gaps.append((prev, nxt))
    if gaps:
        print(f"[fatal] missing state: diff/ has gaps in the count "
              f"sequence -- {len(gaps)} gap(s) found:", file=sys.stderr)
        for prev, nxt in gaps:
            print(f"        missing count(s) {prev + 1}..{nxt - 1} "
                  f"(have {prev}, then jump to {nxt})", file=sys.stderr)
        print("        cannot replay a gapped sequence from an empty "
              "directory. Cannot proceed.", file=sys.stderr)
        sys.exit(1)


def format_target_dir(target_dir: Path) -> str:
    """fuselog-apply requires target-dir to be an absolute path ending in '/'."""
    abs_path = str(target_dir.resolve())
    if not abs_path.endswith("/"):
        abs_path += "/"
    return abs_path


def run_fuselog_apply(fuselog_apply_bin, target_dir: Path, diff_file: Path):
    """Returns (success: bool, elapsed_ms: float). Timing wraps only the
    subprocess call itself, not file I/O around it."""
    formatted_target = format_target_dir(target_dir)
    cmd = [fuselog_apply_bin, formatted_target, f"--statediff={diff_file}"]
    t0 = time.perf_counter()
    result = subprocess.run(cmd, capture_output=True, text=True)
    elapsed_ms = (time.perf_counter() - t0) * 1000
    if result.returncode != 0:
        print(f"[error] fuselog-apply failed on {diff_file.name} "
              f"(exit={result.returncode})", file=sys.stderr)
        print(f"        cmd: {' '.join(cmd)}", file=sys.stderr)
        if result.stdout.strip():
            print(f"        stdout: {result.stdout.strip()}", file=sys.stderr)
        if result.stderr.strip():
            print(f"        stderr: {result.stderr.strip()}", file=sys.stderr)
        return False, elapsed_ms
    return True, elapsed_ms


def snapshot_checkpoint(work_dir: Path, out_dir: Path, count: int):
    dest = out_dir / f"checkpoint_{count:07d}"
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(work_dir, dest)
    print(f"[checkpoint] snapshotted state after count={count} -> {dest}")
    return dest


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--app", required=True, help="App key in bench_endpoints.yaml")
    ap.add_argument("--endpoints-config", type=Path,
                    default=Path(__file__).resolve().parent / "bench_endpoints.yaml")
    ap.add_argument("--snp-base", required=False, type=Path, default=None,
                    help="Optional baseline directory to start replay from. "
                         "If omitted, replay starts from an empty directory, which "
                         "requires diff/ to be complete starting at count=0 "
                         "(validated automatically).")
    ap.add_argument("--cmtdiff-dir", required=True, type=Path,
                    help="diff/ directory containing p<pe>:<app_key>:<count>.diff files")
    ap.add_argument("--checkpoints", required=True,
                    help="Comma-separated list of counts to snapshot at")
    ap.add_argument("--out-dir", required=True, type=Path,
                    help="Output directory for checkpoint snapshots + compose file")
    ap.add_argument("--fuselog-apply-bin", default="fuselog-apply",
                    help="Path to the fuselog-apply binary (default: on PATH)")
    ap.add_argument("--stop-after-last-checkpoint", action="store_true",
                    help="Stop replaying once the highest requested checkpoint is reached, "
                         "instead of continuing through all remaining diffs")
    ap.add_argument("--timeline-csv", required=False, type=Path, default=None,
                    help="Optional path to a capture_bench_concurrent.py timeline.csv. "
                         "If given, diffs are replayed in actual capture-time order "
                         "(by capture_start_s) instead of by count -- use this to test "
                         "whether count-order replay (the default, matching production's "
                         "snpDiffApplyThread) differs from the true chronological capture "
                         "order under concurrent writes. If omitted, behavior is unchanged "
                         "from count-order replay.")
    ap.add_argument("--skip-app-filter", action="store_true",
                    help="Don't filter diff/ by app key -- accept any file matching "
                         "p<epoch>:<primary>:<count>.diff regardless of the <primary> "
                         "field. Use this for real XDN diffs, where <primary> is the "
                         "primary node's ID (e.g. 'west1b'), not the app key -- the "
                         "default app-key filter is only meaningful for diffs produced "
                         "by capture_bench_concurrent.py/capture_bench.py.")
    args = ap.parse_args()

    entry, descriptor = bc.load_app_config(args.endpoints_config, args.app)
    base_port = entry.get("replay_base_port", 33061)

    checkpoint_counts = sorted(int(c.strip()) for c in args.checkpoints.split(","))
    if not checkpoint_counts:
        print("[error] no checkpoints specified", file=sys.stderr)
        sys.exit(1)

    if args.snp_base is not None and not args.snp_base.is_dir():
        print(f"[error] --snp-base is not a directory: {args.snp_base}", file=sys.stderr)
        sys.exit(1)
    if not args.cmtdiff_dir.is_dir():
        print(f"[error] --cmtdiff-dir is not a directory: {args.cmtdiff_dir}", file=sys.stderr)
        sys.exit(1)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    work_dir = args.out_dir / "_work_state"
    if work_dir.exists():
        shutil.rmtree(work_dir)

    diffs = discover_diffs(args.cmtdiff_dir, None if args.skip_app_filter else args.app)
    if not diffs:
        print(f"[error] no diffs found in --cmtdiff-dir for app={args.app}", file=sys.stderr)
        sys.exit(1)
    print(f"[init] found {len(diffs)} diffs, count range {diffs[0][0]}..{diffs[-1][0]}")

    if args.snp_base is not None:
        shutil.copytree(args.snp_base, work_dir)
        print(f"[init] copied baseline -> {work_dir}")
    else:
        validate_complete_sequence(diffs)
        work_dir.mkdir(parents=True)
        print(f"[init] no --snp-base given -- starting from an empty directory "
              f"({work_dir}), verified diff/ is complete and gap-free from count=0")

    # Default: replay in count order (ticket order), exactly as production's
    # snpDiffApplyThread does. If --timeline-csv is given, re-sort into true
    # capture order instead -- this only changes the order the apply loop
    # below walks through; `diffs` itself (and the count-order completeness
    # check above, which always validates ticket order regardless) is
    # untouched. See the module docstring's "Replay ORDER" section for why
    # these two orders can differ under concurrent writers.
    if args.timeline_csv is not None:
        capture_order = load_capture_order(args.timeline_csv)
        # count=0 is the initial bootstrap diff, captured before the writer
        # threads (and thus the counter) start -- it never gets a timeline.csv
        # row. It must always sort first regardless, since every other diff
        # is a delta on top of it -- so it's exempt from the missing-mapping
        # check, and given an explicit sort key instead of a dict lookup.
        missing_ts = [count for count, _ in diffs
                      if count not in capture_order and count != 0]
        if missing_ts:
            print(f"[fatal] --timeline-csv is missing capture_start_s for "
                  f"{len(missing_ts)} count(s) present in --cmtdiff-dir: "
                  f"{sorted(missing_ts)[:10]}{'...' if len(missing_ts) > 10 else ''}. "
                  f"Refusing to replay with a partial ordering mapping.",
                  file=sys.stderr)
            sys.exit(1)
        replay_order = sorted(
            diffs,
            key=lambda kv: capture_order[kv[0]] if kv[0] != 0 else float("-inf"))
        print(f"[init] replay order: capture-time-order "
              f"(via --timeline-csv={args.timeline_csv}, "
              f"{len(capture_order)} capture timestamps loaded, "
              f"count=0 pinned first as bootstrap diff)")
    else:
        replay_order = diffs
        print(f"[init] replay order: count-order (default)")

    checkpoint_set = set(checkpoint_counts)
    last_checkpoint = max(checkpoint_counts)
    checkpoint_dirs = []
    apply_latencies_ms = []

    for count, diff_file in replay_order:
        ok, elapsed_ms = run_fuselog_apply(args.fuselog_apply_bin, work_dir, diff_file)
        apply_latencies_ms.append(elapsed_ms)
        if not ok:
            print(f"[fatal] stopping replay: fuselog-apply itself failed on "
                  f"count={count}. This may BE the corruption point -- worth "
                  f"noting this count separately from a container-boot crash.",
                  file=sys.stderr)
            break

        if count in checkpoint_set:
            dest = snapshot_checkpoint(work_dir, args.out_dir, count)
            checkpoint_dirs.append((count, dest))

        if args.stop_after_last_checkpoint and count >= last_checkpoint:
            break

    missing = checkpoint_set - {c for c, _ in checkpoint_dirs}
    if missing:
        print(f"[warn] requested checkpoints never reached (no diff at that "
              f"exact count, or replay stopped early): {sorted(missing)}",
              file=sys.stderr)

    apply_stats = bc.percentiles(apply_latencies_ms)
    print(f"\n[apply-latency] {apply_stats['count']} fuselog-apply calls -- "
          f"p50={apply_stats['p50']:.2f}ms p90={apply_stats['p90']:.2f}ms "
          f"p95={apply_stats['p95']:.2f}ms p99={apply_stats['p99']:.2f}ms "
          f"avg={apply_stats['avg']:.2f}ms")
    apply_latency_path = args.out_dir / "apply_latency_summary.json"
    apply_latency_path.write_text(json.dumps({
        "app": args.app,
        "cmtdiff_dir": str(args.cmtdiff_dir),
        "apply_latency_ms": apply_stats,
    }, indent=2))
    print(f"[apply-latency] written to {apply_latency_path}")

    if not checkpoint_dirs:
        print("[error] no checkpoints were captured, nothing to write a "
              "compose file for", file=sys.stderr)
        sys.exit(1)

    compose_path = args.out_dir / "docker-compose.yml"
    container_port = bc.generate_replay_compose(
        descriptor, args.app, checkpoint_dirs, base_port, compose_path
    )
    print(f"[compose] wrote {compose_path}")

    print()
    for count, _ in checkpoint_dirs:
        print(f"docker compose -f {compose_path} up checkpoint-{count}")

    print()
    container_names = " ".join(
        f"repro-{args.app}-checkpoint-{count}" for count, _ in checkpoint_dirs
    )
    print(f"docker rm -f {container_names}")


if __name__ == "__main__":
    main()
