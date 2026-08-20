#!/usr/bin/env python3
"""Compare interleaved A/B results from eval/nightly_perf.py and detect regressions.

Reads results/{base,head}_r*.json produced by the nightly-perf workflow, takes
per-metric medians across rounds, and compares head against base RELATIVELY --
the runs were interleaved on the same runner in the same job, so the ratio is
meaningful even though absolute numbers from shared runners are not.

Thresholds (head vs base): low-load p50 +15%, low-load p99 +25%, max tput -15%.
A metric missing on either side (e.g. a service failed to measure) is reported
but never flagged as a regression on its own.

Outputs: a markdown summary (--summary), a single-line JSON record (--record)
to append to the perf-results branch, and 'regression=true|false' appended to
--github-output when given. Exit code is always 0; the workflow decides how to
react so the record is pushed before the job is failed.
"""

import argparse
import json
import statistics
from datetime import datetime, timezone
from pathlib import Path

P50_UP_LIMIT = 0.15
P99_UP_LIMIT = 0.25
TPUT_DOWN_LIMIT = 0.15


def load_side(results_dir: Path, side: str) -> list:
    return [json.loads(p.read_text()) for p in sorted(results_dir.glob(f"{side}_r*.json"))]


def median_metrics(rounds: list) -> dict:
    """Per-service medians across rounds: p50/p99 low-load latency + max tput."""
    services = {}
    for rnd in rounds:
        for name, svc in rnd.get("services", {}).items():
            slot = services.setdefault(name, {"p50": [], "p99": [], "tput": [], "errors": []})
            if "error" in svc:
                slot["errors"].append(svc["error"])
            if svc.get("instrumented") and "error" not in svc["instrumented"]:
                # per-stage request-flow breakdown (diagnostic, never gated)
                slot.setdefault("flow", svc["instrumented"])
            low = svc.get("low_load") or {}
            if low.get("p50_ms") is not None:
                slot["p50"].append(low["p50_ms"])
            if low.get("p99_ms") is not None:
                slot["p99"].append(low["p99_ms"])
            tput = svc.get("max_tput") or {}
            if tput.get("achieved_rps") is not None:
                slot["tput"].append(tput["achieved_rps"])
    out = {}
    for name, slot in services.items():
        out[name] = {
            "p50_ms": round(statistics.median(slot["p50"]), 2) if slot["p50"] else None,
            "p99_ms": round(statistics.median(slot["p99"]), 2) if slot["p99"] else None,
            # max (not median): a round can only undershoot the true knee (noise
            # makes rungs fail early), so the best demonstrated rate is the
            # stable estimator across rounds.
            "max_tput_rps": round(max(slot["tput"]), 1) if slot["tput"] else None,
            "rounds": {"p50": len(slot["p50"]), "tput": len(slot["tput"])},
            "errors": slot["errors"],
            "flow": slot.get("flow"),
        }
    return out


def pct(new, old):
    if new is None or old is None or old == 0:
        return None
    return round((new - old) / old * 100, 1)


def fmt(value, unit=""):
    return f"{value}{unit}" if value is not None else "n/a"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", required=True)
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("--base-sha", default=None)
    parser.add_argument("--summary", required=True)
    parser.add_argument("--record", required=True)
    parser.add_argument("--github-output", default=None)
    args = parser.parse_args()

    results_dir = Path(args.results)
    head_rounds = load_side(results_dir, "head")
    base_rounds = load_side(results_dir, "base")
    if not head_rounds:
        raise SystemExit("no head_r*.json results found")
    head = median_metrics(head_rounds)
    base = median_metrics(base_rounds) if base_rounds else None

    regressions = []
    deltas = {}
    if base:
        for name, h in head.items():
            b = base.get(name, {})
            d = {
                "p50_pct": pct(h["p50_ms"], b.get("p50_ms")),
                "p99_pct": pct(h["p99_ms"], b.get("p99_ms")),
                "tput_pct": pct(h["max_tput_rps"], b.get("max_tput_rps")),
            }
            deltas[name] = d
            if d["p50_pct"] is not None and d["p50_pct"] > P50_UP_LIMIT * 100:
                regressions.append(
                    f"{name}: low-load p50 {b['p50_ms']}ms -> {h['p50_ms']}ms (+{d['p50_pct']}%)")
            if d["p99_pct"] is not None and d["p99_pct"] > P99_UP_LIMIT * 100:
                regressions.append(
                    f"{name}: low-load p99 {b['p99_ms']}ms -> {h['p99_ms']}ms (+{d['p99_pct']}%)")
            if d["tput_pct"] is not None and d["tput_pct"] < -TPUT_DOWN_LIMIT * 100:
                regressions.append(
                    f"{name}: max throughput {b['max_tput_rps']} -> {h['max_tput_rps']} rps"
                    f" ({d['tput_pct']}%)")

    record = {
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "head_sha": args.head_sha,
        "base_sha": args.base_sha,
        "head": head,
        "base": base,
        "delta_pct": deltas or None,
        "regression": bool(regressions),
        "regressions": regressions,
        "calibration": head_rounds[0].get("calibration"),
    }
    Path(args.record).write_text(json.dumps(record) + "\n")

    lines = ["## Nightly XDN performance", ""]
    lines.append(f"head `{args.head_sha[:10]}`"
                 + (f" vs base `{args.base_sha[:10]}` (interleaved A/B, same runner)"
                    if base else " (bootstrap run, no baseline yet)"))
    lines.append("")
    lines.append("| service | metric | base | head | delta |")
    lines.append("|---|---|---|---|---|")
    for name, h in head.items():
        b = base.get(name, {}) if base else {}
        d = deltas.get(name, {})
        lines.append(f"| {name} | low-load p50 | {fmt(b.get('p50_ms'), 'ms')} "
                     f"| {fmt(h['p50_ms'], 'ms')} | {fmt(d.get('p50_pct'), '%')} |")
        lines.append(f"| {name} | low-load p99 | {fmt(b.get('p99_ms'), 'ms')} "
                     f"| {fmt(h['p99_ms'], 'ms')} | {fmt(d.get('p99_pct'), '%')} |")
        lines.append(f"| {name} | max tput | {fmt(b.get('max_tput_rps'), ' rps')} "
                     f"| {fmt(h['max_tput_rps'], ' rps')} | {fmt(d.get('tput_pct'), '%')} |")
        for err in h["errors"]:
            lines.append(f"| {name} | error | | `{err[:80]}` | |")
    lines.append("")
    for name, h in head.items():
        flow = h.get("flow")
        if not flow:
            continue
        def as_us(value_ms):
            return f"{round(value_ms * 1000):,}" if value_ms is not None else "n/a"

        lines.append(f"<details><summary>{name} request-flow breakdown "
                     f"(instrumented run, head; p50 per stage, µs)</summary>")
        lines.append("")
        lines.append("| stage | p50 (µs) |")
        lines.append("|---|---|")
        lines.append(f"| client total | {as_us(flow.get('client_p50_ms'))} |")
        for stage, value in (flow.get("stages_p50_ms") or {}).items():
            lines.append(f"| {stage} | {as_us(value)} |")
        for stage, value in (flow.get("pbm_p50_ms") or {}).items():
            lines.append(f"| pbm: {stage} | {as_us(value)} |")
        lines.append("")
        lines.append("</details>")
    lines.append("")
    if regressions:
        lines.append("### REGRESSION detected")
        lines.extend(f"- {r}" for r in regressions)
    elif base:
        lines.append("No regression beyond thresholds "
                     f"(p50 +{int(P50_UP_LIMIT*100)}%, p99 +{int(P99_UP_LIMIT*100)}%, "
                     f"tput -{int(TPUT_DOWN_LIMIT*100)}%).")
    summary = "\n".join(lines) + "\n"
    Path(args.summary).write_text(summary)
    print(summary)

    if args.github_output:
        with open(args.github_output, "a") as fh:
            fh.write(f"regression={'true' if regressions else 'false'}\n")


if __name__ == "__main__":
    main()
