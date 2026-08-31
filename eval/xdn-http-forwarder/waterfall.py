#!/usr/bin/env python3
"""
waterfall_compare.py — compares multiple forwarder benchmark runs side-by-side, one
horizontal stacked bar per run, in three separate images (p50/p90/p95 by default, one
image per percentile — not one image with three rows per benchmark).

Modeled directly on this project's existing conventions:
  - Each --run is LABEL:TYPE=..., repeatable, one per benchmark being compared
    (analyze.py's --run LABEL=... convention, extended with an explicit TYPE tag since
    runs can now be genuinely different shapes).
  - Segment numbers are shown as colored text after each bar, chained "A + B + C = total"
    (visualize.py's draw_colored_sequence), never printed inside the bars.
  - Wherever a real reqId join is possible (outer+k6, tier0+k6), the "client overhead"
    segment is a REAL per-row difference, filtered to the matched-row subset — never a
    difference of two independently-computed quantiles. Per visualize.py's own module
    docstring: quantile(A) - quantile(B) can behave confusingly when A and B have
    different sample sizes; quantile(A - B) computed row-by-row does not have that
    problem. Only the vegeta path (no per-request ID exists at all) falls back to
    percentile-level subtraction, and is labeled "residual" wherever it appears so it's
    never confused with a real per-row measurement.

--run TYPES:
  LABEL:outer=CSVFILE
      forwarder-timings.log format (receive->handoff, forwarder.execute, response->flush)

  LABEL:outer=CSVFILE:CLIENTFILE
      + a 4th client-overhead segment. CLIENTFILE is auto-detected as either a k6
      `--out json=...` file (real per-row join — requires the outer log to have real,
      non-'unknown' reqId values) or a `vegeta report -type=json` file (residual,
      percentile-level only, since vegeta has no per-request ID to join on).

  LABEL:inner=CSVFILE
      ForwarderFrontend's *-inner.log format (pool acquire, write request, backend wait)

  LABEL:inner=CSVFILE:VEGETAJSON
      + a residual segment vs. vegeta's client-observed total. A k6 real join is NOT
      supported for inner logs — they have no receive/flush timestamps to anchor a join
      against a client-observed total at all.

  LABEL:tier0=CONTAINERLOG
      No forwarder in the loop. CONTAINERLOG is the backend's own access log, in the same
      "[reqId=...] [lat=...]" format analyze.py's DOCKER_LOG_PATTERN already expects.
      Segment: container proc. time only.

  LABEL:tier0=CONTAINERLOG:K6JSON
      + a real per-row client-overhead segment (k6 total - container latency), exactly
      analyze.py's own tier-0 join logic.

Usage:
    python3 waterfall_compare.py \\
        --run direct_nginx:tier0=nginx-access.log:k6-direct.json \\
        --run tier1_blocking:outer=forwarder-timings-blocking.log:k6-blocking.json \\
        --run tier2b_async:outer=forwarder-timings-async.log:k6-async.json \\
        --run tier2b_async_inner:inner=forwarder-timings-async-inner.log:vegeta-report.json \\
        -o-prefix results/compare
    # writes results/compare_p50.png, results/compare_p90.png, results/compare_p95.png
"""

import argparse
import csv
import json
import re
import sys

import matplotlib.pyplot as plt
import numpy as np

DOCKER_LOG_PATTERN = re.compile(
    r"(?P<method>GET|POST|PUT|DELETE|PATCH) (?P<path>\S+) "
    r"\[reqId=(?P<reqId>[\w-]+)\] "
    r"\[lat=(?P<lat>[\d.]+)(?P<unit>ns|µs|μs|us|ms|s)\]"
)
UNIT_TO_MS = {
    "ns": 1e-6,
    "µs": 1e-3, "μs": 1e-3, "us": 1e-3,
    "ms": 1.0,
    "s": 1000.0,
}

COLOR_NEUTRAL = "#333333"


class LabelColorAssigner:
    """Assigns a stable color per distinct segment-label string, in first-seen order, so
    the same kind of segment (e.g. the client-overhead label) always renders the same
    color across every run in the comparison — even though different run TYPES have
    entirely different, unrelated segment label sets.

    Pulls from matplotlib's tab20 colormap (20 distinct colors) rather than a small fixed
    list — with only outer/inner/tier0 segment kinds this project currently has at most
    ~7 distinct labels, but comparisons tend to grow (more tiers, more client-overhead
    variants with slightly different names), and a palette that silently wraps around and
    REUSES a color for an unrelated segment is actively misleading, not just less pretty —
    it happened during this script's own testing (8-color list, 9th distinct label wrapped
    back to color 0). 20 is generous headroom against that recurring.
    """

    def __init__(self):
        self._colors = {}
        cmap = plt.get_cmap("tab20")
        self._palette = [cmap(i) for i in range(20)]

    def color_for(self, label):
        if label not in self._colors:
            if len(self._colors) >= len(self._palette):
                print(f"WARNING: more than {len(self._palette)} distinct segment labels — "
                      f"colors will start repeating across UNRELATED segments. Consider "
                      f"shortening/unifying labels if this happens.", file=sys.stderr)
            self._colors[label] = self._palette[len(self._colors) % len(self._palette)]
        return self._colors[label]


def parse_container_log(path):
    """Returns {reqId: latency_ms}, same log shape as analyze.py's parse_container_log."""
    lat_by_reqid = {}
    with open(path) as f:
        for line in f:
            m = DOCKER_LOG_PATTERN.search(line)
            if not m:
                continue
            reqid = m.group("reqId")
            if reqid == "-":
                continue
            lat_by_reqid[reqid] = float(m.group("lat")) * UNIT_TO_MS[m.group("unit")]
    return lat_by_reqid


def parse_k6_reqid_durations(path):
    """Returns {reqId: http_req_duration_ms} from a k6 --out json=... file."""
    per_reqid_ms = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if obj.get("type") != "Point" or obj.get("metric") != "req_latency":
                continue
            data = obj.get("data", {})
            value = data.get("value")
            reqid = data.get("tags", {}).get("reqId")
            if value is not None and reqid and reqid != "unknown":
                per_reqid_ms[reqid] = value
    return per_reqid_ms


def parse_vegeta_percentiles(path, percentiles):
    """Returns {percentile: client_observed_total_ms}. Vegeta's own json report only ever
    contains 50th/95th/99th -- any other requested percentile is skipped with a warning."""
    with open(path) as f:
        report = json.load(f)
    latencies = report.get("latencies", {})
    result = {}
    for p in percentiles:
        key = f"{int(p)}th" if float(p).is_integer() else None
        if key is None or key not in latencies:
            print(f"WARNING ({path}): no '{key}' percentile in this vegeta report "
                  f"(only 50th/95th/99th exist) — skipping p{p} for this run's client segment.",
                  file=sys.stderr)
            continue
        result[p] = latencies[key] / 1_000_000.0
    return result


def detect_client_file_kind(path):
    """Distinguishes a k6 --out json file (line-delimited, one JSON object per line) from
    a vegeta `report -type=json` file (one single JSON document with a 'latencies' key)."""
    with open(path) as f:
        first_line = f.readline()
    try:
        obj = json.loads(first_line)
    except json.JSONDecodeError:
        with open(path) as f:
            try:
                obj = json.load(f)
            except json.JSONDecodeError:
                raise ValueError(f"Could not parse {path} as either k6 ndjson or a vegeta JSON report.")
    if isinstance(obj, dict) and "latencies" in obj:
        return "vegeta"
    return "k6"


def load_outer_run(csv_path, client_path, percentiles):
    """Returns (labels, segments, vegeta_pcts_or_None)."""
    divisor = 1_000_000.0  # nanoseconds, per project convention
    seg1, seg2, seg3, reqids, t_recv_ms, t_flush_ms = [], [], [], [], [], []
    skipped = 0
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                t_recv = int(row["tReceivedMs"])
                t_before = int(row["tBeforeExecuteMs"])
                t_after = int(row["tAfterExecuteMs"])
                t_flushed = int(row["tFlushedMs"])
                status = int(row["statusCode"])
            except (ValueError, KeyError):
                skipped += 1
                continue
            if status == -1 or t_flushed == -1:
                skipped += 1
                continue
            if not (t_recv <= t_before <= t_after <= t_flushed):
                # A truncated final field (e.g. a cut-off write) can still parse as a
                # valid, wrong number -- this catches it by checking the timestamps are
                # actually in chronological order, which a truncated number almost always
                # breaks dramatically.
                skipped += 1
                continue
            seg1.append((t_before - t_recv) / divisor)
            seg2.append((t_after - t_before) / divisor)
            seg3.append((t_flushed - t_after) / divisor)
            reqids.append(row.get("reqId"))
            t_recv_ms.append(t_recv / divisor)
            t_flush_ms.append(t_flushed / divisor)
    if skipped:
        print(f"({csv_path}: skipped {skipped} rows — parse errors or failed requests)", file=sys.stderr)

    labels = ["receive \u2192 handoff", "forwarder.execute", "response \u2192 flush"]
    segments = [np.array(seg1), np.array(seg2), np.array(seg3)]

    if not client_path:
        return labels, segments, None

    kind = detect_client_file_kind(client_path)
    if kind == "k6":
        if all(r == "unknown" or r is None for r in reqids):
            print(f"WARNING ({csv_path}): every reqId is 'unknown' — cannot join against k6. "
                  f"Skipping client-overhead segment for this run.", file=sys.stderr)
            return labels, segments, None
        k6_by_reqid = parse_k6_reqid_durations(client_path)
        matched = [i for i, rid in enumerate(reqids) if rid in k6_by_reqid]
        if not matched:
            print(f"WARNING ({csv_path}): no reqId overlap with {client_path} — skipping client segment.",
                  file=sys.stderr)
            return labels, segments, None
        idx = np.array(matched)
        segments = [seg[idx] for seg in segments]
        client_overhead = np.array(
            [k6_by_reqid[reqids[i]] - (t_flush_ms[i] - t_recv_ms[i]) for i in matched]
        )
        return labels + ["client-observed overhead (k6, real per-row)"], segments + [client_overhead], None

    vegeta_pcts = parse_vegeta_percentiles(client_path, percentiles)
    return labels, segments, vegeta_pcts


def load_inner_run(csv_path, client_path, percentiles):
    """Returns (labels, segments, vegeta_pcts_or_None)."""
    seg_acq, seg_write, seg_wait = [], [], []
    skipped = 0
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                t_before = int(row["tBeforeExecuteNanos"])
                t_acquire = int(row["tAcquireNanos"])
                t_write = int(row["tWriteNanos"])
                t_resp = int(row["tRespRecvNanos"])
            except (ValueError, KeyError, TypeError):
                skipped += 1
                continue
            if t_acquire == 0:
                skipped += 1
                continue
            if not (t_before <= t_acquire <= t_write <= t_resp):
                skipped += 1
                continue
            seg_acq.append((t_acquire - t_before) / 1_000_000.0)
            seg_write.append((t_write - t_acquire) / 1_000_000.0)
            seg_wait.append((t_resp - t_write) / 1_000_000.0)
    if skipped:
        print(f"({csv_path}: skipped {skipped} rows)", file=sys.stderr)

    labels = ["pool acquire", "write request", "backend wait"]
    segments = [np.array(seg_acq), np.array(seg_write), np.array(seg_wait)]

    if not client_path:
        return labels, segments, None

    kind = detect_client_file_kind(client_path)
    if kind == "k6":
        print(f"WARNING ({csv_path}): inner logs have no receive/flush timestamps to join "
              f"against a k6 total — k6 client join isn't supported for inner-format runs. "
              f"Use a vegeta report instead, or attach client data at the outer-log level.",
              file=sys.stderr)
        return labels, segments, None

    vegeta_pcts = parse_vegeta_percentiles(client_path, percentiles)
    return labels, segments, vegeta_pcts


def load_tier0_run(container_path, k6_path):
    """Returns (labels, segments, vegeta_pcts_or_None) — vegeta_pcts always None here;
    tier0 only supports a real k6 per-row join, matching analyze.py's own tier-0 branch."""
    container_by_reqid = parse_container_log(container_path)
    if not k6_path:
        vals = np.array(list(container_by_reqid.values()))
        return ["container proc. time"], [vals], None

    k6_by_reqid = parse_k6_reqid_durations(k6_path)
    matched = [rid for rid in container_by_reqid if rid in k6_by_reqid]
    if not matched:
        print(f"WARNING ({container_path}): no reqId overlap with {k6_path} — client segment skipped.",
              file=sys.stderr)
        vals = np.array(list(container_by_reqid.values()))
        return ["container proc. time"], [vals], None

    container_vals = np.array([container_by_reqid[r] for r in matched])
    client_overhead = np.array([k6_by_reqid[r] - container_by_reqid[r] for r in matched])
    return (
        ["container proc. time", "client-observed overhead (k6 \u2212 container, real per-row)"],
        [container_vals, client_overhead],
        None,
    )


def load_inner_rows(csv_path):
    """Like load_inner_run, but keeps reqId aligned per row (and each row's inner-log
    total span) instead of just returning aggregate segment arrays -- needed for tier2's
    4-way join, which must match rows across FOUR separate files by reqId. Kept as its own
    function rather than modifying load_inner_run, since that one's existing callers
    (standalone inner+vegeta plots) have no need for reqId or per-row joining at all."""
    reqids, seg_acq, seg_write, seg_wait, inner_total = [], [], [], [], []
    skipped = 0
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                t_before = int(row["tBeforeExecuteNanos"])
                t_acquire = int(row["tAcquireNanos"])
                t_write = int(row["tWriteNanos"])
                t_resp = int(row["tRespRecvNanos"])
            except (ValueError, KeyError, TypeError):
                skipped += 1
                continue
            if t_acquire == 0:
                skipped += 1
                continue
            if not (t_before <= t_acquire <= t_write <= t_resp):
                skipped += 1
                continue
            reqids.append(row.get("reqId"))
            seg_acq.append((t_acquire - t_before) / 1_000_000.0)
            seg_write.append((t_write - t_acquire) / 1_000_000.0)
            seg_wait.append((t_resp - t_write) / 1_000_000.0)
            inner_total.append((t_resp - t_before) / 1_000_000.0)
    if skipped:
        print(f"({csv_path}: skipped {skipped} rows)", file=sys.stderr)
    if not reqids:
        raise ValueError(f"{csv_path}: zero valid rows parsed.")
    return reqids, np.array(seg_acq), np.array(seg_write), np.array(seg_wait), np.array(inner_total)


def load_outer_segments(path: str, unit: str, keep_reqids: bool = False):
    """Reads the outer CSV format (forwarder-timings.log): receive->handoff,
    forwarder.execute, response->flush.

    If keep_reqids: ALSO returns (reqids, t_recv_ms, t_flush_ms) aligned by index to the
    segment arrays, so a caller (tier1/tier2's joins) can select matching rows via fancy
    indexing, e.g. segments[0][idx]. t_recv_ms/t_flush_ms are returned as real numpy
    arrays specifically so that indexing works the same way it does on the segment
    arrays -- a plain Python list doesn't support `arr[idx]` with an array of positions.

    Returns (labels, segments) normally, or (labels, segments, reqids, t_recv_ms,
    t_flush_ms) if keep_reqids=True. This is the function load_tier1_run/load_tier2_run
    already call -- it was missing from this file entirely until now.
    """
    divisor = 1_000_000.0 if unit == "ns" else 1.0
    seg1, seg2, seg3, reqids, t_recv_ms, t_flush_ms = [], [], [], [], [], []
    skipped = 0
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                t_recv = int(row["tReceivedMs"])
                t_before = int(row["tBeforeExecuteMs"])
                t_after = int(row["tAfterExecuteMs"])
                t_flushed = int(row["tFlushedMs"])
                status = int(row["statusCode"])
            except (ValueError, KeyError, TypeError):
                skipped += 1
                continue
            if status == -1 or t_flushed == -1:
                skipped += 1
                continue
            if not (t_recv <= t_before <= t_after <= t_flushed):
                skipped += 1
                continue
            seg1.append((t_before - t_recv) / divisor)
            seg2.append((t_after - t_before) / divisor)
            seg3.append((t_flushed - t_after) / divisor)
            if keep_reqids:
                reqids.append(row.get("reqId"))
                t_recv_ms.append(t_recv / divisor)
                t_flush_ms.append(t_flushed / divisor)
    if skipped:
        print(f"({path}: skipped {skipped} rows — parse errors or failed requests)", file=sys.stderr)

    labels = ["receive \u2192 handoff", "forwarder.execute", "response \u2192 flush"]
    segments = [np.array(seg1), np.array(seg2), np.array(seg3)]
    if not segments[0].size:
        raise ValueError(f"{path}: zero valid rows parsed — check the file's contents.")

    if keep_reqids:
        return labels, segments, reqids, np.array(t_recv_ms), np.array(t_flush_ms)
    return labels, segments


def load_tier1_run(container_path, outer_path, k6_path):
    """3-way real per-row join (container log + outer java log + k6) on reqId.

    forwarder.execute already INCLUDES the container's own real processing time inside
    it -- so container time is SUBTRACTED out of it here, not added as an extra segment,
    to avoid double-counting the same span of time twice in the chart's total."""
    outer_labels, outer_segments, reqids, t_recv_ms, t_flush_ms = load_outer_segments(
        outer_path, "ns", keep_reqids=True)
    container_by_reqid = parse_container_log(container_path)
    if not container_by_reqid:
        raise ValueError(f"{container_path}: zero lines matched the container-log pattern.")
    k6_by_reqid = parse_k6_reqid_durations(k6_path)
    if not k6_by_reqid:
        raise ValueError(f"{k6_path}: no req_latency points with a real reqId found.")

    matched = [i for i, rid in enumerate(reqids) if rid in container_by_reqid and rid in k6_by_reqid]
    print(f"tier1 join ({outer_path}): {len(matched)}/{len(reqids)} requests had complete "
          f"data across container+outer+k6 logs", file=sys.stderr)
    if not matched:
        raise ValueError(f"No rows had matching reqId across all three files for this tier1 run.")

    idx = np.array(matched)
    receive_handoff = outer_segments[0][idx]
    forwarder_execute = outer_segments[1][idx]
    response_flush = outer_segments[2][idx]
    container_vals = np.array([container_by_reqid[reqids[i]] for i in matched])
    t_recv_vals = t_recv_ms[idx]
    t_flush_vals = t_flush_ms[idx]

    network_pool_overhead = forwarder_execute - container_vals
    neg = int((network_pool_overhead < 0).sum())
    if neg:
        print(f"WARNING ({outer_path}): {neg}/{len(matched)} rows had forwarder.execute < "
              f"container time (clock skew between JVM and container) — clamped to 0.",
              file=sys.stderr)
        network_pool_overhead = np.clip(network_pool_overhead, 0, None)

    client_overhead = np.array(
        [k6_by_reqid[reqids[i]] for i in matched]) - (t_flush_vals - t_recv_vals)

    labels = [
        "receive \u2192 handoff",
        "network+pool overhead (forwarder.execute \u2212 container)",
        "container proc. time",
        "response \u2192 flush",
        "client-observed overhead (k6, real per-row)",
    ]
    segments = [receive_handoff, network_pool_overhead, container_vals, response_flush, client_overhead]
    return labels, segments, None


def load_tier2_run(container_path, outer_path, inner_path, k6_path):
    """4-way real per-row join (container log + outer java log + inner java log + k6) on
    reqId. Two subtractions happen here, both for the same reason as tier1's: avoiding
    double-counting a span of time that's measured at two different levels of detail.
      - network overhead = backend wait (inner) minus container time -- backend wait
        already includes the real container processing time inside it.
      - instrumentation gap = forwarder.execute (outer) minus the inner log's own total
        span -- both measure the same call, just from two different vantage points; any
        small leftover difference is measurement gap between the two instrumentation
        points, not a new span of real time."""
    outer_labels, outer_segments, outer_reqids, t_recv_ms, t_flush_ms = load_outer_segments(
        outer_path, "ns", keep_reqids=True)
    inner_reqids, seg_acq, seg_write, seg_wait, inner_total = load_inner_rows(inner_path)
    container_by_reqid = parse_container_log(container_path)
    if not container_by_reqid:
        raise ValueError(f"{container_path}: zero lines matched the container-log pattern.")
    k6_by_reqid = parse_k6_reqid_durations(k6_path)
    if not k6_by_reqid:
        raise ValueError(f"{k6_path}: no req_latency points with a real reqId found.")

    outer_idx_by_reqid = {rid: i for i, rid in enumerate(outer_reqids)}
    inner_idx_by_reqid = {rid: i for i, rid in enumerate(inner_reqids)}

    matched = [rid for rid in dict.fromkeys(outer_reqids)
               if rid in inner_idx_by_reqid and rid in container_by_reqid and rid in k6_by_reqid]
    print(f"tier2 join ({outer_path}): {len(matched)}/{len(set(outer_reqids))} requests had "
          f"complete data across container+outer+inner+k6 logs", file=sys.stderr)
    if not matched:
        raise ValueError(f"No rows had matching reqId across all four files for this tier2 run.")

    o_idx = np.array([outer_idx_by_reqid[r] for r in matched])
    i_idx = np.array([inner_idx_by_reqid[r] for r in matched])

    receive_handoff = outer_segments[0][o_idx]
    forwarder_execute = outer_segments[1][o_idx]
    response_flush = outer_segments[2][o_idx]
    t_recv_vals = t_recv_ms[o_idx]
    t_flush_vals = t_flush_ms[o_idx]

    pool_acquire = seg_acq[i_idx]
    write_request = seg_write[i_idx]
    backend_wait = seg_wait[i_idx]
    inner_tot = inner_total[i_idx]

    container_vals = np.array([container_by_reqid[r] for r in matched])
    k6_vals = np.array([k6_by_reqid[r] for r in matched])

    network_overhead = backend_wait - container_vals
    neg1 = int((network_overhead < 0).sum())
    if neg1:
        print(f"WARNING ({inner_path}): {neg1}/{len(matched)} rows had backend wait < "
              f"container time (clock skew) — clamped to 0.", file=sys.stderr)
        network_overhead = np.clip(network_overhead, 0, None)

    instrumentation_gap = forwarder_execute - inner_tot
    neg2 = int((instrumentation_gap < 0).sum())
    if neg2:
        print(f"WARNING ({outer_path} vs {inner_path}): {neg2}/{len(matched)} rows had "
              f"forwarder.execute < inner log total (measurement gap) — clamped to 0.",
              file=sys.stderr)
        instrumentation_gap = np.clip(instrumentation_gap, 0, None)

    client_overhead = k6_vals - (t_flush_vals - t_recv_vals)

    labels = [
        "receive \u2192 handoff",
        "pool acquire",
        "write request",
        "network overhead (backend wait \u2212 container)",
        "container proc. time",
        "instrumentation gap (outer \u2212 inner)",
        "response \u2192 flush",
        "client-observed overhead (k6, real per-row)",
    ]
    segments = [receive_handoff, pool_acquire, write_request, network_overhead,
                container_vals, instrumentation_gap, response_flush, client_overhead]
    return labels, segments, None


def parse_run_arg(run_arg):
    """LABEL:TYPE=REST -> (label, type, rest)."""
    if "=" not in run_arg:
        raise ValueError(f"Malformed --run (expected LABEL:TYPE=...): {run_arg}")
    prefix, rest = run_arg.split("=", 1)
    if ":" not in prefix:
        raise ValueError(f"Malformed --run (expected LABEL:TYPE before '='): {run_arg}")
    label, type_ = prefix.split(":", 1)
    return label, type_, rest


def load_run(label, type_, rest, percentiles):
    """Returns (label, labels, segments, vegeta_pcts_or_None)."""
    if type_ == "outer":
        parts = rest.split(":", 1)
        csv_path = parts[0]
        client_path = parts[1] if len(parts) > 1 and parts[1] else None
        labels, segments, vegeta_pcts = load_outer_run(csv_path, client_path, percentiles)
        return label, labels, segments, vegeta_pcts

    if type_ == "inner":
        parts = rest.split(":", 1)
        csv_path = parts[0]
        client_path = parts[1] if len(parts) > 1 and parts[1] else None
        labels, segments, vegeta_pcts = load_inner_run(csv_path, client_path, percentiles)
        return label, labels, segments, vegeta_pcts

    if type_ == "tier0":
        parts = rest.split(":", 1)
        container_path = parts[0]
        k6_path = parts[1] if len(parts) > 1 and parts[1] else None
        labels, segments, vegeta_pcts = load_tier0_run(container_path, k6_path)
        return label, labels, segments, vegeta_pcts

    if type_ == "tier1":
        parts = rest.split(":", 2)
        if len(parts) != 3:
            raise ValueError(
                f"tier1 run needs exactly 3 colon-separated files "
                f"(CONTAINERLOG:JAVALOG:K6JSON), got {len(parts)}: {rest}")
        container_path, outer_path, k6_path = parts
        labels, segments, vegeta_pcts = load_tier1_run(container_path, outer_path, k6_path)
        return label, labels, segments, vegeta_pcts

    if type_ == "tier2":
        parts = rest.split(":", 3)
        if len(parts) != 4:
            raise ValueError(
                f"tier2 run needs exactly 4 colon-separated files "
                f"(CONTAINERLOG:JAVAOUTERLOG:JAVAINNERLOG:K6JSON), got {len(parts)}: {rest}")
        container_path, outer_path, inner_path, k6_path = parts
        labels, segments, vegeta_pcts = load_tier2_run(container_path, outer_path, inner_path, k6_path)
        return label, labels, segments, vegeta_pcts

    raise ValueError(f"Unknown --run type '{type_}' (expected outer, inner, tier0, tier1, or tier2)")


def draw_colored_sequence(ax, fig, x0, y, parts, fontsize=8):
    """Same technique as visualize.py: place each piece of text immediately after the
    previous one's ACTUAL rendered width, rather than guessing width from character
    count (which varies by content/font)."""
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    x = x0
    for text, color in parts:
        t = ax.text(x, y, text, color=color, fontsize=fontsize, va="center", ha="left")
        bbox = t.get_window_extent(renderer=renderer)
        x_disp0, y_disp = ax.transData.transform((x, y))
        x_disp1 = x_disp0 + bbox.width
        x, _ = ax.transData.inverted().transform((x_disp1, y_disp))


def plot_percentile(runs, p, out_path, color_assigner):
    """runs: list of (run_label, labels, segments, vegeta_pcts_or_None)."""
    # Legend rows needed scales with how many DISTINCT labels exist across every run, not
    # just this run's own segment count -- a fixed-height reservation (this script's
    # earlier version) clips the legend the moment total distinct labels grows past
    # whatever was originally guessed. Compute it from the real, current label set instead.
    all_labels = {seg_label for _, labels, segments, vegeta_pcts in runs
                  for seg_label in (labels + (["_residual_placeholder"] if vegeta_pcts is not None else []))}
    legend_ncol = 2
    legend_rows = -(-len(all_labels) // legend_ncol)  # ceil division
    bottom_margin = 0.05 + 0.035 * legend_rows
    fig_height = 0.8 * len(runs) + 1.5 + 0.3 * legend_rows
    fig, ax = plt.subplots(figsize=(20, fig_height))

    row_data = []  # (run_label, [(seg_label, value), ...], total)
    max_total = 1.0
    for run_label, labels, segments, vegeta_pcts in runs:
        parts = [(lab, float(np.percentile(seg, p))) for lab, seg in zip(labels, segments)]
        if vegeta_pcts is not None:
            known_total = sum(v for _, v in parts)
            if p in vegeta_pcts:
                residual = vegeta_pcts[p] - known_total
                if residual < 0:
                    print(f"WARNING ({run_label}, p{p}): vegeta total is less than the sum of "
                          f"known segments — clamping residual to 0 (expected with "
                          f"percentile-level, non-per-row subtraction).", file=sys.stderr)
                    residual = 0.0
            else:
                residual = 0.0
            parts.append(("unaccounted (client + network, vegeta residual, not per-row verified)", residual))
        total = sum(v for _, v in parts)
        max_total = max(max_total, abs(total))
        row_data.append((run_label, parts, total))

    labeled = set()
    for y, (run_label, parts, total) in enumerate(row_data):
        left = 0.0
        for seg_label, val in parts:
            color = color_assigner.color_for(seg_label)
            ax.barh(y, val, left=left, color=color, label=None if seg_label in labeled else seg_label)
            labeled.add(seg_label)
            left += val

    ax.set_yticks(range(len(row_data)))
    ax.set_yticklabels([r[0] for r in row_data])
    ax.invert_yaxis()
    ax.set_xlabel("Duration (ms)")
    ax.set_title(f"Forwarder benchmark comparison \u2014 p{p}")
    handles, legend_labels = ax.get_legend_handles_labels()
    fig.legend(handles, legend_labels, loc="lower center", ncol=legend_ncol,
               bbox_to_anchor=(0.5, -0.01), fontsize=8)
    x0, x1 = ax.get_xlim()
    ax.set_xlim(x0, x1 * 2.5)

    # Layout finalized BEFORE placing text -- draw_colored_sequence reads current pixel
    # geometry; anything that moves the axes afterward (tight_layout etc.) invalidates it.
    plt.tight_layout(rect=(0, bottom_margin, 1, 1))

    for y, (run_label, parts, total) in enumerate(row_data):
        seq = []
        for i, (seg_label, val) in enumerate(parts):
            if i > 0:
                seq.append((" + ", COLOR_NEUTRAL))
            seq.append((f"{val:.4f}ms", color_assigner.color_for(seg_label)))
        seq.append((" = ", COLOR_NEUTRAL))
        seq.append((f"{total:.4f}ms", COLOR_NEUTRAL))
        x_start = max(total, 0) + max_total * 0.02
        draw_colored_sequence(ax, fig, x_start, y, seq, fontsize=8)

    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"Saved: {out_path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run", action="append", required=True,
                        help="LABEL:TYPE=... (TYPE is outer, inner, tier0, tier1, or tier2). Repeatable, "
                        "one per benchmark. See module docstring for the exact syntax of each TYPE.")
    parser.add_argument("--percentiles", type=float, nargs="+", default=[50, 90, 95],
                        help="Percentiles to generate one image per (default: 50 90 95). "
                        "NOTE: any run using a vegeta client file only has 50/95/99 available "
                        "for ITS client-overhead segment specifically -- other segments are unaffected.")
    parser.add_argument("-o-prefix", dest="out_prefix", default="waterfall_compare",
                         help="Output file prefix -- writes PREFIX_p50.png, PREFIX_p90.png, etc.")
    args = parser.parse_args()

    runs = []
    for run_arg in args.run:
        try:
            label, type_, rest = parse_run_arg(run_arg)
            runs.append(load_run(label, type_, rest, args.percentiles))
        except (ValueError, FileNotFoundError) as e:
            print(f"ERROR loading --run '{run_arg}': {e}", file=sys.stderr)
            sys.exit(1)

    color_assigner = LabelColorAssigner()
    for p in args.percentiles:
        p_str = str(int(p)) if float(p).is_integer() else str(p)
        out_path = f"{args.out_prefix}_p{p_str}.png"
        plot_percentile(runs, p, out_path, color_assigner)


if __name__ == "__main__":
    main()
