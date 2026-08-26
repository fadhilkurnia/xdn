#!/usr/bin/env python3
"""Sequential closed-loop write-latency probe for the latency-optimization harness.

One outstanding request at a time over a persistent connection, so each sample
is a single end-to-end write (no pipelining). Reports p50/p95/p99/mean in ms and
harvests the X-XDN-Breakdown response header (per-stage microseconds) when present.

    python3 eval/opt_seq_latency.py --host 10.10.1.1 --port 2300 \
        --service bookcatalog --duration 30 [--warmup 5] [--json out.json]
"""
import argparse
import http.client
import json
import statistics
import sys
import time

PAYLOAD = '{"author": "opt", "title": "latency"}'
PATH = "/api/books"


def percentile(sorted_vals, frac):
    if not sorted_vals:
        return None
    i = min(len(sorted_vals) - 1, int(frac * (len(sorted_vals) - 1) + 0.5))
    return round(sorted_vals[i], 3)


def parse_breakdown(headers):
    """X-XDN-Breakdown: stage=us;stage=us;... -> {stage: us}."""
    val = headers.get("X-XDN-Breakdown")
    if not val:
        return {}
    out = {}
    for part in val.split(";"):
        k, _, v = part.partition("=")
        try:
            out[k.strip()] = float(v.strip())
        except ValueError:
            pass
    return out


def run(host, port, service, duration_s, warmup_s):
    conn = None
    headers = {"XDN": service, "Content-Type": "application/json",
               "Content-Length": str(len(PAYLOAD))}
    lat = []
    stage_samples = []
    errors = 0
    end = time.time() + duration_s + warmup_s
    warm_until = time.time() + warmup_s
    while time.time() < end:
        try:
            if conn is None:
                conn = http.client.HTTPConnection(host, port, timeout=15)
            t0 = time.perf_counter()
            conn.request("POST", PATH, body=PAYLOAD, headers=headers)
            resp = conn.getresponse()
            resp.read()
            dt = (time.perf_counter() - t0) * 1000
            if resp.status >= 400:
                errors += 1
                continue
            if time.time() >= warm_until:
                lat.append(dt)
                bd = parse_breakdown(dict(resp.getheaders()))
                if bd:
                    stage_samples.append(bd)
        except Exception:
            errors += 1
            try:
                conn.close()
            except Exception:
                pass
            conn = None
            time.sleep(0.2)
    if conn:
        conn.close()
    lat.sort()
    if not lat:
        return {"error": f"no successful samples ({errors} errors)"}
    stages = {}
    if stage_samples:
        keys = set().union(*stage_samples)
        stages = {k: round(statistics.median([s[k] for s in stage_samples if k in s]), 1)
                  for k in sorted(keys)}
    return {
        "host": host, "port": port, "service": service,
        "samples": len(lat), "errors": errors,
        "mean_ms": round(statistics.fmean(lat), 3),
        "p50_ms": percentile(lat, 0.50),
        "p95_ms": percentile(lat, 0.95),
        "p99_ms": percentile(lat, 0.99),
        "p100_ms": round(lat[-1], 3),  # worst observed (max)
        "stages_us": stages,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", type=int, default=2300)
    ap.add_argument("--service", default="bookcatalog")
    ap.add_argument("--duration", type=int, default=30)
    ap.add_argument("--warmup", type=int, default=5)
    ap.add_argument("--json")
    a = ap.parse_args()
    r = run(a.host, a.port, a.service, a.duration, a.warmup)
    print(json.dumps(r, indent=2))
    if a.json:
        with open(a.json, "w") as f:
            json.dump(r, f, indent=2)
    if "error" in r:
        sys.exit(1)


if __name__ == "__main__":
    main()
