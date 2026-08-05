#!/usr/bin/env python3
"""Protocol-switch latency demo for XDN.

Drives bookcatalog with the large-request / small-statediff workload (padded
POSTs whose extra fields the app parses and drops, so the on-disk row is
tiny) while measuring per-request latency over time. Partway through, it
flips the service from active replication (Paxos) to primary-backup via an
in-place placement update. Under this workload primary-backup replicates the
small statediff instead of the large request, so median latency drops after
the switch.

Output: a JSON time series (t, latency_ms, phase) and, if matplotlib is
present, a median-latency-over-time plot with the switch annotated.

Open-loop by design: a fixed pool of workers issues requests concurrently at
a target rate, so the seconds-long primary-election window after the switch
does not freeze the timeline (a closed-loop driver would serialize behind the
first timeout and lose all post-switch samples). Requires a fast statediff
recorder to show the latency win: on Linux use FUSELOG/FUSERUST, not RSYNC
(rsync spawns a subprocess per capture, adding a ~250ms floor that swamps the
request-vs-diff saving). Tune the primary's capture accumulation low, e.g.
-DPB_CAPTURE_ACCUMULATION_US=500.

Usage (from a running cluster with bookcatalog launched under Paxos):
  python3 eval/protocol_switch_demo.py \
      --frontends 10.10.1.1:2300,10.10.1.2:2300,10.10.1.3:2300 \
      --rc 10.10.1.1:3300 --service bookcatalog \
      --nodes AR1,AR2,AR3 --primary AR1 \
      --pad-bytes 200000 --duration 90 --switch-at 45 \
      --rate 40 --workers 16 --out /tmp/psw-demo.json
"""

import argparse
import base64
import json
import os
import queue
import statistics
import sys
import threading
import time
import urllib.request


def make_body(book_id, pad_bytes):
    """Request body with an INCOMPRESSIBLE pad. The 'blob' field is not in the
    app schema, so the server parses and discards it: large request, tiny
    statediff. base64(os.urandom) is incompressible -- a naive 'x'*N pad
    gzips to nothing, so any compression in the HTTP/replication path would
    make the 'large' request tiny on the wire and the bandwidth experiment
    would measure nothing."""
    blob = base64.b64encode(os.urandom((pad_bytes * 3) // 4)).decode()
    return json.dumps({"id": book_id, "title": "T", "author": "A", "blob": blob}).encode()


def post_book(frontend, service, book_id, pad_bytes, update_id=None):
    """One write. Default: POST a new row (unique id). With update_id set:
    PUT the SAME row, so the table size stays constant (an idempotent
    re-write -- the persisted columns don't change, only the dropped blob),
    which keeps the statediff small and constant and avoids growing the
    epoch-transition state tar."""
    if update_id is not None:
        url = f"http://{frontend}/api/books/{update_id}"
        method = "PUT"
        data = make_body(update_id, pad_bytes)
    else:
        url = f"http://{frontend}/api/books"
        method = "POST"
        data = make_body(book_id, pad_bytes)
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={"XDN": service, "Content-Type": "application/json"})
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            resp.read()
        ok = True
    except Exception:
        ok = False
    return (time.time() - t0) * 1000.0, ok


def seed_book(frontend, service, book_id):
    """POST a book so a later PUT to it succeeds (UpdateBook 404s otherwise)."""
    data = json.dumps({"id": book_id, "title": "T", "author": "A"}).encode()
    req = urllib.request.Request(
        f"http://{frontend}/api/books", data=data, method="POST",
        headers={"XDN": service, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            resp.read()
    except Exception:
        pass


def switch_to_pb(rc, service, nodes, primary):
    body = {"NODES": nodes, "COORDINATOR": primary, "REPLICATION": "primary-backup"}
    req = urllib.request.Request(
        f"http://{rc}/api/v2/services/{service}/placement",
        data=json.dumps(body).encode(), method="PUT",
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        resp.read()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--frontends", required=True, help="comma-separated host:port")
    ap.add_argument("--rc", required=True, help="reconfigurator host:port")
    ap.add_argument("--service", default="bookcatalog")
    ap.add_argument("--nodes", required=True, help="comma-separated AR ids for the switch")
    ap.add_argument("--primary", required=True, help="AR id to become PB primary")
    ap.add_argument("--entry", help="single frontend host:port for ALL requests "
                    "(default: rotate). Set to the primary's frontend so the switch does "
                    "not send traffic to non-serving backups.")
    ap.add_argument("--pad-bytes", type=int, default=200000)
    ap.add_argument("--update-id", type=int,
                    help="PUT this fixed book id every request (constant table size, "
                    "idempotent re-write) instead of POSTing unique rows")
    ap.add_argument("--duration", type=float, default=60.0)
    ap.add_argument("--switch-at", type=float, default=30.0)
    ap.add_argument("--rate", type=float, default=40.0, help="target requests/sec")
    ap.add_argument("--workers", type=int, default=16, help="concurrent request workers")
    ap.add_argument("--warmup", type=float, default=20.0,
                    help="seconds of unrecorded warmup before measurement (JVM JIT + "
                    "connection-pool ramp), so the pre-switch baseline is flat")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    frontends = args.frontends.split(",")
    nodes = args.nodes.split(",")
    samples = []
    samples_lock = threading.Lock()
    switch_done = {"t": None}
    stop = threading.Event()
    recording = threading.Event()  # set once warmup ends; workers only record then
    start_holder = {"t": time.time()}  # measurement t=0, reset after warmup

    def do_switch():
        # Mark the switch at INITIATION, not completion: the placement PUT takes
        # a few seconds to coordinate at the RC, during which PB already takes
        # over and latency drops. Timestamping at return would plot those fast
        # samples just before the switch line (a spurious pre-switch dip).
        switch_done["t"] = time.time()
        switch_to_pb(args.rc, args.service, nodes, args.primary)

    # Open-loop generator: enqueue one job per tick at the target rate; a
    # bounded pool of workers services them concurrently, so slow requests
    # (e.g. during the election window) never stall the timeline.
    jobs = queue.Queue(maxsize=args.workers * 4)
    counter = {"n": 0}
    counter_lock = threading.Lock()

    def worker():
        while not stop.is_set():
            try:
                book_id = jobs.get(timeout=0.5)
            except queue.Empty:
                continue
            fe = args.entry if args.entry else frontends[book_id % len(frontends)]
            lat, ok = post_book(fe, args.service, book_id, args.pad_bytes, args.update_id)
            if recording.is_set():
                phase = "paxos" if switch_done["t"] is None else "primary-backup"
                with samples_lock:
                    samples.append({"t": round(time.time() - start_holder["t"], 4),
                                    "latency_ms": round(lat, 3), "ok": ok,
                                    "phase": phase, "frontend": fe})
            jobs.task_done()

    # For PUT mode, seed the target row so UpdateBook doesn't 404.
    if args.update_id is not None:
        seed_fe = args.entry if args.entry else frontends[0]
        seed_book(seed_fe, args.service, args.update_id)
        time.sleep(1)

    pool = [threading.Thread(target=worker, daemon=True) for _ in range(args.workers)]
    for w in pool:
        w.start()

    def generate(until, tag):
        """Enqueue at the target rate until `until` seconds elapse from now."""
        t0 = time.time()
        interval = 1.0 / args.rate if args.rate > 0 else 0
        switched = [False]
        while True:
            now = time.time()
            elapsed = now - t0
            if elapsed >= until:
                break
            if tag == "measure" and not switched[0] and elapsed >= args.switch_at:
                threading.Thread(target=do_switch, daemon=True).start()
                switched[0] = True
            with counter_lock:
                book_id = 1000 + counter["n"]
                counter["n"] += 1
            try:
                jobs.put(book_id, timeout=1.0)
            except queue.Full:
                pass
            if interval:
                sleep = interval - (time.time() - now)
                if sleep > 0:
                    time.sleep(sleep)

    # Warmup: drive load, record nothing (lets the JVM JIT-compile and the
    # HTTP connection pools fill, so the pre-switch baseline is already flat).
    if args.warmup > 0:
        print(f"[warmup] {args.warmup:.0f}s (unrecorded)", flush=True)
        generate(args.warmup, "warmup")

    # Measurement window: reset t=0 and start recording.
    start_holder["t"] = time.time()
    recording.set()
    generate(args.duration, "measure")

    stop.set()
    for w in pool:
        w.join(timeout=20)

    switch_rel = (switch_done["t"] - start_holder["t"]) if switch_done["t"] else args.switch_at
    samples.sort(key=lambda s: s["t"])
    out = {
        "service": args.service,
        "pad_bytes": args.pad_bytes,
        "duration": args.duration,
        "switch_at_rel": round(switch_rel, 3),
        "samples": samples,
    }
    with open(args.out, "w") as f:
        json.dump(out, f)

    # Per-sample CSV alongside the JSON (same basename), for external analysis.
    csv_path = args.out.rsplit(".", 1)[0] + ".csv"
    with open(csv_path, "w") as f:
        f.write("t_s,latency_ms,ok,phase,frontend,switch_at_s,pad_bytes\n")
        for s in samples:
            f.write("%s,%s,%d,%s,%s,%s,%d\n" % (
                s["t"], s["latency_ms"], 1 if s["ok"] else 0,
                s["phase"], s["frontend"], switch_rel, args.pad_bytes))

    ok_lat = [s["latency_ms"] for s in samples if s["ok"]]
    pre = [s["latency_ms"] for s in samples if s["ok"] and s["phase"] == "paxos"]
    post = [s["latency_ms"] for s in samples if s["ok"] and s["phase"] == "primary-backup"]
    print(f"[done] {len(samples)} reqs ({len(ok_lat)} ok) -> {args.out}")
    if pre:
        print(f"  paxos          median={statistics.median(pre):.1f}ms  n={len(pre)}")
    if post:
        print(f"  primary-backup median={statistics.median(post):.1f}ms  n={len(post)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
