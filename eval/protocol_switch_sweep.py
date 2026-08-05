#!/usr/bin/env python3
"""Steady-state latency vs request size, for one running service (Paxos or PB).

Warms up ONCE (past the ~90s gigapaxos ramp), then measures median latency at
each request size against a fixed frontend. Run it against a Paxos service and
again against the same service after switching to primary-backup, then plot
both curves with eval/plot_switch_sweep.py to find the crossover request size
(the decision threshold for a dynamic protocol selector).

  python3 eval/protocol_switch_sweep.py --frontend 10.10.1.1:2300 \
      --service bookcatalog --update-id 1 --label paxos \
      --sizes 1024,4096,16384,65536,262144,1048576 \
      --warmup 100 --per-size 40 --rate 4 --workers 3 --out /tmp/sweep-paxos.json
"""
import argparse
import base64
import json
import os
import queue
import statistics
import threading
import time
import urllib.request


def body(book_id, n):
    blob = base64.b64encode(os.urandom((n * 3) // 4)).decode()
    return json.dumps({"id": book_id, "title": "T", "author": "A", "blob": blob}).encode()


def seed(frontend, service, book_id):
    d = json.dumps({"id": book_id, "title": "T", "author": "A"}).encode()
    r = urllib.request.Request(f"http://{frontend}/api/books", data=d, method="POST",
                               headers={"XDN": service, "Content-Type": "application/json"})
    try:
        urllib.request.urlopen(r, timeout=15).read()
    except Exception:
        pass


def put(frontend, service, book_id, n):
    r = urllib.request.Request(f"http://{frontend}/api/books/{book_id}", data=body(book_id, n),
                               method="PUT", headers={"XDN": service, "Content-Type": "application/json"})
    t0 = time.time()
    try:
        urllib.request.urlopen(r, timeout=30).read()
        ok = True
    except Exception:
        ok = False
    return (time.time() - t0) * 1000.0, ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--frontend", required=True)
    ap.add_argument("--service", default="bookcatalog")
    ap.add_argument("--update-id", type=int, default=1)
    ap.add_argument("--label", required=True, help="paxos | primary-backup")
    ap.add_argument("--sizes", required=True, help="comma-separated request sizes in bytes")
    ap.add_argument("--warmup", type=float, default=100.0)
    ap.add_argument("--per-size", type=float, default=40.0)
    ap.add_argument("--rate", type=float, default=4.0)
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    sizes = [int(x) for x in args.sizes.split(",")]
    cur = {"n": sizes[0]}                 # current request size for workers
    lat = []                              # (size, latency_ms, ok) collected in the active window
    lat_lock = threading.Lock()
    recording = threading.Event()
    stop = threading.Event()
    jobs = queue.Queue(maxsize=args.workers * 4)

    def worker():
        while not stop.is_set():
            try:
                jobs.get(timeout=0.5)
            except queue.Empty:
                continue
            n = cur["n"]
            ms, ok = put(args.frontend, args.service, args.update_id, n)
            if recording.is_set():
                with lat_lock:
                    lat.append((n, ms, ok))
            jobs.task_done()

    seed(args.frontend, args.service, args.update_id)
    time.sleep(1)
    pool = [threading.Thread(target=worker, daemon=True) for _ in range(args.workers)]
    for w in pool:
        w.start()

    interval = 1.0 / args.rate if args.rate > 0 else 0

    def drive(seconds):
        t0 = time.time()
        while time.time() - t0 < seconds:
            now = time.time()
            try:
                jobs.put(1, timeout=1.0)
            except queue.Full:
                pass
            if interval:
                s = interval - (time.time() - now)
                if s > 0:
                    time.sleep(s)

    # Warm up at the largest size (heaviest path) so every measured point is post-ramp.
    print(f"[{args.label}] warmup {args.warmup:.0f}s", flush=True)
    cur["n"] = sizes[-1]
    drive(args.warmup)

    results = []
    recording.set()
    for n in sizes:
        cur["n"] = n
        with lat_lock:
            lat.clear()
        drive(args.per_size)
        with lat_lock:
            oks = [ms for (sz, ms, ok) in lat if ok and sz == n]
        med = statistics.median(oks) if oks else None
        p90 = statistics.quantiles(oks, n=10)[8] if len(oks) >= 10 else None
        results.append({"size": n, "median_ms": med, "p90_ms": p90, "n": len(oks)})
        print(f"  size={n:8d}B  median={med}  n={len(oks)}", flush=True)

    stop.set()
    out = {"label": args.label, "frontend": args.frontend, "service": args.service,
           "results": results}
    with open(args.out, "w") as f:
        json.dump(out, f)
    # CSV too
    with open(args.out.rsplit(".", 1)[0] + ".csv", "w") as f:
        f.write("label,size_bytes,median_ms,p90_ms,n\n")
        for r in results:
            f.write(f"{args.label},{r['size']},{r['median_ms']},{r['p90_ms']},{r['n']}\n")
    print(f"[done] -> {args.out}")


if __name__ == "__main__":
    main()
