#!/usr/bin/env python3
"""trace_bw.py — drive HTTP traffic at one XDN service while bpftrace counts
inter-replica TCP bytes, then write a per-interval CSV.

Usage:
    sudo python3 xdn-bw-trace/trace_bw.py \
        --config conf/gigapaxos.xdn.local.properties \
        --service bookcatalog \
        --duration 30 --interval 5 --rate 20

Pre-flight:
  - Linux host with bpftrace installed (apt install bpftrace).
  - XDN cluster running with the given properties file.
  - Service deployed (e.g. xdn launch bookcatalog --image=... --state=... --deterministic=true).
  - Run as root, or with passwordless sudo (bpftrace requires CAP_BPF).

This first cut assumes a single service is hosted in the cluster, so the
cluster-wide (peer_i, peer_j) byte matrix from bpftrace is itself the
per-service bandwidth map (no attribution needed).
"""

import argparse
import csv
import os
import re
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import requests

# AR HTTP frontend port = NIO port + 300. The offset is hardcoded in the XDN
# codebase (see CLAUDE.md "Local ports" line); this script mirrors that.
HTTP_PORT_OFFSET = 300

DEFAULT_BPFTRACE_SCRIPT = Path(__file__).resolve().parent / "inter_replica_bw.bt"
DEFAULT_RESULTS_DIR = Path(__file__).resolve().parent / "results"

HEADER_LINE_RE = re.compile(r"^# trace start:")
# Matches lines like:  @out[10.0.0.5, 2001]: 1234567
MAP_LINE_RE = re.compile(
    r"^@(out|in)\[\s*([0-9a-fA-F:.]+)\s*,\s*(\d+)\s*\]:\s*(\d+)\s*$"
)


def parse_properties(path):
    """Parse a gigapaxos properties file.

    Returns (actives, reconfigurators, geolocations) where:
      actives        = {name: (host, nio_port)}
      reconfigurators = {name: (host, port)}
      geolocations   = {name: (lat, lon)}
    """
    actives = {}
    reconfigurators = {}
    geolocations = {}
    with open(path) as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"')

            if key.startswith("active.") and key.endswith(".geolocation"):
                name = key[len("active."):-len(".geolocation")]
                try:
                    lat_s, lon_s = value.split(",", 1)
                    geolocations[name] = (float(lat_s), float(lon_s))
                except ValueError:
                    pass
            elif key.startswith("active."):
                name = key[len("active."):]
                if ":" in value:
                    host, port_s = value.rsplit(":", 1)
                    try:
                        actives[name] = (host, int(port_s))
                    except ValueError:
                        pass
            elif key.startswith("reconfigurator."):
                name = key[len("reconfigurator."):]
                if ":" in value:
                    host, port_s = value.rsplit(":", 1)
                    try:
                        reconfigurators[name] = (host, int(port_s))
                    except ValueError:
                        pass
    return actives, reconfigurators, geolocations


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", required=True,
                   help="path to gigapaxos.properties")
    p.add_argument("--service", required=True,
                   help="XDN service name (sent in the XDN: header)")
    p.add_argument("--target",
                   help="active replica name to drive HTTP requests against. "
                        "Default: the first active.<name> entry in the config.")
    p.add_argument("--path", default="/",
                   help="HTTP request path. Default: /")
    p.add_argument("--method", default="GET",
                   help="HTTP method. Default: GET")
    p.add_argument("--rate", type=float, default=20.0,
                   help="HTTP request rate (req/s). Default: 20")
    p.add_argument("--duration", type=float, default=60.0,
                   help="Total trace duration in seconds. Default: 60")
    p.add_argument("--interval", type=int, default=5,
                   help="bpftrace snapshot interval in seconds. Default: 5")
    p.add_argument("--warmup", type=float, default=2.0,
                   help="Seconds between bpftrace start and HTTP traffic start. "
                        "Default: 2")
    p.add_argument("--output",
                   help="CSV output path. Default: "
                        "xdn-bw-trace/results/<service>-<unix-ts>.csv")
    p.add_argument("--bpftrace-script", default=str(DEFAULT_BPFTRACE_SCRIPT),
                   help="path to .bt script. Default: sibling inter_replica_bw.bt")
    p.add_argument("--timeout", type=float, default=5.0,
                   help="Per-request HTTP timeout in seconds. Default: 5")
    return p.parse_args()


class BpftraceReader(threading.Thread):
    """Tail bpftrace stdout; parse SNAP / @out / @in / END frames into rows.

    Frame model (one snapshot block emitted every interval, plus one on END):
        SNAP
        @out[<ip>, <port>]: <bytes>
        @in[<ip>, <port>]: <bytes>
        END

    Each row's interval is [previous_snap_wall, current_snap_wall]. For the
    very first snapshot we synthesize a start by subtracting `interval`.
    """

    def __init__(self, proc, on_header, interval_seconds):
        super().__init__(daemon=True)
        self.proc = proc
        self.on_header = on_header
        self.interval_seconds = interval_seconds
        self.rows = []
        self.lock = threading.Lock()
        self.error = None
        self._headed = False
        self._prev_snap_wall = None
        self._cur_snap_wall = None

    def run(self):
        try:
            assert self.proc.stdout is not None
            for line in self.proc.stdout:
                line = line.rstrip("\n")
                if not line:
                    continue
                if HEADER_LINE_RE.match(line):
                    if not self._headed:
                        self._headed = True
                        self.on_header()
                    continue
                if line.startswith("SNAP"):
                    now = time.time()
                    self._prev_snap_wall = self._cur_snap_wall
                    self._cur_snap_wall = now
                    continue
                if line == "END":
                    continue
                m = MAP_LINE_RE.match(line)
                if not m:
                    # bpftrace also emits "Attaching N probes..." etc on stderr,
                    # but tolerate any uninteresting stdout chatter here too.
                    continue
                direction = m.group(1)
                ip = m.group(2)
                port = int(m.group(3))
                nbytes = int(m.group(4))

                if self._cur_snap_wall is None:
                    # Map output before any SNAP is unexpected; skip it.
                    continue
                end_ts = self._cur_snap_wall
                if self._prev_snap_wall is not None:
                    start_ts = self._prev_snap_wall
                else:
                    start_ts = end_ts - self.interval_seconds

                with self.lock:
                    self.rows.append({
                        "interval_start_unix": int(start_ts),
                        "interval_end_unix": int(end_ts),
                        "direction": direction,
                        "peer_ip": ip,
                        "peer_port": port,
                        "bytes": nbytes,
                    })
        except Exception as e:
            self.error = e

    def snapshot_rows(self):
        with self.lock:
            return list(self.rows)


def http_loop(target_url, headers, rate, duration, method, timeout):
    """Drive HTTP requests at the given rate for `duration` seconds.

    Pattern adapted from eval/geo_demand_smoke.py:125,139,145-148,167-170 —
    rate-limited via sleep against a wall-clock schedule.
    """
    period = 1.0 / rate if rate > 0 else 0.0
    sent = ok = failed = 0
    session = requests.Session()
    start = time.monotonic()
    deadline = start + duration
    i = 0
    while time.monotonic() < deadline:
        try:
            resp = session.request(method, target_url, headers=headers,
                                   timeout=timeout)
            sent += 1
            if 200 <= resp.status_code < 500:
                ok += 1
            else:
                failed += 1
        except requests.RequestException:
            sent += 1
            failed += 1
        i += 1
        if period > 0:
            target = start + i * period
            sleep_for = target - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)
    return sent, ok, failed


def main():
    args = parse_args()

    actives, _reconfigurators, _geolocations = parse_properties(args.config)
    if not actives:
        sys.exit(f"error: no active.<name>=host:port entries in {args.config}")

    if args.target is None:
        target_name = sorted(actives.keys())[0]
    else:
        target_name = args.target
        if target_name not in actives:
            sys.exit(f"error: --target '{target_name}' not in config; "
                     f"available: {sorted(actives)}")
    target_host, target_nio = actives[target_name]
    target_http = target_nio + HTTP_PORT_OFFSET

    nio_ports = sorted({p for _, p in actives.values()})
    port_low = min(nio_ports)
    port_high = max(nio_ports)

    print(f"cluster: {len(actives)} ARs, NIO ports {nio_ports}",
          file=sys.stderr)
    print(f"target: {target_name} -> "
          f"http://{target_host}:{target_http}{args.path}",
          file=sys.stderr)
    print(f"bpftrace: ports=[{port_low},{port_high}] "
          f"interval={args.interval}s duration={args.duration}s",
          file=sys.stderr)

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        DEFAULT_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        out_path = DEFAULT_RESULTS_DIR / f"{args.service}-{int(time.time())}.csv"

    script_path = str(Path(args.bpftrace_script).resolve())
    bpf_cmd = ["bpftrace", script_path,
               str(port_low), str(port_high), str(args.interval)]
    if os.geteuid() != 0:
        bpf_cmd = ["sudo"] + bpf_cmd

    print(f"launching: {' '.join(bpf_cmd)}", file=sys.stderr)
    proc = subprocess.Popen(
        bpf_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )

    header_event = threading.Event()
    reader = BpftraceReader(proc, on_header=header_event.set,
                            interval_seconds=args.interval)
    reader.start()

    if not header_event.wait(timeout=15.0):
        try:
            stderr_data = proc.stderr.read() if proc.stderr else ""
        except Exception:
            stderr_data = ""
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except Exception:
            pass
        sys.exit("error: bpftrace did not emit '# trace start:' within 15s.\n"
                 f"stderr:\n{stderr_data}")

    if args.warmup > 0:
        time.sleep(args.warmup)

    target_url = f"http://{target_host}:{target_http}{args.path}"
    headers = {"XDN": args.service}
    print(f"driving traffic: {args.rate} req/s for {args.duration}s ...",
          file=sys.stderr)
    t0 = time.monotonic()
    sent, ok, failed = http_loop(
        target_url, headers, args.rate, args.duration, args.method, args.timeout
    )
    elapsed = time.monotonic() - t0
    print(f"traffic done: sent={sent} ok={ok} failed={failed} "
          f"elapsed={elapsed:.1f}s", file=sys.stderr)

    # Give bpftrace one more interval tick so the last full window is captured
    # before we ask it to flush via SIGINT.
    final_wait = max(args.interval + 1, 2)
    print(f"waiting {final_wait}s for final bpftrace snapshot ...",
          file=sys.stderr)
    time.sleep(final_wait)

    print("stopping bpftrace ...", file=sys.stderr)
    try:
        proc.send_signal(signal.SIGINT)
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

    # Allow reader thread to drain any final lines.
    reader.join(timeout=5)

    rows = reader.snapshot_rows()
    if reader.error:
        print(f"warning: reader thread error: {reader.error}", file=sys.stderr)

    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["interval_start_unix", "interval_end_unix",
                        "direction", "peer_ip", "peer_port", "bytes"],
        )
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    print(f"wrote {len(rows)} rows -> {out_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
