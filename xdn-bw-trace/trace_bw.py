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
import json
import os
import random
import re
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import requests

# Reconfigurator HTTP port = NIO port + 300 (HTTP_PORT_OFFSET in
# ReconfigurationConfig.java). Used to query the placement endpoint below.
HTTP_PORT_OFFSET = 300

# Endpoint served by HttpReconfigurator (see
# src/edu/umass/cs/reconfiguration/http/HttpReconfigurator.java) and consumed
# by `xdn service info` (xdn-cli/cmd/service.go). Returns
#   {"DATA": {"NODES": [{"ID": "AR1",
#                        "ADDRESS": "/127.0.0.1:2001",
#                        "HTTP_ADDRESS": "/127.0.0.1:2301",
#                        "ROLE": "leader|...|"}, ...]}, ...}
PLACEMENT_PATH_FMT = "/api/v2/services/{service}/placement"

DEFAULT_BPFTRACE_SCRIPT = Path(__file__).resolve().parent / "inter_replica_bw.bt"
DEFAULT_RESULTS_DIR = Path(__file__).resolve().parent / "results"

HEADER_LINE_RE = re.compile(r"^# trace start:")
# Matches lines like:  @out[14489, 52896, ::ffff:127.0.0.1, 2003]: 71
MAP_LINE_RE = re.compile(
    r"^@(out|in)\[\s*"
    r"(\d+)\s*,\s*"                       # pid
    r"(\d+)\s*,\s*"                       # local port
    r"([0-9a-fA-F:.]+)\s*,\s*"            # peer ip
    r"(\d+)\s*"                           # peer port
    r"\]:\s*(\d+)\s*$"                    # bytes
)

# AR JVM cmdlines look like
#   java ... edu.umass.cs.reconfiguration.ReconfigurableNode AR1
# We pull the AR id by walking the null-separated cmdline once at startup.
RECONF_NODE_CLASS = "edu.umass.cs.reconfiguration.ReconfigurableNode"


def build_pid_to_ar():
    """Scan /proc and return {pid: ar_id} for every running AR JVM.

    Used to attribute each bpftrace event to a specific replica via the
    `pid` builtin, without needing handshake-time hooks (i.e., bpftrace can
    attach to an already-running cluster).
    """
    mapping = {}
    proc = Path("/proc")
    for entry in proc.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            cmdline = (entry / "cmdline").read_bytes()
        except (OSError, PermissionError):
            continue
        if not cmdline:
            continue
        parts = cmdline.split(b"\0")
        for i, p in enumerate(parts):
            if p.decode("utf-8", "replace") == RECONF_NODE_CLASS:
                if i + 1 < len(parts):
                    ar_id = parts[i + 1].decode("utf-8", "replace")
                    if ar_id:
                        mapping[int(entry.name)] = ar_id
                break
    return mapping


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


def parse_java_inet_addr(s):
    """Parse a Java InetSocketAddress.toString() value like '/127.0.0.1:2001'
    or 'host/127.0.0.1:2001' into (host, port)."""
    if s is None:
        return None
    addr = s.split("/", 1)[-1]  # drop the optional 'hostname/' prefix
    if ":" not in addr:
        return None
    host, _, port_s = addr.rpartition(":")
    try:
        return host, int(port_s)
    except ValueError:
        return None


def fetch_service_placement(rc_host, rc_http_port, service, timeout=5.0):
    """GET /api/v2/services/<service>/placement on the Reconfigurator.

    Returns a list of nodes preserving server-side order:
        [{"id": "AR1",
          "nio_host": "127.0.0.1", "nio_port": 2001,
          "http_host": "127.0.0.1", "http_port": 2301,
          "role": "leader" | "" | ...,
          "geolocation": (lat, lon) | None}, ...]
    """
    url = f"http://{rc_host}:{rc_http_port}{PLACEMENT_PATH_FMT.format(service=service)}"
    try:
        resp = requests.get(url, timeout=timeout)
    except requests.RequestException as e:
        sys.exit(f"error: cannot reach reconfigurator at {url}: {e}")
    if resp.status_code == 404:
        sys.exit(f"error: service '{service}' not found on reconfigurator "
                 f"({url}). Is it deployed?")
    if resp.status_code != 200:
        sys.exit(f"error: reconfigurator returned HTTP {resp.status_code} "
                 f"for {url}: {resp.text[:200]}")
    try:
        body = resp.json()
        raw_nodes = body["DATA"]["NODES"]
    except (ValueError, KeyError, TypeError) as e:
        sys.exit(f"error: unexpected placement response shape from {url}: {e}")

    nodes = []
    for n in raw_nodes:
        nio = parse_java_inet_addr(n.get("ADDRESS"))
        http = parse_java_inet_addr(n.get("HTTP_ADDRESS"))
        if nio is None or http is None:
            continue
        geo = n.get("GEOLOCATION")
        geolocation = None
        if isinstance(geo, dict):
            try:
                geolocation = (float(geo["LATITUDE"]),
                               float(geo["LONGITUDE"]))
            except (KeyError, TypeError, ValueError):
                geolocation = None
        nodes.append({
            "id": n.get("ID", ""),
            "nio_host": nio[0], "nio_port": nio[1],
            "http_host": http[0], "http_port": http[1],
            "role": n.get("ROLE", "") or "",
            "geolocation": geolocation,
        })
    if not nodes:
        sys.exit(f"error: placement for '{service}' contained no usable nodes")
    return nodes


def fetch_replica_info(http_host, http_port, service, timeout=5.0):
    """GET /api/v2/services/<service>/replica/info on an AR's HTTP
    frontend. Returns service-wide metadata that the RC's placement
    endpoint doesn't expose (protocol, consistency model, requested
    consistency, deterministic flag). The AR HTTP frontend requires the
    `XDN` header to route the request into the XDN-specific handler.

    Best-effort: warns and returns an empty dict if the AR can't be
    reached or the response is malformed, so the caller can still write
    out a meta.json without this field.
    """
    url = (f"http://{http_host}:{http_port}"
           f"/api/v2/services/{service}/replica/info")
    headers = {"XDN": service}
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
    except requests.RequestException as e:
        print(f"warning: replica/info fetch from {url} failed: {e}",
              file=sys.stderr)
        return {}
    if r.status_code != 200:
        print(f"warning: replica/info {url} returned HTTP {r.status_code}",
              file=sys.stderr)
        return {}
    try:
        body = r.json()
    except ValueError as e:
        print(f"warning: replica/info JSON parse failed for {url}: {e}",
              file=sys.stderr)
        return {}
    return {
        "protocol": body.get("protocol", "") or "",
        "consistency": body.get("consistency", "") or "",
        "requested_consistency": body.get("requestedConsistency", "") or "",
        "deterministic": body.get("deterministic"),
    }


def select_targets(nodes, target_arg):
    """Pick the set of replicas to drive HTTP traffic at.

    `target_arg` may be `None` (drive every replica in the placement) or a
    comma-separated allowlist of replica ids (e.g. "AR1,AR3"). All matched
    nodes are returned in placement order; unknown ids are an error.
    """
    if not target_arg:
        return list(nodes)
    requested = [t.strip() for t in target_arg.split(",") if t.strip()]
    by_id = {n["id"]: n for n in nodes}
    missing = [t for t in requested if t not in by_id]
    if missing:
        sys.exit(f"error: --target value(s) {missing} not in placement "
                 f"({[n['id'] for n in nodes]})")
    seen = set()
    out = []
    for t in requested:
        if t in seen:
            continue
        seen.add(t)
        out.append(by_id[t])
    return out


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", required=True,
                   help="path to gigapaxos.properties (used to locate the "
                        "reconfigurator host:port)")
    p.add_argument("--service", required=True,
                   help="XDN service name (sent in the XDN: header)")
    p.add_argument("--target",
                   help="comma-separated allowlist of replica ids (e.g. "
                        "'AR1,AR3') to drive traffic at. Each id must be in "
                        "the placement. Default: drive every replica hosting "
                        "the service, in parallel.")
    p.add_argument("--reconfigurator",
                   help="reconfigurator HTTP endpoint as host[:port] "
                        "(default: derive host from the first reconfigurator "
                        "in --config, port = NIO port + 300).")
    # Read spec (--path / --method preserved as the "read request"). Defaults
    # target a specific bookcatalog item so the response payload is the same
    # constant book row on every read.
    p.add_argument("--path", default="/api/books/1",
                   help="HTTP path used for READ requests. Defaults to a "
                        "specific book id so the response payload is "
                        "constant. Override for non-bookcatalog services.")
    p.add_argument("--method", default="GET",
                   help="HTTP method for READ requests. Default: GET.")
    # Read/write mix.
    p.add_argument("--read-ratio", type=float, default=0.8,
                   help="Fraction of requests that are reads (vs. writes). "
                        "Default: 0.8 (80%% read / 20%% write). "
                        "Use 1.0 for read-only, 0.0 for write-only.")
    # Write spec.
    p.add_argument("--write-path", default="/api/books/1",
                   help="HTTP path used for WRITE requests. Default targets "
                        "the same book id as --path, so writes mutate the "
                        "row that reads observe.")
    p.add_argument("--write-method", default="PUT",
                   help="HTTP method for WRITE requests. Default: PUT.")
    p.add_argument("--write-body",
                   default='{"title":"trace_bw","author":"benchmark"}',
                   help="Request body for WRITE requests (sent as bytes).")
    p.add_argument("--write-content-type", default="application/json",
                   help="Content-Type header for WRITE requests. "
                        "Default: application/json.")
    # Seeding the target book so reads/writes find it.
    p.add_argument("--seed-path", default="/api/books",
                   help="HTTP path used to create the target item if it does "
                        "not exist before traffic starts. Default: "
                        "/api/books (bookcatalog).")
    p.add_argument("--seed-body",
                   default='{"id":1,"title":"trace_bw","author":"benchmark"}',
                   help="Request body posted to --seed-path during the "
                        "seeding step.")
    p.add_argument("--no-seed", action="store_true",
                   help="Skip the seeding step. Use this when the target "
                        "item is already present, or when the service has "
                        "no create endpoint at --seed-path.")
    p.add_argument("--rate", type=float, default=20.0,
                   help="Per-replica HTTP request rate (req/s). Each replica "
                        "is driven by its own client thread at this rate, so "
                        "the total offered load is rate * len(targets). "
                        "Default: 20.")
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
        @out[<pid>, <local_port>, <peer_ip>, <peer_port>]: <bytes>
        @in[<pid>, <local_port>, <peer_ip>, <peer_port>]: <bytes>
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
                pid = int(m.group(2))
                local_port = int(m.group(3))
                ip = m.group(4)
                peer_port = int(m.group(5))
                nbytes = int(m.group(6))

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
                        "local_pid": pid,
                        "local_port": local_port,
                        "peer_ip": ip,
                        "peer_port": peer_port,
                        "bytes": nbytes,
                    })
        except Exception as e:
            self.error = e

    def snapshot_rows(self):
        with self.lock:
            return list(self.rows)


CLIENT_PREFIX = "client"


def _client_label(ar_id):
    """Per-replica client vertex label (e.g. 'client-AR1'). Falls back to a
    bare 'client' if the serving AR can't be determined."""
    return f"{CLIENT_PREFIX}-{ar_id}" if ar_id else CLIENT_PREFIX


def resolve_rows(rows, pid_to_ar, nio_port_to_ar, http_port_to_ar):
    """Annotate each event with `local_id` and `peer_id`.

    Two flow categories are handled:
      - Inter-replica (NIO): keyed off NIO listener ports. Cross-correlation
        on ephemeral ports recovers the (i, j) pair on loopback.
      - Client<->replica (HTTP): keyed off AR HTTP frontend listener ports.
        The non-AR side is labeled as `client-<AR>` (one client vertex per
        replica) so the resulting graph keeps each AR's request/response
        traffic on its own pair of edges instead of collapsing every external
        client into a single hub vertex.

    Resolution rules per row:
      local_id =
        pid_to_ar[pid]                              if pid is a known AR JVM
        else nio_port_to_ar[local_port]             if local is a NIO listener
        else http_port_to_ar[local_port]            if local is an HTTP listener
        else ephemeral_owner[local_port]            if local is an AR ephemeral
        else "client-<peer_ar>"                     if peer_port is an HTTP listener
        else "unknown"
      peer_id =
        nio_port_to_ar[peer_port]                   if peer is a NIO listener
        else http_port_to_ar[peer_port]             if peer is an HTTP listener
        else ephemeral_owner[peer_port]             if peer is an AR ephemeral
        else "client-<local_ar>"                    if local_port is an HTTP listener
        else "unknown"
    """
    nio_listener_ports = set(nio_port_to_ar.keys())
    http_listener_ports = set(http_port_to_ar.keys())
    listener_ports = nio_listener_ports | http_listener_ports

    # Pass 1: ephemeral_port -> ar_id (only when the local pid is known and
    # the local port is *not* a well-known listener port).
    ephemeral_owner = {}
    for r in rows:
        local = pid_to_ar.get(r["local_pid"])
        if not local:
            continue
        lp = r["local_port"]
        if lp in listener_ports:
            continue
        # Last writer wins on ephemeral reuse — vanishingly rare in short
        # traces, and the alternative (silently dropping) is worse.
        ephemeral_owner[lp] = local

    # Pass 2: annotate.
    for r in rows:
        lp = r["local_port"]
        pp = r["peer_port"]
        local = pid_to_ar.get(r["local_pid"])
        if not local:
            local = (nio_port_to_ar.get(lp)
                     or http_port_to_ar.get(lp)
                     or ephemeral_owner.get(lp))
        if not local and pp in http_listener_ports:
            local = _client_label(http_port_to_ar.get(pp))
        r["local_id"] = local or "unknown"

        peer = (nio_port_to_ar.get(pp)
                or http_port_to_ar.get(pp)
                or ephemeral_owner.get(pp))
        if not peer and lp in http_listener_ports:
            peer = _client_label(http_port_to_ar.get(lp))
        r["peer_id"] = peer or "unknown"
    return rows


def http_loop(base_url, headers, rate, duration, read_spec, write_spec,
              read_ratio, timeout, rng=None):
    """Drive a mixed read/write workload at `rate` req/s for `duration` s.

    `base_url`     — http://host:port (no path).
    `read_spec`    — {"method": ..., "path": ...}
    `write_spec`   — {"method": ..., "path": ..., "body": str|None,
                       "content_type": str|None}
    `read_ratio`   — probability that each request is a read.

    Returns a dict {sent, ok, failed, reads, writes}.
    Pattern adapted from eval/geo_demand_smoke.py:125,139,145-148,167-170 —
    rate-limited via sleep against a wall-clock schedule.
    """
    if rng is None:
        rng = random.Random()
    period = 1.0 / rate if rate > 0 else 0.0
    sent = ok = failed = reads = writes = 0
    write_body_bytes = (write_spec["body"].encode("utf-8")
                        if write_spec.get("body") is not None else None)
    write_extra_headers = dict(headers)
    if write_spec.get("content_type"):
        write_extra_headers["Content-Type"] = write_spec["content_type"]
    session = requests.Session()
    start = time.monotonic()
    deadline = start + duration
    i = 0
    try:
        while time.monotonic() < deadline:
            is_read = rng.random() < read_ratio
            if is_read:
                spec = read_spec
                req_headers = headers
                req_body = None
                reads += 1
            else:
                spec = write_spec
                req_headers = write_extra_headers
                req_body = write_body_bytes
                writes += 1
            url = base_url + spec["path"]
            try:
                resp = session.request(spec["method"], url,
                                       headers=req_headers, data=req_body,
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
    finally:
        # Close pooled keep-alive sockets cleanly (FIN, not RST). Without
        # this, abrupt teardown when the thread's deadline fires shows up
        # on the AR side as "HttpActiveReplicaHandler Error: Connection
        # reset" (HttpActiveReplica.java:1218).
        session.close()
    return {"sent": sent, "ok": ok, "failed": failed,
            "reads": reads, "writes": writes}


def seed_target_item(base_url, headers, args):
    """Best-effort: ensure --path is reachable on the cluster before traffic
    starts, by POSTing --seed-body to --seed-path if --path returns 404.

    Replication is linearizable for bookcatalog so a single POST to any
    replica is enough. Failures are logged and tolerated; we don't abort on
    seeding errors because some workloads don't need it.
    """
    if args.no_seed:
        return
    probe_url = base_url + args.path
    try:
        r = requests.get(probe_url, headers=headers, timeout=args.timeout)
    except requests.RequestException as e:
        print(f"seed: probe {probe_url} failed: {e}; "
              f"attempting POST anyway", file=sys.stderr)
        r = None
    if r is not None and 200 <= r.status_code < 300:
        print(f"seed: target {args.path} already exists "
              f"(HTTP {r.status_code} from {probe_url})", file=sys.stderr)
        return

    seed_url = base_url + args.seed_path
    seed_headers = dict(headers)
    if args.write_content_type:
        seed_headers["Content-Type"] = args.write_content_type
    try:
        r = requests.post(seed_url,
                          data=args.seed_body.encode("utf-8"),
                          headers=seed_headers,
                          timeout=args.timeout)
    except requests.RequestException as e:
        print(f"seed: warning, POST {seed_url} failed: {e}. "
              f"Continuing anyway — pass --no-seed to silence this.",
              file=sys.stderr)
        return
    if 200 <= r.status_code < 300:
        print(f"seed: created target via POST {seed_url} "
              f"(HTTP {r.status_code})", file=sys.stderr)
    else:
        print(f"seed: warning, POST {seed_url} returned HTTP "
              f"{r.status_code}: {r.text[:200]}", file=sys.stderr)


def main():
    args = parse_args()

    _actives, reconfigurators, _geolocations = parse_properties(args.config)
    if args.reconfigurator:
        rc_endpoint = args.reconfigurator
        if ":" in rc_endpoint:
            rc_host, _, rc_port_s = rc_endpoint.rpartition(":")
            try:
                rc_http_port = int(rc_port_s)
            except ValueError:
                sys.exit(f"error: invalid --reconfigurator '{rc_endpoint}'")
        else:
            rc_host = rc_endpoint
            rc_http_port = 3000 + HTTP_PORT_OFFSET
    else:
        if not reconfigurators:
            sys.exit(f"error: no reconfigurator.<name>=host:port entries in "
                     f"{args.config}; pass --reconfigurator host[:port]")
        rc_name = sorted(reconfigurators.keys())[0]
        rc_host, rc_nio_port = reconfigurators[rc_name]
        rc_http_port = rc_nio_port + HTTP_PORT_OFFSET

    print(f"reconfigurator: http://{rc_host}:{rc_http_port}", file=sys.stderr)
    nodes = fetch_service_placement(rc_host, rc_http_port, args.service)
    targets = select_targets(nodes, args.target)

    nio_ports = sorted({n["nio_port"] for n in nodes})
    nio_low, nio_high = min(nio_ports), max(nio_ports)
    http_ports = sorted({n["http_port"] for n in nodes})
    http_low, http_high = min(http_ports), max(http_ports)

    placement_str = ", ".join(
        f"{n['id']}@{n['nio_host']}:{n['nio_port']}"
        + (f" ({n['role']})" if n["role"] else "")
        for n in nodes
    )
    targets_str = ", ".join(
        f"{n['id']}->http://{n['http_host']}:{n['http_port']}{args.path}"
        for n in targets
    )
    print(f"placement ({len(nodes)} replicas): {placement_str}",
          file=sys.stderr)
    print(f"targets ({len(targets)}, driven in parallel @ {args.rate} req/s "
          f"each): {targets_str}", file=sys.stderr)
    print(f"bpftrace: nio=[{nio_low},{nio_high}] http=[{http_low},{http_high}] "
          f"interval={args.interval}s duration={args.duration}s",
          file=sys.stderr)

    nio_port_to_ar = {n["nio_port"]: n["id"] for n in nodes}
    http_port_to_ar = {n["http_port"]: n["id"] for n in nodes}
    pid_to_ar = build_pid_to_ar()

    # Pull service-wide metadata (protocol + consistency model) from one
    # of the targets. Requires the XDN header; same endpoint that
    # `xdn service info` queries (xdn-cli/cmd/service.go:884).
    service_info = fetch_replica_info(
        targets[0]["http_host"], targets[0]["http_port"], args.service,
        timeout=args.timeout,
    )
    if service_info.get("protocol") or service_info.get("consistency"):
        print(f"service info: protocol={service_info.get('protocol', '?')} "
              f"consistency={service_info.get('consistency', '?')}",
              file=sys.stderr)
    placement_ids = {n["id"] for n in nodes}
    local_ar_pids = {pid: ar for pid, ar in pid_to_ar.items()
                     if ar in placement_ids}
    if local_ar_pids:
        pid_str = ", ".join(f"{ar}=pid {pid}"
                            for pid, ar in sorted(local_ar_pids.items(),
                                                  key=lambda kv: kv[1]))
        print(f"local AR pids: {pid_str}", file=sys.stderr)
    else:
        print("warning: no AR JVMs for this placement found in /proc; "
              "local_id will fall back to port-based resolution",
              file=sys.stderr)

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        DEFAULT_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        out_path = DEFAULT_RESULTS_DIR / f"{args.service}-{int(time.time())}.csv"

    script_path = str(Path(args.bpftrace_script).resolve())
    bpf_cmd = ["bpftrace", script_path,
               str(nio_low), str(nio_high),
               str(http_low), str(http_high),
               str(args.interval)]
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

    if not 0.0 <= args.read_ratio <= 1.0:
        sys.exit(f"error: --read-ratio must be in [0,1] (got {args.read_ratio})")

    headers = {"XDN": args.service}
    read_spec = {"method": args.method, "path": args.path}
    write_spec = {"method": args.write_method, "path": args.write_path,
                  "body": args.write_body,
                  "content_type": args.write_content_type}

    # Seed the target item via the first target's HTTP frontend, before the
    # warmup window. This way GETs and PUTs against --path see HTTP 200 from
    # the first request, instead of a flood of 404s while replication
    # propagates a write that didn't happen yet.
    seed_base = (f"http://{targets[0]['http_host']}:"
                 f"{targets[0]['http_port']}")
    seed_target_item(seed_base, headers, args)

    if args.warmup > 0:
        time.sleep(args.warmup)

    write_ratio = 1.0 - args.read_ratio
    print(f"driving traffic in parallel: {len(targets)} replicas @ "
          f"{args.rate} req/s each (total ~{args.rate * len(targets):g} req/s) "
          f"for {args.duration}s, mix=read {args.read_ratio:.0%} "
          f"({args.method} {args.path}) / write {write_ratio:.0%} "
          f"({args.write_method} {args.write_path}) ...",
          file=sys.stderr)

    # One client thread per target replica. Threads start back-to-back; the
    # tiny startup skew (sub-millisecond on a single host) is negligible
    # against `--duration` and `--warmup`. Each thread gets its own RNG so
    # the read/write mix is independent across replicas.
    results = {}
    errors = {}
    threads = []
    base_seed = int(time.time() * 1e6)

    def worker(node, seed):
        base = f"http://{node['http_host']}:{node['http_port']}"
        try:
            results[node["id"]] = http_loop(
                base, headers, args.rate, args.duration,
                read_spec, write_spec, args.read_ratio, args.timeout,
                rng=random.Random(seed),
            )
        except Exception as e:
            errors[node["id"]] = e
            results[node["id"]] = {"sent": 0, "ok": 0, "failed": 0,
                                   "reads": 0, "writes": 0}

    t0 = time.monotonic()
    for idx, n in enumerate(targets):
        th = threading.Thread(target=worker, args=(n, base_seed + idx),
                              daemon=True, name=f"client-{n['id']}")
        th.start()
        threads.append(th)
    for th in threads:
        th.join()
    elapsed = time.monotonic() - t0

    total_sent = sum(r["sent"] for r in results.values())
    total_ok = sum(r["ok"] for r in results.values())
    total_failed = sum(r["failed"] for r in results.values())
    total_reads = sum(r["reads"] for r in results.values())
    total_writes = sum(r["writes"] for r in results.values())
    per_replica = ", ".join(
        f"{nid}: sent={results[nid]['sent']} ok={results[nid]['ok']} "
        f"failed={results[nid]['failed']} "
        f"r/w={results[nid]['reads']}/{results[nid]['writes']}"
        for nid in (n["id"] for n in targets)
    )
    print(f"traffic done: sent={total_sent} ok={total_ok} "
          f"failed={total_failed} reads={total_reads} writes={total_writes} "
          f"elapsed={elapsed:.1f}s | per-replica: {per_replica}",
          file=sys.stderr)
    for nid, e in errors.items():
        print(f"warning: client thread for {nid} crashed: {e}",
              file=sys.stderr)

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

    resolve_rows(rows, pid_to_ar, nio_port_to_ar, http_port_to_ar)

    unresolved_local = sum(1 for r in rows if r["local_id"] == "unknown")
    unresolved_peer = sum(1 for r in rows if r["peer_id"] == "unknown")
    if unresolved_local or unresolved_peer:
        print(f"warning: {unresolved_local}/{len(rows)} rows with unresolved "
              f"local_id, {unresolved_peer}/{len(rows)} with unresolved peer_id",
              file=sys.stderr)

    fieldnames = [
        "interval_start_unix", "interval_end_unix", "direction",
        "local_id", "local_pid", "local_port",
        "peer_id", "peer_ip", "peer_port",
        "bytes",
    ]
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in fieldnames})
    print(f"wrote {len(rows)} rows -> {out_path}", file=sys.stderr)

    # Sidecar metadata so plot_bw_graph.py can render a workload subtitle
    # without needing CLI flags repeated. Co-located so `--output` users
    # get the meta in the same directory.
    meta_path = out_path.with_name(out_path.stem + ".meta.json")
    meta = {
        "service": args.service,
        "service_info": service_info,
        "rate_per_replica_rps": args.rate,
        "duration_seconds": args.duration,
        "interval_seconds": args.interval,
        "warmup_seconds": args.warmup,
        "num_targets": len(targets),
        "target_ids": [n["id"] for n in targets],
        "placement_ids": [n["id"] for n in nodes],
        "read_ratio": args.read_ratio,
        "read": {"method": args.method, "path": args.path},
        "write": {"method": args.write_method, "path": args.write_path,
                  "content_type": args.write_content_type},
        "totals": {
            "sent": total_sent, "ok": total_ok, "failed": total_failed,
            "reads": total_reads, "writes": total_writes,
            "elapsed_seconds": elapsed,
        },
    }
    try:
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=2)
        print(f"wrote metadata -> {meta_path}", file=sys.stderr)
    except OSError as e:
        print(f"warning: could not write {meta_path}: {e}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
