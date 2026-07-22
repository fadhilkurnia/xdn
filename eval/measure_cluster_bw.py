#!/usr/bin/env python3
"""Phased bandwidth-graph measurement for XDN cluster-mode services.

Runs from the driver machine against a launched cluster service. Samples every
replica's `bandwidth` section (replica-info endpoint) on a fixed cadence while
driving phased load (idle -> write -> read), and writes the full edge
time-series to JSON for offline analysis. The interesting output is the
per-phase, per-edge byte deltas: protocols leave distinct signatures (Raft's
leader star, Group Replication's certification broadcast, ...).

Examples (from the driver):
  python3 eval/measure_cluster_bw.py --service etcd-demo --kind etcd \
      --ars 10.10.1.1,10.10.1.2,10.10.1.3 --out /tmp/bw-etcd.json
  python3 eval/measure_cluster_bw.py --service rqlite-demo --kind rqlite ...
  python3 eval/measure_cluster_bw.py --service mysql-demo --kind mysql \
      --mysql-host 10.10.1.1 --mysql-port 61234 --mysql-password xdnpass123 ...

Only stdlib is used; MySQL load shells out to the `mysql` client binary.
"""

import argparse
import base64
import json
import subprocess
import sys
import threading
import time
import urllib.request


def http(url, headers=None, data=None, timeout=5):
    req = urllib.request.Request(url, data=data, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, resp.read().decode()


class Sampler(threading.Thread):
    """Polls every AR's replica-info bandwidth section on a fixed cadence."""

    def __init__(self, service, ars, frontend_port, interval_s=2.0):
        super().__init__(daemon=True)
        self.service, self.ars, self.port = service, ars, frontend_port
        self.interval = interval_s
        self.samples = []
        self.stop_flag = threading.Event()

    def run(self):
        url_tpl = "http://{ar}:{port}/api/v2/services/{svc}/replica/info"
        while not self.stop_flag.is_set():
            t = time.time()
            for ar in self.ars:
                try:
                    _, body = http(
                        url_tpl.format(ar=ar, port=self.port, svc=self.service),
                        headers={"XDN": self.service},
                    )
                    bw = json.loads(body).get("bandwidth")
                    if bw:
                        self.samples.append({"t": t, "ar": ar, "edges": bw["edges"]})
                except Exception as e:
                    self.samples.append({"t": t, "ar": ar, "error": str(e)})
            self.stop_flag.wait(self.interval)


# ---------------------------------------------------------------- load drivers


def drive_etcd(ars, port, service, phase, dur):
    url = f"http://{ars[0]}:{port}/v3/kv/" + ("put" if phase == "write" else "range")
    key = base64.b64encode(b"bw-measure-key").decode()
    deadline = time.time() + dur
    n = 0
    while time.time() < deadline:
        if phase == "write":
            val = base64.b64encode(f"value-{n}-padding-for-measurement".encode()).decode()
            payload = json.dumps({"key": key, "value": val}).encode()
        else:
            payload = json.dumps({"key": key}).encode()
        try:
            http(url, headers={"XDN": service}, data=payload)
            n += 1
        except Exception:
            time.sleep(0.2)
    return n


def drive_rqlite(ars, port, service, phase, dur):
    hdrs = {"XDN": service, "Content-Type": "application/json"}
    base = f"http://{ars[0]}:{port}/db/"
    http(
        base + "execute",
        headers=hdrs,
        data=json.dumps(
            ["CREATE TABLE IF NOT EXISTS bw (id INTEGER PRIMARY KEY, v TEXT)"]
        ).encode(),
    )
    deadline = time.time() + dur
    n = 0
    while time.time() < deadline:
        try:
            if phase == "write":
                http(
                    base + "execute",
                    headers=hdrs,
                    data=json.dumps(
                        [f"INSERT OR REPLACE INTO bw VALUES (1, 'value-{n}-padding')"]
                    ).encode(),
                )
            else:
                http(
                    base + "query",
                    headers=hdrs,
                    data=json.dumps(["SELECT * FROM bw"]).encode(),
                )
            n += 1
        except Exception:
            time.sleep(0.2)
    return n


def drive_redis(host, port, phase, dur):
    def cli(*args):
        return subprocess.run(
            ["redis-cli", "-h", host, "-p", str(port), *args],
            capture_output=True, timeout=10,
        ).returncode

    deadline = time.time() + dur
    n = 0
    while time.time() < deadline:
        if phase == "write":
            rc = cli("SET", "bw-key", f"value-{n}-padding-for-measurement")
        else:
            rc = cli("GET", "bw-key")
        if rc == 0:
            n += 1
        else:
            time.sleep(0.2)
    return n


def drive_cassandra(host, port, phase, dur):
    def cql(stmt, timeout=20):
        return subprocess.run(
            ["cqlsh", host, str(port), "-e", stmt],
            capture_output=True, timeout=timeout,
        ).returncode

    cql(
        "CREATE KEYSPACE IF NOT EXISTS bw WITH replication ="
        " {'class':'NetworkTopologyStrategy','dc1':3}"
    )
    cql("CREATE TABLE IF NOT EXISTS bw.kv (id int PRIMARY KEY, v text)")
    deadline = time.time() + dur
    n = 0
    while time.time() < deadline:
        if phase == "write":
            rc = cql(f"CONSISTENCY QUORUM; INSERT INTO bw.kv (id, v) VALUES (1, 'value-{n}')")
        else:
            rc = cql("CONSISTENCY QUORUM; SELECT * FROM bw.kv WHERE id = 1")
        if rc == 0:
            n += 1
        else:
            time.sleep(0.2)
    return n


def drive_mysql(host, port, password, phase, dur):
    def sql(stmt, timeout=10):
        return subprocess.run(
            ["mysql", "--protocol=tcp", "-h", host, "-P", str(port), "-uroot",
             f"-p{password}", "-e", stmt],
            capture_output=True, timeout=timeout,
        ).returncode

    sql("CREATE DATABASE IF NOT EXISTS bwdb")
    sql("CREATE TABLE IF NOT EXISTS bwdb.bw (id INT PRIMARY KEY, v VARCHAR(64))")
    deadline = time.time() + dur
    n = 0
    while time.time() < deadline:
        if phase == "write":
            rc = sql(f"REPLACE INTO bwdb.bw VALUES (1, 'value-{n}-padding')")
        else:
            rc = sql("SELECT * FROM bwdb.bw")
        if rc == 0:
            n += 1
        else:
            time.sleep(0.2)
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--service", required=True)
    ap.add_argument(
        "--kind", required=True, choices=["etcd", "rqlite", "mysql", "redis", "cassandra"])
    ap.add_argument("--ars", required=True, help="comma-separated AR addresses")
    ap.add_argument("--frontend-port", type=int, default=2300)
    ap.add_argument("--phases", default="idle:30,write:60,read:30")
    ap.add_argument("--sample-interval", type=float, default=2.0)
    ap.add_argument("--out", required=True)
    ap.add_argument("--mysql-host")
    ap.add_argument("--mysql-port", type=int)
    ap.add_argument("--mysql-password")
    ap.add_argument("--direct-host", help="service host for direct-protocol kinds")
    ap.add_argument("--direct-port", type=int, help="published port for direct-protocol kinds")
    args = ap.parse_args()

    ars = args.ars.split(",")
    sampler = Sampler(args.service, ars, args.frontend_port, args.sample_interval)
    sampler.start()

    phase_log = []
    for spec in args.phases.split(","):
        name, dur = spec.split(":")
        dur = int(dur)
        t0 = time.time()
        print(f"[phase] {name} for {dur}s", flush=True)
        if name == "idle":
            time.sleep(dur)
            ops = 0
        elif args.kind == "etcd":
            ops = drive_etcd(ars, args.frontend_port, args.service, name, dur)
        elif args.kind == "rqlite":
            ops = drive_rqlite(ars, args.frontend_port, args.service, name, dur)
        elif args.kind == "redis":
            ops = drive_redis(args.direct_host, args.direct_port, name, dur)
        elif args.kind == "cassandra":
            ops = drive_cassandra(args.direct_host, args.direct_port, name, dur)
        else:
            ops = drive_mysql(args.mysql_host, args.mysql_port, args.mysql_password,
                              name, dur)
        phase_log.append({"name": name, "t0": t0, "t1": time.time(), "ops": ops})
        print(f"[phase] {name} done: {ops} ops", flush=True)

    time.sleep(2 * args.sample_interval)  # capture the settled tail
    sampler.stop_flag.set()
    sampler.join()

    out = {
        "service": args.service,
        "kind": args.kind,
        "ars": ars,
        "phases": phase_log,
        "samples": sampler.samples,
    }
    with open(args.out, "w") as f:
        json.dump(out, f)
    ok = sum(1 for s in sampler.samples if "edges" in s)
    print(f"[done] {ok} samples ({len(sampler.samples) - ok} errors) -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
