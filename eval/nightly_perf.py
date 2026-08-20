#!/usr/bin/env python3
"""Nightly performance measurement of XDN: low-load latency + max throughput.

Measures ONE checkout of XDN (--repo) on a local loopback cluster (1 RC + 3 ARs
from conf/gigapaxos.xdn.local.properties) and writes a JSON result:

    python3 eval/nightly_perf.py --repo /path/to/checkout --label head_r1 \
        --out results/head_r1.json [--skip-tput] [--tput-hint-file hint.txt]

The nightly-perf workflow invokes this alternately against a baseline checkout
and the current HEAD in the SAME job (interleaved A/B), so shared-runner noise
largely cancels in the head/base comparison. Absolute numbers from GitHub-hosted
runners are NOT comparable across nights -- only the within-job deltas are; see
eval/nightly_perf_check.py.

Measured per service (bookcatalog -> paxos, bookcatalog-nd -> primary-backup):
  - low-load latency: open-loop Poisson at --low-rate for --low-duration seconds
    (median/p95/p99/avg), after a warmup.
  - max throughput (unless --skip-tput): a geometric rate ladder; a rate passes
    while achieved >= 93% of offered AND avg latency <= --tput-slo-ms. One
    midpoint refinement after the first failure. Seeded from --tput-hint-file
    when present so later rounds converge quickly.
"""

import argparse
import json
import os
import shlex
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

RC_PORT = 3000
AR_HTTP_PORTS = [2300, 2301, 2302]
ENTRY_URL = "http://127.0.0.1:2300/api/books"

# Generated into the measured checkout at runtime (so baseline commits need no
# new checked-in file). Exactly 3 ARs: the default replication factor is 3, so
# every service is placed on ALL ARs and any frontend can serve the load --
# with more ARs the placement picks a subset and a fixed frontend may 404.
# String node ids match XdnTestCluster's proven loopback shape.
LOCAL_CONFIG = "conf/gigapaxos.nightlyperf.generated.properties"
CONFIG_TEXT = """\
APPLICATION=edu.umass.cs.xdn.XdnGigapaxosApp
GIGAPAXOS_DATA_DIR=/tmp/gigapaxos
ENABLE_ACTIVE_REPLICA_HTTP=true
ENABLE_RECONFIGURATOR_HTTP=true
BATCHING_ENABLED=true
REPLICA_COORDINATOR_CLASS=edu.umass.cs.xdn.XdnReplicaCoordinator
ENABLE_STARTUP_LEADER_ELECTION=false
HIBERNATE_OPTION=true
SYNC=true
NIO_MAX_PAYLOAD_SIZE=805306368
INITIAL_STATE_VALIDATOR_CLASS=edu.umass.cs.xdn.XdnServiceInitialStateValidator
INITIAL_STATE_NUM_REPLICAS_EXTRACTOR_CLASS=edu.umass.cs.xdn.XdnServiceNumReplicasExtractor
XDN_PB_STATEDIFF_RECORDER_TYPE=RSYNC
XDN_PB_ENABLE_NON_DETERMINISTIC_INIT=true
REPLICATE_ALL=false
EMULATE_UNREPLICATED=false
HTTP_AR_FRONTEND_BATCH_ENABLED=true
reconfigurator.RC0=127.0.0.1:3000
active.AR0=127.0.0.1:2000
active.AR1=127.0.0.1:2001
active.AR2=127.0.0.1:2002
"""

SERVICES = [
    # (service name, image, deterministic) -- deterministic=True -> paxos,
    # False -> primary-backup (rsync statediff recorder on the runner).
    ("perfcatalog", "fadhilkurnia/xdn-bookcatalog", True),
    ("perfcatalognd", "fadhilkurnia/xdn-bookcatalog-nd", False),
]
PAYLOAD = '{"author": "abc", "title": "xyz"}'


def log(msg: str):
    print(f"[nightly-perf {datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def run(cmd: str, cwd=None, check=True, timeout=600) -> subprocess.CompletedProcess:
    log(f"$ {cmd}")
    return subprocess.run(
        cmd, shell=True, cwd=cwd, check=check, timeout=timeout,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def wait_ports(ports, timeout_s=120):
    deadline = time.time() + timeout_s
    pending = set(ports)
    while pending and time.time() < deadline:
        for port in list(pending):
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=1):
                    pending.discard(port)
            except OSError:
                time.sleep(1)
    if pending:
        raise TimeoutError(f"ports never opened: {sorted(pending)}")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def wait_service_ready(service: str, timeout_s=120):
    """Polls an AR frontend until the service answers anything but 404: right
    after CREATE, the frontend returns 404 until service registration has
    propagated (the same race XdnServiceDestroyRecreateTest works around)."""
    opener = urllib.request.build_opener(_NoRedirect)
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        request = urllib.request.Request("http://127.0.0.1:2300/", headers={"XDN": service})
        try:
            with opener.open(request, timeout=5) as resp:
                last = resp.status
                if resp.status != 404:
                    return
        except urllib.error.HTTPError as exc:
            last = exc.code
            if exc.code != 404:
                return  # any app-generated response means the service is wired
        except Exception as exc:  # noqa: BLE001 - frontend not up yet
            last = str(exc)
        time.sleep(2)
    raise TimeoutError(f"service {service} never became ready; last response: {last}")


class Cluster:
    """A loopback 1-RC + 3-AR cluster run from one checkout."""

    def __init__(self, repo: Path):
        self.repo = repo
        self.config = repo / LOCAL_CONFIG
        self.config.write_text(CONFIG_TEXT)
        self.gp = repo / "bin" / "gpServer.sh"
        self.xdn = repo / "bin" / "xdn"

    def clean(self):
        subprocess.run(
            f"{shlex.quote(str(self.gp))} -DgigapaxosConfig={self.config} forceclear all",
            shell=True, cwd=self.repo, check=False, timeout=180,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run("pkill -9 -f '[R]econfigurableNode' || true", shell=True, check=False)
        subprocess.run("sudo rm -rf /tmp/gigapaxos /tmp/xdn || rm -rf /tmp/gigapaxos /tmp/xdn",
                       shell=True, check=False)

    def start(self):
        log(f"starting cluster from {self.repo}")
        logfile = self.repo / "nightly_perf_cluster.log"
        with open(logfile, "w") as fh:
            subprocess.run(
                f"{shlex.quote(str(self.gp))} -DgigapaxosConfig={self.config} start all",
                shell=True, cwd=self.repo, check=True, timeout=300, stdout=fh, stderr=fh)
        wait_ports([RC_PORT] + AR_HTTP_PORTS, timeout_s=180)
        time.sleep(5)  # let the HTTP frontends finish wiring after the ports open

    def launch(self, name: str, image: str, deterministic: bool):
        cmd = (
            f"XDN_CONTROL_PLANE=localhost {shlex.quote(str(self.xdn))} launch {name}"
            f" --image={image} --state=/app/data/ --consistency=linearizability"
            f" --deterministic={'true' if deterministic else 'false'} --env ENABLE_WAL=true")
        run(cmd, cwd=self.repo, timeout=240)
        time.sleep(5)

    def destroy(self, name: str):
        subprocess.run(
            f"XDN_CONTROL_PLANE=localhost {shlex.quote(str(self.xdn))} service destroy {name} --yes",
            shell=True, cwd=self.repo, check=False, timeout=180,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(3)


class LoadGen:
    """Wrapper around eval/get_latency_at_rate.go (open-loop Poisson client)."""

    def __init__(self, repo: Path):
        self.tool = repo / "eval" / "get_latency_at_rate.go"

    def run(self, service: str, rate: int, duration_s: int) -> dict:
        cmd = (
            f"go run {shlex.quote(str(self.tool))} -H 'XDN: {service}'"
            f" {ENTRY_URL} {shlex.quote(PAYLOAD)} {duration_s} {rate}")
        proc = run(cmd, timeout=duration_s + 120, check=False)
        stats = {}
        for line in proc.stdout.splitlines():
            if ":" in line and not line.startswith(("---", " ")):
                key, _, value = line.partition(":")
                try:
                    stats[key.strip()] = float(value.strip())
                except ValueError:
                    pass
        if "median_latency_ms" not in stats:
            raise RuntimeError(f"load generator produced no stats:\n{proc.stdout[-2000:]}")
        return stats


def measure_low_load(gen: LoadGen, service: str, rate: int, duration_s: int) -> dict:
    stats = gen.run(service, rate, duration_s)
    return {
        "rate": rate,
        "achieved_rps": stats.get("actual_throughput_rps"),
        "avg_ms": stats.get("average_latency_ms"),
        "p50_ms": stats.get("median_latency_ms"),
        "p95_ms": stats.get("p95_latency_ms"),
        "p99_ms": stats.get("p99_latency_ms"),
        "success": stats.get("total_successful_responses"),
        "sent": stats.get("total_requests_sent"),
    }


def rate_passes(stats: dict, offered: int, slo_ms: float) -> bool:
    achieved = stats.get("actual_throughput_rps") or 0.0
    avg = stats.get("average_latency_ms") or float("inf")
    return achieved >= 0.93 * offered and avg <= slo_ms


def measure_max_tput(gen: LoadGen, service: str, hint, slo_ms: float, step_s: int) -> dict:
    ladder = []
    rate = max(25, int(hint * 0.6)) if hint else 50
    best = None
    first_bad = None
    for _ in range(10):
        stats = gen.run(service, rate, step_s)
        ok = rate_passes(stats, rate, slo_ms)
        entry = {"offered": rate, "achieved_rps": stats.get("actual_throughput_rps"),
                 "avg_ms": stats.get("average_latency_ms"), "ok": ok}
        ladder.append(entry)
        log(f"ladder {service}: {entry}")
        if ok:
            best = entry
            rate = int(rate * 1.4)
        elif best is None and rate > 10:
            # first rung already failed: descend to find the passing region
            rate = max(10, int(rate / 1.4))
            first_bad = entry
        else:
            first_bad = entry
            break
        time.sleep(3)
    if best and first_bad and first_bad["offered"] > best["offered"]:
        mid = (best["offered"] + first_bad["offered"]) // 2
        if mid > best["offered"]:
            stats = gen.run(service, mid, step_s)
            ok = rate_passes(stats, mid, slo_ms)
            entry = {"offered": mid, "achieved_rps": stats.get("actual_throughput_rps"),
                     "avg_ms": stats.get("average_latency_ms"), "ok": ok}
            ladder.append(entry)
            log(f"ladder {service} (refine): {entry}")
            if ok:
                best = entry
    # A ladder that ran out of steps while every rung still passed did NOT find
    # the saturation knee -- recording its top rung as "max throughput" would
    # understate by up to the growth factor and pollute cross-round medians.
    # Record max_tput only when a failing rung brackets the knee; the highest
    # passing rung is still exported as the next round's starting hint.
    knee_found = best is not None and first_bad is not None \
        and first_bad["offered"] > best["offered"]
    return {"ladder": ladder,
            "tput_hint": int(best["offered"]) if best else None,
            "max_tput": {"offered": best["offered"], "achieved_rps": best["achieved_rps"],
                         "avg_ms": best["avg_ms"]} if knee_found else None}


def calibration_probe() -> dict:
    """Cheap runner-speed fingerprint, recorded alongside every measurement."""
    with tempfile.NamedTemporaryFile(dir="/tmp", delete=True) as fh:
        start = time.perf_counter()
        for _ in range(100):
            fh.write(b"x" * 4096)
            fh.flush()
            os.fsync(fh.fileno())
        fsync_ms = (time.perf_counter() - start) * 1000 / 100
    start = time.perf_counter()
    acc = 0
    while time.perf_counter() - start < 0.5:
        acc += 1
    return {"fsync_avg_ms": round(fsync_ms, 3), "cpu_loops_per_500ms": acc}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True, help="XDN checkout to measure")
    parser.add_argument("--label", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--low-rate", type=int, default=25)
    parser.add_argument("--low-duration", type=int, default=45)
    parser.add_argument("--warmup", type=int, default=15)
    parser.add_argument("--tput-slo-ms", type=float, default=100.0)
    parser.add_argument("--tput-step-duration", type=int, default=20)
    parser.add_argument("--skip-tput", action="store_true")
    parser.add_argument("--tput-hint-file", default=None,
                        help="File with 'service=rate' hints; updated after each ladder")
    args = parser.parse_args()

    repo = Path(args.repo).resolve()
    hints = {}
    if args.tput_hint_file and Path(args.tput_hint_file).exists():
        for line in Path(args.tput_hint_file).read_text().splitlines():
            key, _, value = line.partition("=")
            if value.strip().isdigit():
                hints[key.strip()] = int(value.strip())

    sha = subprocess.run("git rev-parse HEAD", shell=True, cwd=repo, check=False,
                         stdout=subprocess.PIPE, text=True).stdout.strip()
    result = {
        "label": args.label,
        "sha": sha,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "calibration": calibration_probe(),
        "services": {},
    }

    cluster = Cluster(repo)
    gen = LoadGen(repo)
    cluster.clean()
    try:
        cluster.start()
        for name, image, deterministic in SERVICES:
            svc = {"image": image, "protocol": "paxos" if deterministic else "primary-backup"}
            try:
                cluster.launch(name, image, deterministic)
                wait_service_ready(name)
                log(f"warmup {name} @{args.low_rate}rps x{args.warmup}s")
                gen.run(name, args.low_rate, args.warmup)
                log(f"low-load {name} @{args.low_rate}rps x{args.low_duration}s")
                svc["low_load"] = measure_low_load(gen, name, args.low_rate, args.low_duration)
                if not args.skip_tput:
                    # SLO is relative to this service's own unloaded latency so
                    # slow-pipeline protocols (primary-backup's capture/propose
                    # path) get a saturation knee too, not an unreachable bound.
                    slo = max(args.tput_slo_ms, 3 * (svc["low_load"]["avg_ms"] or 0))
                    svc.update(measure_max_tput(
                        gen, name, hints.get(name), slo, args.tput_step_duration))
                    hint = svc.pop("tput_hint", None)
                    if hint:
                        hints[name] = hint
            except Exception as exc:  # a service failing must not kill the whole pass
                log(f"WARNING: measuring {name} failed: {exc}")
                svc["error"] = str(exc)
            finally:
                cluster.destroy(name)
            result["services"][name] = svc
    finally:
        cluster.clean()

    if args.tput_hint_file:
        Path(args.tput_hint_file).write_text(
            "".join(f"{key}={value}\n" for key, value in sorted(hints.items())))
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(result, indent=2) + "\n")
    log(f"wrote {args.out}")
    errors = [n for n, s in result["services"].items() if "error" in s]
    if len(errors) == len(result["services"]):
        log("all services failed to measure")
        sys.exit(1)


if __name__ == "__main__":
    main()
