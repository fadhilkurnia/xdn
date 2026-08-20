#!/usr/bin/env python3
"""Nightly performance measurement of XDN: low-load latency + max throughput.

Measures ONE checkout of XDN (--repo) on a generated 1-RC + 3-AR loopback
cluster and writes a JSON result:

    python3 eval/nightly_perf.py --repo /path/to/checkout --label head_r1 \
        --out results/head_r1.json [--skip-tput] [--with-instrumented] \
        [--tput-hint-file hint.txt]

The nightly-perf workflow invokes this alternately against a baseline checkout
and the current HEAD in the SAME job (interleaved A/B), so shared-runner noise
largely cancels in the head/base comparison. Absolute numbers from GitHub-hosted
runners are NOT comparable across nights -- only the within-job deltas are; see
eval/nightly_perf_check.py.

Measured per service (bookcatalog -> paxos, bookcatalog-nd -> primary-backup,
FUSELOG statediff recorder with its working tree on /dev/shm):
  - low-load latency: SEQUENTIAL closed loop (one outstanding request at a
    time over a persistent connection) for --low-duration seconds, with the
    default WARNING-level logging -- this is the clean, gated latency number.
  - max throughput (unless --skip-tput): a geometric rate ladder driven by the
    open-loop Poisson generator (eval/get_latency_at_rate.go); a rate passes
    while achieved >= 93% of offered AND avg latency <= max(--tput-slo-ms,
    3x the service's own unloaded average). Seeded from --tput-hint-file.
  - with --with-instrumented: a SECOND cluster cycle with -DXDN_TIMING_HEADERS
    and -DPB_SAMPLE_LATENCY, repeating the sequential run while harvesting the
    X-XDN-Pipeline / X-XDN-Timing / X-XDN-Forward response headers (and, for
    primary-backup, the PBM pipeline samples in /tmp/pbm_samples.log) into a
    per-stage latency breakdown of the request flow. Recorded for diagnosis,
    never gated: the instrumentation itself adds overhead.

The measured tree's own fuselog/fuselog-apply binaries (bin/) are installed to
/usr/local/bin at the start of the pass, so changes under xdn-fs/ are A/B'd
like everything else.
"""

import argparse
import http.client
import json
import os
import re
import shlex
import socket
import statistics
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
ENTRY_HOST, ENTRY_PORT = "127.0.0.1", 2300
ENTRY_PATH = "/api/books"
ENTRY_URL = f"http://{ENTRY_HOST}:{ENTRY_PORT}{ENTRY_PATH}"

# FUSELOG working tree on tmpfs: fuselog is the production-representative
# recorder (rsync forks a process per capture and dominated PB latency), and
# /dev/shm keeps its snapshot/diff/apply I/O off the runner's disk.
FUSELOG_BASE_DIR = "/dev/shm/xdn-perf/fuselog/"
PBM_SAMPLES_LOG = "/tmp/pbm_samples.log"
INSTRUMENTATION_JVM_ARGS = ["-DXDN_TIMING_HEADERS=true", "-DPB_SAMPLE_LATENCY=true"]

# Generated into the measured checkout at runtime (so baseline commits need no
# new checked-in file). Exactly 3 ARs: the default replication factor is 3, so
# every service is placed on ALL ARs and any frontend can serve the load --
# with more ARs the placement picks a subset and a fixed frontend may 404.
# String node ids match XdnTestCluster's proven loopback shape.
LOCAL_CONFIG = "conf/gigapaxos.nightlyperf.generated.properties"
CONFIG_TEXT = f"""\
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
XDN_PB_STATEDIFF_RECORDER_TYPE=FUSELOG
XDN_FUSELOG_BASE_DIR={FUSELOG_BASE_DIR}
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
    # False -> primary-backup (fuselog statediff recorder).
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


def install_fuselog(repo: Path):
    """Points /usr/local/bin at THIS tree's fuselog binaries so xdn-fs/ changes
    are part of the A/B (the recorder hardcodes the /usr/local/bin paths)."""
    for tool in ("fuselog", "fuselog-apply"):
        src = repo / "bin" / tool
        if not src.exists():
            raise RuntimeError(f"{src} missing; build with bin/build_xdn_fuselog.sh cpp")
        run(f"sudo ln -sf {shlex.quote(str(src))} /usr/local/bin/{tool}")


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
        request = urllib.request.Request(
            f"http://{ENTRY_HOST}:{ENTRY_PORT}/", headers={"XDN": service})
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
        # release any stray fuselog FUSE mounts before removing their trees
        subprocess.run(
            "mount | grep fuse.fuselog | awk '{print $3}' | xargs -r sudo umount -l",
            shell=True, check=False)
        subprocess.run(
            "sudo rm -rf /tmp/gigapaxos /tmp/xdn /dev/shm/xdn-perf"
            " || rm -rf /tmp/gigapaxos /tmp/xdn /dev/shm/xdn-perf",
            shell=True, check=False)

    def start(self, jvm_args=()):
        log(f"starting cluster from {self.repo} jvm_args={list(jvm_args)}")
        logfile = self.repo / "nightly_perf_cluster.log"
        extra = " ".join(jvm_args)
        with open(logfile, "w") as fh:
            subprocess.run(
                f"{shlex.quote(str(self.gp))} -DgigapaxosConfig={self.config} {extra} start all",
                shell=True, cwd=self.repo, check=True, timeout=300, stdout=fh, stderr=fh)
        wait_ports([RC_PORT] + AR_HTTP_PORTS, timeout_s=180)
        time.sleep(5)  # let the HTTP frontends finish wiring after the ports open

    def launch(self, name: str, image: str, deterministic: bool):
        cmd = (
            f"XDN_CONTROL_PLANE=localhost {shlex.quote(str(self.xdn))} launch {name}"
            f" --image={image} --state=/app/data/ --consistency=linearizability"
            f" --deterministic={'true' if deterministic else 'false'} --env ENABLE_WAL=true")
        run(cmd, cwd=self.repo, timeout=240)
        wait_service_ready(name)

    def destroy(self, name: str):
        subprocess.run(
            f"XDN_CONTROL_PLANE=localhost {shlex.quote(str(self.xdn))} service destroy {name} --yes",
            shell=True, cwd=self.repo, check=False, timeout=180,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(3)


# ── sequential closed-loop client (low-load latency) ─────────────────────────


def run_sequential(service: str, duration_s: int, collect_headers=False):
    """One outstanding request at a time over one persistent connection.
    Returns (sorted latencies ms, list of parsed timing-header dicts, errors)."""
    conn = None
    latencies = []
    header_samples = []
    errors = 0
    headers = {"XDN": service, "Content-Type": "application/json",
               "Content-Length": str(len(PAYLOAD))}
    deadline = time.time() + duration_s
    while time.time() < deadline:
        try:
            if conn is None:
                conn = http.client.HTTPConnection(ENTRY_HOST, ENTRY_PORT, timeout=15)
            start = time.perf_counter()
            conn.request("POST", ENTRY_PATH, body=PAYLOAD, headers=headers)
            resp = conn.getresponse()
            resp.read()
            elapsed_ms = (time.perf_counter() - start) * 1000
            if resp.status >= 400:
                errors += 1
                continue
            latencies.append(elapsed_ms)
            if collect_headers:
                parsed = parse_timing_headers(dict(resp.getheaders()))
                if parsed:
                    header_samples.append(parsed)
        except Exception:  # noqa: BLE001 - reconnect and continue
            errors += 1
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
            conn = None
            time.sleep(0.2)
    if conn is not None:
        conn.close()
    latencies.sort()
    return latencies, header_samples, errors


def percentile(sorted_values, fraction):
    if not sorted_values:
        return None
    index = min(len(sorted_values) - 1, int(fraction * (len(sorted_values) - 1) + 0.5))
    return round(sorted_values[index], 3)


def sequential_stats(latencies, errors):
    if not latencies:
        return None
    return {
        "mode": "sequential-closed-loop",
        "requests": len(latencies),
        "errors": errors,
        "avg_ms": round(statistics.fmean(latencies), 3),
        "p50_ms": percentile(latencies, 0.50),
        "p95_ms": percentile(latencies, 0.95),
        "p99_ms": percentile(latencies, 0.99),
    }


# ── instrumentation parsing ──────────────────────────────────────────────────


def parse_timing_headers(headers: dict) -> dict:
    """X-XDN-Pipeline: callback=12ms / X-XDN-Timing: exec=..;fwd=.. /
    X-XDN-Forward: container=..;proxy=..;copyreq=.. -> {stage: ms}."""
    out = {}
    for name in ("X-XDN-Pipeline", "X-XDN-Timing", "X-XDN-Forward"):
        value = headers.get(name)
        if not value:
            continue
        for part in value.split(";"):
            key, _, raw = part.partition("=")
            raw = raw.strip().removesuffix("ms")
            try:
                out[key.strip()] = float(raw)
            except ValueError:
                pass
    return out


PBM_SAMPLE_RE = re.compile(
    r"FIRST queueWait=([\d.]+) exec=([\d.]+) captureWait=([\d.]+) capture=([\d.]+) "
    r"proposePrep=([\d.]+) paxos=([\d.]+) total=([\d.]+)ms")
PBM_STAGES = ("queue_wait", "exec", "capture_wait", "capture", "propose_prep", "paxos", "total")


def parse_pbm_samples(path: str) -> dict:
    """Medians of the PBM capture-pipeline stage samples (primary-backup only)."""
    stage_values = {stage: [] for stage in PBM_STAGES}
    try:
        text = Path(path).read_text()
    except OSError:
        return {}
    for match in PBM_SAMPLE_RE.finditer(text):
        for stage, value in zip(PBM_STAGES, match.groups()):
            stage_values[stage].append(float(value))
    return {stage: round(statistics.median(values), 3)
            for stage, values in stage_values.items() if values}


def stage_medians(header_samples: list) -> dict:
    keys = set().union(*header_samples) if header_samples else set()
    return {key: round(statistics.median(
        [sample[key] for sample in header_samples if key in sample]), 3)
        for key in sorted(keys)}


# ── open-loop load generator wrapper (throughput ladder) ─────────────────────


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


# ── measurement phases ───────────────────────────────────────────────────────


def quiet_phase(cluster: Cluster, gen: LoadGen, args, hints: dict) -> dict:
    """Default WARNING-level logging, no instrumentation: the gated numbers."""
    services = {}
    cluster.clean()
    try:
        cluster.start()
        for name, image, deterministic in SERVICES:
            svc = {"image": image, "protocol": "paxos" if deterministic else "primary-backup"}
            try:
                cluster.launch(name, image, deterministic)
                log(f"warmup {name} (sequential x{args.warmup}s)")
                run_sequential(name, args.warmup)
                log(f"low-load {name} (sequential x{args.low_duration}s)")
                latencies, _, errors = run_sequential(name, args.low_duration)
                svc["low_load"] = sequential_stats(latencies, errors)
                if svc["low_load"] is None:
                    raise RuntimeError(f"sequential run produced no successes ({errors} errors)")
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
            services[name] = svc
    finally:
        cluster.clean()
    return services


def instrumented_phase(cluster: Cluster, args) -> dict:
    """Second cluster cycle with timing headers + PBM sampling: a per-stage
    latency breakdown of the request flow. Diagnostic only, never gated."""
    flows = {}
    cluster.clean()
    Path(PBM_SAMPLES_LOG).unlink(missing_ok=True)
    try:
        cluster.start(jvm_args=INSTRUMENTATION_JVM_ARGS)
        for name, image, deterministic in SERVICES:
            flow = {}
            try:
                cluster.launch(name, image, deterministic)
                run_sequential(name, min(args.warmup, 10))
                Path(PBM_SAMPLES_LOG).unlink(missing_ok=True)
                log(f"instrumented low-load {name} (sequential x{args.low_duration}s)")
                latencies, header_samples, errors = run_sequential(
                    name, args.low_duration, collect_headers=True)
                stats = sequential_stats(latencies, errors)
                if stats is None:
                    raise RuntimeError(f"no successful instrumented requests ({errors} errors)")
                flow["client_p50_ms"] = stats["p50_ms"]
                flow["requests"] = stats["requests"]
                flow["stages_p50_ms"] = stage_medians(header_samples)
                if not deterministic:
                    pbm = parse_pbm_samples(PBM_SAMPLES_LOG)
                    if pbm:
                        flow["pbm_p50_ms"] = pbm
            except Exception as exc:  # noqa: BLE001
                log(f"WARNING: instrumented run for {name} failed: {exc}")
                flow["error"] = str(exc)
            finally:
                cluster.destroy(name)
            flows[name] = flow
    finally:
        cluster.clean()
    return flows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True, help="XDN checkout to measure")
    parser.add_argument("--label", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--low-duration", type=int, default=30)
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--tput-slo-ms", type=float, default=100.0)
    parser.add_argument("--tput-step-duration", type=int, default=20)
    parser.add_argument("--skip-tput", action="store_true")
    parser.add_argument("--with-instrumented", action="store_true",
                        help="Add a second, instrumented cluster cycle recording "
                             "the per-stage request-flow latency breakdown")
    parser.add_argument("--tput-hint-file", default=None,
                        help="File with 'service=rate' hints; updated after each ladder")
    args = parser.parse_args()

    repo = Path(args.repo).resolve()
    install_fuselog(repo)
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
    }

    cluster = Cluster(repo)
    gen = LoadGen(repo)
    result["services"] = quiet_phase(cluster, gen, args, hints)
    if args.with_instrumented:
        flows = instrumented_phase(cluster, args)
        for name, flow in flows.items():
            result["services"].setdefault(name, {})["instrumented"] = flow

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
