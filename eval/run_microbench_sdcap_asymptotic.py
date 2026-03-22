#!/usr/bin/env python3
"""
Statediff Capture Asymptotic Microbenchmark

Measures how statediff capture latency and artifact size scale with increasing
initial state size. Uses a synthetic filewriter app that writes a fixed-size
random payload at a random offset into a pre-allocated state file.

Three capture baselines are compared:
  - fuselog  : FUSE-based filesystem logging (captures only the write)
  - rsync    : rsync --write-batch between live and backup copy
  - btrfs    : btrfs send -p between snapshots

Typical usage:
    python3 eval/run_microbench_sdcap_asymptotic.py

    python3 eval/run_microbench_sdcap_asymptotic.py \\
        --baselines fuselog rsync btrfs \\
        --state-mbs 64,128,256,512,1024 \\
        --write-size 4096 \\
        --requests-per-state 20

The script is a driver that stages files to a remote worker (10.10.1.4) and
runs the benchmark there via SSH.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import shlex
import shutil
import socket
import struct
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from statistics import mean, median

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths and constants
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent if SCRIPT_DIR.name == "eval" else SCRIPT_DIR
RESULTS_BASE = (ROOT_DIR / "eval" / "results") if (ROOT_DIR / "eval").exists() else (SCRIPT_DIR / "results")

WORKDIR = Path("/tmp/xdn-statediff")
FUSELOG_SHM_WORKDIR = Path("/dev/shm/xdn-statediff")

FILEWRITER_IMAGE = "xdn-filewriter-bench"
FILEWRITER_PORT = 18081
CONTAINER_NAME = "sdcap-filewriter"

DEFAULT_BASELINES = ["fuselog", "btrfs", "rsync"]
DEFAULT_STATE_MBS = [64, 128, 256, 512, 1024]
DEFAULT_WRITE_SIZE = 4096
DEFAULT_REQUESTS = 20

DEFAULT_REMOTE_HOST = "10.10.1.4"
DEFAULT_REMOTE_DIR = "~/xdn-sdcap-asymptotic"

# Per-action overhead in fuselog wire format
COALESCE_ACTION_OVERHEAD = 25


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def configure_logging(log_path: Path | None, verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(formatter)
    root.addHandler(console)

    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_path)
        fh.setLevel(level)
        fh.setFormatter(formatter)
        root.addHandler(fh)


# ---------------------------------------------------------------------------
# Shell helpers
# ---------------------------------------------------------------------------

def run(cmd: str, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    log.debug("$ %s", cmd)
    return subprocess.run(cmd, shell=True, check=check, text=True, capture_output=capture)


def sudo(cmd: str, check: bool = True) -> subprocess.CompletedProcess:
    return run(f"sudo -n bash -lc {json.dumps(cmd)}", check=check)


def ssh(host: str, cmd: str, check: bool = True) -> subprocess.CompletedProcess:
    full = ["ssh", "-o", "StrictHostKeyChecking=no", host, f"bash -lc {shlex.quote(cmd)}"]
    log.debug("[ssh %s] %s", host, cmd)
    return subprocess.run(full, check=check, text=True)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def reset_dir(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)


def find_existing(paths: list[Path]) -> Path:
    for path in paths:
        if path.exists():
            return path
    raise FileNotFoundError(f"none of these paths exist: {paths}")


def docker_rm(name: str) -> None:
    run(f"docker rm -f {name} >/dev/null 2>&1 || true", check=False)


def bytes_of(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    total = 0
    for root_dir, _, files in os.walk(path):
        for filename in files:
            total += (Path(root_dir) / filename).stat().st_size
    return total


def http_post(url: str, payload: bytes, content_type: str) -> float:
    req = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("Content-Type", content_type)
    start = time.perf_counter()
    with urllib.request.urlopen(req, timeout=30) as resp:
        resp.read()
    return (time.perf_counter() - start) * 1000.0


def wait_http(url: str, timeout_sec: int = 120) -> None:
    deadline = time.time() + timeout_sec
    last_err = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                if resp.status < 500:
                    return
        except urllib.error.HTTPError as exc:
            if exc.code < 500:
                return
            last_err = exc
        except Exception as exc:
            last_err = exc
        time.sleep(1)
    raise RuntimeError(f"timeout waiting for {url}: {last_err}")


def create_state_file(path: Path, size_mb: int) -> None:
    ensure_dir(path.parent)
    log.info("  Creating %d MB state file at %s ...", size_mb, path)
    run(f"dd if=/dev/urandom of={path} bs=1M count={size_mb} status=none")


def parse_int_list(value: str) -> list[int]:
    return [int(x.strip()) for x in value.split(",") if x.strip()]


# ---------------------------------------------------------------------------
# Fuselog collector (socket protocol)
# ---------------------------------------------------------------------------

class FuselogCollector:
    def __init__(self, socket_path: Path):
        self.socket_path = socket_path
        self.conn: socket.socket | None = None

    def connect(self) -> None:
        self.conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.conn.connect(str(self.socket_path))

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def drain(self) -> dict:
        if self.conn is None:
            raise RuntimeError("collector not connected")
        start = time.perf_counter()
        self.conn.sendall(b"g")
        size_buf = self._read_exact(8)
        payload_size = struct.unpack("<Q", size_buf)[0]
        payload = self._read_exact(payload_size)
        elapsed_ms = (time.perf_counter() - start) * 1000.0

        num_actions = -1
        if len(payload) >= 16:
            try:
                pos = 0
                num_file = struct.unpack_from("<Q", payload, pos)[0]
                pos += 8
                for _ in range(num_file):
                    _, path_len = struct.unpack_from("<QQ", payload, pos)
                    pos += 16 + path_len
                if pos + 8 <= len(payload):
                    num_actions = struct.unpack_from("<Q", payload, pos)[0]
            except (struct.error, OverflowError, ValueError):
                num_actions = -1

        return {
            "capture_ms": elapsed_ms,
            "artifact_bytes": payload_size + 8,
            "num_actions": num_actions,
        }

    def _read_exact(self, n: int) -> bytes:
        chunks = []
        got = 0
        while got < n:
            chunk = self.conn.recv(n - got)
            if not chunk:
                raise RuntimeError("socket closed while reading fuselog payload")
            chunks.append(chunk)
            got += len(chunk)
        return b"".join(chunks)


# ---------------------------------------------------------------------------
# Baseline harnesses
# ---------------------------------------------------------------------------

class BaselineHarness:
    def __init__(self, baseline: str, root: Path):
        self.baseline = baseline
        self.root = root
        self.live_dir = root / baseline / "live"
        self.meta_dir = root / baseline / "meta"
        ensure_dir(self.live_dir)
        ensure_dir(self.meta_dir)

    def start(self) -> None:
        raise NotImplementedError

    def stop(self) -> None:
        raise NotImplementedError

    def initialize_after_seed(self) -> None:
        raise NotImplementedError

    def capture(self, iteration: int | str) -> dict:
        raise NotImplementedError

    def mount_path(self) -> Path:
        return self.live_dir


class FuselogHarness(BaselineHarness):
    def __init__(self, root: Path):
        super().__init__("fuselog", root)
        self.socket_path = self.meta_dir / "fuselog.sock"
        self.proc: subprocess.Popen | None = None
        self.collector: FuselogCollector | None = None
        self.fuselog_bin = find_existing([
            Path("/usr/local/bin/fuselog"),
            Path(__file__).resolve().parent.parent / "xdn-fs" / "cpp" / "fuselog",
            Path.cwd() / "xdn-fs" / "cpp" / "fuselog",
            Path.cwd() / "cpp" / "fuselog",
        ])

    def start(self) -> None:
        ensure_dir(self.live_dir)
        reset_dir(self.meta_dir)
        env = {
            "FUSELOG_SOCKET_FILE": str(self.socket_path),
            "FUSELOG_DAEMON_LOGS": "1",
            "RUST_LOG": "info",
        }
        cmd = ["sudo", "-n", "env"]
        for key, value in env.items():
            cmd.append(f"{key}={value}")
        cmd.extend([str(self.fuselog_bin), "-o", "allow_other", str(self.live_dir)])
        self.proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.time() + 15
        while time.time() < deadline:
            if self.socket_path.exists():
                break
            if self.proc.poll() is not None:
                time.sleep(0.2)
                if self.socket_path.exists():
                    break
                mp = subprocess.run(["mountpoint", "-q", str(self.live_dir)], check=False)
                if mp.returncode == 0:
                    for _ in range(50):
                        if self.socket_path.exists():
                            break
                        time.sleep(0.1)
                    if self.socket_path.exists():
                        break
                raise RuntimeError(f"fuselog exited (code={self.proc.returncode})")
            time.sleep(0.1)
        else:
            raise RuntimeError(f"timed out waiting for fuselog socket {self.socket_path}")
        sudo(f"chmod 777 {self.socket_path}", check=False)
        self.collector = FuselogCollector(self.socket_path)
        self.collector.connect()

    def stop(self) -> None:
        if self.collector is not None:
            self.collector.close()
            self.collector = None
        sudo(f"fusermount -u {self.live_dir} >/dev/null 2>&1 || true", check=False)
        sudo(f"umount -l {self.live_dir} >/dev/null 2>&1 || true", check=False)
        if self.proc is not None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
            self.proc = None

    def initialize_after_seed(self) -> None:
        if self.collector is None:
            raise RuntimeError("collector not connected")
        self.collector.drain()

    def capture(self, iteration: int | str) -> dict:
        if self.collector is None:
            raise RuntimeError("collector not connected")
        data = self.collector.drain()
        data["batch_path"] = ""
        return data


class RsyncHarness(BaselineHarness):
    def __init__(self, root: Path):
        super().__init__("rsync", root)
        self.backup_dir = self.meta_dir / "backup"

    def start(self) -> None:
        reset_dir(self.live_dir)
        reset_dir(self.meta_dir)
        reset_dir(self.backup_dir)

    def stop(self) -> None:
        return

    def initialize_after_seed(self) -> None:
        reset_dir(self.backup_dir)
        run(f"rsync -a --delete {self.live_dir}/ {self.backup_dir}/")

    def capture(self, iteration: int | str) -> dict:
        batch_prefix = self.meta_dir / f"batch_{iteration}"
        for suffix in ("", ".sh"):
            try:
                Path(str(batch_prefix) + suffix).unlink()
            except FileNotFoundError:
                pass
        start = time.perf_counter()
        run(f"rsync -a --delete --write-batch={batch_prefix} "
            f"{self.live_dir}/ {self.backup_dir}/ >/dev/null")
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        artifact_bytes = bytes_of(batch_prefix) + bytes_of(Path(str(batch_prefix) + ".sh"))
        return {
            "capture_ms": elapsed_ms,
            "artifact_bytes": artifact_bytes,
            "num_actions": -1,
            "batch_path": str(batch_prefix),
        }


class BtrfsHarness(BaselineHarness):
    def __init__(self, root: Path):
        super().__init__("btrfs", root)
        self.img = self.meta_dir / "btrfs.img"
        self.mount_dir = self.meta_dir / "mnt"
        self.snapshots_dir = self.mount_dir / ".snapshots"
        self.prev_snap = self.snapshots_dir / "prev_snap"

    def mount_path(self) -> Path:
        return self.mount_dir / "live"

    def start(self) -> None:
        reset_dir(self.meta_dir)
        ensure_dir(self.mount_dir)
        sudo(f"truncate -s 4G {self.img}")
        sudo(f"mkfs.btrfs -f {self.img}")
        sudo(f"mount -o loop {self.img} {self.mount_dir}")
        sudo(f"btrfs subvolume create {self.mount_dir / 'live'}")
        sudo(f"mkdir -p {self.snapshots_dir}")
        run(f"sudo -n chown -R $(id -u):$(id -g) {self.mount_dir}", check=False)

    def stop(self) -> None:
        sudo(f"umount {self.mount_dir}", check=False)

    def initialize_after_seed(self) -> None:
        sudo(f"btrfs -q subvolume delete {self.prev_snap} >/dev/null 2>&1 || true", check=False)
        sudo(f"btrfs -q subvolume snapshot -r {self.mount_dir / 'live'} {self.prev_snap}")

    def capture(self, iteration: int | str) -> dict:
        curr_snap = self.snapshots_dir / f"curr_snap_{iteration}"
        diff_path = self.meta_dir / f"diff_{iteration}.bin"
        sudo(f"btrfs -q subvolume delete {curr_snap} >/dev/null 2>&1 || true", check=False)
        run(f"rm -f {diff_path}", check=False)
        start = time.perf_counter()
        # Single sudo call to avoid double shell startup overhead.
        # -q suppresses progress output; -f writes directly to file (no pipe).
        sudo(
            f"btrfs -q subvolume snapshot -r {self.mount_dir / 'live'} {curr_snap} && "
            f"btrfs -q send -p {self.prev_snap} -f {diff_path} {curr_snap}"
        )
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        artifact_bytes = bytes_of(diff_path)
        sudo(f"btrfs -q subvolume delete {self.prev_snap} >/dev/null 2>&1 || true", check=False)
        self.prev_snap = curr_snap
        return {
            "capture_ms": elapsed_ms,
            "artifact_bytes": artifact_bytes,
            "num_actions": -1,
            "batch_path": str(diff_path),
        }


def make_harness(name: str, root: Path) -> BaselineHarness:
    if name == "fuselog":
        return FuselogHarness(root)
    if name == "rsync":
        return RsyncHarness(root)
    if name == "btrfs":
        return BtrfsHarness(root)
    raise ValueError(f"unknown baseline {name}")


# ---------------------------------------------------------------------------
# Summarization
# ---------------------------------------------------------------------------

def summarize(rows: list[dict], group_keys: list[str]) -> list[dict]:
    grouped: dict[tuple, list[dict]] = {}
    for row in rows:
        key = tuple(row[k] for k in group_keys)
        grouped.setdefault(key, []).append(row)

    summaries = []
    for key, items in grouped.items():
        request_vals = [x["request_ms"] for x in items]
        capture_vals = [x["capture_ms"] for x in items]
        artifact_vals = [x["artifact_bytes"] for x in items]
        data = {k: v for k, v in zip(group_keys, key)}
        data.update({
            "n": len(items),
            "request_ms_avg": mean(request_vals),
            "request_ms_p50": median(request_vals),
            "capture_ms_avg": mean(capture_vals),
            "capture_ms_p50": median(capture_vals),
            "artifact_bytes_avg": mean(artifact_vals),
            "artifact_bytes_p50": median(artifact_vals),
            "total_ms_avg": mean([x["request_ms"] + x["capture_ms"] for x in items]),
        })
        summaries.append(data)
    return summaries


# ---------------------------------------------------------------------------
# Worker main: runs on the remote machine
# ---------------------------------------------------------------------------

def worker_main(args: argparse.Namespace) -> int:
    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    configure_logging(results_dir / "run.log", args.verbose)

    baselines = args.baselines
    state_mbs = args.state_mbs
    write_size = args.write_size
    requests_per_state = args.requests_per_state

    total_experiments = len(baselines) * len(state_mbs)

    log.info("=" * 60)
    log.info("Statediff Capture Asymptotic Microbenchmark")
    log.info("  Baselines  : %s", ", ".join(baselines))
    log.info("  State sizes: %s MB", ", ".join(str(s) for s in state_mbs))
    log.info("  Write size : %d bytes", write_size)
    log.info("  Requests   : %d per (baseline, state_size)", requests_per_state)
    log.info("  Experiments: %d (%d baselines x %d sizes)",
             total_experiments, len(baselines), len(state_mbs))
    log.info("  Results    : %s", results_dir)
    log.info("=" * 60)

    log.info("[Phase 1] Building filewriter image ...")
    filewriter_dir = find_existing([
        Path(__file__).resolve().parent.parent / "services" / "filewriter",
        Path.cwd() / "services" / "filewriter",
        Path.cwd() / "filewriter",
    ])
    run(f"docker build -t {FILEWRITER_IMAGE} {filewriter_dir}")

    log.info("[Phase 2] Running %d experiments ...", total_experiments)
    results: list[dict] = []
    exp_root = WORKDIR / "sdcap_asymptotic"
    fuselog_root = FUSELOG_SHM_WORKDIR / "sdcap_asymptotic"
    ensure_dir(exp_root)
    ensure_dir(fuselog_root)

    exp_idx = 0
    for baseline in baselines:
        for state_mb in state_mbs:
            exp_idx += 1
            label = f"{baseline}/{state_mb}MB"
            log.info("=" * 60)
            log.info("[%d/%d] Starting: %s", exp_idx, total_experiments, label)
            log.info("=" * 60)

            harness_base = fuselog_root if baseline == "fuselog" else exp_root
            harness_root = harness_base / f"state_{state_mb}mb"
            harness = make_harness(baseline, harness_root)

            try:
                log.info("  [Step 1] Setting up %s harness ...", baseline)
                if baseline == "fuselog":
                    reset_dir(harness.mount_path())
                    create_state_file(harness.mount_path() / "state.bin", state_mb)
                harness.start()
                if baseline != "fuselog":
                    create_state_file(harness.mount_path() / "state.bin", state_mb)

                log.info("  [Step 2] Starting filewriter container (state=%dMB) ...", state_mb)
                docker_rm(CONTAINER_NAME)
                run(f"docker run -d --name {CONTAINER_NAME} "
                    f"-p {FILEWRITER_PORT}:8000 "
                    f"-e STATEDIR=/data "
                    f"-e INITSIZE={state_mb} "
                    f"-v {harness.mount_path()}:/data "
                    f"{FILEWRITER_IMAGE}")
                wait_http(f"http://127.0.0.1:{FILEWRITER_PORT}/")

                log.info("  [Step 3] Initializing baseline ...")
                harness.initialize_after_seed()

                log.info("  [Step 4] Measuring %d requests (write_size=%d) ...",
                         requests_per_state, write_size)
                for i in range(requests_per_state):
                    payload = f"size={write_size}".encode()
                    request_ms = http_post(
                        f"http://127.0.0.1:{FILEWRITER_PORT}/",
                        payload,
                        "application/x-www-form-urlencoded",
                    )
                    capture = harness.capture(f"{state_mb}_{i}")
                    results.append({
                        "experiment": "filewriter_state_size",
                        "baseline": baseline,
                        "state_mb": state_mb,
                        "write_size": write_size,
                        "iteration": i,
                        "request_ms": request_ms,
                        "capture_ms": capture["capture_ms"],
                        "artifact_bytes": capture["artifact_bytes"],
                        "num_actions": capture["num_actions"],
                    })

                # Log per-experiment summary
                exp_rows = [r for r in results
                            if r["baseline"] == baseline and r["state_mb"] == state_mb]
                cap_p50 = median([r["capture_ms"] for r in exp_rows])
                art_p50 = median([r["artifact_bytes"] for r in exp_rows])
                log.info("[%d/%d] Done: %s — capture_ms_p50=%.3f artifact_p50=%.0f",
                         exp_idx, total_experiments, label, cap_p50, art_p50)

            except Exception:
                log.exception("[%d/%d] FAILED: %s — skipping", exp_idx, total_experiments, label)
            finally:
                docker_rm(CONTAINER_NAME)
                harness.stop()

    log.info("[Phase 3] Writing results ...")
    raw_path = results_dir / "eval_statediff_capture_init_state_time.csv"
    summary_path = results_dir / "summary.csv"
    summary_json_path = results_dir / "summary.json"

    summary_rows: list[dict] = []
    if results:
        with open(raw_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(results[0].keys()))
            writer.writeheader()
            writer.writerows(results)

        summary_rows = summarize(results, ["baseline", "state_mb"])
        with open(summary_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(summary_rows[0].keys()))
            writer.writeheader()
            writer.writerows(summary_rows)
        summary_json_path.write_text(json.dumps(summary_rows, indent=2), encoding="utf-8")

        log.info("  Wrote %s", raw_path)
        log.info("  Wrote %s", summary_path)
        log.info("  Wrote %s", summary_json_path)

    log.info("=" * 60)
    log.info("Summary")
    if summary_rows:
        log.info("  %-10s %10s %16s %14s %5s",
                 "Baseline", "State MB", "Capture ms P50", "Artifact P50", "N")
        log.info("  %s", "-" * 60)
        for row in sorted(summary_rows, key=lambda r: (r["baseline"], r["state_mb"])):
            log.info("  %-10s %10s %16.2f %14.0f %5d",
                     row["baseline"], row["state_mb"],
                     row["capture_ms_p50"], row["artifact_bytes_p50"], row["n"])
    log.info("=" * 60)

    return 0


# ---------------------------------------------------------------------------
# Driver main: stages files to worker and runs via SSH
# ---------------------------------------------------------------------------

def driver_main(args: argparse.Namespace) -> int:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_dir = RESULTS_BASE / f"microbench_sdcap_asymptotic_{timestamp}"
    results_dir.mkdir(parents=True, exist_ok=True)
    configure_logging(results_dir / "driver.log", args.verbose)

    remote_dir = args.remote_dir
    remote_dir_expr = remote_dir.replace("~/", "$HOME/") if remote_dir.startswith("~/") else remote_dir

    log.info("=" * 60)
    log.info("Statediff Capture Asymptotic — Driver")
    log.info("  Worker host  : %s", args.remote_host)
    log.info("  Remote dir   : %s", remote_dir)
    log.info("  Baselines    : %s", ", ".join(args.baselines))
    log.info("  State sizes  : %s MB", ", ".join(str(s) for s in args.state_mbs))
    log.info("  Write size   : %d bytes", args.write_size)
    log.info("  Requests     : %d per point", args.requests_per_state)
    log.info("  Results      : %s", results_dir)
    log.info("=" * 60)

    log.info("[Phase 1] Staging files to worker ...")
    ssh(args.remote_host, f"mkdir -p {remote_dir_expr}")
    files_to_stage = [
        str(Path(__file__).resolve()),
        str(ROOT_DIR / "services" / "filewriter"),
        str(ROOT_DIR / "xdn-fs" / "cpp"),
    ]
    run(f"rsync -az {' '.join(shlex.quote(f) for f in files_to_stage)} "
        f"{args.remote_host}:{remote_dir}/")

    log.info("[Phase 2] Running benchmark on worker ...")
    worker_cmd = [
        "python3", Path(__file__).name,
        "--worker-mode",
        f"--results-dir results/{results_dir.name}",
        f"--baselines {' '.join(args.baselines)}",
        f"--state-mbs {','.join(str(s) for s in args.state_mbs)}",
        f"--write-size {args.write_size}",
        f"--requests-per-state {args.requests_per_state}",
    ]
    if args.verbose:
        worker_cmd.append("--verbose")

    remote_cmd = f"cd {remote_dir_expr} && " + " ".join(worker_cmd)
    rc = 0
    try:
        ssh(args.remote_host, remote_cmd)
    except subprocess.CalledProcessError as exc:
        rc = exc.returncode
        log.exception("Remote benchmark failed.")

    log.info("[Phase 3] Collecting results from worker ...")
    remote_results = f"{remote_dir}/results/{results_dir.name}"
    # rsync doesn't expand $HOME but does expand ~
    remote_results_rsync = remote_results.replace("$HOME", "~")
    try:
        run(f"rsync -az {args.remote_host}:{remote_results_rsync}/ {results_dir}/")
    except subprocess.CalledProcessError:
        log.exception("Failed to copy remote results")

    log.info("=" * 60)
    log.info("Done. Results in %s", results_dir)
    if rc != 0:
        log.warning("Worker exited with code %d", rc)
    log.info("=" * 60)
    return rc


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--remote-host", default=DEFAULT_REMOTE_HOST)
    parser.add_argument("--remote-dir", default=DEFAULT_REMOTE_DIR)
    parser.add_argument("--baselines", nargs="+", default=DEFAULT_BASELINES)
    parser.add_argument("--state-mbs", type=parse_int_list,
                        default=DEFAULT_STATE_MBS)
    parser.add_argument("--write-size", type=int, default=DEFAULT_WRITE_SIZE)
    parser.add_argument("--requests-per-state", type=int, default=DEFAULT_REQUESTS)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--worker-mode", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--results-dir", default=None, help=argparse.SUPPRESS)
    return parser.parse_args()


if __name__ == "__main__":
    parsed = parse_args()
    if parsed.worker_mode:
        sys.exit(worker_main(parsed))
    else:
        sys.exit(driver_main(parsed))
