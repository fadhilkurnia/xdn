#!/usr/bin/env python3
"""
Run statediff-capture microbenchmarks locally on one worker machine.

This script is meant to execute on 10.10.1.4. It benchmarks:
  1. bookcatalog with realistic SQLite writes
  2. filewriter with large initial state and variable write sizes

Each benchmark can be run against three baselines:
  - fuselog
  - rsync
  - btrfs
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import socket
import struct
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from statistics import mean, median


WORKDIR = Path("/tmp/xdn-statediff")
FUSELOG_SHM_WORKDIR = Path("/dev/shm/xdn-statediff")
RESULTS_DIR = Path("/tmp/xdn-statediff-results")
BOOKCATALOG_IMAGE = os.environ.get("BOOKCATALOG_IMAGE", "fadhilkurnia/xdn-bookcatalog")
FILEWRITER_IMAGE = "xdn-filewriter-bench"
BOOKCATALOG_PORT = 18080
FILEWRITER_PORT = 18081
DEFAULT_BASELINES = ["fuselog", "btrfs", "rsync"]
BOOKCATALOG_MEASURED_PAYLOAD = json.dumps(
    {"title": "Distributed Systems", "author": "Tanenbaum"}
).encode()


def find_existing(paths: list[Path]) -> Path:
    for path in paths:
        if path.exists():
            return path
    raise FileNotFoundError(f"none of these paths exist: {paths}")


def run(cmd: str, check: bool = True, capture: bool = False, env: dict | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        shell=True,
        check=check,
        text=True,
        capture_output=capture,
        env=env,
    )


def sudo(cmd: str, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    return run(f"sudo -n bash -lc {json.dumps(cmd)}", check=check, capture=capture)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def reset_dir(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)


def http_post(url: str, payload: bytes, content_type: str) -> float:
    req = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("Content-Type", content_type)
    start = time.perf_counter()
    with urllib.request.urlopen(req, timeout=30) as resp:
        resp.read()
    return (time.perf_counter() - start) * 1000.0


def wait_http(url: str, timeout_sec: int = 60) -> None:
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
        except Exception as exc:  # noqa: BLE001
            last_err = exc
        time.sleep(1)
    raise RuntimeError(f"timeout waiting for {url}: {last_err}")


def docker_rm(name: str) -> None:
    run(f"docker rm -f {name} >/dev/null 2>&1 || true", check=False)


def bytes_of(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    total = 0
    for root, _, files in os.walk(path):
        for filename in files:
            total += (Path(root) / filename).stat().st_size
    return total


def create_state_file(path: Path, size_mb: int) -> None:
    ensure_dir(path.parent)
    run(f"dd if=/dev/urandom of={path} bs=1M count={size_mb} status=none")


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
        data.update(
            {
                "n": len(items),
                "request_ms_avg": mean(request_vals),
                "request_ms_p50": median(request_vals),
                "capture_ms_avg": mean(capture_vals),
                "capture_ms_p50": median(capture_vals),
                "artifact_bytes_avg": mean(artifact_vals),
                "artifact_bytes_p50": median(artifact_vals),
                "total_ms_avg": mean([x["request_ms"] + x["capture_ms"] for x in items]),
            }
        )
        summaries.append(data)
    return summaries


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
            except (struct.error, OverflowError, UnicodeDecodeError, ValueError):
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

    def capture(self, iteration: int) -> dict:
        raise NotImplementedError

    def mount_path(self) -> Path:
        return self.live_dir


class FuselogHarness(BaselineHarness):
    def __init__(self, root: Path, env_overrides: dict[str, str] | None = None):
        super().__init__("fuselog", root)
        self.socket_path = self.meta_dir / "fuselog.sock"
        self.proc: subprocess.Popen | None = None
        self.collector: FuselogCollector | None = None
        self.env_overrides = env_overrides or {}
        self.fuselog_bin = find_existing(
            [
                Path("/usr/local/bin/fuselog"),
                Path(__file__).resolve().parent.parent / "xdn-fs" / "cpp" / "fuselog",
                Path.cwd() / "xdn-fs" / "cpp" / "fuselog",
                Path.cwd() / "cpp" / "fuselog",
            ]
        )

    def start(self) -> None:
        ensure_dir(self.live_dir)
        reset_dir(self.meta_dir)
        env = {
            "FUSELOG_SOCKET_FILE": str(self.socket_path),
            "FUSELOG_DAEMON_LOGS": "1",
            "RUST_LOG": "info",
        }
        env.update(self.env_overrides)
        cmd = ["sudo", "-n", "env"]
        for key, value in env.items():
            cmd.append(f"{key}={value}")
        cmd.extend([str(self.fuselog_bin), "-o", "allow_other", str(self.live_dir)])
        self.proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.time() + 10
        while time.time() < deadline:
            if self.socket_path.exists():
                break
            time.sleep(0.1)
        if self.socket_path.exists():
            sudo(f"chmod 777 {self.socket_path}", check=False)
        self.collector = FuselogCollector(self.socket_path)
        self.collector.connect()

    def stop(self) -> None:
        if self.collector is not None:
            self.collector.close()
            self.collector = None
        sudo(f"umount {self.live_dir} >/dev/null 2>&1 || true", check=False)
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

    def capture(self, iteration: int) -> dict:
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

    def capture(self, iteration: int) -> dict:
        batch_prefix = self.meta_dir / f"batch_{iteration}"
        for suffix in ("", ".sh"):
            try:
                (Path(str(batch_prefix) + suffix)).unlink()
            except FileNotFoundError:
                pass
        start = time.perf_counter()
        run(
            f"rsync -a --delete --write-batch={batch_prefix} "
            f"{self.live_dir}/ {self.backup_dir}/ >/dev/null"
        )
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
        sudo(f"btrfs subvolume delete {self.prev_snap}", check=False)
        sudo(f"btrfs subvolume snapshot -r {self.mount_dir / 'live'} {self.prev_snap}")

    def capture(self, iteration: int) -> dict:
        curr_snap = self.snapshots_dir / f"curr_snap_{iteration}"
        diff_path = self.meta_dir / f"diff_{iteration}.bin"
        sudo(f"btrfs subvolume delete {curr_snap}", check=False)
        run(f"rm -f {diff_path}", check=False)
        start = time.perf_counter()
        sudo(f"btrfs subvolume snapshot -r {self.mount_dir / 'live'} {curr_snap}")
        sudo(f"btrfs send -p {self.prev_snap} {curr_snap} > {diff_path}")
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        artifact_bytes = bytes_of(diff_path)
        sudo(f"btrfs subvolume delete {self.prev_snap}", check=False)
        self.prev_snap = curr_snap
        return {
            "capture_ms": elapsed_ms,
            "artifact_bytes": artifact_bytes,
            "num_actions": -1,
            "batch_path": str(diff_path),
        }


def build_filewriter_image() -> None:
    filewriter_dir = find_existing(
        [
            Path(__file__).resolve().parent.parent / "services" / "filewriter",
            Path.cwd() / "services" / "filewriter",
            Path.cwd() / "filewriter",
        ]
    )
    run(f"docker build -t {FILEWRITER_IMAGE} {filewriter_dir}")


def build_bookcatalog_image(tag: str) -> None:
    bookcatalog_dir = find_existing(
        [
            Path(__file__).resolve().parent.parent / "services" / "bookcatalog",
            Path.cwd() / "services" / "bookcatalog",
            Path.cwd() / "bookcatalog",
        ]
    )
    run(f"docker build -t {tag} {bookcatalog_dir}")


def setup_bookcatalog_container(state_dir: Path) -> None:
    docker_rm("bookcatalog-bench")
    run(
        f"docker run -d --name bookcatalog-bench "
        f"-p {BOOKCATALOG_PORT}:80 "
        f"-v {state_dir}:/app/data "
        f"{BOOKCATALOG_IMAGE}"
    )
    wait_http(f"http://127.0.0.1:{BOOKCATALOG_PORT}/api/books")


def seed_bookcatalog(num_books: int) -> None:
    for i in range(num_books):
        payload = json.dumps({"title": f"Book {i}", "author": f"Author {i}"}).encode()
        http_post(f"http://127.0.0.1:{BOOKCATALOG_PORT}/api/books", payload, "application/json")


def setup_filewriter_container(state_dir: Path, state_mb: int) -> None:
    docker_rm("filewriter-bench")
    run(
        f"docker run -d --name filewriter-bench "
        f"-p {FILEWRITER_PORT}:8000 "
        f"-e STATEDIR=/data "
        f"-e INITSIZE={state_mb} "
        f"-e SKIP_INIT=true "
        f"-v {state_dir}:/data "
        f"{FILEWRITER_IMAGE}"
    )
    wait_http(f"http://127.0.0.1:{FILEWRITER_PORT}/", timeout_sec=120)


def make_harness(name: str, root: Path) -> BaselineHarness:
    if name == "fuselog":
        return FuselogHarness(root)
    if name == "rsync":
        return RsyncHarness(root)
    if name == "btrfs":
        return BtrfsHarness(root)
    raise ValueError(f"unknown baseline {name}")


def make_fuselog_harness(root: Path, env_overrides: dict[str, str]) -> FuselogHarness:
    return FuselogHarness(root, env_overrides=env_overrides)


def run_bookcatalog(args: argparse.Namespace) -> None:
    results = []
    exp_root = WORKDIR / "bookcatalog"
    ensure_dir(exp_root)

    for baseline in args.baselines:
        harness = make_harness(baseline, exp_root)
        try:
            harness.start()
            setup_bookcatalog_container(harness.mount_path())
            seed_bookcatalog(args.seed_books)
            harness.initialize_after_seed()

            for i in range(args.requests):
                request_ms = http_post(
                    f"http://127.0.0.1:{BOOKCATALOG_PORT}/api/books",
                    BOOKCATALOG_MEASURED_PAYLOAD,
                    "application/json",
                )
                capture = harness.capture(i)
                results.append(
                    {
                        "experiment": "bookcatalog",
                        "baseline": baseline,
                        "iteration": i,
                        "request_ms": request_ms,
                        "capture_ms": capture["capture_ms"],
                        "artifact_bytes": capture["artifact_bytes"],
                        "num_actions": capture["num_actions"],
                    }
                )
        finally:
            docker_rm("bookcatalog-bench")
            harness.stop()

    write_results("bookcatalog", results, ["baseline"])


def run_bookcatalog_opt_size(args: argparse.Namespace) -> None:
    configs = [
        ("none", {"WRITE_COALESCING": "false", "FUSELOG_PRUNE": "false", "FUSELOG_COMPRESSION": "false"}),
        ("coalescing", {"WRITE_COALESCING": "true", "FUSELOG_PRUNE": "false", "FUSELOG_COMPRESSION": "false"}),
        ("pruning", {"WRITE_COALESCING": "true", "FUSELOG_PRUNE": "true", "FUSELOG_COMPRESSION": "false"}),
        ("compression", {"WRITE_COALESCING": "true", "FUSELOG_PRUNE": "true", "FUSELOG_COMPRESSION": "true"}),
    ]
    results = []
    exp_root = WORKDIR / "bookcatalog_opt_size"
    ensure_dir(exp_root)

    for label, env_overrides in configs:
        harness = make_fuselog_harness(exp_root, env_overrides)
        try:
            reset_dir(harness.live_dir)
            harness.start()
            setup_bookcatalog_container(harness.mount_path())
            seed_bookcatalog(args.seed_books)
            harness.initialize_after_seed()

            for i in range(args.requests):
                request_ms = http_post(
                    f"http://127.0.0.1:{BOOKCATALOG_PORT}/api/books",
                    BOOKCATALOG_MEASURED_PAYLOAD,
                    "application/json",
                )
                capture = harness.capture(i)
                results.append(
                    {
                        "experiment": "bookcatalog_opt_size",
                        "config": label,
                        "iteration": i,
                        "request_ms": request_ms,
                        "capture_ms": capture["capture_ms"],
                        "artifact_bytes": capture["artifact_bytes"],
                        "num_actions": capture["num_actions"],
                    }
                )
        finally:
            docker_rm("bookcatalog-bench")
            harness.stop()

    write_results("bookcatalog_opt_size", results, ["config"])


def run_filewriter(args: argparse.Namespace) -> None:
    build_filewriter_image()
    results = []
    exp_root = WORKDIR / "filewriter"
    fuselog_root = FUSELOG_SHM_WORKDIR / "filewriter"
    ensure_dir(exp_root)
    ensure_dir(fuselog_root)

    for baseline in args.baselines:
        harness_root = fuselog_root if baseline == "fuselog" else exp_root
        harness = make_harness(baseline, harness_root)
        try:
            if baseline == "fuselog":
                reset_dir(harness.mount_path())
                create_state_file(harness.mount_path() / "state.bin", args.state_mb)
            harness.start()
            if baseline != "fuselog":
                create_state_file(harness.mount_path() / "state.bin", args.state_mb)
            setup_filewriter_container(harness.mount_path(), args.state_mb)
            harness.initialize_after_seed()

            for size in args.write_sizes:
                for i in range(args.requests_per_size):
                    payload = f"size={size}".encode()
                    request_ms = http_post(
                        f"http://127.0.0.1:{FILEWRITER_PORT}/",
                        payload,
                        "application/x-www-form-urlencoded",
                    )
                    capture = harness.capture(f"{size}_{i}")
                    results.append(
                        {
                            "experiment": "filewriter",
                            "baseline": baseline,
                            "write_size": size,
                            "iteration": i,
                            "request_ms": request_ms,
                            "capture_ms": capture["capture_ms"],
                            "artifact_bytes": capture["artifact_bytes"],
                            "num_actions": capture["num_actions"],
                        }
                    )
        finally:
            docker_rm("filewriter-bench")
            harness.stop()

    write_results("filewriter", results, ["baseline", "write_size"])


def run_filewriter_state_size(args: argparse.Namespace) -> None:
    build_filewriter_image()
    results = []
    exp_root = WORKDIR / "filewriter_state_size"
    fuselog_root = FUSELOG_SHM_WORKDIR / "filewriter_state_size"
    ensure_dir(exp_root)
    ensure_dir(fuselog_root)

    for baseline in args.baselines:
        for state_mb in args.state_mbs:
            harness_base = fuselog_root if baseline == "fuselog" else exp_root
            harness_root = harness_base / f"state_{state_mb}mb"
            harness = make_harness(baseline, harness_root)
            try:
                if baseline == "fuselog":
                    reset_dir(harness.mount_path())
                    create_state_file(harness.mount_path() / "state.bin", state_mb)
                harness.start()
                if baseline != "fuselog":
                    create_state_file(harness.mount_path() / "state.bin", state_mb)
                setup_filewriter_container(harness.mount_path(), state_mb)
                harness.initialize_after_seed()

                for i in range(args.requests_per_state):
                    payload = f"size={args.write_size}".encode()
                    request_ms = http_post(
                        f"http://127.0.0.1:{FILEWRITER_PORT}/",
                        payload,
                        "application/x-www-form-urlencoded",
                    )
                    capture = harness.capture(f"{state_mb}_{i}")
                    results.append(
                        {
                            "experiment": "filewriter_state_size",
                            "baseline": baseline,
                            "state_mb": state_mb,
                            "write_size": args.write_size,
                            "iteration": i,
                            "request_ms": request_ms,
                            "capture_ms": capture["capture_ms"],
                            "artifact_bytes": capture["artifact_bytes"],
                            "num_actions": capture["num_actions"],
                        }
                    )
            finally:
                docker_rm("filewriter-bench")
                harness.stop()

    write_results("filewriter_state_size", results, ["baseline", "state_mb"])


def write_results(name: str, rows: list[dict], group_keys: list[str]) -> None:
    out_dir = RESULTS_DIR / name
    reset_dir(out_dir)

    if rows:
        csv_path = out_dir / "raw.csv"
        with open(csv_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

        summary = summarize(rows, group_keys)
        with open(out_dir / "summary.json", "w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=2)

        print(f"wrote {csv_path}")
        print(f"wrote {out_dir / 'summary.json'}")


def prep(args: argparse.Namespace) -> None:
    packages = ["btrfs-progs", "rsync", "pkg-config", "libfuse3-dev", "libzstd-dev", "g++"]
    sudo(f"apt-get update && apt-get install -y {' '.join(packages)}")
    cpp_dir = find_existing(
        [
            Path(__file__).resolve().parent.parent / "xdn-fs" / "cpp",
            Path.cwd() / "xdn-fs" / "cpp",
            Path.cwd() / "cpp",
        ]
    )
    run(
        " ".join(
            [
                f"cd {cpp_dir}",
                "&&",
                "g++ -Wall fuselogv2.cpp -o fuselog -D_FILE_OFFSET_BITS=64",
                "$(pkg-config fuse3 --cflags --libs)",
                "$(pkg-config libzstd --cflags --libs)",
                "-pthread -O3 -std=c++11",
                "&&",
                "g++ -Wall fuselog-apply.cpp -o fuselog-apply",
                "$(pkg-config libzstd --cflags --libs)",
                "-O3 -std=c++20",
            ]
        )
    )
    run(f"docker pull {BOOKCATALOG_IMAGE}")
    build_filewriter_image()
    print("prep complete")


def parse_write_sizes(value: str) -> list[int]:
    return [int(x.strip()) for x in value.split(",") if x.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("prep")
    p.set_defaults(fn=prep)

    p = sub.add_parser("bookcatalog")
    p.add_argument("--baselines", nargs="+", default=DEFAULT_BASELINES)
    p.add_argument("--seed-books", type=int, default=200)
    p.add_argument("--requests", type=int, default=100)
    p.set_defaults(fn=run_bookcatalog)

    p = sub.add_parser("bookcatalog-opt-size")
    p.add_argument("--seed-books", type=int, default=200)
    p.add_argument("--requests", type=int, default=100)
    p.set_defaults(fn=run_bookcatalog_opt_size)

    p = sub.add_parser("filewriter")
    p.add_argument("--baselines", nargs="+", default=DEFAULT_BASELINES)
    p.add_argument("--state-mb", type=int, default=1024)
    p.add_argument("--requests-per-size", type=int, default=20)
    p.add_argument(
        "--write-sizes",
        type=parse_write_sizes,
        default=parse_write_sizes("8,64,512,4096,16384,65536,262144,1048576"),
    )
    p.set_defaults(fn=run_filewriter)

    p = sub.add_parser("filewriter-state-size")
    p.add_argument("--baselines", nargs="+", default=DEFAULT_BASELINES)
    p.add_argument("--requests-per-state", type=int, default=20)
    p.add_argument("--write-size", type=int, default=4096)
    p.add_argument(
        "--state-mbs",
        type=parse_write_sizes,
        default=parse_write_sizes("64,128,256,512,1024"),
    )
    p.set_defaults(fn=run_filewriter_state_size)

    args = parser.parse_args()
    ensure_dir(WORKDIR)
    ensure_dir(RESULTS_DIR)
    args.fn(args)


if __name__ == "__main__":
    try:
        main()
    except urllib.error.HTTPError as exc:
        print(f"http error: {exc.code} {exc.reason}", file=sys.stderr)
        raise
