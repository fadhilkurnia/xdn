#!/usr/bin/env python3
"""
run_microbench_fuse_sdsize_db.py

Standalone microbenchmark for statediff size across BookCatalog backends using
fuselog directly on a worker machine. The benchmark does not run XDN. Instead,
it runs Docker containers for the application and database, mounts fuselog on
the datastore path in /dev/shm, and records per-request statediff sizes by
draining fuselog's UNIX socket after every HTTP request.

Default flow:
1. Stage this script, `services/bookcatalog`, `services/bookcatalog-mongo`,
   and `xdn-fs/cpp` to worker `10.10.1.4`.
2. Re-invoke this script on the worker in `--worker-mode`.
3. Copy the timestamped raw results back into `eval/results/`.

Backends:
- sqlite
- mysql
- postgres
- mongodb

Optimization modes:
- none
- coalescing
- pruning
- compression

Output files:
- raw.csv
- summary.csv
- summary.json
- run.log
- logs/<backend>_<mode>_{app,db,fuselog}.log

Typical usage:
    python3 eval/run_microbench_fuse_sdsize_db.py

    python3 eval/run_microbench_fuse_sdsize_db.py \
        --backends sqlite,mysql,postgres,mongodb \
        --modes none,coalescing,pruning,compression \
        --requests 1000 \
        --seed-books 200
"""

from __future__ import annotations

import argparse
import atexit
import csv
import json
import logging
import os
import shlex
import shutil
import socket
import statistics
import struct
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent if SCRIPT_DIR.name == "eval" else SCRIPT_DIR
RESULTS_BASE = (ROOT_DIR / "eval" / "results") if (ROOT_DIR / "eval").exists() else (SCRIPT_DIR / "results")

DEFAULT_REMOTE_HOST = "10.10.1.4"
DEFAULT_REMOTE_DIR = "~/xdn-microbench-fuse-sdsize-db"
DEFAULT_BACKENDS = ["sqlite", "mysql", "postgres", "mongodb"]
DEFAULT_MODES = ["none", "pruning", "coalescing", "compression"]

APP_PORT = 18080
MYSQL_PORT = 3306
POSTGRES_PORT = 5432
MONGO_PORT = 27017

APP_CONTAINER = "mb-sdsize-app"
DB_CONTAINERS = {
    "mysql": "mb-sdsize-mysql",
    "postgres": "mb-sdsize-postgres",
    "mongodb": "mb-sdsize-mongodb",
}

BOOKCATALOG_IMAGE = "mb-bookcatalog-sdsize"
BOOKCATALOG_MONGO_IMAGE = "mb-bookcatalog-mongo-sdsize"
MYSQL_IMAGE = "mysql:8"
POSTGRES_IMAGE = "postgres:bookworm"
MONGO_IMAGE = "mongodb/mongodb-atlas-local:8.0"

SEED_PAYLOAD_TEMPLATE = {"title": "seed-title-{i:04d}", "author": "seed-author-{i:04d}"}
MEASURED_PAYLOAD = {
    "title": "T" * 20,
    "author": "A" * 20,
}

MODE_ENVS = {
    "none": {
        "WRITE_COALESCING": "false",
        "FUSELOG_PRUNE": "false",
        "FUSELOG_COMPRESSION": "false",
    },
    "pruning": {
        "WRITE_COALESCING": "false",
        "FUSELOG_PRUNE": "true",
        "FUSELOG_COMPRESSION": "false",
    },
    "coalescing": {
        "WRITE_COALESCING": "true",
        "FUSELOG_PRUNE": "true",
        "FUSELOG_COMPRESSION": "false",
    },
    "compression": {
        "WRITE_COALESCING": "true",
        "FUSELOG_PRUNE": "true",
        "FUSELOG_COMPRESSION": "true",
    },
}

log = logging.getLogger("microbench-fuse-sdsize-db")


def parse_csv_list(value: str | None, default: list[str]) -> list[str]:
    if not value:
        return list(default)
    return [item.strip() for item in value.split(",") if item.strip()]


def configure_logging(log_path: Path, verbose: bool) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    level = logging.DEBUG if verbose else logging.INFO
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(formatter)
    root.addHandler(console)

    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)


def run(cmd: str, *, check: bool = True, capture: bool = False,
        env: dict[str, str] | None = None, cwd: Path | None = None,
        timeout: int | None = None) -> subprocess.CompletedProcess:
    log.info("$ %s", cmd)
    return subprocess.run(
        cmd,
        shell=True,
        check=check,
        text=True,
        capture_output=capture,
        env=env,
        cwd=str(cwd) if cwd else None,
        timeout=timeout,
    )


def sudo(cmd: str, *, check: bool = True, capture: bool = False,
         timeout: int | None = None) -> subprocess.CompletedProcess:
    return run(f"sudo -n bash -lc {shlex.quote(cmd)}", check=check, capture=capture, timeout=timeout)


def ssh(host: str, cmd: str, *, check: bool = True, capture: bool = False,
        timeout: int | None = None) -> subprocess.CompletedProcess:
    log.info("[ssh %s] %s", host, cmd)
    return subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", host, f"bash -lc {shlex.quote(cmd)}"],
        check=check,
        text=True,
        capture_output=capture,
        timeout=timeout,
    )


def rsync_to_remote(host: str, remote_dir: str, paths: list[Path]) -> None:
    cmd = ["rsync", "-az"]
    cmd.extend(str(path) for path in paths)
    cmd.append(f"{host}:{remote_dir}/")
    log.info("$ %s", " ".join(shlex.quote(part) for part in cmd))
    subprocess.run(cmd, check=True)


def expand_remote_path(path: str) -> str:
    if path == "~":
        return "$HOME"
    if path.startswith("~/"):
        return "$HOME/" + path[2:]
    return path


def rsync_from_remote(host: str, remote_path: str, local_dir: Path) -> None:
    local_dir.mkdir(parents=True, exist_ok=True)
    # rsync doesn't expand $HOME but does expand ~
    remote_path = remote_path.replace("$HOME", "~")
    cmd = ["rsync", "-az", f"{host}:{remote_path}/", str(local_dir) + "/"]
    log.info("$ %s", " ".join(shlex.quote(part) for part in cmd))
    subprocess.run(cmd, check=True)


def docker_image_exists_locally(image: str) -> bool:
    result = run(f"docker image inspect {shlex.quote(image)}", check=False, capture=True)
    return result.returncode == 0


def required_app_images(backends: list[str]) -> set[str]:
    images: set[str] = set()
    for backend in backends:
        if backend == "mongodb":
            images.add(BOOKCATALOG_MONGO_IMAGE)
        else:
            images.add(BOOKCATALOG_IMAGE)
    return images


def remote_docker_image_exists(host: str, image: str) -> bool:
    result = ssh(host, f"docker image inspect {shlex.quote(image)} >/dev/null 2>&1", check=False)
    return result.returncode == 0


def remote_path_exists(host: str, path_expr: str) -> bool:
    result = ssh(host, f"test -e {path_expr}", check=False)
    return result.returncode == 0


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


def wait_http_ok(url: str, timeout_sec: int = 60) -> None:
    deadline = time.time() + timeout_sec
    last_err: Exception | None = None
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


def http_post(url: str, payload: bytes, content_type: str = "application/json") -> tuple[float, int, bytes]:
    req = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("Content-Type", content_type)
    start = time.perf_counter()
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = resp.read()
        status = resp.status
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    return elapsed_ms, status, body


def wait_tcp(host: str, port: int, timeout_sec: int = 60) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                return
        except OSError:
            time.sleep(1)
    raise RuntimeError(f"timeout waiting for {host}:{port}")


def docker_rm(name: str) -> None:
    run(f"docker rm -f {name} >/dev/null 2>&1 || true", check=False)


def docker_logs(name: str, out_path: Path) -> None:
    result = run(f"docker logs {name}", check=False, capture=True)
    if result.returncode == 0:
        out_path.write_text(result.stdout + result.stderr, encoding="utf-8")


def summarize_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str], list[dict[str, object]]] = {}
    for row in rows:
        key = (str(row["backend"]), str(row["mode"]))
        grouped.setdefault(key, []).append(row)

    summary_rows: list[dict[str, object]] = []
    for (backend, mode), items in sorted(grouped.items()):
        request_ms = [float(item["request_ms"]) for item in items]
        capture_ms = [float(item["capture_ms"]) for item in items]
        statediff_bytes = [int(item["statediff_bytes"]) for item in items]
        summary_rows.append(
            {
                "backend": backend,
                "mode": mode,
                "n": len(items),
                "request_ms_avg": statistics.mean(request_ms),
                "request_ms_p50": statistics.median(request_ms),
                "capture_ms_avg": statistics.mean(capture_ms),
                "capture_ms_p50": statistics.median(capture_ms),
                "statediff_bytes_avg": statistics.mean(statediff_bytes),
                "statediff_bytes_p50": statistics.median(statediff_bytes),
            }
        )
    return summary_rows


@dataclass
class ExperimentPaths:
    base_dir: Path
    mount_dir: Path
    socket_path: Path
    fuselog_log: Path
    app_log: Path
    db_log: Path


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

    def drain(self) -> dict[str, int | float]:
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
            "statediff_bytes": payload_size + 8,
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


class LocalWorkerRunner:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.results_dir = Path(args.results_dir)
        self.logs_dir = self.results_dir / "logs"
        self.work_root = Path(args.work_root)
        self.cpp_candidates = [
            ROOT_DIR / "xdn-fs" / "cpp",
            Path.cwd() / "xdn-fs" / "cpp",
        ]
        self.bookcatalog_candidates = [
            ROOT_DIR / "services" / "bookcatalog",
            Path.cwd() / "services" / "bookcatalog",
        ]
        self.bookcatalog_mongo_candidates = [
            ROOT_DIR / "services" / "bookcatalog-mongo",
            Path.cwd() / "services" / "bookcatalog-mongo",
        ]
        self.cpp_dir = find_existing(self.cpp_candidates)
        self.fuselog_bin = self.cpp_dir / "fuselog"
        self.fuselog_proc: subprocess.Popen | None = None
        self.current_paths: ExperimentPaths | None = None

    def _collect_container_logs(self) -> None:
        app_log = self.current_paths.app_log if self.current_paths else self.logs_dir / "cleanup_app.log"
        docker_logs(APP_CONTAINER, app_log)
        docker_rm(APP_CONTAINER)
        for name in DB_CONTAINERS.values():
            db_log = self.current_paths.db_log if self.current_paths else self.logs_dir / f"{name}.cleanup.log"
            docker_logs(name, db_log)
            docker_rm(name)

    @staticmethod
    def _unmount_stale_fuse(root: Path) -> None:
        """Find and unmount any stale FUSE mounts under root."""
        try:
            result = subprocess.run(
                ["mount"], capture_output=True, text=True, check=False
            )
            for line in result.stdout.splitlines():
                # e.g. "fuselog on /dev/shm/.../mount type fuse.fuselog ..."
                parts = line.split()
                if len(parts) >= 3 and parts[2].startswith(str(root)):
                    mnt = parts[2]
                    log.info("Unmounting stale FUSE mount: %s", mnt)
                    sudo(f"fusermount -u {mnt} >/dev/null 2>&1 || true", check=False)
                    sudo(f"umount -l {mnt} >/dev/null 2>&1 || true", check=False)
        except Exception as e:
            log.warning("Failed to check for stale mounts: %s", e)

    def cleanup_run_state(self, label: str = "") -> None:
        if label:
            log.info("Cleaning isolated run state: %s", label)
        self._collect_container_logs()
        if self.fuselog_proc is not None:
            self.fuselog_proc.terminate()
            try:
                self.fuselog_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.fuselog_proc.kill()
            self.fuselog_proc = None
        if self.current_paths is not None:
            sudo(f"fusermount -u {self.current_paths.mount_dir} >/dev/null 2>&1 || true", check=False)
            sudo(f"umount -l {self.current_paths.mount_dir} >/dev/null 2>&1 || true", check=False)
            sudo(f"rm -rf {self.current_paths.base_dir}", check=False)
        else:
            # Unmount any stale FUSE mounts under work_root before rm -rf
            self._unmount_stale_fuse(self.work_root)
            sudo(f"rm -rf {self.work_root}", check=False)
        self.current_paths = None

    def cleanup(self) -> None:
        self.cleanup_run_state("global cleanup")
        sudo(f"rm -rf {self.work_root}", check=False)

    def ensure_fuselog(self) -> None:
        if self.fuselog_bin.exists():
            return
        compile_cmd = " ".join(
            [
                f"cd {shlex.quote(str(self.cpp_dir))}",
                "&&",
                "g++ -Wall fuselogv2.cpp -o fuselog -D_FILE_OFFSET_BITS=64",
                "$(pkg-config fuse3 --cflags --libs)",
                "$(pkg-config libzstd --cflags --libs)",
                "-pthread -O3 -std=c++11",
            ]
        )
        run(compile_cmd)

    def build_images(self) -> None:
        if self.args.skip_build:
            log.info("Skipping Docker image builds (--skip-build).")
            return
        if BOOKCATALOG_IMAGE in required_app_images(self.args.backends):
            if docker_image_exists_locally(BOOKCATALOG_IMAGE):
                log.info("Reusing existing Docker image: %s", BOOKCATALOG_IMAGE)
            else:
                bookcatalog_dir = find_existing(self.bookcatalog_candidates)
                run(f"docker build -t {BOOKCATALOG_IMAGE} {shlex.quote(str(bookcatalog_dir))}")
        if BOOKCATALOG_MONGO_IMAGE in required_app_images(self.args.backends):
            if docker_image_exists_locally(BOOKCATALOG_MONGO_IMAGE):
                log.info("Reusing existing Docker image: %s", BOOKCATALOG_MONGO_IMAGE)
            else:
                bookcatalog_mongo_dir = find_existing(self.bookcatalog_mongo_candidates)
                run(f"docker build -t {BOOKCATALOG_MONGO_IMAGE} {shlex.quote(str(bookcatalog_mongo_dir))}")

    def setup_paths(self, backend: str, mode: str) -> ExperimentPaths:
        base = self.work_root / backend / mode
        mount_dir = base / "mount"
        reset_dir(base)
        ensure_dir(mount_dir)
        paths = ExperimentPaths(
            base_dir=base,
            mount_dir=mount_dir,
            socket_path=base / "fuselog.sock",
            fuselog_log=self.logs_dir / f"{backend}_{mode}_fuselog.log",
            app_log=self.logs_dir / f"{backend}_{mode}_app.log",
            db_log=self.logs_dir / f"{backend}_{mode}_db.log",
        )
        self.current_paths = paths
        return paths

    def mount_fuselog(self, backend: str, mode: str) -> FuselogCollector:
        paths = self.setup_paths(backend, mode)
        env = {
            "FUSELOG_SOCKET_FILE": str(paths.socket_path),
            "FUSELOG_DAEMON_LOGS": "1",
            "RUST_LOG": "info",
        }
        env.update(MODE_ENVS[mode])

        log.info("Mounting fuselog on %s", paths.mount_dir)
        log.info("fuselog env: %s", json.dumps(env, sort_keys=True))
        fuselog_log_fh = open(paths.fuselog_log, "w", encoding="utf-8")
        cmd = ["sudo", "-n", "env"]
        for key, value in env.items():
            cmd.append(f"{key}={value}")
        cmd.extend([str(self.fuselog_bin), "-o", "allow_other", str(paths.mount_dir)])
        self.fuselog_proc = subprocess.Popen(cmd, stdout=fuselog_log_fh, stderr=subprocess.STDOUT, text=True)

        # fuselog daemonizes (forks), so the parent exits with code 0.
        # Wait for the socket file to appear as the readiness signal.
        deadline = time.time() + 15
        while time.time() < deadline:
            if paths.socket_path.exists():
                break
            # If the process exited AND no socket appeared, check mountpoint
            if self.fuselog_proc.poll() is not None:
                # Daemonized: parent exited but mount may still be setting up
                time.sleep(0.2)
                if paths.socket_path.exists():
                    break
                # Check if mount itself is active even without socket yet
                mp = subprocess.run(
                    ["mountpoint", "-q", str(paths.mount_dir)], check=False
                )
                if mp.returncode == 0:
                    # Mount is active, wait a bit more for socket
                    for _ in range(50):
                        if paths.socket_path.exists():
                            break
                        time.sleep(0.1)
                    if paths.socket_path.exists():
                        break
                raise RuntimeError(
                    f"fuselog exited (code={self.fuselog_proc.returncode}) "
                    f"and socket {paths.socket_path} not found"
                )
            time.sleep(0.1)
        else:
            raise RuntimeError(f"timed out waiting for fuselog socket {paths.socket_path}")

        sudo(f"chmod 777 {paths.socket_path}", check=False)
        result = run(f"mountpoint -q {paths.mount_dir} && echo mounted", capture=True, check=False)
        if "mounted" not in result.stdout:
            raise RuntimeError(f"mountpoint check failed for {paths.mount_dir}")

        collector = FuselogCollector(paths.socket_path)
        collector.connect()
        return collector

    def start_sqlite_app(self, paths: ExperimentPaths) -> None:
        run(
            " ".join(
                [
                    "docker run -d",
                    f"--name {APP_CONTAINER}",
                    "--network host",
                    f"-e PORT={APP_PORT}",
                    f"-v {shlex.quote(str(paths.mount_dir))}:/app/data",
                    BOOKCATALOG_IMAGE,
                ]
            )
        )
        wait_http_ok(f"http://127.0.0.1:{APP_PORT}/api/books")

    def start_mysql_stack(self, paths: ExperimentPaths) -> None:
        run(
            " ".join(
                [
                    "docker run -d",
                    f"--name {DB_CONTAINERS['mysql']}",
                    "--network host",
                    "-e MYSQL_ROOT_PASSWORD=root",
                    "-e MYSQL_DATABASE=books",
                    f"-v {shlex.quote(str(paths.mount_dir))}:/var/lib/mysql",
                    MYSQL_IMAGE,
                ]
            )
        )
        wait_tcp("127.0.0.1", MYSQL_PORT, timeout_sec=90)
        run(
            f"docker exec {DB_CONTAINERS['mysql']} mysqladmin ping -h 127.0.0.1 -proot --silent",
            check=False,
        )
        run(
            " ".join(
                [
                    "docker run -d",
                    f"--name {APP_CONTAINER}",
                    "--network host",
                    f"-e PORT={APP_PORT}",
                    "-e DB_TYPE=mysql",
                    BOOKCATALOG_IMAGE,
                ]
            )
        )
        wait_http_ok(f"http://127.0.0.1:{APP_PORT}/api/books", timeout_sec=90)

    def start_postgres_stack(self, paths: ExperimentPaths) -> None:
        run(
            " ".join(
                [
                    "docker run -d",
                    f"--name {DB_CONTAINERS['postgres']}",
                    "--network host",
                    "-e POSTGRES_PASSWORD=root",
                    "-e POSTGRES_DB=books",
                    f"-v {shlex.quote(str(paths.mount_dir))}:/var/lib/postgresql",
                    POSTGRES_IMAGE,
                ]
            )
        )
        wait_tcp("127.0.0.1", POSTGRES_PORT, timeout_sec=90)
        run(
            f"docker exec {DB_CONTAINERS['postgres']} pg_isready -U postgres -d books",
            check=False,
        )
        run(
            " ".join(
                [
                    "docker run -d",
                    f"--name {APP_CONTAINER}",
                    "--network host",
                    f"-e PORT={APP_PORT}",
                    "-e DB_TYPE=postgres",
                    "-e DB_HOST=127.0.0.1",
                    BOOKCATALOG_IMAGE,
                ]
            )
        )
        wait_http_ok(f"http://127.0.0.1:{APP_PORT}/api/books", timeout_sec=90)

    def start_mongo_stack(self, paths: ExperimentPaths) -> None:
        run(
            " ".join(
                [
                    "docker run -d",
                    f"--name {DB_CONTAINERS['mongodb']}",
                    "--network host",
                    f"-v {shlex.quote(str(paths.mount_dir))}:/data/db",
                    MONGO_IMAGE,
                ]
            )
        )
        wait_tcp("127.0.0.1", MONGO_PORT, timeout_sec=90)
        run(
            f"docker exec {DB_CONTAINERS['mongodb']} mongosh --quiet --eval 'db.runCommand({{ ping: 1 }}).ok'",
            check=False,
        )
        run(
            " ".join(
                [
                    "docker run -d",
                    f"--name {APP_CONTAINER}",
                    "--network host",
                    f"-e PORT={APP_PORT}",
                    "-e MONGO_URI=mongodb://127.0.0.1:27017/",
                    BOOKCATALOG_MONGO_IMAGE,
                ]
            )
        )
        wait_http_ok(f"http://127.0.0.1:{APP_PORT}/api/books", timeout_sec=90)

    def start_stack(self, backend: str, paths: ExperimentPaths) -> None:
        if backend == "sqlite":
            self.start_sqlite_app(paths)
            return
        if backend == "mysql":
            self.start_mysql_stack(paths)
            return
        if backend == "postgres":
            self.start_postgres_stack(paths)
            return
        if backend == "mongodb":
            self.start_mongo_stack(paths)
            return
        raise ValueError(f"unsupported backend {backend}")

    def seed_books(self, count: int) -> None:
        url = f"http://127.0.0.1:{APP_PORT}/api/books"
        for i in range(count):
            payload = json.dumps(
                {
                    "title": SEED_PAYLOAD_TEMPLATE["title"].format(i=i),
                    "author": SEED_PAYLOAD_TEMPLATE["author"].format(i=i),
                }
            ).encode()
            http_post(url, payload)

    def measure_backend_mode(self, backend: str, mode: str) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        collector: FuselogCollector | None = None
        run_label = f"{backend}/{mode}"
        try:
            log.info("  [Step 1] Cleaning previous state ...")
            self.cleanup_run_state(f"pre-run reset for {run_label}")
            log.info("  [Step 2] Mounting fuselog (mode=%s) ...", mode)
            collector = self.mount_fuselog(backend, mode)
            assert self.current_paths is not None
            log.info("  [Step 3] Starting %s stack ...", backend)
            self.start_stack(backend, self.current_paths)
            log.info("  [Step 4] Seeding %d books ...", self.args.seed_books)
            self.seed_books(self.args.seed_books)
            collector.drain()

            log.info("  [Step 5] Measuring %d requests ...", self.args.requests)
            url = f"http://127.0.0.1:{APP_PORT}/api/books"
            payload = json.dumps(MEASURED_PAYLOAD).encode()
            for i in range(self.args.requests):
                request_ms, status_code, body = http_post(url, payload)
                capture = collector.drain()
                row = {
                    "backend": backend,
                    "mode": mode,
                    "request_index": i,
                    "request_ms": request_ms,
                    "capture_ms": capture["capture_ms"],
                    "statediff_bytes": capture["statediff_bytes"],
                    "num_actions": capture["num_actions"],
                    "status_code": status_code,
                    "response_bytes": len(body),
                    "seed_books": self.args.seed_books,
                    "payload_bytes": len(payload),
                }
                rows.append(row)
                if (i + 1) % 100 == 0 or i == self.args.requests - 1:
                    log.info(
                        "[%s/%s] request %d/%d -> request_ms=%.3f statediff_bytes=%s num_actions=%s",
                        backend,
                        mode,
                        i + 1,
                        self.args.requests,
                        request_ms,
                        capture["statediff_bytes"],
                        capture["num_actions"],
                    )
        finally:
            if collector is not None:
                collector.close()
            self.cleanup_run_state(f"post-run cleanup for {run_label}")
        return rows

    def run_all(self) -> None:
        total_experiments = len(self.args.backends) * len(self.args.modes)

        log.info("=" * 60)
        log.info("Statediff Size Microbenchmark (fuselog)")
        log.info("  Backends     : %s", ", ".join(self.args.backends))
        log.info("  Modes        : %s", ", ".join(self.args.modes))
        log.info("  Experiments  : %d (%d backends x %d modes)",
                 total_experiments, len(self.args.backends), len(self.args.modes))
        log.info("  Requests     : %d per experiment", self.args.requests)
        log.info("  Seed books   : %d", self.args.seed_books)
        log.info("  Work root    : %s", self.work_root)
        log.info("  Results dir  : %s", self.results_dir)
        log.info("  Fuselog bin  : %s", self.fuselog_bin)
        log.info("=" * 60)

        log.info("[Phase 1] Ensuring fuselog binary and Docker images ...")
        ensure_dir(self.results_dir)
        ensure_dir(self.logs_dir)
        ensure_dir(self.work_root)
        self.ensure_fuselog()
        self.build_images()

        log.info("[Phase 2] Running %d experiments ...", total_experiments)
        raw_rows: list[dict[str, object]] = []
        succeeded = 0
        failed = 0
        failed_list: list[str] = []
        for i, (backend, mode) in enumerate(
            [(b, m) for b in self.args.backends for m in self.args.modes], start=1
        ):
            run_label = f"{backend}/{mode}"
            log.info("=" * 60)
            log.info("[%d/%d] Starting experiment: %s", i, total_experiments, run_label)
            log.info("=" * 60)
            try:
                rows = self.measure_backend_mode(backend, mode)
                raw_rows.extend(rows)
                succeeded += 1
                if rows:
                    avg_bytes = sum(float(r["statediff_bytes"]) for r in rows) / len(rows)
                    avg_ms = sum(float(r["request_ms"]) for r in rows) / len(rows)
                    log.info("[%d/%d] Done: %s — avg statediff=%.0f bytes, avg request=%.3f ms",
                             i, total_experiments, run_label, avg_bytes, avg_ms)
            except Exception:
                log.exception("[%d/%d] FAILED: %s — skipping", i, total_experiments, run_label)
                failed += 1
                failed_list.append(run_label)

        log.info("[Phase 3] Writing results ...")
        raw_path = self.results_dir / "raw.csv"
        summary_path = self.results_dir / "summary.csv"
        summary_json_path = self.results_dir / "summary.json"

        summary_rows: list[dict[str, object]] = []
        if raw_rows:
            with raw_path.open("w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=list(raw_rows[0].keys()))
                writer.writeheader()
                writer.writerows(raw_rows)

            summary_rows = summarize_rows(raw_rows)
            with summary_path.open("w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=list(summary_rows[0].keys()))
                writer.writeheader()
                writer.writerows(summary_rows)
            summary_json_path.write_text(json.dumps(summary_rows, indent=2), encoding="utf-8")

            log.info("  Wrote %s", raw_path)
            log.info("  Wrote %s", summary_path)
            log.info("  Wrote %s", summary_json_path)

        log.info("=" * 60)
        log.info("Summary")
        log.info("  Succeeded : %d/%d", succeeded, total_experiments)
        if failed > 0:
            log.info("  Failed    : %d/%d (%s)", failed, total_experiments, ", ".join(failed_list))
        if summary_rows:
            log.info("  %-12s %-15s %12s %12s %12s", "Backend", "Mode", "SD Bytes Avg", "SD Bytes P50", "Req ms Avg")
            log.info("  %s", "-" * 67)
            for row in summary_rows:
                log.info("  %-12s %-15s %12.0f %12.0f %12.3f",
                         row["backend"], row["mode"],
                         row["statediff_bytes_avg"], row["statediff_bytes_p50"],
                         row["request_ms_avg"])
        log.info("=" * 60)


def driver_main(args: argparse.Namespace) -> int:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_dir = RESULTS_BASE / f"microbench_fuse_sdsize_db_{timestamp}"
    results_dir.mkdir(parents=True, exist_ok=True)
    configure_logging(results_dir / "driver.log", args.verbose)

    remote_dir_expr = expand_remote_path(args.remote_dir)
    remote_results_dir = f"{remote_dir_expr}/results/{results_dir.name}"

    log.info("=" * 60)
    log.info("Statediff Size Microbenchmark — Driver")
    log.info("  Worker host  : %s", args.remote_host)
    log.info("  Remote dir   : %s", args.remote_dir)
    log.info("  Backends     : %s", ", ".join(args.backends))
    log.info("  Modes        : %s", ", ".join(args.modes))
    log.info("  Requests     : %d", args.requests)
    log.info("  Seed books   : %d", args.seed_books)
    log.info("  Results      : %s", results_dir)
    log.info("=" * 60)

    log.info("[Phase 1] Preparing remote worker ...")
    ssh(
        args.remote_host,
        f"mkdir -p {remote_dir_expr} {remote_dir_expr}/services {remote_dir_expr}/xdn-fs {remote_dir_expr}/results",
    )

    needed_images = required_app_images(args.backends)
    remote_images = {image: remote_docker_image_exists(args.remote_host, image) for image in needed_images}
    for image, exists in sorted(remote_images.items()):
        log.info("Worker image %s: %s", image, "present" if exists else "missing")

    fuselog_present = remote_path_exists(args.remote_host, f"{remote_dir_expr}/xdn-fs/cpp/fuselog")
    log.info("Worker fuselog binary in staged dir: %s", "present" if fuselog_present else "missing")

    log.info("[Phase 2] Syncing files to worker ...")
    rsync_to_remote(args.remote_host, args.remote_dir, [Path(__file__).resolve()])
    if BOOKCATALOG_IMAGE in needed_images and not remote_images.get(BOOKCATALOG_IMAGE, False) and not args.skip_build:
        rsync_to_remote(args.remote_host, f"{args.remote_dir}/services", [ROOT_DIR / "services" / "bookcatalog"])
    else:
        log.info("Skipping transfer of services/bookcatalog; worker image already exists or builds are skipped.")
    if BOOKCATALOG_MONGO_IMAGE in needed_images and not remote_images.get(BOOKCATALOG_MONGO_IMAGE, False) and not args.skip_build:
        rsync_to_remote(args.remote_host, f"{args.remote_dir}/services", [ROOT_DIR / "services" / "bookcatalog-mongo"])
    else:
        log.info("Skipping transfer of services/bookcatalog-mongo; worker image already exists or builds are skipped.")
    if not fuselog_present:
        rsync_to_remote(args.remote_host, f"{args.remote_dir}/xdn-fs", [ROOT_DIR / "xdn-fs" / "cpp"])
    else:
        log.info("Skipping transfer of xdn-fs/cpp; staged fuselog binary already exists on worker.")

    worker_cmd = [
        "python3",
        str(Path(__file__).name),
        "--worker-mode",
        f"--results-dir results/{results_dir.name}",
        f"--work-root {shlex.quote(args.remote_work_root)}",
        f"--requests {args.requests}",
        f"--seed-books {args.seed_books}",
        f"--backends {shlex.quote(','.join(args.backends))}",
        f"--modes {shlex.quote(','.join(args.modes))}",
    ]
    if args.skip_build:
        worker_cmd.append("--skip-build")
    if args.verbose:
        worker_cmd.append("--verbose")

    remote_cmd = f"cd {remote_dir_expr} && " + " ".join(worker_cmd)
    rc = 0
    log.info("[Phase 3] Running benchmark on worker ...")
    try:
        ssh(args.remote_host, remote_cmd)
    except subprocess.CalledProcessError as exc:
        rc = exc.returncode
        log.exception("Remote benchmark failed.")
    finally:
        log.info("[Phase 4] Collecting results from worker ...")
        try:
            rsync_from_remote(args.remote_host, remote_results_dir, results_dir)
        except subprocess.CalledProcessError:
            log.exception("Failed to copy remote results from %s", remote_results_dir)

    log.info("=" * 60)
    log.info("Done. Results in %s", results_dir)
    if rc != 0:
        log.warning("Worker exited with code %d — some experiments may have failed", rc)
    log.info("=" * 60)
    return rc


def worker_main(args: argparse.Namespace) -> int:
    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    configure_logging(results_dir / "run.log", args.verbose)
    runner = LocalWorkerRunner(args)
    atexit.register(runner.cleanup)
    try:
        runner.run_all()
    finally:
        runner.cleanup()
    log.info("Done. Results in %s", results_dir)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--remote-host", default=DEFAULT_REMOTE_HOST)
    parser.add_argument("--remote-dir", default=DEFAULT_REMOTE_DIR)
    parser.add_argument("--remote-work-root", default="/dev/shm/microbench-fuse-sdsize-db")
    parser.add_argument("--results-dir", default=None)
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--seed-books", type=int, default=200)
    parser.add_argument("--backends", type=str, default=",".join(DEFAULT_BACKENDS))
    parser.add_argument("--modes", type=str, default=",".join(DEFAULT_MODES))
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--worker-mode", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--work-root", default="/dev/shm/microbench-fuse-sdsize-db", help=argparse.SUPPRESS)
    args = parser.parse_args()

    args.backends = parse_csv_list(args.backends, DEFAULT_BACKENDS)
    args.modes = parse_csv_list(args.modes, DEFAULT_MODES)

    valid_backends = set(DEFAULT_BACKENDS)
    valid_modes = set(DEFAULT_MODES)
    invalid_backends = [backend for backend in args.backends if backend not in valid_backends]
    invalid_modes = [mode for mode in args.modes if mode not in valid_modes]
    if invalid_backends:
        parser.error(f"unsupported backends: {invalid_backends}")
    if invalid_modes:
        parser.error(f"unsupported modes: {invalid_modes}")
    if args.worker_mode and not args.results_dir:
        parser.error("--worker-mode requires --results-dir")
    return args


if __name__ == "__main__":
    parsed = parse_args()
    if parsed.worker_mode:
        sys.exit(worker_main(parsed))
    sys.exit(driver_main(parsed))
