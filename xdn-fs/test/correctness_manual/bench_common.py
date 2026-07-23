"""
bench_common.py - Shared helpers for capture_bench.py and replay_checkpoints.py

Covers:
  - Loading XDN app descriptors (apps/*.yaml) and bench_endpoints.yaml
  - Generating docker-compose.yml files from an app descriptor
  - The fuselog Unix socket capture protocol ('g' -> 8-byte LE size -> payload)
  - The p<pepoch>:<app_key>:<count>.diff filename convention

Assumptions flagged explicitly (adjust if your setup differs):
  - Entry component readiness is checked via `curl -f http://localhost:<port><healthcheck.path>`
    inside the container. If the app image doesn't have curl, swap for wget --spider.
  - DB healthcheck command / default port is chosen by a substring match on the image name
    ("mysql" / "postgres"). Add more branches to db_healthcheck_test/db_default_port if you
    add other state backends.
  - fuselog-apply is invoked as: fuselog-apply <target_dir> --statediff=<diff_file>
    (matches replay_checkpoints.py's original assumption).
"""

import re
import socket
import struct
import time
from pathlib import Path

import yaml

# ── Diff filename / protocol constants ───────────────────────────────────────

DIFF_NAME_RE = re.compile(r"^p(?P<pepoch>\d+):(?P<primary>.+):(?P<count>\d+)\.diff$")

# Maximum plausible state diff for a single request (500 MB). Above this indicates
# protocol desynchronization (garbage size header) -- matches
# FuselogStateDiffRecorder.MAX_STATEDIFF_BYTES.
MAX_STATEDIFF_BYTES = 2 * 1024 * 1024 * 1024


def diff_filename(app_key: str, count: int, pepoch: int = 0) -> str:
    return f"p{pepoch}:{app_key}:{count:07d}.diff"


# ── Config loading ────────────────────────────────────────────────────────────


def load_yaml(path: Path) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def load_app_config(endpoints_cfg_path: Path, app_key: str):
    """Returns (bench_entry_dict, descriptor_dict) for the given app key."""
    endpoints_cfg = load_yaml(endpoints_cfg_path)
    if app_key not in endpoints_cfg:
        raise KeyError(
            f"app '{app_key}' not found in {endpoints_cfg_path}. "
            f"Available: {list(endpoints_cfg.keys())}"
        )
    entry = endpoints_cfg[app_key]
    descriptor_path = (endpoints_cfg_path.parent / entry["descriptor"]).resolve()
    descriptor = load_yaml(descriptor_path)
    return entry, descriptor


def parse_state(descriptor: dict):
    """'mysql:/var/lib/mysql/' -> ('mysql', '/var/lib/mysql/')"""
    component, path = descriptor["state"].split(":", 1)
    return component, path


def find_component(descriptor: dict, name: str):
    for comp in descriptor["components"]:
        key = next(iter(comp))
        if key == name:
            return key, comp[key]
    raise KeyError(f"component '{name}' not found in descriptor")


def find_entry_component(descriptor: dict):
    for comp in descriptor["components"]:
        key = next(iter(comp))
        if comp[key].get("entry"):
            return key, comp[key]
    raise KeyError("no component with entry: true found in descriptor")


def env_list_to_lines(environments, indent="      "):
    """environments is a list of single-key dicts, e.g. [{'MYSQL_DATABASE': 'books'}, ...]"""
    lines = []
    for e in environments or []:
        for k, v in e.items():
            lines.append(f'{indent}{k}: "{v}"')
    return lines


# ── DB-specific heuristics (extend here for new state backends) ─────────────


def db_healthcheck_test(image: str):
    image_lower = image.lower()
    if "mysql" in image_lower:
        return '["CMD-SHELL", "mysqladmin ping -h 127.0.0.1 --silent"]'
    if "postgres" in image_lower:
        return '["CMD-SHELL", "pg_isready -U postgres"]'
    return None


def db_default_port(image: str) -> int:
    image_lower = image.lower()
    if "mysql" in image_lower:
        return 3306
    if "postgres" in image_lower:
        return 5432
    raise ValueError(f"don't know default port for image: {image}")


# ── Compose generation: capture-side (script A) ──────────────────────────────


def generate_capture_compose(descriptor: dict, live_dir: Path, run_id: str,
                              host_port: int, out_path: Path):
    """One service per descriptor component. The stateful component's volume
    source is live_dir. The entry component depends_on the state component
    being healthy, and publishes host_port -> its container port."""
    state_component, state_path = parse_state(descriptor)
    entry_name, _ = find_entry_component(descriptor)
    network_name = f"benchnet-{run_id}"

    lines = ["services:"]
    for comp in descriptor["components"]:
        name = next(iter(comp))
        spec = comp[name]
        container_name = f"{run_id}-{name}"

        lines.append(f"  {name}:")
        lines.append(f"    image: {spec['image']}")
        lines.append(f"    container_name: {container_name}")

        env_lines = env_list_to_lines(spec.get("environments"))
        if env_lines:
            lines.append("    environment:")
            lines.extend(env_lines)

        if name == state_component:
            # Run as the host UID/GID (same user that runs fuselog) so the
            # entrypoint never needs to chown the fuselog-mounted directory --
            # MySQL/Postgres images only attempt that chown when running as
            # root, and that chown fails through FUSE for a non-root daemon.
            lines.append('    user: "${UID}:${GID}"')
            lines.append("    volumes:")
            lines.append(f"      - {Path(live_dir).resolve()}:{state_path}")

        if spec.get("entry"):
            port = spec.get("port", 80)
            lines.append("    ports:")
            lines.append(f'      - "{host_port}:{port}"')
            # No container-level healthcheck here -- we can't assume curl/wget
            # exist inside the app image. Readiness is instead polled over
            # HTTP directly from the host (see wait_for_http_healthy in
            # capture_bench.py), using this same healthcheck.path.

        if spec.get("stateful"):
            hc = db_healthcheck_test(spec["image"])
            if hc:
                lines.append("    healthcheck:")
                lines.append(f"      test: {hc}")
                lines.append("      interval: 3s")
                lines.append("      timeout: 3s")
                lines.append("      retries: 30")

        if name == entry_name:
            lines.append("    depends_on:")
            lines.append(f"      {state_component}:")
            lines.append("        condition: service_healthy")

        lines.append("    networks:")
        lines.append("      - benchnet")
        lines.append("")

    lines.append("networks:")
    lines.append("  benchnet:")
    lines.append(f"    name: {network_name}")

    out_path.write_text("\n".join(lines))


# ── Compose generation: replay-side (script B) ───────────────────────────────


def generate_replay_compose(descriptor: dict, app_key: str, checkpoint_dirs,
                             base_port: int, out_path: Path):
    """One service per checkpoint. Only the stateful component is started --
    no network, no entry/app container, matching the "only need to check the
    DB container" replay workflow."""
    state_component, state_path = parse_state(descriptor)
    _, state_spec = find_component(descriptor, state_component)
    image = state_spec["image"]
    env_lines = env_list_to_lines(state_spec.get("environments"))
    hc = db_healthcheck_test(image)
    container_port = db_default_port(image)

    lines = ["services:"]
    for i, (count, dest) in enumerate(checkpoint_dirs):
        service = f"checkpoint-{count}"
        lines.append(f"  {service}:")
        lines.append(f"    image: {image}")
        lines.append(f"    container_name: repro-{app_key}-{service}")
        if env_lines:
            lines.append("    environment:")
            lines.extend(env_lines)
        lines.append("    volumes:")
        lines.append(f"      - {Path(dest).resolve()}:{state_path}")
        lines.append("    ports:")
        lines.append(f'      - "{base_port + i}:{container_port}"')
        if hc:
            lines.append("    healthcheck:")
            lines.append(f"      test: {hc}")
            lines.append("      interval: 5s")
            lines.append("      timeout: 5s")
            lines.append("      retries: 30")
        lines.append("")

    out_path.write_text("\n".join(lines))
    return container_port


# ── fuselog Unix socket protocol ──────────────────────────────────────────────


def connect_fuselog_socket(sock_path: Path, max_attempts: int = 100, delay: float = 0.1):
    """Retries since fuselog daemonizes asynchronously (mirrors
    FuselogStateDiffRecorder.preInitialization's retry loop)."""
    last_err = None
    for _ in range(max_attempts):
        try:
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.connect(str(sock_path))
            return s
        except OSError as e:
            last_err = e
            time.sleep(delay)
    raise RuntimeError(f"failed to connect to fuselog socket at {sock_path}: {last_err}")


def recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise RuntimeError(f"socket closed after reading {len(buf)}/{n} bytes")
        buf.extend(chunk)
    return bytes(buf)


class StateDiffDesyncError(RuntimeError):
    """Raised when the fuselog socket protocol appears desynchronized
    (garbage size header). This is a signal worth investigating, not
    something to silently paper over by reconnecting."""


def percentiles(values):
    """p50/p90/p95/p99 + avg, in the same units as the input values."""
    if not values:
        return {"p50": None, "p90": None, "p95": None, "p99": None, "avg": None, "count": 0}
    s = sorted(values)
    n = len(s)

    def pct(p):
        idx = min(int(n * p), n - 1)
        return s[idx]

    return {
        "p50": pct(0.50),
        "p90": pct(0.90),
        "p95": pct(0.95),
        "p99": pct(0.99),
        "avg": sum(s) / n,
        "count": n,
    }


def capture_state_diff(sock: socket.socket) -> bytes:
    """Sends 'g', reads the 8-byte LE size header, then reads exactly that
    many payload bytes. Returns the raw payload verbatim (no size header) --
    matching what FuselogStateDiffRecorder.saveStateDiff writes to disk, so
    files produced here are directly consumable by fuselog-apply."""
    sock.sendall(b"g")
    header = recv_exact(sock, 8)
    size = struct.unpack("<q", header)[0]
    if size < 0 or size > MAX_STATEDIFF_BYTES:
        raise StateDiffDesyncError(
            f"garbage stateDiffSize={size} (raw LE bytes={header.hex()})"
        )
    if size == 0:
        return b""
    return recv_exact(sock, size)
