#!/usr/bin/env python3
"""
capture_bench.py - Deploys an app (from an XDN descriptor) with fuselog mounted
(or not, via --mode none, for baseline comparison) on its state directory, runs
a fixed-count closed-loop read/write workload against it, and captures
fuselog stateDiff(s) according to --capture-strategy:

  per-write (default): capture a baseline diff #0 right after the app becomes
    healthy, then capture again after every single write. This is the
    fine-grained variant -- fuselog gets many small natural checkpoints.

  end-only: capture NOTHING until every request has completed, then capture
    EXACTLY ONCE. Since fuselog's 'g' command returns everything accumulated
    since the last capture (or since mount, if there's never been one), that
    single diff already contains the container-init baseline AND every
    workload write folded together -- there's no separate baseline diff in
    this mode. This asks a different question than per-write: does fuselog
    produce a correct stateDiff when it has to account for everything at
    once, in one much larger diff, rather than being interrupted after every
    write?

Flow:
  1. Mount fuselog on live/ (mode=fuselog only) -- ALWAYS before docker compose up,
     since the DB container's volume source is live/ from the start. Whenever
     mode=fuselog, FUSELOG_TRACE_FILE is always set to results/fuselog_trace.log
     (fixed path, not a flag) so WRITE_DONE/PUSH/HARVEST events are always
     available for correlating write completion against harvest boundaries.
  2. Generate a docker-compose.yml from the app's XDN descriptor (one service per
     component, dedicated network, entry depends_on state via service_healthy).
  3. docker compose up -d; wait for the stateful container healthy, then poll
     the entry app's HTTP healthcheck path directly from the host.
  4. [per-write only] Capture diff #0 (the container-init baseline) immediately
     after healthy.
  5. Run --requests iterations in a fixed interleaved read/write pattern derived
     from --read-write-ratio (e.g. 0.7 -> 7 reads, 3 writes, repeating). Writes
     embed a monotonic counter in the payload. [per-write only] after each
     write, hold the HTTP response, capture the stateDiff, THEN record the
     request as complete.
  6. [end-only only] After every request has completed, capture the single
     end-of-run stateDiff, saved as count=0.
  7. Write results/requests.csv + results/summary.json (shape depends on
     --capture-strategy -- see below).
  8. Teardown: docker compose down, unmount fuselog.

summary.json shape varies honestly by --capture-strategy rather than padding
missing fields with null:
  - per-write adds: write_latency_incl_capture_ms, write_latency_excl_capture_ms,
    capture_latency_ms, diff_sequence_gaps
  - end-only adds: write_latency_ms, final_capture_ms

Usage:
  python3 capture_bench.py --app bookcatalog-nd-mysql \
      --requests 2000 --read-write-ratio 0.7 --out-dir runs/run1

  python3 capture_bench.py --app bookcatalog-nd-mysql --mode none \
      --capture-strategy end-only --requests 2000 --out-dir runs/run2
"""

import argparse
import csv
import json
import os
import random
import statistics
import subprocess
import sys
import time
from fractions import Fraction
from pathlib import Path

import requests as http

import bench_common as bc

SCRIPT_DIR = Path(__file__).resolve().parent


def log(run_log_fh, msg: str):
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    print(line)
    if run_log_fh:
        run_log_fh.write(line + "\n")
        run_log_fh.flush()


def build_pattern(read_write_ratio: float):
    """Fixed interleaved pattern, e.g. ratio=0.7 -> 7 reads then 3 writes,
    repeating (not randomized per-request, for reproducibility)."""
    frac = Fraction(read_write_ratio).limit_denominator(20)
    reads, total = frac.numerator, frac.denominator
    writes = total - reads
    pattern = (["read"] * reads) + (["write"] * writes)
    if not pattern:
        raise ValueError("read-write-ratio produced an empty pattern")
    return pattern


def mount_fuselog(live_dir: Path, sock_path: Path, fuselog_bin: str, log_fh,
                   disable_coalescing: bool = False, disable_prune: bool = False,
                   trace_file: Path = None):
    live_dir.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["FUSELOG_SOCKET_FILE"] = str(sock_path)
    if disable_coalescing:
        env["WRITE_COALESCING"] = "false"
    if disable_prune:
        env["FUSELOG_PRUNE"] = "false"
    if trace_file:
        env["FUSELOG_TRACE_FILE"] = str(trace_file)
    log(log_fh, f"mounting fuselog on {live_dir} (socket={sock_path}) "
                f"coalescing={'off' if disable_coalescing else 'on'} "
                f"prune={'off' if disable_prune else 'on'} "
                f"trace={trace_file if trace_file else '(disabled)'}")
    subprocess.Popen(
        [fuselog_bin, "-o", "allow_other", str(live_dir.resolve())],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    deadline = time.time() + 10
    mounted = False
    while time.time() < deadline:
        result = subprocess.run(["mountpoint", "-q", str(live_dir.resolve())])
        if result.returncode == 0:
            mounted = True
            break
        time.sleep(0.2)
    if not mounted:
        raise RuntimeError(f"fuselog failed to mount on {live_dir} within 10s")

    # mountpoint -q only confirms the mount table entry exists -- it does not
    # confirm fuselog's FUSE handler is actually ready to service I/O yet.
    # Probe with a real write+read+delete before handing off to docker compose,
    # since a container touching the mount too early can fail its own init.
    probe_file = live_dir / ".fuselog_ready_probe"
    probe_deadline = time.time() + 10
    while time.time() < probe_deadline:
        try:
            probe_file.write_text("ready")
            assert probe_file.read_text() == "ready"
            probe_file.unlink()
            log(log_fh, f"fuselog I/O probe succeeded on {live_dir}")
            return
        except (OSError, AssertionError):
            time.sleep(0.2)
    raise RuntimeError(f"fuselog mounted on {live_dir} but did not become I/O-ready within 10s")


def unmount_fuselog(live_dir: Path, log_fh):
    log(log_fh, f"unmounting fuselog from {live_dir}")
    subprocess.run(["fusermount", "-u", str(live_dir)], check=False,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["fusermount3", "-u", str(live_dir)], check=False,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def compose_up(compose_path: Path, log_fh):
    log(log_fh, f"docker compose -f {compose_path} up -d")
    env = os.environ.copy()
    env["UID"] = str(os.getuid())
    env["GID"] = str(os.getgid())
    subprocess.run(["docker", "compose", "-f", str(compose_path), "up", "-d"],
                   check=True, env=env)


def compose_down(compose_path: Path, log_fh):
    log(log_fh, f"docker compose -f {compose_path} down -v")
    subprocess.run(["docker", "compose", "-f", str(compose_path), "down", "-v"], check=False)


def wait_for_healthy(container_name: str, timeout: int, log_fh) -> bool:
    log(log_fh, f"waiting for {container_name} to report healthy (timeout={timeout}s)")
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Health.Status}}", container_name],
            capture_output=True, text=True,
        )
        status = result.stdout.strip()
        if status == "healthy":
            log(log_fh, f"{container_name} is healthy")
            return True
        time.sleep(2)
    log(log_fh, f"TIMEOUT waiting for {container_name} to become healthy")
    return False


def wait_for_http_healthy(url: str, timeout: int, log_fh) -> bool:
    log(log_fh, f"waiting for {url} to respond (timeout={timeout}s)")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = http.get(url, timeout=3)
            if resp.status_code == 200:
                log(log_fh, f"{url} responded 200 -- app is up")
                return True
        except http.exceptions.RequestException:
            pass
        time.sleep(1)
    log(log_fh, f"TIMEOUT waiting for {url} to respond with 200")
    return False


def save_diff(diff_dir: Path, app_key: str, count: int, payload: bytes):
    fname = bc.diff_filename(app_key, count)
    (diff_dir / fname).write_bytes(payload)
    return fname


def check_diff_sequence_gaps(diff_dir: Path, app_key: str, expected_max: int, log_fh):
    """Sanity check: after the run, the diff/ dir should contain exactly
    0..expected_max with no gaps. Flag (don't silently ignore) any deviation --
    a gap here likely means a capture failure earlier in the run."""
    found = set()
    for entry in diff_dir.iterdir():
        m = bc.DIFF_NAME_RE.match(entry.name)
        if m and m.group("primary") == app_key:
            found.add(int(m.group("count")))
    expected = set(range(expected_max + 1))
    missing = expected - found
    if missing:
        log(log_fh, f"WARNING: diff sequence has gaps -- missing counts: {sorted(missing)}")
    else:
        log(log_fh, f"diff sequence OK: counts 0..{expected_max} all present")
    return missing


def percentiles(values):
    if not values:
        return {"p50": None, "p95": None, "p99": None, "avg": None, "count": 0}
    s = sorted(values)
    n = len(s)
    return {
        "p50": s[int(n * 0.50) if n * 0.50 < n else n - 1],
        "p95": s[int(n * 0.95) if n * 0.95 < n else n - 1],
        "p99": s[int(n * 0.99) if n * 0.99 < n else n - 1],
        "avg": statistics.mean(s),
        "count": n,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--app", required=True, help="App key in bench_endpoints.yaml")
    ap.add_argument("--mode", default="fuselog", choices=["fuselog", "none"],
                     help="Default fuselog; use 'none' for a no-fuselog baseline run")
    ap.add_argument("--capture-strategy", default="per-write", choices=["per-write", "end-only"],
                     help="per-write: baseline + one diff per write (default). "
                          "end-only: no baseline, exactly one diff after all requests complete.")
    ap.add_argument("--requests", type=int, required=True, help="Total closed-loop request count")
    ap.add_argument("--read-write-ratio", type=float, default=0.7,
                     help="Fraction of requests that are reads (default 0.7)")
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--endpoints-config", type=Path, default=SCRIPT_DIR / "bench_endpoints.yaml")
    ap.add_argument("--fuselog-bin", default="fuselog")
    ap.add_argument("--health-timeout", type=int, default=120)
    ap.add_argument("--disable-coalescing", action="store_true",
                     help="Set WRITE_COALESCING=false for the fuselog process "
                          "(fuselogv2.cpp reads this via getenv_bool at startup, "
                          "default true -- no rebuild needed to toggle it)")
    ap.add_argument("--disable-prune", action="store_true",
                     help="Set FUSELOG_PRUNE=false for the fuselog process "
                          "(same mechanism as --disable-coalescing, default true)")
    args = ap.parse_args()
    args.out_dir = args.out_dir.resolve()

    entry, descriptor = bc.load_app_config(args.endpoints_config, args.app)

    run_id = args.out_dir.name
    live_dir = args.out_dir / "live"
    diff_dir = args.out_dir / "diff"
    results_dir = args.out_dir / "results"
    for d in (live_dir, diff_dir, results_dir):
        d.mkdir(parents=True, exist_ok=True)

    log_fh = open(results_dir / "run.log", "w")
    log(log_fh, f"app={args.app} mode={args.mode} capture_strategy={args.capture_strategy} "
                f"requests={args.requests} ratio={args.read_write_ratio} out_dir={args.out_dir} "
                f"disable_coalescing={args.disable_coalescing} "
                f"disable_prune={args.disable_prune}")

    sock_path = args.out_dir / "fuselog.sock"
    compose_path = args.out_dir / "docker-compose.yml"
    host_port = entry["capture_host_port"]
    # Fixed path, always used when mode=fuselog -- no CLI flag needed.
    trace_file = results_dir / "fuselog_trace.log"

    entry_name, entry_spec = bc.find_entry_component(descriptor)
    state_component, _ = bc.parse_state(descriptor)
    entry_container = f"{run_id}-{entry_name}"
    state_container = f"{run_id}-{state_component}"

    fs_sock = None
    aborted = False
    write_counter = 0
    completed_count = 0
    final_capture_ms = None

    try:
        # Step 1: mount fuselog BEFORE compose up, always.
        if args.mode == "fuselog":
            mount_fuselog(live_dir, sock_path, args.fuselog_bin, log_fh,
                          disable_coalescing=args.disable_coalescing,
                          disable_prune=args.disable_prune,
                          trace_file=trace_file)
            fs_sock = bc.connect_fuselog_socket(sock_path)
        else:
            live_dir.mkdir(parents=True, exist_ok=True)

        # Step 2: generate compose file from descriptor.
        bc.generate_capture_compose(descriptor, live_dir, run_id, host_port, compose_path)
        log(log_fh, f"generated {compose_path}")

        # Step 3: bring up, wait for stateful then entry healthy.
        t_state_start = time.perf_counter()
        compose_up(compose_path, log_fh)
        state_healthy = wait_for_healthy(state_container, args.health_timeout, log_fh)
        state_health_time_s = time.perf_counter() - t_state_start
        log(log_fh, f"stateful container ({state_container}) became healthy in "
                f"{state_health_time_s:.3f}s")
        if not state_healthy:
            raise RuntimeError(f"{state_container} never became healthy; aborting")

        hc_path = entry_spec.get("healthcheck", {}).get("path", "/")
        app_url = f"http://localhost:{host_port}{hc_path}"
        healthy = wait_for_http_healthy(app_url, args.health_timeout, log_fh)
        if not healthy:
            raise RuntimeError(f"{app_url} never responded; aborting")

        # Step 4: baseline diff #0 -- per-write strategy only. end-only
        # deliberately captures nothing until after the request loop.
        if args.capture_strategy == "per-write" and args.mode == "fuselog":
            diff0 = bc.capture_state_diff(fs_sock)
            fname = save_diff(diff_dir, args.app, 0, diff0)
            log(log_fh, f"captured baseline diff #0 -> {fname} ({len(diff0)} bytes)")

        # Step 5: request loop.
        pattern = build_pattern(args.read_write_ratio)
        log(log_fh, f"read/write pattern (repeating): {pattern}")

        base_url = f"http://localhost:{host_port}"
        read_tmpl = entry["read_endpoint"]
        write_endpoint = entry["write_endpoint"]
        write_method = entry.get("write_method", "POST")
        write_payload_tmpl = entry["write_payload"]
        write_headers = entry.get("write_headers", {})

        requests_csv = results_dir / "requests.csv"
        csv_fh = open(requests_csv, "w", newline="")
        writer = csv.writer(csv_fh)
        writer.writerow(["idx", "type", "id_or_counter", "http_status",
                         "latency_ms", "capture_ms"])

        read_latencies = []
        write_latencies_incl_capture = []
        write_latencies_excl_capture = []
        capture_latencies = []

        for i in range(args.requests):
            kind = pattern[i % len(pattern)]

            if kind == "write":
                write_counter += 1
                payload = write_payload_tmpl.replace("{counter}", str(write_counter))
                t0 = time.perf_counter()
                try:
                    resp = http.request(write_method, base_url + write_endpoint,
                                         data=payload, headers=write_headers, timeout=30)
                    status = resp.status_code
                except Exception as e:
                    log(log_fh, f"request #{i} write failed: {e}")
                    status = -1
                t_resp = time.perf_counter()

                capture_ms = None
                if args.capture_strategy == "per-write" and args.mode == "fuselog":
                    try:
                        diff_bytes = bc.capture_state_diff(fs_sock)
                        save_diff(diff_dir, args.app, write_counter, diff_bytes)
                    except bc.StateDiffDesyncError as e:
                        log(log_fh, f"FATAL: stateDiff desync at write #{write_counter}: {e}")
                        aborted = True
                    except RuntimeError as e:
                        log(log_fh, f"FATAL: capture failed at write #{write_counter}: {e}")
                        aborted = True
                t_end = time.perf_counter()
                if args.capture_strategy == "per-write" and args.mode == "fuselog":
                    capture_ms = (t_end - t_resp) * 1000

                latency_ms = (t_end - t0) * 1000
                excl_capture_ms = (t_resp - t0) * 1000
                write_latencies_incl_capture.append(latency_ms)
                write_latencies_excl_capture.append(excl_capture_ms)
                if capture_ms is not None:
                    capture_latencies.append(capture_ms)

                writer.writerow([i, "write", write_counter, status,
                                 f"{latency_ms:.3f}", f"{capture_ms:.3f}" if capture_ms else ""])
                completed_count = i + 1

                if aborted:
                    log(log_fh, "aborting request loop due to stateDiff capture failure "
                                "(continuing would produce a corrupted/incomplete diff sequence)")
                    break

            else:  # read
                # ids are arbitrary/possibly nonexistent -- fine per spec.
                book_id = random.randint(1, max(write_counter, 1))
                url = base_url + read_tmpl.replace("{id}", str(book_id))
                t0 = time.perf_counter()
                try:
                    resp = http.get(url, timeout=30)
                    status = resp.status_code
                except Exception as e:
                    log(log_fh, f"request #{i} read failed: {e}")
                    status = -1
                t_end = time.perf_counter()
                latency_ms = (t_end - t0) * 1000
                read_latencies.append(latency_ms)
                writer.writerow([i, "read", book_id, status, f"{latency_ms:.3f}", ""])
                completed_count = i + 1

        csv_fh.close()

        # Step 6: end-only strategy's single capture, now that every request
        # has completed.
        if args.capture_strategy == "end-only" and args.mode == "fuselog" and not aborted:
            log(log_fh, "request loop complete -- capturing the single end-of-run stateDiff")
            t_cap0 = time.perf_counter()
            try:
                diff_bytes = bc.capture_state_diff(fs_sock)
                fname = save_diff(diff_dir, args.app, 0, diff_bytes)
                final_capture_ms = (time.perf_counter() - t_cap0) * 1000
                log(log_fh, f"captured end-of-run diff -> {fname} "
                            f"({len(diff_bytes)} bytes, {final_capture_ms:.3f}ms)")
            except bc.StateDiffDesyncError as e:
                log(log_fh, f"FATAL: stateDiff desync on end-of-run capture: {e}")
                aborted = True
            except RuntimeError as e:
                log(log_fh, f"FATAL: end-of-run capture failed: {e}")
                aborted = True

        # Step 6 (part of, per-write only): gap check on the diff sequence
        # actually produced. Not meaningful for end-only (trivially one file).
        gaps = set()
        if args.capture_strategy == "per-write" and args.mode == "fuselog":
            gaps = check_diff_sequence_gaps(diff_dir, args.app, write_counter, log_fh)

        summary = {
            "app": args.app,
            "mode": args.mode,
            "capture_strategy": args.capture_strategy,
            "requested_count": args.requests,
            "completed_count": completed_count,
            "aborted": aborted,
            "pattern": pattern,
            "read_write_ratio": args.read_write_ratio,
            "write_count": write_counter,
            "state_container_health_time_s": state_health_time_s,
            "read_latency_ms": percentiles(read_latencies),
        }
        if args.capture_strategy == "per-write":
            summary["write_latency_incl_capture_ms"] = percentiles(write_latencies_incl_capture)
            summary["write_latency_excl_capture_ms"] = percentiles(write_latencies_excl_capture)
            summary["capture_latency_ms"] = percentiles(capture_latencies) if args.mode == "fuselog" else None
            summary["diff_sequence_gaps"] = sorted(gaps) if gaps else []
        else:  # end-only
            summary["write_latency_ms"] = percentiles(write_latencies_excl_capture)
            summary["final_capture_ms"] = final_capture_ms

        (results_dir / "summary.json").write_text(json.dumps(summary, indent=2))
        log(log_fh, f"summary written to {results_dir / 'summary.json'}")
        print(json.dumps(summary, indent=2))

    finally:
        if fs_sock:
            try:
                fs_sock.close()
            except OSError:
                pass
        compose_down(compose_path, log_fh)
        if args.mode == "fuselog":
            unmount_fuselog(live_dir, log_fh)
        log(log_fh, "teardown complete")
        log_fh.close()

    if aborted:
        sys.exit(1)


if __name__ == "__main__":
    main()
