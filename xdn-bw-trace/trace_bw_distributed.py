#!/usr/bin/env python3
"""trace_bw_distributed.py — orchestrate a multi-host bandwidth trace.

Splits the work across hosts:
  - One bpftrace probe per AR host, started over SSH. Each probe captures
    its own kernel's events and writes a raw CSV + pid_to_ar.json sidecar
    on the remote host.
  - One driver instance (run locally on the orchestrator) that drives
    the HTTP workload at every replica.
  - After both finish, per-host CSVs are SCP'd back and the local
    aggregator stitches them into a single resolved CSV.

This is the "Option A" architecture sketched in xdn-bw-trace/README.md's
"Distributed deployment" section. The single-host `trace_bw.py --mode local`
path is unchanged and still works for loopback testing.

Usage:
    python3 xdn-bw-trace/trace_bw_distributed.py \\
        --config conf/gigapaxos.cloudlab.properties \\
        --service bookcatalog \\
        --ssh-key ~/.ssh/cloudlab \\
        --username fadhil \\
        --duration 60 --interval 5 --rate 50 --read-ratio 0.0

Required:
  --config        gigapaxos properties file (used for RC discovery and
                  AR host enumeration on both the orchestrator and remote
                  side; the orchestrator copies it to each host first).
  --service       XDN service name.
  --ssh-key       SSH private key for connecting to AR hosts.
  --username      remote SSH user.

Optional:
  --remote-xdn-path   absolute path to the xdn checkout on remote hosts
                      (default: same as local). Must contain
                      xdn-bw-trace/trace_bw.py and inter_replica_bw.bt.
  --hosts             comma-separated allowlist of AR hosts. Default:
                      every distinct host found in active.* entries of
                      --config.
  --output            final aggregated CSV path. Default:
                      xdn-bw-trace/results/<service>-<ts>.csv
  --tmpdir            remote tmpdir for per-host CSV outputs. Default:
                      /tmp/xdn-bw-trace
  --rate, --duration, --interval, --warmup, --read-ratio, --path,
  --method, --write-path, --write-method, --write-body,
  --write-content-type, --seed-path, --seed-body, --no-seed, --timeout
                      passed through to both the remote probe and the
                      local driver.
"""

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path

# Reuse trace_bw.py helpers for parsing the gigapaxos config.
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
import trace_bw  # noqa: E402

DEFAULT_REMOTE_TMPDIR = "/tmp/xdn-bw-trace"


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--config", required=True,
                   help="gigapaxos.properties path (must exist on the "
                        "orchestrator and at the same path on remote hosts, "
                        "or pass --remote-config-path).")
    p.add_argument("--service", required=True)
    p.add_argument("--ssh-key", required=True,
                   help="SSH private key used to reach AR hosts.")
    p.add_argument("--username", required=True,
                   help="Remote SSH user.")
    p.add_argument("--remote-xdn-path",
                   help="Absolute path to the xdn checkout on each remote "
                        "host. Default: same as local checkout. Must contain "
                        "xdn-bw-trace/trace_bw.py + inter_replica_bw.bt.")
    p.add_argument("--remote-config-path",
                   help="Absolute path to gigapaxos.properties on remote "
                        "hosts. Default: same as --config.")
    p.add_argument("--hosts",
                   help="Comma-separated allowlist of remote AR hosts. "
                        "Default: every distinct host in active.* entries "
                        "of --config.")
    p.add_argument("--output",
                   help="Aggregated CSV output path. Default: "
                        "xdn-bw-trace/results/<service>-<unix-ts>.csv")
    p.add_argument("--tmpdir", default=DEFAULT_REMOTE_TMPDIR,
                   help=f"Remote tmpdir for per-host outputs. Default: "
                        f"{DEFAULT_REMOTE_TMPDIR}")
    p.add_argument("--reconfigurator",
                   help="RC HTTP endpoint as host[:port] (default: derive "
                        "from --config).")

    # Workload knobs — passed through to driver and probe.
    p.add_argument("--rate", type=float, default=20.0)
    p.add_argument("--duration", type=float, default=60.0)
    p.add_argument("--interval", type=int, default=5)
    p.add_argument("--warmup", type=float, default=2.0)
    p.add_argument("--read-ratio", type=float, default=0.8)
    p.add_argument("--path", default="/api/books/1")
    p.add_argument("--method", default="GET")
    p.add_argument("--write-path", default="/api/books/1")
    p.add_argument("--write-method", default="PUT")
    p.add_argument("--write-body",
                   default='{"title":"trace_bw","author":"benchmark"}')
    p.add_argument("--write-content-type", default="application/json")
    p.add_argument("--seed-path", default="/api/books")
    p.add_argument("--seed-body",
                   default='{"id":1,"title":"trace_bw","author":"benchmark"}')
    p.add_argument("--no-seed", action="store_true")
    p.add_argument("--timeout", type=float, default=5.0)
    p.add_argument("--target",
                   help="Comma-separated allowlist of replica ids the driver "
                        "should drive traffic at. Default: every replica in "
                        "the placement.")

    # Pre-flight knobs.
    p.add_argument("--probe-startup-buffer", type=float, default=3.0,
                   help="Seconds to wait between starting probes and the "
                        "driver, so kprobes are attached before traffic. "
                        "Default: 3.")
    p.add_argument("--keep-remote-files", action="store_true",
                   help="Skip remote tmpdir cleanup at end (useful for "
                        "debugging).")
    return p.parse_args()


def discover_ar_hosts(config_path):
    """Return sorted unique hosts from active.* entries in the properties."""
    actives, _, _ = trace_bw.parse_properties(config_path)
    hosts = sorted({host for (host, _port) in actives.values()})
    if not hosts:
        sys.exit(f"error: no active.<id>=host:port entries in {config_path}")
    return hosts


def ssh_cmd(ssh_key, username, host, remote_cmd, capture=False):
    """Return a subprocess.Popen for `ssh`. Auto-StrictHostKeyChecking=no
    + BatchMode=yes for unattended runs (no password prompts)."""
    args = [
        "ssh",
        "-i", ssh_key,
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=10",
        f"{username}@{host}",
        remote_cmd,
    ]
    if capture:
        return subprocess.Popen(args, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, text=True)
    return subprocess.Popen(args, text=True)


def scp_pull(ssh_key, username, host, remote_path, local_path):
    """Run `scp host:remote_path local_path`. Blocks until done; returns
    (returncode, stderr)."""
    cmd = [
        "scp",
        "-i", ssh_key,
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=10",
        f"{username}@{host}:{remote_path}",
        str(local_path),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    return proc.returncode, proc.stderr


def quote_remote(s):
    """Shell-quote a string for embedding in a remote ssh command."""
    return shlex.quote(s)


def _build_probe_cmd(args, remote_xdn_path, remote_config_path, remote_csv,
                       host_idx):
    """Construct the bash command run on each remote host: cd to the xdn
    checkout, run trace_bw.py in probe mode under sudo (bpftrace needs root)."""
    # Compose the probe args. We don't pass --ar-id because each host's
    # trace_bw probe will scan /proc and pick up exactly the AR JVM(s) that
    # belong to this placement; the sidecar pid_to_ar.json captures who's
    # who. (In practice each AR host has exactly one AR JVM.)
    probe_args = [
        "python3", f"{remote_xdn_path}/xdn-bw-trace/trace_bw.py",
        "--mode", "probe",
        "--config", remote_config_path,
        "--service", args.service,
        "--duration", str(args.duration + args.warmup + args.probe_startup_buffer),
        "--interval", str(args.interval),
        "--output", remote_csv,
    ]
    if args.reconfigurator:
        probe_args += ["--reconfigurator", args.reconfigurator]
    # Note: probes don't drive traffic, so workload args (rate, paths, etc.)
    # are not needed here. Probes only need port-range info derived from
    # the placement, which they fetch from the RC themselves.
    cmd = (
        f"mkdir -p {quote_remote(args.tmpdir)} && "
        f"cd {quote_remote(remote_xdn_path)} && "
        f"sudo -n " + " ".join(quote_remote(a) for a in probe_args)
    )
    return cmd


def _build_driver_cmd(args, local_xdn_path, local_config_path, driver_csv):
    """Local driver: trace_bw.py --mode driver. Pass through workload knobs."""
    driver_args = [
        "python3", str(Path(local_xdn_path) / "xdn-bw-trace" / "trace_bw.py"),
        "--mode", "driver",
        "--config", str(local_config_path),
        "--service", args.service,
        "--rate", str(args.rate),
        "--duration", str(args.duration),
        "--interval", str(args.interval),
        "--warmup", str(args.warmup),
        "--read-ratio", str(args.read_ratio),
        "--path", args.path, "--method", args.method,
        "--write-path", args.write_path, "--write-method", args.write_method,
        "--write-body", args.write_body,
        "--write-content-type", args.write_content_type,
        "--seed-path", args.seed_path, "--seed-body", args.seed_body,
        "--timeout", str(args.timeout),
        "--output", str(driver_csv),
    ]
    if args.no_seed:
        driver_args.append("--no-seed")
    if args.target:
        driver_args += ["--target", args.target]
    if args.reconfigurator:
        driver_args += ["--reconfigurator", args.reconfigurator]
    return driver_args


def main():
    args = parse_args()

    local_xdn_path = SCRIPT_DIR.parent  # repo root
    remote_xdn_path = args.remote_xdn_path or str(local_xdn_path)
    local_config_path = Path(args.config).resolve()
    remote_config_path = args.remote_config_path or str(local_config_path)

    # Discover hosts.
    if args.hosts:
        hosts = [h.strip() for h in args.hosts.split(",") if h.strip()]
    else:
        hosts = discover_ar_hosts(args.config)
    if not hosts:
        sys.exit("error: no remote AR hosts to probe")
    print(f"orchestrator: probing {len(hosts)} hosts: {', '.join(hosts)}",
          file=sys.stderr)
    print(f"  remote xdn path: {remote_xdn_path}", file=sys.stderr)
    print(f"  remote config:   {remote_config_path}", file=sys.stderr)
    print(f"  remote tmpdir:   {args.tmpdir}", file=sys.stderr)

    ts = int(time.time())
    final_out = Path(args.output) if args.output else (
        SCRIPT_DIR / "results" / f"{args.service}-{ts}.csv"
    )
    final_out.parent.mkdir(parents=True, exist_ok=True)

    # Per-host probe outputs (named by host so SCP back doesn't collide).
    remote_csvs = {h: f"{args.tmpdir}/{args.service}-{ts}-{_safe(h)}.csv"
                   for h in hosts}

    # Phase 1: launch probes on every remote host (in parallel).
    print(f"\n=== launching {len(hosts)} remote probes ===", file=sys.stderr)
    probe_procs = {}
    for h in hosts:
        cmd = _build_probe_cmd(args, remote_xdn_path, remote_config_path,
                                remote_csvs[h], host_idx=hosts.index(h))
        print(f"[{h}] ssh: {cmd}", file=sys.stderr)
        probe_procs[h] = ssh_cmd(args.ssh_key, args.username, h, cmd,
                                  capture=True)

    # Wait for probes to attach kprobes before driving traffic.
    print(f"\n=== sleeping {args.probe_startup_buffer}s for kprobe attach ===",
          file=sys.stderr)
    time.sleep(args.probe_startup_buffer)

    # Phase 2: drive workload locally.
    driver_csv = final_out.with_name(final_out.stem + ".driver.csv")
    print(f"\n=== driving workload locally ({args.duration}s) ===",
          file=sys.stderr)
    driver_proc = subprocess.run(
        _build_driver_cmd(args, local_xdn_path, local_config_path, driver_csv),
        text=True,
    )
    if driver_proc.returncode != 0:
        print(f"warning: driver exited with code {driver_proc.returncode}",
              file=sys.stderr)

    # Phase 3: wait for all probes to finish.
    print(f"\n=== waiting for {len(hosts)} probes to finish ===",
          file=sys.stderr)
    for h, proc in probe_procs.items():
        try:
            stdout, stderr = proc.communicate(timeout=args.duration + 60)
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout, stderr = proc.communicate()
            print(f"[{h}] timed out, killed", file=sys.stderr)
        if proc.returncode != 0:
            print(f"[{h}] probe exited rc={proc.returncode}", file=sys.stderr)
            if stderr:
                print(f"[{h}] stderr (last 500B):\n  "
                      + (stderr[-500:].replace("\n", "\n  ")), file=sys.stderr)
        else:
            print(f"[{h}] probe ok", file=sys.stderr)

    # Phase 4: pull per-host CSVs + sidecars back.
    print(f"\n=== fetching per-host CSVs back ===", file=sys.stderr)
    local_csvs = []
    for h in hosts:
        for suffix in (".csv", ".pid_to_ar.json"):
            remote = remote_csvs[h].replace(".csv", suffix)
            local = final_out.parent / f"{args.service}-{ts}-{_safe(h)}{suffix}"
            rc, stderr = scp_pull(args.ssh_key, args.username, h,
                                   remote, local)
            if rc != 0:
                print(f"[{h}] scp {suffix} FAILED: {stderr.strip()}",
                      file=sys.stderr)
            else:
                print(f"[{h}] pulled {local}", file=sys.stderr)
                if suffix == ".csv":
                    local_csvs.append(local)

    if not local_csvs:
        sys.exit("error: no per-host CSVs were retrieved; cannot aggregate")

    # Phase 5: aggregate.
    print(f"\n=== aggregating {len(local_csvs)} per-host CSVs ===",
          file=sys.stderr)
    agg_args = [
        "python3", str(SCRIPT_DIR / "trace_bw.py"),
        "--mode", "aggregate",
        "--config", str(local_config_path),
        "--service", args.service,
        "--inputs", ",".join(str(p) for p in local_csvs),
        "--output", str(final_out),
    ]
    if args.reconfigurator:
        agg_args += ["--reconfigurator", args.reconfigurator]
    rc = subprocess.run(agg_args, text=True).returncode
    if rc != 0:
        sys.exit(f"error: aggregator exited with rc={rc}")

    print(f"\n=== distributed trace complete ===", file=sys.stderr)
    print(f"final csv: {final_out}", file=sys.stderr)
    if driver_csv.exists() or driver_csv.with_name(driver_csv.stem + ".meta.json").exists():
        print(f"driver meta: {driver_csv.with_name(driver_csv.stem + '.meta.json')}",
              file=sys.stderr)

    # Phase 6: optional remote cleanup.
    if not args.keep_remote_files:
        print(f"\n=== cleaning up remote tmp files ===", file=sys.stderr)
        for h in hosts:
            cleanup = (
                f"rm -f {quote_remote(remote_csvs[h])} "
                f"{quote_remote(remote_csvs[h].replace('.csv', '.pid_to_ar.json'))}"
            )
            ssh_cmd(args.ssh_key, args.username, h, cleanup).wait()
    else:
        print(f"\n=== --keep-remote-files: leaving remote tmp files alone ===",
              file=sys.stderr)
    return 0


def _safe(s):
    """Convert a hostname to a filename-safe form."""
    return re.sub(r"[^A-Za-z0-9._-]", "_", s)


if __name__ == "__main__":
    sys.exit(main())
