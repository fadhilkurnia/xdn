#!/usr/bin/env python3
"""run_interreplica_bandwidth.py — CLI to capture per-peer bandwidth between
XDN ActiveReplicas during a measurement window.

Reads a gigapaxos properties file to discover AR hosts and coordination ports,
starts bpftrace on each host (locally or over SSH), sleeps for `--duration`
seconds while the operator drives workload, stops bpftrace, parses the output,
and writes a per-peer CSV plus raw logs under `--out`.

Examples
--------
Local 3-AR loopback cluster:
    sudo python3 eval/run_interreplica_bandwidth.py \\
        --config conf/gigapaxos.xdn.local.properties \\
        --local --duration 30 --out /tmp/bw-smoke/

CloudLab multi-host:
    python3 eval/run_interreplica_bandwidth.py \\
        --config conf/gigapaxos.xdn.cloudlab.local.13nodes.properties \\
        --ssh-key /ssh/key --user fadhil \\
        --duration 60 --out results/bw-run1/ \\
        [--include-rc]
"""

import argparse
import os
import signal
import sys
import time
from pathlib import Path

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from interreplica_bandwidth_bpftrace import (  # noqa: E402
    BandwidthProbe,
    ar_hosts_from_config,
    parse_gigapaxos_config,
    watched_ports_from_config,
)


def _parse_args():
    p = argparse.ArgumentParser(
        description="Capture per-peer TCP bandwidth between XDN replicas.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--config", required=True,
                   help="Path to gigapaxos properties file.")
    p.add_argument("--duration", type=float, required=True,
                   help="Seconds to trace. Drive workload during this window.")
    p.add_argument("--out", required=True,
                   help="Output directory. CSV + raw bpftrace logs land here.")
    p.add_argument("--include-rc", action="store_true",
                   help="Also watch reconfigurator (RC) ports.")
    p.add_argument("--local", action="store_true",
                   help="Run bpftrace locally (single-host dev cluster). "
                        "Skips SSH; ignores --ssh-key / --user.")
    p.add_argument("--hosts", default=None,
                   help="Comma-separated host list that overrides hosts "
                        "discovered from --config. Useful when AR IPs differ "
                        "between the config and your SSH reachability.")
    p.add_argument("--ssh-key", default=None,
                   help="SSH identity file; forwarded as 'ssh -i KEY'.")
    p.add_argument("--user", default=None,
                   help="SSH username; forwarded as 'ssh user@host'.")
    return p.parse_args()


def _format_row(r):
    return (f"  {r['host']:<20} {r['local_port']:>5} -> "
            f"{r['peer_ip']:>15}:{r['peer_port']:<5}  "
            f"tx={r['tx_bytes']:>12}B ({r['tx_msgs']:>7} msgs)  "
            f"rx={r['rx_bytes']:>12}B ({r['rx_msgs']:>7} msgs)")


def main():
    args = _parse_args()

    cfg = parse_gigapaxos_config(args.config)
    watched_ports = watched_ports_from_config(args.config,
                                              include_rc=args.include_rc)
    if not watched_ports:
        sys.exit(f"ERROR: no active.* ports found in {args.config}")

    if args.hosts:
        hosts = [h.strip() for h in args.hosts.split(",") if h.strip()]
    elif args.local:
        hosts = ["localhost"]
    else:
        hosts = ar_hosts_from_config(args.config)
        if not hosts:
            sys.exit(f"ERROR: no active.* hosts found in {args.config}")

    print("=" * 72)
    print("  interreplica_bandwidth probe")
    print("=" * 72)
    print(f"  config:         {args.config}")
    print(f"  hosts:          {hosts}")
    print(f"  watched ports:  {watched_ports} "
          f"(AR={[p for _, p in cfg['ar_endpoints']]}"
          f"{', RC=' + str([p for _, p in cfg['rc_endpoints']]) if args.include_rc else ''})")
    print(f"  mode:           {'local' if args.local else 'ssh'}")
    print(f"  duration:       {args.duration}s")
    print(f"  out:            {args.out}")

    probe = BandwidthProbe(
        hosts, watched_ports,
        local=args.local,
        ssh_user=args.user,
        ssh_key=args.ssh_key,
    )

    stopped = {"flag": False}

    def _graceful(*_):
        stopped["flag"] = True

    signal.signal(signal.SIGINT, _graceful)
    signal.signal(signal.SIGTERM, _graceful)

    try:
        print("\n  Starting bpftrace on all hosts ...")
        probe.start()
        print(f"  Tracing. Drive your workload now. (Ctrl-C to stop early)")
        deadline = time.monotonic() + args.duration
        while not stopped["flag"] and time.monotonic() < deadline:
            time.sleep(0.25)
        elapsed = args.duration - max(deadline - time.monotonic(), 0.0)
        print(f"  Stopping after {elapsed:.1f}s ...")
    finally:
        rows, raw = probe.stop_and_collect(results_dir=args.out)

    if not rows:
        print("\n  No peer traffic observed. Possible reasons:")
        print("    - No workload was driven during the window.")
        print("    - Watched ports don't match the running cluster.")
        print("    - bpftrace failed to attach (see raw logs in --out).")
    else:
        print(f"\n  Captured {len(rows)} peer-flow rows:\n")
        for r in rows:
            print(_format_row(r))

    out = Path(args.out)
    csv_path = out / "interreplica_bandwidth.csv"
    print(f"\n  CSV:  {csv_path}")
    print(f"  Raw:  {out}/bpftrace_raw_<host>.txt")
    print("=" * 72)


if __name__ == "__main__":
    main()
