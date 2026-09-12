#!/usr/bin/env python3
"""Quorum-size inference via latency injection (binary search over delayed links).

Companion to measure_cluster_bw.py: the bandwidth graph identifies the
coordinator/leader L; this probe identifies how many peers L actually waits
for on a write (q, counting L itself). Model: L blocks until the fastest
q-1 of its N-1 peers respond, so delaying m leader-outbound links shifts
client write latency iff m > N - q. The shift predicate is monotone in m
with flip point m* = N - q + 1, hence q = N - m* + 1, found by bisection in
2 + ceil(log2(N-1)) inject-measure-clear rounds. See
papers/xdn-paper/quorum-size-inference.md for the full derivation.

Injection point: coordination traffic rides the attachable overlay, which is
VXLAN-encapsulated on the host NIC, so host-level tc cannot match peer IPs.
Instead tc runs INSIDE the leader member's netns through its bwprobe sidecar
(alpine + iproute2, shares the netns): docker exec --privileged supplies
CAP_NET_ADMIN regardless of the probe's own capability set. A prio qdisc on
the overlay interface steers packets whose dst matches a delayed peer's
overlay IP into a netem delay band; everything else (band 0), including all
client traffic on the bridge interface, is untouched. One-way delay: only
the leader->peer request leg is delayed.

Latency measurement is a closed-loop sequence of PUTs through the HTTP KV
shim frontend (the campaign's canonical vantage point), one outstanding
request at a time with a gap, so each sample reflects a single quorum wait
(no pipelining). Median over --writes samples after --warmup discards.

Example (from the driver, leader on 10.10.1.1, peers' overlay IPs known from
`docker network inspect xdn-cluster-<svc>` or the replica-info endpoint):

  python3 eval/infer_quorum_size.py --service etcd-demo \\
      --ars 10.10.1.1:2300,10.10.1.2:2300,10.10.1.3:2300 \\
      --leader-ssh fadhil@10.10.1.1 --ssh-key ~/.ssh/cloudlab2 \\
      --peers 10.11.0.5,10.11.0.6 \\
      --delta-ms 50 --out /tmp/quorum-etcd.json

Only stdlib is used. Campaign lesson applied: every remote action is a short
one-shot ssh (long-lived channels die silently mid-measure).
"""

import argparse
import json
import statistics
import subprocess
import sys
import time
import urllib.request

SSH_OPTS = ["-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=10"]


def log(msg):
    print(f"[quorum] {msg}", flush=True)


# ------------------------------------------------------------------ remote tc


class Injector:
    """Drives tc inside the leader member's netns via its bwprobe sidecar."""

    def __init__(self, ssh_target, ssh_key, service, probe_container, dev):
        self.target = ssh_target
        self.key = ssh_key
        self.service = service
        self.probe = probe_container
        self.dev = dev

    def _ssh(self, cmd, check=True):
        argv = ["ssh"] + SSH_OPTS
        if self.key:
            argv += ["-i", self.key]
        argv += [self.target, cmd]
        r = subprocess.run(argv, capture_output=True, text=True, timeout=60)
        if check and r.returncode != 0:
            raise RuntimeError(f"ssh failed ({cmd!r}): {r.stderr.strip()}")
        return r.stdout.strip()

    def _tc(self, args, check=True):
        return self._ssh(
            f"docker exec --privileged {self.probe} tc {args}", check=check)

    def discover_probe(self):
        """Finds the bwprobe container for the service on the leader host."""
        # Probe names are bwprobe.<clusterContainerName>, e.g.
        # bwprobe.c0.e0.<service>.<nodeid>.xdn.io -- match the service segment,
        # not a bwprobe.<service> prefix (the cluster container name sits between).
        out = self._ssh(
            "docker ps --format '{{.Names}}' | grep '^bwprobe[.]' "
            "| grep '[.]%s[.]' | head -1" % self.service)
        if not out:
            raise RuntimeError(
                f"no bwprobe.{self.service}* container on {self.target}")
        self.probe = out
        log(f"probe container: {self.probe}")

    def discover_dev(self, peer_ip):
        """Picks the interface that routes to the first peer (the overlay)."""
        out = self._ssh(
            f"docker exec --privileged {self.probe} ip route get {peer_ip}")
        for tok_prev, tok in zip(out.split(), out.split()[1:]):
            if tok_prev == "dev":
                self.dev = tok
                log(f"overlay device: {self.dev} (routes to {peer_ip})")
                return
        raise RuntimeError(f"cannot find device routing to {peer_ip}: {out}")

    def inject(self, peer_ips, delta_ms):
        """Delays egress toward the given peer overlay IPs by delta_ms."""
        self.clear()  # idempotent fresh start each round
        self._tc(f"qdisc add dev {self.dev} root handle 1: prio bands 4 "
                 "priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0")
        self._tc(f"qdisc add dev {self.dev} parent 1:4 handle 40: "
                 f"netem delay {delta_ms}ms")
        for ip in peer_ips:
            self._tc(f"filter add dev {self.dev} parent 1: protocol ip "
                     f"prio 1 u32 match ip dst {ip}/32 flowid 1:4")

    def clear(self):
        self._tc(f"qdisc del dev {self.dev} root", check=False)

    def ping_ms(self, peer_ip):
        """Best-effort RTT check to confirm injection took effect."""
        try:
            out = self._ssh(
                f"docker exec --privileged {self.probe} "
                f"ping -c 2 -W 2 {peer_ip}", check=False)
            for line in out.splitlines():  # busybox: round-trip min/avg/max = ...
                if "min/avg/max" in line:
                    return float(line.split("=")[1].split("/")[1])
        except Exception:
            pass
        return None


# ------------------------------------------------------------- write latency


def put(ar, service, n):
    body = f"value-{n}-".encode().ljust(256, b"x")
    req = urllib.request.Request(
        f"http://{ar}/kv/quorum-probe-key", data=body, method="PUT",
        headers={"XDN": service})
    with urllib.request.urlopen(req, timeout=15):
        pass


def discover_write_ar(ars, service):
    """First frontend whose member accepts writes (mirrors drive_httpkv)."""
    for ar in ars:
        try:
            put(ar, service, 0)
            return ar
        except Exception:
            continue
    raise RuntimeError("no AR accepts writes")


def measure_median_ms(ar, service, writes, warmup, gap_ms):
    lats = []
    for i in range(warmup + writes):
        t0 = time.time()
        try:
            put(ar, service, i)
            dt = (time.time() - t0) * 1000.0
            if i >= warmup:
                lats.append(dt)
        except Exception as e:
            log(f"  write error (ignored): {e}")
        time.sleep(gap_ms / 1000.0)
    if len(lats) < writes // 2:
        raise RuntimeError(f"too few latency samples ({len(lats)})")
    return statistics.median(lats)


# ----------------------------------------------------------------- inference


def run(args):
    ars = args.ars.split(",")
    peers = args.peers.split(",")
    n = len(peers) + 1  # N replicas = leader + peers
    inj = Injector(args.leader_ssh, args.ssh_key, args.service,
                   args.probe_container, args.dev)
    if not inj.probe:
        inj.discover_probe()
    if not inj.dev:
        inj.discover_dev(peers[0])

    write_ar = discover_write_ar(ars, args.service)
    log(f"write frontend: {write_ar}; N={n}, peers={peers}")

    rounds = []

    def measure_with(subset, label):
        """One inject-measure-clear round; returns (median_ms, shifted)."""
        if subset:
            inj.inject(subset, args.delta_ms)
            rtt = inj.ping_ms(subset[0])
            if rtt is not None and rtt < args.delta_ms:
                log(f"  WARNING: ping {subset[0]} = {rtt:.1f} ms < delta; "
                    "injection may not be active")
        try:
            med = measure_median_ms(write_ar, args.service, args.writes,
                                    args.warmup, args.gap_ms)
        finally:
            if subset:
                inj.clear()
        shifted = None if label == "baseline" else \
            (med - rounds[0]["median_ms"]) >= args.delta_ms / 2.0
        rounds.append({"label": label, "m": len(subset), "subset": subset,
                       "median_ms": round(med, 2), "shifted": shifted})
        log(f"round {label}: m={len(subset)} median={med:.1f} ms "
            f"shifted={shifted}")
        return shifted

    inj.clear()  # clean slate (a crashed prior run may have left a qdisc)
    measure_with([], "baseline")

    # Boundary: does the leader wait for anyone at all?
    if not measure_with(peers, "boundary"):
        q, m_star = 1, None
        log("no shift with all peers delayed: q = 1 (no synchronous wait)")
    else:
        lo, hi = 1, len(peers)  # invariant: P(lo-1)=False, P(hi)=True
        while lo < hi:
            mid = (lo + hi) // 2
            if measure_with(peers[:mid], f"bisect m={mid}"):
                hi = mid
            else:
                lo = mid + 1
        m_star = lo
        q = n - m_star + 1
        log(f"flip point m* = {m_star}: q = {n} - {m_star} + 1 = {q}")

        # Subset-independence check: same size, different links.
        if args.verify and 0 < m_star < len(peers):
            agree = measure_with(peers[-m_star:], "verify") is True
            if not agree:
                log("WARNING: verification subset disagrees; peers are not "
                    "interchangeable (adaptive protocol?) -- q unreliable, "
                    "fall back to the O(N) sequential scan")
            rounds[-1]["agrees"] = agree

    result = {
        "service": args.service, "n": n, "peers": peers,
        "delta_ms": args.delta_ms, "writes": args.writes,
        "write_ar": write_ar, "rounds": rounds,
        "m_star": m_star, "q": q,
    }
    with open(args.out, "w") as f:
        json.dump(result, f, indent=2)
    log(f"q = {q} ({len(rounds)} rounds) -> {args.out}")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--service", required=True)
    ap.add_argument("--ars", required=True,
                    help="comma list host:port of AR frontends; leader's AR "
                         "first for the cleanest signal")
    ap.add_argument("--leader-ssh", required=True,
                    help="ssh target (user@host) of the leader member's host")
    ap.add_argument("--ssh-key")
    ap.add_argument("--peers", required=True,
                    help="comma list of the N-1 peer members' OVERLAY IPs")
    ap.add_argument("--probe-container",
                    help="bwprobe container on the leader host "
                         "(default: auto-discover bwprobe.<service>*)")
    ap.add_argument("--dev",
                    help="overlay interface inside the member netns "
                         "(default: auto via 'ip route get <peer>')")
    ap.add_argument("--delta-ms", type=int, default=50,
                    help="injected one-way delay; keep well below the "
                         "protocol's election/failure-detection timeout")
    ap.add_argument("--writes", type=int, default=200)
    ap.add_argument("--warmup", type=int, default=20)
    ap.add_argument("--gap-ms", type=int, default=20,
                    help="gap between closed-loop writes (defeats pipelining)")
    ap.add_argument("--no-verify", dest="verify", action="store_false",
                    help="skip the different-subset flip-round verification")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    try:
        run(args)
    except KeyboardInterrupt:
        log("interrupted; clearing injection")
        try:
            inj = Injector(args.leader_ssh, args.ssh_key, args.service,
                           args.probe_container, args.dev or "eth1")
            if not inj.probe:
                inj.discover_probe()
            inj.clear()
        except Exception as e:
            log(f"cleanup failed ({e}); check tc state on the leader manually")
        sys.exit(1)


if __name__ == "__main__":
    main()
