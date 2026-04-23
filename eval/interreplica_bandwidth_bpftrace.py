"""interreplica_bandwidth_bpftrace.py — per-peer TCP bandwidth probe for XDN.

Orchestrates bpftrace on one or more ActiveReplica hosts to measure the bytes
sent/received over each inter-replica peer socket, keyed by
(local_port, peer_ip, peer_port). The bpftrace script itself is rendered at
runtime with the exact port allowlist parsed from a gigapaxos properties file,
so the probe adapts to local dev (AR ports 2000/2001/2002) and CloudLab
deployments (arbitrary bases) without edits.

Reuses the SSH + bpftrace-install helpers in coordination_size_sanity_check.py.
"""

import csv
import os
import re
import subprocess
import sys
from pathlib import Path

# Ensure sibling-module import works regardless of cwd.
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from coordination_size_sanity_check import ssh_output, ensure_bpftrace  # noqa: E402


# ── config parsing ────────────────────────────────────────────────────────

_CONFIG_ACTIVE_RE = re.compile(r"^\s*active\.[^=\s]+\s*=\s*([^\s#]+)")
_CONFIG_RECONF_RE = re.compile(r"^\s*reconfigurator\.[^=\s]+\s*=\s*([^\s#]+)")


def parse_gigapaxos_config(config_path):
    """Return {'ar_endpoints': [(host, port)], 'rc_endpoints': [(host, port)]}.

    Only parses active.* and reconfigurator.* entries, which is sufficient for
    identifying inter-replica coordination sockets.
    """
    ar_endpoints = []
    rc_endpoints = []
    with open(config_path) as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            m = _CONFIG_ACTIVE_RE.match(line)
            if m:
                ep = m.group(1)
                if ":" in ep:
                    host, port = ep.rsplit(":", 1)
                    ar_endpoints.append((host.strip(), int(port.strip())))
                continue
            m = _CONFIG_RECONF_RE.match(line)
            if m:
                ep = m.group(1)
                if ":" in ep:
                    host, port = ep.rsplit(":", 1)
                    rc_endpoints.append((host.strip(), int(port.strip())))
    return {"ar_endpoints": ar_endpoints, "rc_endpoints": rc_endpoints}


def watched_ports_from_config(config_path, include_rc=False):
    cfg = parse_gigapaxos_config(config_path)
    ports = [p for _, p in cfg["ar_endpoints"]]
    if include_rc:
        ports += [p for _, p in cfg["rc_endpoints"]]
    return sorted(set(ports))


def ar_hosts_from_config(config_path):
    """Return AR hosts in config order, deduped."""
    cfg = parse_gigapaxos_config(config_path)
    seen = {}
    for h, _ in cfg["ar_endpoints"]:
        seen.setdefault(h, None)
    return list(seen.keys())


# ── bpftrace script rendering ────────────────────────────────────────────

_BT_TEMPLATE = r"""#include <net/sock.h>

BEGIN
{{
{watched_init}
  printf("interreplica_bandwidth: attached\n");
}}

kprobe:tcp_sendmsg
{{
  $sk = (struct sock *)arg0;
  $lport = (uint16)$sk->__sk_common.skc_num;
  $draw  = (uint16)$sk->__sk_common.skc_dport;
  $dport = (uint16)(($draw >> 8) | ($draw << 8));
  if (@watched[$lport] || @watched[$dport]) {{
    @tx_bytes[$lport, ntop($sk->__sk_common.skc_daddr), $dport] = sum(arg2);
    @tx_msgs [$lport, ntop($sk->__sk_common.skc_daddr), $dport] = count();
  }}
}}

kprobe:tcp_cleanup_rbuf
{{
  $sk = (struct sock *)arg0;
  $copied = (int32)arg1;
  if ($copied <= 0) {{ return; }}
  $lport = (uint16)$sk->__sk_common.skc_num;
  $draw  = (uint16)$sk->__sk_common.skc_dport;
  $dport = (uint16)(($draw >> 8) | ($draw << 8));
  if (@watched[$lport] || @watched[$dport]) {{
    @rx_bytes[$lport, ntop($sk->__sk_common.skc_daddr), $dport] = sum((uint64)$copied);
    @rx_msgs [$lport, ntop($sk->__sk_common.skc_daddr), $dport] = count();
  }}
}}

END
{{
  clear(@watched);
}}
"""


def render_bt_script(watched_ports):
    """Render the bpftrace script with the @watched port allowlist inlined."""
    if not watched_ports:
        raise ValueError("render_bt_script: watched_ports is empty")
    init_lines = "\n".join(f"  @watched[{int(p)}] = 1;" for p in watched_ports)
    return _BT_TEMPLATE.format(watched_init=init_lines)


# ── exec helpers (local + remote) ────────────────────────────────────────

def _remote_run(host, cmd, *, input_=None, timeout=30, local=False,
                ssh_user=None, ssh_key=None):
    """Run a shell command on host (or locally if local=True). Returns CompletedProcess.

    ssh_user / ssh_key are honored only for non-local calls. When omitted the
    call relies on the operator's ~/.ssh/config / ssh-agent.
    """
    if local:
        return subprocess.run(
            ["bash", "-c", cmd] if isinstance(cmd, str) else cmd,
            input=input_, capture_output=True, text=True, timeout=timeout,
        )
    ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no"]
    if ssh_key:
        ssh_cmd.extend(["-i", ssh_key])
    target = f"{ssh_user}@{host}" if ssh_user else host
    ssh_cmd.append(target)
    if isinstance(cmd, list):
        ssh_cmd.extend(cmd)
    else:
        ssh_cmd.append(cmd)
    return subprocess.run(
        ssh_cmd, input=input_, capture_output=True, text=True, timeout=timeout,
    )


# ── trace lifecycle ──────────────────────────────────────────────────────

def _tag_files(tag):
    return f"/tmp/xdn_bw_{tag}.bt", f"/tmp/xdn_bw_{tag}.out"


def start_bw_trace(host, watched_ports, tag="bw", *, local=False,
                   ssh_user=None, ssh_key=None):
    """Ship the rendered .bt to host, launch bpftrace in the background.

    Waits briefly for the 'attached' marker to appear in the output file so
    probes are in place before the caller drives workload.
    """
    bt_file, out_file = _tag_files(tag)
    script = render_bt_script(watched_ports)

    run = lambda cmd, **kw: _remote_run(
        host, cmd, local=local, ssh_user=ssh_user, ssh_key=ssh_key, **kw)

    run(f"sudo pkill -9 -f 'bpftrace.*{bt_file}' 2>/dev/null; "
        f"sudo rm -f {out_file}",
        timeout=10)
    run(f"cat > {bt_file}", input_=script, timeout=10)
    run(f"sudo nohup bpftrace {bt_file} -o {out_file} "
        f"</dev/null >/dev/null 2>&1 & "
        f"sleep 0.5; "
        f"for i in $(seq 1 20); do "
        f"  if grep -q 'attached' {out_file} 2>/dev/null; then break; fi; "
        f"  sleep 0.2; "
        f"done",
        timeout=20)
    # Sanity: confirm the bpftrace process actually started.
    check = run(
        f"sudo pgrep -f 'bpftrace.*{bt_file}' >/dev/null 2>&1 && echo ok || echo missing",
        timeout=10)
    if (check.stdout or "").strip() != "ok":
        raise RuntimeError(
            f"bpftrace failed to start on {host!r}; check {out_file} on that host"
        )
    return {"bt_file": bt_file, "out_file": out_file}


def stop_bw_trace(host, tag="bw", *, local=False, ssh_user=None, ssh_key=None):
    """SIGINT bpftrace, read the output file, clean up. Returns raw text."""
    bt_file, out_file = _tag_files(tag)
    run = lambda cmd, **kw: _remote_run(
        host, cmd, local=local, ssh_user=ssh_user, ssh_key=ssh_key, **kw)

    run(f"sudo pkill -INT -f 'bpftrace.*{bt_file}' 2>/dev/null; "
        f"for i in $(seq 1 40); do "
        f"  if ! sudo pgrep -f 'bpftrace.*{bt_file}' >/dev/null 2>&1; then break; fi; "
        f"  sleep 0.3; "
        f"done; "
        f"sudo pkill -9 -f 'bpftrace.*{bt_file}' 2>/dev/null || true",
        timeout=30)
    r = run(f"sudo cat {out_file} 2>/dev/null", timeout=15)
    raw = r.stdout or ""
    run(f"sudo rm -f {out_file} {bt_file} 2>/dev/null", timeout=10)
    return raw


# ── parser ───────────────────────────────────────────────────────────────

# bpftrace auto-prints each map on SIGINT/END as "@name[key1, key2, ...]: value".
_MAP_RE = re.compile(r"^@(\w+)\[(.+)\]:\s*(\d+)\s*$")

# Track which maps we actually care about; @watched is intentionally ignored.
_TARGET_MAPS = {"tx_bytes", "tx_msgs", "rx_bytes", "rx_msgs"}


def parse_bt_output(raw):
    """Parse raw bpftrace output into a list of per-peer rows.

    Each row: {local_port, peer_ip, peer_port, tx_bytes, tx_msgs, rx_bytes, rx_msgs}.
    """
    aggregated = {}
    for line in (raw or "").splitlines():
        line = line.strip()
        m = _MAP_RE.match(line)
        if not m:
            continue
        name, key_str, val_str = m.group(1), m.group(2), m.group(3)
        if name not in _TARGET_MAPS:
            continue
        # Key shape: "<lport>, <ip>, <dport>". IPv4 dotted-quad has no commas.
        parts = [p.strip() for p in key_str.split(",")]
        if len(parts) != 3:
            continue
        try:
            lport = int(parts[0])
            peer_ip = parts[1]
            dport = int(parts[2])
            val = int(val_str)
        except ValueError:
            continue
        k = (lport, peer_ip, dport)
        entry = aggregated.setdefault(
            k, {"tx_bytes": 0, "tx_msgs": 0, "rx_bytes": 0, "rx_msgs": 0})
        entry[name] = val

    rows = []
    for (lport, peer_ip, dport), v in sorted(aggregated.items()):
        rows.append({
            "local_port": lport,
            "peer_ip": peer_ip,
            "peer_port": dport,
            "tx_bytes": v["tx_bytes"],
            "tx_msgs": v["tx_msgs"],
            "rx_bytes": v["rx_bytes"],
            "rx_msgs": v["rx_msgs"],
        })
    return rows


# ── probe orchestration ──────────────────────────────────────────────────

CSV_FIELDS = [
    "host", "local_port", "peer_ip", "peer_port",
    "tx_bytes", "tx_msgs", "rx_bytes", "rx_msgs",
]


def _safe(host):
    return re.sub(r"[^A-Za-z0-9_-]", "_", host)


class BandwidthProbe:
    """Fleet-wide per-peer bandwidth probe.

    Typical usage:
        probe = BandwidthProbe(["10.0.0.1", "10.0.0.2"], [2000], local=False)
        probe.start()
        # ... drive workload ...
        rows, raw = probe.stop_and_collect(results_dir="out/")
    """

    def __init__(self, hosts, watched_ports, *, local=False, tag_prefix="bw",
                 ssh_user=None, ssh_key=None):
        if not watched_ports:
            raise ValueError("watched_ports is empty; nothing to trace")
        self.hosts = list(hosts)
        self.watched_ports = list(watched_ports)
        self.local = local
        self.tag_prefix = tag_prefix
        self.ssh_user = ssh_user
        self.ssh_key = ssh_key
        self._started = []

    def _tag(self, host):
        return f"{self.tag_prefix}_{_safe(host)}"

    def start(self):
        if self.local:
            # On a local run we trust the operator to have bpftrace installed;
            # a no-op check keeps the error message clean.
            r = subprocess.run(["which", "bpftrace"], capture_output=True, text=True)
            if r.returncode != 0:
                raise RuntimeError(
                    "bpftrace not found on PATH. Install it "
                    "(e.g. 'sudo apt install bpftrace') and retry."
                )
        else:
            # ensure_bpftrace uses the operator's default SSH config. If a
            # custom ssh_user / ssh_key is supplied we skip the install step
            # (it would reach hosts via a different identity anyway) and rely
            # on start_bw_trace to fail loudly if bpftrace is missing.
            if self.ssh_user is None and self.ssh_key is None:
                ready = ensure_bpftrace(self.hosts)
                missing = [h for h in self.hosts if h not in ready]
                if missing:
                    raise RuntimeError(f"bpftrace unavailable on: {missing}")
        for host in self.hosts:
            start_bw_trace(host, self.watched_ports, tag=self._tag(host),
                           local=self.local, ssh_user=self.ssh_user,
                           ssh_key=self.ssh_key)
            self._started.append(host)

    def stop_and_collect(self, results_dir=None):
        rows = []
        raw_per_host = {}
        for host in self._started:
            raw = stop_bw_trace(host, tag=self._tag(host), local=self.local,
                                ssh_user=self.ssh_user, ssh_key=self.ssh_key)
            raw_per_host[host] = raw
            for r in parse_bt_output(raw):
                r = dict(r)
                r["host"] = host
                rows.append(r)
        self._started = []

        if results_dir:
            out = Path(results_dir)
            out.mkdir(parents=True, exist_ok=True)
            csv_path = out / "interreplica_bandwidth.csv"
            with open(csv_path, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
                w.writeheader()
                for r in rows:
                    w.writerow({k: r.get(k, "") for k in CSV_FIELDS})
            for host, raw in raw_per_host.items():
                (out / f"bpftrace_raw_{_safe(host)}.txt").write_text(raw or "")
        return rows, raw_per_host
