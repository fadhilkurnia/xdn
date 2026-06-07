#!/usr/bin/env python3
"""Try to start (and tear down) bare-metal machines across CloudLab sites.

This is the first real provisioning probe -- the rough equivalent of a tiny
`terraform apply`. It creates ONE slice and reserves a single raw PC in it at
each requested site (the GENI model: one slice spanning multiple aggregates),
then prints SSH login info. Use it to confirm end-to-end provisioning works
before we build the full 1xRC + 3xAR XDN topology.

    python start_machines.py start                 # default sites (utah, clemson)
    python start_machines.py start --sites utah,clemson,umass
    python start_machines.py start --dry-run       # print the request RSpec, no API calls
    python start_machines.py status                # sliverstatus per site
    python start_machines.py delete                # tear everything down

IMPORTANT: `start` allocates REAL, exclusive bare-metal nodes that count against
your project until they expire (--hours, default 2) or you run `delete`. Always
`delete` when done. First run builds the geni-lib context; see list_rawpc.py
--help for the one-time --cert/--pubkey/--project setup (shared context).
"""

import argparse
import datetime
import getpass
import os
import socket
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

import geni.rspec.pg as pg

import clcontext
# Reuse the cluster registry + selection logic from the listing tool.
from list_rawpc import SITES, resolve_sites

# A conservative default for a provisioning *test*: two geographically distinct,
# reliably-up clusters -> proves cross-site without grabbing 5 machines. Widen
# with --sites (e.g. --sites all). Note this differs from list_rawpc's read-only
# default, which sweeps all live clusters.
DEFAULT_START_SITES = ["utah", "clemson"]


def build_request(node_name, count=1, hwtype=None, image=None):
    """A raw-PC request with `count` nodes. With no hardware_type, CloudLab
    binds whatever exclusive raw PCs are free at that cluster. Node client_ids
    are <node_name> for a single node, else <node_name>0, <node_name>1, ..."""
    req = pg.Request()
    for i in range(count):
        cid = node_name if count == 1 else "%s%d" % (node_name, i)
        node = pg.RawPC(cid)
        if hwtype:
            node.hardware_type = hwtype
        if image:
            node.disk_image = image
        req.addResource(node)
    return req


def _ssh_key_hint(context):
    """Best-effort private-key path for a copy-paste ssh command. The context
    stores the *public* key path; the private key is conventionally the same
    minus '.pub'. Note context._users is a *set*, so iterate, don't index."""
    try:
        for user in context._users:
            for pub in user._keys:
                if pub.endswith(".pub") and os.path.exists(pub[:-4]):
                    return pub[:-4]
    except Exception:  # noqa: BLE001
        pass
    return None


# Emulab/CloudLab "hardware types" that are NOT a real machine model: generic
# class names and network-shaping pseudo-nodes. A physical node advertises its
# real model type *plus* several of these, so we filter them before picking.
_PSEUDO_HWTYPES = {
    "pc", "pcvm", "vm", "lan", "vlan", "delay", "bridge", "blockstore",
    "firewall", "stitch", "tour",
}
# Pseudo-types also appear as prefixed variants, e.g. 'bridge-c6525-25g',
# 'delay-...'. Filter by prefix too.
_PSEUDO_PREFIXES = ("bridge", "delay", "blockstore")


def _is_pseudo_hwtype(name):
    n = name.lower()
    return n in _PSEUDO_HWTYPES or n.startswith(_PSEUDO_PREFIXES)


def _ad_hwtype_raw(context, am):
    """Map component_id -> ordered list of advertised hardware-type names. Raw
    (unfiltered) -- used by both the resolver and the --debug-type dump."""
    out = {}
    ad = am.listresources(context)  # advertisement (no slice arg)
    for n in ad.nodes:
        hts = getattr(n, "hardware_types", None)
        if hts:
            out[n.component_id] = list(hts)  # dict preserves XML order
    return out


def _ad_hwtype_map(context, am):
    """Map component_id -> real compute hardware type from the advertisement.

    The advertisement lists each node's real model alongside generic class
    ('pc') and pseudo ('bridge', 'bridge-c6525-25g', 'lan', ...) types, so we
    drop those and keep the remaining real model."""
    out = {}
    for cid, hts in _ad_hwtype_raw(context, am).items():
        real = [h for h in hts if not _is_pseudo_hwtype(h)]
        if real:
            out[cid] = sorted(real)[0]  # physical nodes have exactly one
    return out


def _human(nbytes):
    """Bytes -> human string (GiB/TiB with one decimal)."""
    n = float(nbytes)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if n < 1024.0:
            return ("%.0f %s" % (n, unit)) if unit in ("B", "KiB", "MiB") \
                else ("%.1f %s" % (n, unit))
        n /= 1024.0
    return "%.1f EiB" % n


# Remote one-liner that emits parseable hardware facts. Kept POSIX-sh simple so
# it runs on whatever default image the node booted.
_PROBE_SCRIPT = (
    'echo "cores=$(nproc)"; '
    'echo "arch=$(uname -m)"; '
    'echo "ram_kb=$(grep -m1 MemTotal /proc/meminfo | tr -dc 0-9)"; '
    'echo "DISKS:"; lsblk -dn -b -o NAME,SIZE,ROTA 2>/dev/null'
)


def ssh_probe(user, host, port, key, timeout=20):
    """SSH into a node and return a one-line spec summary, or a '(probe failed:
    ...)' string. Used to report cores/arch/RAM/disk/NVMe of live nodes."""
    cmd = ["ssh", "-o", "StrictHostKeyChecking=no",
           "-o", "UserKnownHostsFile=/dev/null", "-o", "BatchMode=yes",
           "-o", "LogLevel=ERROR", "-o", "ConnectTimeout=%d" % timeout]
    if key:
        cmd += ["-i", key]
    if port and port != 22:
        cmd += ["-p", str(port)]
    cmd += ["%s@%s" % (user, host), _PROBE_SCRIPT]
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 10)
    except Exception as exc:  # noqa: BLE001 (timeout, ssh missing, etc.)
        return "(probe failed: %s)" % exc
    if p.returncode != 0:
        err = (p.stderr or "").strip().splitlines()
        return "(probe failed: %s)" % (err[-1] if err else "ssh rc=%d" % p.returncode)
    return _format_specs(p.stdout)


def _format_specs(out):
    cores = arch = ram_kb = None
    disks = []  # (name, size_bytes, rotational)
    in_disks = False
    for line in out.splitlines():
        line = line.strip()
        if line.startswith("cores="):
            cores = line[6:]
        elif line.startswith("arch="):
            arch = line[5:]
        elif line.startswith("ram_kb="):
            ram_kb = line[7:]
        elif line == "DISKS:":
            in_disks = True
        elif in_disks and line:
            parts = line.split()
            if len(parts) >= 3 and parts[1].isdigit():
                disks.append((parts[0], int(parts[1]), parts[2]))
    bits = []
    if cores:
        bits.append("%s cores" % cores)
    if arch:
        bits.append(arch)
    if ram_kb and ram_kb.isdigit():
        bits.append("%s RAM" % _human(int(ram_kb) * 1024))
    if disks:
        total = sum(d[1] for d in disks)
        nvme = any(d[0].startswith("nvme") for d in disks)
        bits.append("%d disk%s/%s" % (len(disks), "" if len(disks) == 1 else "s",
                                      _human(total)))
        bits.append("nvme=%s" % ("yes" if nvme else "no"))
    return " · ".join(bits) if bits else "(no data)"


def _fmt_remaining(expires_utc):
    """expires_utc: naive UTC datetime -> 'in 1h23m' / 'EXPIRED' / None."""
    if not expires_utc:
        return None
    delta = expires_utc - datetime.datetime.utcnow()
    secs = int(delta.total_seconds())
    if secs <= 0:
        return "EXPIRED"
    h, m = secs // 3600, (secs % 3600) // 60
    return "in %dh%02dm" % (h, m) if h else "in %dm" % m


def slice_expiration(context, slicename):
    """Slice expiry (naive UTC). Slivers can't outlast the slice, so this is the
    'about to be terminated' signal. Returns None if unavailable."""
    try:
        return context.getSliceInfo(slicename).expires
    except Exception:  # noqa: BLE001
        return None


def _resolve_ips(hostname):
    """Public (ipv4, ipv6) for a hostname via DNS -- i.e. the A / AAAA record
    values you'd point DNS at. Either may be None."""
    v4 = v6 = None
    try:
        for fam, _, _, _, sa in socket.getaddrinfo(hostname, None):
            if fam == socket.AF_INET and not v4:
                v4 = sa[0]
            elif fam == socket.AF_INET6 and not v6:
                v6 = sa[0]
    except Exception:  # noqa: BLE001
        pass
    return v4, v6


def hosts_from_manifest(context, am, manifest, probe=False, key=None, probe_timeout=20):
    """Build a list of host dicts from a manifest. Hardware type comes from the
    aggregate advertisement (reliable, by component_id); public IPs from the
    manifest <host> element / DNS; specs from an SSH probe when probe=True."""
    ad_map = {}
    if context is not None and am is not None:
        try:
            ad_map = _ad_hwtype_map(context, am)
        except Exception:  # noqa: BLE001
            ad_map = {}
    hosts = []
    for node in getattr(manifest, "nodes", None) or []:
        hwtype = ad_map.get(node.component_id)
        # Manifest <host ipv4=...> is authoritative for v4 when present.
        manifest_ipv4 = None
        try:
            manifest_ipv4 = node.hostipv4
        except Exception:  # noqa: BLE001
            pass
        for login in node.logins:
            v4, v6 = _resolve_ips(login.hostname)
            # Prefer the publicly-resolved A record (what DNS deployment needs);
            # the manifest <host ipv4> can be a private/experimental address.
            ipv4 = v4 or manifest_ipv4
            specs = ssh_probe(login.username, login.hostname, login.port,
                              key, probe_timeout) if probe else None
            hosts.append({
                "client_id": node.client_id, "user": login.username,
                "host": login.hostname, "port": login.port,
                "hwtype": hwtype, "ipv4": ipv4, "ipv6": v6, "specs": specs,
            })
    return hosts


def format_hosts(hosts, key):
    """Print host dicts: login line (+type), specs line, ready-to-run ssh."""
    if not hosts:
        print("  hosts: (none reported yet -- node may still be booting)")
        return
    for h in hosts:
        tag = " (type=%s)" % h["hwtype"] if h["hwtype"] else ""
        print("  [%s] %s@%s:%d%s" % (
            h["client_id"], h["user"], h["host"], h["port"], tag))
        if h.get("ipv4") or h.get("ipv6"):
            parts = []
            if h.get("ipv4"):
                parts.append("ipv4=%s" % h["ipv4"])
            if h.get("ipv6"):
                parts.append("ipv6=%s" % h["ipv6"])
            print("       %s" % "  ".join(parts))
        if h["specs"] is not None:
            print("       %s" % h["specs"])
        ssh = "ssh"
        if key:
            ssh += " -i %s" % key
        if h["port"] and h["port"] != 22:
            ssh += " -p %d" % h["port"]
        print("       %s %s@%s" % (ssh, h["user"], h["host"]))


def ensure_slice(context, slicename, hours):
    """Create the slice if it doesn't already exist. Returns 'created'/'exists'.

    Slices live at the home clearinghouse (emulab.net) and are shared by all the
    per-site slivers. Expiration is UTC (the SA date format is ...Z)."""
    exp = datetime.datetime.utcnow() + datetime.timedelta(hours=hours)
    try:
        context.cf.createSlice(context, slicename, exp=exp)
        return "created"
    except Exception as exc:  # noqa: BLE001
        msg = str(exc).lower()
        if any(s in msg for s in ("exist", "already", "duplicate")):
            return "exists"
        raise


def do_start(context, sites, args):
    req = build_request(args.node_name, args.count, args.type, args.image)

    if args.dry_run:
        print("# request RSpec (dry run -- no slice created, nothing reserved):\n")
        xml = req.toXMLString(pretty_print=True)
        if isinstance(xml, bytes):  # lxml returns bytes
            xml = xml.decode("utf-8")
        print(xml)
        return 0

    state = ensure_slice(context, args.slice, args.hours)
    print("slice %r: %s (expires in ~%dh)" % (args.slice, state, args.hours))

    key = _ssh_key_hint(context)
    ok, failed = [], []
    for name, am in sites:
        try:
            print("\n-> reserving %d raw PC%s at %s ..."
                  % (args.count, "" if args.count == 1 else "s", name))
            manifest = am.createsliver(context, args.slice, req)
            ok.append(name)
            # Don't SSH-probe here: the node was just allocated and is still
            # booting. `status` (run later) reports full specs.
            format_hosts(hosts_from_manifest(context, am, manifest), key)
        except Exception as exc:  # noqa: BLE001
            failed.append((name, exc))
            print("!  %s failed: %s" % (name, exc))

    print("\n=== summary ===")
    print("reserved : %s" % (", ".join(ok) or "(none)"))
    if failed:
        print("failed   : %s" % ", ".join(n for n, _ in failed))
    if ok:
        print("\nNodes are booting -- SSH may take a few minutes. Check with:")
        print("  python start_machines.py status --slice %s --sites %s"
              % (args.slice, ",".join(ok)))
        print("Tear down when done with:")
        print("  python start_machines.py delete --slice %s --sites %s"
              % (args.slice, ",".join(n for n, _ in sites)))
    return 1 if failed and not ok else 0


def _gather_site(name, am, context, args, key):
    """All per-site I/O (status + manifest + advertisement + ssh probe), run in
    a worker thread. Returns a result dict; never raises."""
    res = {"name": name}
    try:
        st = am.sliverstatus(context, args.slice)
        res["status"] = st.get("geni_status", st.get("status", "?"))
        res["resources"] = st.get("geni_resources", [])
    except Exception as exc:  # noqa: BLE001
        res["status_error"] = str(exc)
        return res
    try:
        manifest = am.listresources(context, args.slice)
        res["hosts"] = hosts_from_manifest(
            context, am, manifest, probe=not args.no_probe,
            key=key, probe_timeout=args.probe_timeout)
    except Exception as exc:  # noqa: BLE001
        res["manifest_error"] = str(exc)
    return res


def do_debug_type(context, sites, args):
    """Dump the raw advertised hardware types for each provisioned node, plus
    what the resolver currently picks. Diagnostic for type-resolution issues."""
    for name, am in sites:
        print("\n##### %s #####" % name)
        try:
            cids = [n.component_id
                    for n in (am.listresources(context, args.slice).nodes or [])]
            raw = _ad_hwtype_raw(context, am)
            for cid in cids:
                hts = raw.get(cid, [])
                real = [h for h in hts if not _is_pseudo_hwtype(h)]
                print("  %s" % cid)
                print("      advertised : %s" % (hts or "(none)"))
                print("      after-filter: %s  -> picked: %s"
                      % (real, sorted(real)[0] if real else None))
        except Exception as exc:  # noqa: BLE001
            print("  error: %s" % exc)
    return 0


def do_status(context, sites, args):
    if args.debug_type:
        return do_debug_type(context, sites, args)
    key = _ssh_key_hint(context)

    # Warm the shared credential caches single-threaded, so parallel workers
    # only ever *read* the cred files (avoids first-fetch write races).
    try:
        _ = context.usercred_path
    except Exception:  # noqa: BLE001
        pass
    exp = slice_expiration(context, args.slice)
    rem = _fmt_remaining(exp)
    if rem:
        print("slice %r expires %s (%s UTC)"
              % (args.slice, rem, exp.strftime("%Y-%m-%d %H:%M")))

    # Fan out across sites: each site is independent blocking I/O.
    with ThreadPoolExecutor(max_workers=min(8, max(1, len(sites)))) as ex:
        gathered = list(ex.map(
            lambda s: _gather_site(s[0], s[1], context, args, key), sites))

    for res in gathered:
        print("\n=== %s ===" % res["name"])
        if "status_error" in res:
            print("  status: (no sliver / error: %s)" % res["status_error"])
            continue
        print("  status: %s" % res.get("status", "?"))
        for r in res.get("resources", []):
            print("    %s: %s (%s)" % (
                r.get("geni_urn", "?"), r.get("geni_status", "?"),
                r.get("geni_error", "") or "ok"))
        if "manifest_error" in res:
            print("  hosts: (unavailable: %s)" % res["manifest_error"])
            continue
        format_hosts(res.get("hosts", []), key)
    return 0


def do_delete(context, sites, args):
    for name, am in sites:
        try:
            am.deletesliver(context, args.slice)
            print("deleted sliver at %s" % name)
        except Exception as exc:  # noqa: BLE001
            print("! %s: %s" % (name, exc))
    print("\n(The slice itself persists until it expires; only slivers were "
          "released.)")
    return 0


def parse_args(argv):
    p = argparse.ArgumentParser(
        description="Start/inspect/delete bare-metal nodes across CloudLab sites.")
    p.add_argument("action", choices=["start", "status", "delete"],
                   nargs="?", default="start")
    p.add_argument("--slice", default="xdn-test", help="slice name (default: xdn-test)")
    p.add_argument("--sites", default=",".join(DEFAULT_START_SITES),
                   help="comma-separated sites, or 'all'/'default' (default: %s)"
                        % ",".join(DEFAULT_START_SITES))
    p.add_argument("--type", dest="type",
                   help="hardware type to request (default: let CloudLab pick "
                        "any free raw PC). Use list_rawpc.py to see types.")
    p.add_argument("--image", help="disk image URN (default: CloudLab default Ubuntu)")
    p.add_argument("--node-name", default="node", help="node client_id in the RSpec")
    p.add_argument("--count", type=int, default=1,
                   help="number of raw PCs to request PER SITE (default: 1). "
                        "client_ids become <node-name>0, <node-name>1, ...")
    p.add_argument("--hours", type=int, default=2,
                   help="slice expiration in hours (default: 2)")
    p.add_argument("--dry-run", action="store_true",
                   help="for 'start': print the request RSpec and exit")
    p.add_argument("--timeout", type=int, default=60,
                   help="per-call timeout in seconds (default: 60; provisioning "
                        "is slower than listing)")
    p.add_argument("--no-probe", action="store_true",
                   help="for 'status': skip the SSH spec probe "
                        "(cores/arch/RAM/disk/nvme)")
    p.add_argument("--probe-timeout", type=int, default=20,
                   help="SSH probe connect timeout in seconds (default: 20)")
    p.add_argument("--debug-type", action="store_true",
                   help="for 'status': dump the raw advertised hardware types "
                        "per provisioned node (diagnostics)")
    p.add_argument("--passphrase-env",
                   help="read cloudlab.pem key passphrase from this env var")
    p.add_argument("--no-passphrase", action="store_true",
                   help="key is unencrypted; don't prompt")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)

    sites = resolve_sites(args.sites)

    import geni.minigcf.config as gcfg
    gcfg.HTTP.TIMEOUT = args.timeout

    if not os.path.exists(clcontext.context_path()):
        sys.exit("no saved context at %s -- build it once with list_rawpc.py "
                 "--cert/--pubkey/--project (see its --help)."
                 % clcontext.context_path())

    if args.action == "start" and args.dry_run:
        # No credentials needed just to render the RSpec.
        return do_start(None, sites, args)

    if args.no_passphrase:
        passphrase = None
    elif args.passphrase_env:
        passphrase = os.environ.get(args.passphrase_env)
        if passphrase is None:
            sys.exit("env var %s is not set" % args.passphrase_env)
    else:
        passphrase = getpass.getpass("cloudlab.pem key passphrase: ")

    try:
        context = clcontext.load(passphrase=passphrase or None)
    except Exception as exc:  # noqa: BLE001
        if exc.__class__.__name__ == "KeyDecryptionError" or "decrypt" in str(exc).lower():
            sys.exit("could not decrypt cloudlab.pem key -- wrong passphrase?")
        raise

    return {"start": do_start, "status": do_status, "delete": do_delete}[args.action](
        context, sites, args)


if __name__ == "__main__":
    sys.exit(main())
