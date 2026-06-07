#!/usr/bin/env python3
"""List available raw PCs (bare-metal, exclusive nodes) across CloudLab clusters.

This is the read-only "what can I get right now" probe -- the rough equivalent
of eyeballing a region's spot capacity before a `terraform apply`. It queries
each aggregate's GENI advertisement (AM API listresources) and counts, per
hardware type, how many exclusive raw-pc nodes are currently free vs. total.

First run (one-time): point it at your downloaded credentials to build the
geni-lib context.

    python list_rawpc.py \\
        --cert ~/cloudlab.pem --pubkey ~/.ssh/id_rsa.pub --project YOUR_PROJECT

Subsequent runs just read the saved context (~/.bssw/geni/context.json):

    python list_rawpc.py                 # all clusters
    python list_rawpc.py --sites utah,clemson
    python list_rawpc.py --type c6420    # filter hardware type (substring)
    python list_rawpc.py --json          # machine-readable

The cloudlab.pem key is passphrase-encrypted; you'll be prompted unless you pass
--passphrase-env VAR or --no-passphrase.
"""

import argparse
import getpass
import json
import os
import sys
from collections import defaultdict

import geni.aggregate.cloudlab as cloudlab

import clcontext

# UMass runs its own CloudLab/ProtoGENI testbed (boss.cloudlab.umass.edu) with
# its own clearinghouse + CA, so geni-lib (2020) has no aggregate for it. We
# build one by hand using the same CloudLabAM(name, host, authority_urn) form as
# the built-ins; whether our emulab.net credential is federated/trusted there is
# only knowable from a live call (it's opt-in below, not in the defaults).
UMass = cloudlab.CloudLabAM(
    "cl-umass", "boss.cloudlab.umass.edu",
    "urn:publicid:IDN+cloudlab.umass.edu+authority+cm")

# Friendly name -> aggregate object. The built-ins come from geni-lib's cloudlab
# module; Apt/Emulab are federated and reachable with the same credentials.
SITES = {
    "utah": cloudlab.Utah,
    "clemson": cloudlab.Clemson,
    "wisconsin": cloudlab.Wisconsin,
    "apt": cloudlab.Apt,
    "umass": UMass,
    "utahddc": cloudlab.UtahDDC,
}

# What a bare `list_rawpc.py` (no --sites) sweeps: the live clusters our
# credential can actually reach. umass runs a separate clearinghouse but our
# emulab.net credential is federated/trusted there (confirmed live), so it's in.
# Excludes only utahddc (a retired InstaGENI rack that just times out); it stays
# reachable via `--sites utahddc`, and `--sites all` hits everything.
DEFAULT_SITES = ["utah", "clemson", "wisconsin", "apt", "umass"]


def summarize_site(context, am):
    """Return {hardware_type: {"free": n, "total": n}} for exclusive raw PCs."""
    ad = am.listresources(context)
    stats = defaultdict(lambda: {"free": 0, "total": 0})
    for node in ad.nodes:
        if not node.exclusive or "raw-pc" not in node.sliver_types:
            continue
        # A raw PC normally advertises exactly one hardware type.
        htypes = list(node.hardware_types) or ["(unknown)"]
        for ht in htypes:
            stats[ht]["total"] += 1
            if node.available:
                stats[ht]["free"] += 1
    return stats


def parse_args(argv):
    p = argparse.ArgumentParser(
        description="List currently-available CloudLab raw PCs per cluster.")
    # Context-building (only needed the first time / to refresh credentials).
    p.add_argument("--cert", help="path to cloudlab.pem (builds the context)")
    p.add_argument("--pubkey", help="path to your SSH public key (.pub); "
                   "required with --cert")
    p.add_argument("--project", help="CloudLab project name; required with --cert")
    # Query options.
    p.add_argument("--sites", default="default",
                   help="'default' (%s), 'all' (adds %s), or a comma-separated "
                        "subset of: %s"
                        % (", ".join(DEFAULT_SITES),
                           ", ".join(s for s in SITES if s not in DEFAULT_SITES),
                           ", ".join(SITES)))
    p.add_argument("--type", dest="hwtype",
                   help="only show hardware types containing this substring")
    p.add_argument("--timeout", type=int, default=15,
                   help="per-site connect/response timeout in seconds "
                        "(default: 15; a dead cluster fails fast instead of "
                        "hanging ~60s)")
    p.add_argument("--json", action="store_true", help="emit JSON")
    # Passphrase handling for the encrypted private key.
    p.add_argument("--passphrase-env",
                   help="read key passphrase from this env var (non-interactive)")
    p.add_argument("--no-passphrase", action="store_true",
                   help="key is unencrypted; don't prompt")
    return p.parse_args(argv)


def resolve_sites(spec):
    if spec == "default":
        names = list(DEFAULT_SITES)
    elif spec == "all":
        names = list(SITES)
    else:
        names = [s.strip().lower() for s in spec.split(",") if s.strip()]
    out = []
    for name in names:
        if name not in SITES:
            sys.exit("unknown site %r; choose from: %s (or 'default'/'all')"
                     % (name, ", ".join(SITES)))
        out.append((name, SITES[name]))
    return out


def main(argv=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)

    # 0. Validate site selection up front so typos fail before any credentials
    #    or network work.
    sites = resolve_sites(args.sites)

    # Apply the per-site HTTP timeout (geni-lib's global default is 60s).
    import geni.minigcf.config as gcfg
    gcfg.HTTP.TIMEOUT = args.timeout

    # 1. Build the context if credentials were supplied.
    if args.cert:
        if not (args.pubkey and args.project):
            sys.exit("--cert requires --pubkey and --project")
        path = clcontext.build(args.cert, args.pubkey, args.project)
        print("context written to %s" % path, file=sys.stderr)
    elif not os.path.exists(clcontext.context_path()):
        sys.exit("no saved context at %s -- run once with --cert/--pubkey/"
                 "--project first (see --help)." % clcontext.context_path())

    # 2. Decide how to unlock the private key.
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
        # The most common failure is a wrong/empty passphrase for the encrypted
        # cloudlab.pem key; give a readable message instead of a traceback.
        if exc.__class__.__name__ == "KeyDecryptionError" or "decrypt" in str(exc).lower():
            sys.exit("could not decrypt cloudlab.pem key -- wrong passphrase?")
        raise

    # 3. Query each site (failures are per-site, not fatal).
    results = {}
    for name, am in sites:
        try:
            results[name] = summarize_site(context, am)
        except Exception as exc:  # noqa: BLE001 - surface, keep going
            results[name] = {"_error": str(exc)}
            print("! %-9s query failed: %s" % (name, exc), file=sys.stderr)

    # 4. Filter by hardware type if requested.
    if args.hwtype:
        needle = args.hwtype.lower()
        for name, stats in results.items():
            if "_error" in stats:
                continue
            results[name] = {ht: v for ht, v in stats.items()
                             if needle in ht.lower()}

    # 5. Output.
    if args.json:
        print(json.dumps(results, indent=2, sort_keys=True))
        return 0

    for name, stats in results.items():
        if "_error" in stats:
            print("\n=== %s === (error: %s)" % (name, stats["_error"]))
            continue
        if not stats:
            print("\n=== %s === (no matching raw PCs)" % name)
            continue
        print("\n=== %s ===" % name)
        print("  %-16s %8s %8s" % ("hardware_type", "free", "total"))
        for ht in sorted(stats):
            v = stats[ht]
            print("  %-16s %8d %8d" % (ht, v["free"], v["total"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
