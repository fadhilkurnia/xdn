# XDN on CloudLab (geni-lib tooling)

Programmatic access to [CloudLab](https://www.cloudlab.us/) via **geni-lib**, the
counterpart to the AWS/Terraform setup in `../aws`. CloudLab gives **bare-metal,
multi-site** nodes (good for real WAN latencies / geolocations), but slivers are
**ephemeral** (expire in ~16h, renewable) and there are no AMIs or persistent
IPs — node setup happens at boot.

Start small: `list_rawpc.py` is a **read-only** probe of what's currently free.

## Setup

geni-lib targets Python 2.7/3.6 and under-declares its deps, but it imports fine
on the stock `python3` (3.10) once the deps in `requirements.txt` are present.
Its bundled `build-context` CLI is Python-2-only and crashes here, so
`clcontext.py` reproduces it in Py3-safe code.

```bash
cd cloudlab
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
```

## Credentials (one-time)

1. Log in to the [CloudLab portal](https://www.cloudlab.us/) → top-right
   **username dropdown → Download Credentials** → save as `cloudlab.pem`. This
   single PEM holds your X509 cert + a **passphrase-encrypted** private key.
2. Note your **project name** (user dashboard → Membership tab).
3. Have an SSH **public** key handy (installed on nodes you provision).

Build the geni-lib context once (writes `~/.bssw/geni/context.json`):

```bash
python list_rawpc.py \
    --cert ~/cloudlab.pem \
    --pubkey ~/.ssh/id_rsa.pub \
    --project YOUR_PROJECT
```

## List available raw PCs

```bash
python list_rawpc.py                    # default: live main-federation clusters
python list_rawpc.py --sites all        # every cluster incl. umass + utahddc
python list_rawpc.py --sites utah,clemson
python list_rawpc.py --type c6420       # filter hardware type (substring)
python list_rawpc.py --timeout 10       # fail fast on an unresponsive cluster
python list_rawpc.py --json             # machine-readable
```

You'll be prompted for the `cloudlab.pem` passphrase. For non-interactive use:
`--passphrase-env VAR` (read from env) or `--no-passphrase` (unencrypted key).

Output is per-cluster, per-hardware-type `free / total` counts of exclusive
`raw-pc` nodes — i.e. what you could `createsliver` right now. Per-site query
failures (e.g. a cluster your project can't reach) are reported but non-fatal.

Clusters in the **default** sweep: `utah`, `clemson`, `wisconsin`, `apt`, and
`umass`. One opt-in extra reachable via `--sites`:

- `utahddc` — a retired InstaGENI rack; usually just times out.

Note on `umass`: UMass runs its **own** CloudLab/ProtoGENI testbed
(`boss.cloudlab.umass.edu`) with a separate clearinghouse + CA, and geni-lib has
no built-in aggregate for it, so `list_rawpc.py` defines one. Your `emulab.net`
credential is federated/trusted there (confirmed), so it's part of the default
sweep.

## Start machines (provisioning test)

`start_machines.py` is the first real provisioning probe — it creates **one
slice** and reserves a **single raw PC per site** in it (the GENI model: one
slice spanning aggregates), then prints SSH login info. This is the rough
equivalent of a tiny `terraform apply`.

```bash
python start_machines.py start                      # default: utah, clemson
python start_machines.py start --sites utah         # validate one site first (cheap)
python start_machines.py start --sites utah,clemson,umass --type c6420
python start_machines.py start --count 3 --sites utah,clemson   # 3 machines PER site
python start_machines.py start --dry-run            # print the request RSpec, no API calls
python start_machines.py status                     # state + specs + expiry, all sites
python start_machines.py status --no-probe          # skip the SSH spec probe (faster)
python start_machines.py delete                     # release all slivers
```

`status` queries all sites **in parallel** and, for each node, prints the
hardware type (resolved from the cluster advertisement), the slice's **remaining
time** before expiry, and a live **SSH spec probe** (cores · arch · RAM · disks ·
nvme), e.g.:

```
slice 'xdn-test' expires in 1h29m (2026-06-06 20:00 UTC)

=== utah ===
  status: ready
    urn:...+sliver+2401749: ready (ok)
  [node] fadhil@amd128.utah.cloudlab.us:22 (type=d6515)
       32 cores · x86_64 · 125.4 GiB RAM · 2 disks/894.3 GiB · nvme=no
       ssh -i /home/fadhil/.ssh/cloudlab2 fadhil@amd128.utah.cloudlab.us
```

> ⚠️ `start` allocates **real, exclusive bare-metal nodes** that count against
> your project until they expire (`--hours`, default 2) or you run `delete`.
> Always `delete` when done. With no `--type`, CloudLab binds any free raw PC;
> use `list_rawpc.py` to see what's available first.

## Files

| File | Purpose |
|------|---------|
| `list_rawpc.py`  | Read-only probe: available raw PCs per cluster/hardware type |
| `start_machines.py` | Provisioning test: start/status/delete a raw PC per site |
| `clcontext.py`   | Build/load the geni-lib context from `cloudlab.pem` (Py3-safe) |
| `requirements.txt` | geni-lib + its (under-declared) runtime deps |

## Next steps (not yet built)

- A geni-lib **profile** (request RSpec) for the 1×RC + 3×AR XDN topology.
- A boot **bootstrap.sh** (ports the logic from `../aws/*-userdata.tftpl`).
- A **provision.py** driver: `createsliver` / `sliverstatus` / `renewsliver` /
  `deletesliver` — the `terraform apply` / `cluster_power.sh` equivalent.
- Reservations: per-cluster/type, **admin-reviewed**, mostly a portal web form —
  not cleanly scriptable via geni-lib.
