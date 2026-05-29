# xdn-bw-trace

Per-service inter-replica + client⇄replica TCP bandwidth tracer for XDN,
plus a small set of plotting utilities for turning the resulting CSV
into bandwidth/latency graphs.

## What's in this directory

| File | Purpose |
|------|---------|
| `inter_replica_bw.bt` | bpftrace probe: kprobes on `tcp_sendmsg` / `tcp_cleanup_rbuf`, aggregates bytes by `(pid, local_port, peer_ip, peer_port)` and emits per-interval snapshots. |
| `trace_bw.py` | driver: discovers the cluster via the RC HTTP API, launches the probe, drives a mixed read/write workload at every replica in parallel, writes a per-event CSV plus a `.meta.json` sidecar. Supports four `--mode`s (`local` / `probe` / `driver` / `aggregate`) — see "Distributed deployment" below. |
| `trace_bw_distributed.py` | SSH orchestrator that runs `trace_bw.py --mode probe` on every AR host, drives the HTTP workload locally, then merges per-host CSVs into a single resolved trace via `--mode aggregate`. |
| `plot_bw_graph.py` | renders a directed bandwidth graph (`DiGraph`) from the CSV: replicas on a circle, per-replica `client-ARx` vertices on an outer ring. |
| `plot_lat_graph.py` | renders an inter-replica latency graph computed analytically from each replica's `(lat, lon)` (Haversine / fiber-speed); supports `circle` and `geo` layouts. |
| `_plotutil.py` | helpers shared by the two plot scripts (Bezier label placement, AR-on-circle layout). |
| `install_bpftrace.sh` | one-shot installer: pulls the upstream static `bpftrace` AppImage, transparently falls back to `--appimage-extract` on hosts where FUSE auto-mount is broken, symlinks `/usr/local/bin/bpftrace`, and is idempotent + self-healing. |
| `results/` | default output directory for CSVs, both bandwidth and latency PNGs, and `.meta.json` sidecars. |

## Pre-flight

- Linux host with `bpftrace` ≥ 0.21 (uses kernel BTF, no kernel headers
  required). The `inter_replica_bw.bt` script does not `#include` any
  Linux header. **Distro packages are usually too old** — Ubuntu jammy's
  apt ships 0.14, which rejects builtins like `bswap` used by the probe.
  Use `install_bpftrace.sh` to grab a current upstream build:
  ```bash
  xdn-bw-trace/install_bpftrace.sh                  # pinned default
  xdn-bw-trace/install_bpftrace.sh --version v0.25.1
  xdn-bw-trace/install_bpftrace.sh --force          # force reinstall
  ```
  The script is self-healing: re-running it detects a previously broken
  install (e.g. a bare AppImage on a host where FUSE auto-mount fails)
  and reinstalls into `/opt/bpftrace-<ver>/AppRun` with a symlink at
  `/usr/local/bin/bpftrace`.
- An XDN cluster running with the same `gigapaxos.properties` file you
  pass via `--config`.
- The service deployed, e.g.:
  ```
  xdn launch bookcatalog --image=fadhilkurnia/xdn-bookcatalog \
      --state=/app/data/ --deterministic=true
  ```
- Run as root, or with passwordless `sudo` (bpftrace requires `CAP_BPF`).
- Python deps: `requests`, `networkx`, `matplotlib`, `numpy`.

## Quick start

```bash
# 1. Trace: drives 80% GET / 20% PUT at every replica in parallel.
sudo python3 xdn-bw-trace/trace_bw.py \
    --config conf/gigapaxos.xdn.local.properties \
    --service bookcatalog \
    --duration 30 --interval 5 --rate 20

# 2. Bandwidth graph.
python3 xdn-bw-trace/plot_bw_graph.py \
    xdn-bw-trace/results/bookcatalog-<ts>.csv

# 3. Latency graph (estimated from geolocations, no live measurement).
python3 xdn-bw-trace/plot_lat_graph.py \
    --config conf/gigapaxos.xdn.local.properties \
    --service bookcatalog
```

`trace_bw.py` produces `<service>-<unix-ts>.csv` plus a sibling
`<service>-<unix-ts>.meta.json` in `xdn-bw-trace/results/`. The plot
scripts auto-pick up the meta sidecar to render a workload subtitle.

## What `trace_bw.py` captures

The bpftrace probe matches every TCP flow where either endpoint sits in
the AR NIO range **or** the AR HTTP frontend range, so the CSV includes
both inter-replica Paxos traffic (NIO listeners) and client⇄replica
HTTP traffic (HTTP frontends, port = NIO port + 300).

Replica identity is reconstructed in userspace by:

- mapping the kernel `pid` for each event to an AR id via a one-shot
  scan of `/proc/<pid>/cmdline` for `…ReconfigurableNode AR<N>` (so
  bpftrace can attach to an already-running cluster — no need to start
  before the JVMs);
- mapping listener-port → AR id from the RC's placement endpoint;
- cross-correlating ephemeral ports against rows where the same number
  appears as someone else's `local_port` (recovers `(i, j)` even on
  loopback).

Non-AR participants in HTTP flows collapse into one `client-<AR>`
vertex per replica (so each replica's external traffic is visible as
its own pair of edges instead of a shared client hub).

## CSV schema

```
interval_start_unix,interval_end_unix,direction,
local_id,local_pid,local_port,
peer_id,peer_ip,peer_port,
bytes
```

`local_id` and `peer_id` are AR ids (`AR0`, `AR1`, …) for inter-replica
flows or `client-AR<X>` for the client side of HTTP flows. The raw
`local_pid`, `local_port`, `peer_ip`, `peer_port` columns are kept for
debugging / re-resolution.

## Meta sidecar

Alongside each CSV, `trace_bw.py` writes a `.meta.json` with:
service name, target ids, rate / duration / interval / warmup, read
ratio, read & write request specs, run totals (sent/ok/failed/reads/
writes/elapsed), and a `service_info` block populated from each AR's
`/api/v2/services/<svc>/replica/info` endpoint (`protocol`,
`consistency`, `requested_consistency`, `deterministic`).

`plot_bw_graph.py` renders these fields as the figure's subtitle:

```
service=bookcatalog · protocol=PaxosReplicaCoordinator ·
consistency=Linearizability · r/w=80%/20% · rate=20 req/s × 5 replicas ·
duration=30s
read=GET /api/books/1 · write=PUT /api/books/1 · sent reads=… writes=…
```

## Workload mix and seeding

By default the driver sends 80% reads (`GET /api/books/1`) and 20%
writes (`PUT /api/books/1`) — the read targets a specific id so the
response payload size is constant per request. Before traffic starts,
the driver probes `--path` on the first target; if that returns 404 it
POSTs `--seed-body` to `--seed-path` (default `/api/books`) so the
read/write targets resolve cleanly.

For services other than bookcatalog, override `--path`, `--write-path`,
`--write-body`, `--write-method`, and either `--seed-path`/`--seed-body`
or pass `--no-seed`.

## CLI flags — `trace_bw.py`

Discovery / topology:

| Flag | Default | Purpose |
|------|---------|---------|
| `--config` | (required) | gigapaxos properties (used to derive RC endpoint and report node geolocations) |
| `--service` | (required) | service name; sent in `XDN:` header and used to query the RC's placement endpoint |
| `--reconfigurator` | derived from `--config` | `host[:port]` override for the RC HTTP endpoint (port defaults to NIO port + 300) |
| `--target` | every replica in the placement | comma-separated allowlist of replica ids (e.g. `AR1,AR3`) to drive traffic at |

Workload:

| Flag | Default | Purpose |
|------|---------|---------|
| `--rate` | `20` | per-replica request rate (req/s); aggregate offered load is `rate × len(targets)` |
| `--duration` | `60` | trace duration (seconds) |
| `--read-ratio` | `0.8` | fraction of requests that are reads (vs. writes); use `1.0` / `0.0` for read- or write-only |
| `--path`, `--method` | `/api/books/1`, `GET` | read request path / method |
| `--write-path`, `--write-method` | `/api/books/1`, `PUT` | write request path / method |
| `--write-body`, `--write-content-type` | small JSON / `application/json` | write request body and `Content-Type` |
| `--seed-path`, `--seed-body`, `--no-seed` | `/api/books`, JSON, off | one-shot seeding of the target item before traffic starts |
| `--timeout` | `5` | per-request HTTP timeout (seconds) |
| `--warmup` | `2` | wait between bpftrace start and traffic start (seconds) |

Probe:

| Flag | Default | Purpose |
|------|---------|---------|
| `--interval` | `5` | bpftrace snapshot interval (seconds) |
| `--bpftrace-script` | sibling `inter_replica_bw.bt` | override probe script |
| `--output` | auto | CSV path; meta sidecar lives next to it |

## CLI flags — `plot_bw_graph.py`

| Flag | Default | Purpose |
|------|---------|---------|
| `csv` (positional) | (required) | path to a `trace_bw.py` CSV |
| `--direction` | `out` | `out` (sender-side, counts each flow once), `in` (receiver-side), or `both` (double-count, sanity only) |
| `--include-unknown` | off | keep rows whose `local_id` / `peer_id` is `unknown` |
| `--unit` | `KB` | `B` / `KB` / `MB` for matrix and edge labels |
| `--output` | sibling PNG | output image path |

Vertices: ARs on an inner unit circle (round, blue); each
`client-ARx` on an outer ring at the matching angle (square, orange).
Directed edges, two arcs per pair, edge labels positioned along their
own arc with a fixed data-coordinate offset before the arrowhead so
labels never cover the arrow.

## CLI flags — `plot_lat_graph.py`

Latency is computed analytically from each replica's `(lat, lon)`:
Haversine great-circle distance divided by `c × fiber_fraction`. This
is a propagation-only lower bound — real internet RTT is typically
1.5–2× higher.

| Flag | Default | Purpose |
|------|---------|---------|
| `--config` | (required if no `--service`) | gigapaxos properties; reads `active.<id>.geolocation` |
| `--service` | — | if set, queries the RC's placement endpoint and uses only the replicas hosting this service (and their geolocations) |
| `--reconfigurator` | derived from `--config` | RC endpoint override, same shape as `trace_bw.py` |
| `--unit` | `ms` | `ms` or `us` for labels and the printed matrix |
| `--rtt` | off (one-way) | display 2× one-way |
| `--fiber-fraction` | `0.667` | effective signal speed as a fraction of c |
| `--layout` | `circle` | `circle` (matches `plot_bw_graph.py`) or `geo` (equirectangular projection of `(lon, lat)`) |
| `--min-node-spacing` | `0.20` | (geo only) minimum distance in normalized data units between any two replicas after projection; tunable to trade ratio fidelity against label readability |
| `--output` | `xdn-bw-trace/results/<service-or-config>-latency.png` | output image path; default lives next to the bandwidth PNGs in `results/` |

`geo` mode forces equal lat/lon aspect (`set_aspect("equal",
adjustable="datalim")`), so 1° lat renders the same length as 1° lon.
Replicas closer than `--min-node-spacing` after projection are pushed
apart along their connecting line by the smallest amount that prevents
disk overlap, so geographic ratios remain close to the real ones.

## Distributed deployment

`bpftrace` only sees its host's kernel, so a single `trace_bw.py
--mode local` invocation captures everything cleanly only when the AR
JVMs and the driver share a host (loopback / single-machine). For a
real distributed cluster, `trace_bw.py` exposes three split-mode entry
points and `trace_bw_distributed.py` ties them together over SSH.

### Modes

`trace_bw.py --mode {local | probe | driver | aggregate}` (default `local`).

| Mode | What it does | Where to run it | Outputs |
|------|--------------|-----------------|---------|
| `local` | drive workload **and** run bpftrace **and** resolve in one process — single-host all-in-one path. **Backward-compatible default.** | one host where every AR JVM lives | `<service>-<ts>.csv` (resolved) + `.meta.json` |
| `probe` | run bpftrace only, dump raw events + a `pid_to_ar.json` sidecar | each AR host (one probe per host) | `<output>.csv` (raw, no `local_id`/`peer_id`) + `<output>.pid_to_ar.json` |
| `driver` | drive HTTP workload only (no bpftrace) | any host that can reach all AR HTTP frontends | `<output>.meta.json` |
| `aggregate` | union N raw probe CSVs + sidecars, run `resolve_rows`, write a single resolved CSV | the orchestrator (or anywhere the per-host CSVs are gathered) | `<output>.csv` (resolved) |

The aggregator's resolution works because GigaPaxos uses long-lived
TCP connections between replicas: AR-i's outbound ephemeral port shows
up as `peer_port` in AR-j's probe, and as `local_port` in AR-i's probe.
Unioning the events lets the existing `ephemeral_owner` cross-correlation
in `resolve_rows` recover the `(i, j)` pair across hosts.

### Orchestrator: `trace_bw_distributed.py`

End-to-end multi-host run with SSH fan-out:

```bash
python3 xdn-bw-trace/trace_bw_distributed.py \
    --config conf/gigapaxos.cloudlab.properties \
    --service bookcatalog \
    --ssh-key ~/.ssh/cloudlab \
    --username fadhil \
    --duration 60 --interval 5 --rate 50 --read-ratio 0.0
```

The orchestrator:

1. Reads `--config` to enumerate AR hosts (or honors `--hosts host1,host2,…`).
2. For each host, opens an SSH session and starts `trace_bw.py --mode probe`
   under `sudo -n` (bpftrace needs `CAP_BPF`). Probes write raw CSVs +
   sidecars into the remote `--tmpdir`.
3. Sleeps `--probe-startup-buffer` seconds (default 3) so kprobes attach
   before traffic begins.
4. Locally runs `trace_bw.py --mode driver` to drive the HTTP workload
   against every replica in the placement.
5. Waits for all probes to finish.
6. SCPs each per-host CSV + sidecar back to `xdn-bw-trace/results/`.
7. Runs `trace_bw.py --mode aggregate` over all retrieved CSVs to write a
   single resolved CSV, identical in shape to a `--mode local` output.
8. Cleans up remote tmp files (skip with `--keep-remote-files`).

Pre-flight on remote hosts:
- `bpftrace` ≥ 0.21 installed (use `xdn-bw-trace/install_bpftrace.sh`).
- `python3` + `requests`.
- The xdn checkout at the same path as locally, or pass
  `--remote-xdn-path /path/to/remote/xdn`.
- `gigapaxos.properties` at the same path as `--config`, or pass
  `--remote-config-path /path/to/remote/properties`.
- Passwordless SSH from the orchestrator and passwordless `sudo` for
  `bpftrace` on each AR host.

### Manual split-mode invocation

If you don't want SSH orchestration, you can run the modes by hand. For
each AR host:

```bash
sudo python3 xdn-bw-trace/trace_bw.py \
    --mode probe \
    --config /path/to/gigapaxos.properties \
    --service bookcatalog \
    --duration 65 --interval 5 \
    --output /tmp/probe-host-A.csv
```

On any host that can reach all AR frontends:

```bash
python3 xdn-bw-trace/trace_bw.py \
    --mode driver \
    --config /path/to/gigapaxos.properties \
    --service bookcatalog \
    --rate 50 --duration 60 --read-ratio 0.0 \
    --output /tmp/driver.csv
```

Once all per-host probes finish and you've collected their CSVs +
`.pid_to_ar.json` sidecars to the same directory:

```bash
python3 xdn-bw-trace/trace_bw.py \
    --mode aggregate \
    --config /path/to/gigapaxos.properties \
    --service bookcatalog \
    --inputs /tmp/probe-host-A.csv,/tmp/probe-host-B.csv,/tmp/probe-host-C.csv \
    --output xdn-bw-trace/results/bookcatalog-distributed.csv
```

## Caveats

- **Linux + root.** No macOS path.
- **Wire bytes.** Counts include TCP/TLS framing and retransmits, not
  just application payload. Fine for placement decisions; flag if
  publishing numbers.
- **Linearizable reads go through Paxos.** With the default
  `linearizability` consistency, every GET also runs through consensus,
  so the bandwidth graph reflects all traffic, not just writes.
- **Latency graph is geodesic.** `plot_lat_graph.py` reports a
  propagation lower bound (great-circle / fiber). Real RTT typically
  exceeds it by 1.5–2× because of routing/queueing/serialization.
- **`trace_bw.py` runs under sudo**, so the CSV / `.meta.json` and the
  `results/` directory end up root-owned. The plot scripts run as your
  user and will hit `PermissionError` when writing the PNG. Either run
  the plot scripts with `sudo` too, or chown after each trace:
  ```bash
  sudo chown -R "$USER:$USER" xdn-bw-trace/results/
  ```

## Deferred follow-ons

- SSH harness so a single driver invocation runs bpftrace on every AR
  and merges the CSVs.
- A `XdnBandwidthProfiler` Java worker (analog of
  `XdnGeoDemandProfiler`) that reports bandwidth deltas to the
  Reconfigurator and feeds `XdnReplicaPlacementProfile`.
- Per-service attribution via per-service virtual ports (the
  blackbox-per-port architecture).
- Live RTT measurement to cross-validate the analytical latency graph.
