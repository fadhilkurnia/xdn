# xdn-bw-trace

Per-service inter-replica TCP bandwidth tracer for XDN.

A bpftrace probe (`inter_replica_bw.bt`) counts TCP bytes per
`(peer_ip, peer_port)` per direction across the AR NIO port range, and a
Python driver (`trace_bw.py`) parses the gigapaxos properties file for
cluster discovery, launches the probe, drives HTTP traffic at a single
service, and writes a per-interval CSV.

This first cut assumes **one service per cluster at a time** — under that
assumption, the cluster-wide `(peer_i, peer_j)` byte matrix from bpftrace
*is* the per-service bandwidth map; no application instrumentation is
needed. Multi-service attribution and AR-JVM integration are deferred.

## Pre-flight

- Linux host with `bpftrace` installed (`apt install bpftrace`).
- An XDN cluster running with the same properties file you'll pass via
  `--config`.
- A service deployed, e.g.:
  ```
  xdn launch bookcatalog --image=fadhilkurnia/xdn-bookcatalog \
      --state=/app/data/ --deterministic=true
  ```
- Run as root, or with passwordless `sudo` (bpftrace requires `CAP_BPF`).

## Quick start

```bash
sudo python3 xdn-bw-trace/trace_bw.py \
    --config conf/gigapaxos.xdn.local.properties \
    --service bookcatalog \
    --duration 30 --interval 5 --rate 20
```

Default output: `xdn-bw-trace/results/<service>-<unix-ts>.csv`.

## CSV schema

One row per `(snapshot, direction, peer_ip, peer_port)`:

```
interval_start_unix,interval_end_unix,direction,peer_ip,peer_port,bytes
1730000005,1730000010,out,127.0.0.2,2001,1832412
1730000005,1730000010,in,127.0.0.2,2001,512768
1730000010,1730000015,out,127.0.0.2,2001,1755904
...
```

Long-form so you can pivot in pandas/CSV/Excel into matrices, time series,
or directional rollups without the tool prejudging the use case.

## CLI flags

| Flag | Default | Purpose |
|------|---------|---------|
| `--config` | (required) | path to gigapaxos properties file |
| `--service` | (required) | service name (sent in `XDN:` header) |
| `--target` | first `active.*` in config | which AR to drive HTTP traffic against |
| `--path` | `/` | HTTP request path |
| `--method` | `GET` | HTTP method |
| `--rate` | `20` | request rate (req/s) |
| `--duration` | `60` | total trace duration (seconds) |
| `--interval` | `5` | bpftrace snapshot interval (seconds) |
| `--warmup` | `2` | wait between bpftrace start and traffic start |
| `--output` | auto | CSV output path |
| `--bpftrace-script` | sibling `inter_replica_bw.bt` | override probe script |
| `--timeout` | `5` | per-request HTTP timeout |

## How it works

1. **Discovery.** Reads `active.<name>=host:port` and
   `reconfigurator.<name>=host:port` from the properties file. Computes
   each AR's HTTP frontend port as `nio_port + 300` (the offset is
   hardcoded in the XDN codebase per `CLAUDE.md`).
2. **Probe.** `bpftrace` hooks `kprobe:tcp_sendmsg` (outbound) and
   `kprobe:tcp_cleanup_rbuf` (inbound). Filters to flows where either
   endpoint port is within `[min(nio_ports), max(nio_ports)]`. Aggregates
   bytes per `(peer_ip, peer_port)` per direction. Snapshots+resets every
   `--interval` seconds, plus once on shutdown.
3. **Traffic.** A single `requests.Session()` drives `--rate` req/s at
   `http://<target_host>:<target_http><--path>` with `XDN: <service>` for
   `--duration` seconds.
4. **CSV.** Snapshot frames are tagged with the wall-clock time at which
   the driver observed each `SNAP` line; intervals are
   `[prev_snap, current_snap]`.

## Caveats

- **Linux + root.** No macOS path. Use a Linux dev box, a CloudLab node,
  or a VM.
- **Wire bytes.** Counts include TCP/TLS framing and any retransmits, not
  just application payload. That's fine for placement; flag it if
  publishing numbers.
- **Single-host trace.** This iteration runs bpftrace only on the host
  where the script is executed, so the resulting CSV is one row of the
  inter-replica matrix from that host's perspective. Run the tool on each
  AR (with the same `--config`, `--service`, etc.) to assemble the full
  symmetric matrix.
- **Port range filtering.** Any TCP flow on a port within
  `[min(nio_ports), max(nio_ports)]` is counted, including non-AR
  processes. On a dedicated AR host this is fine; on shared hosts you
  may want to narrow the filter.

## Verifying the output

Quick sanity checks on a single CSV:

- With `--rate 0` (no driven traffic), per-interval totals are small and
  roughly constant — paxos heartbeats only.
- Doubling `--rate` roughly doubles per-interval `out` and `in` bytes.
- Per-AR-pair `out` from host A toward host B over an interval should
  approximately equal `in` reported by host B from host A over the same
  interval. (Run the tool on both sides with synced clocks to compare.)

## Deferred follow-ons

- SSH harness so a single driver invocation starts bpftrace on every AR
  and assembles the full matrix.
- A `XdnBandwidthProfiler` Java worker (analog of `XdnGeoDemandProfiler`)
  that reports bandwidth deltas to the Reconfigurator.
- Wire bandwidth into `XdnReplicaPlacementProfile` next to geo-demand for
  placement decisions.
- Per-service attribution via per-service virtual ports (the
  blackbox-per-port architecture discussed in design).
