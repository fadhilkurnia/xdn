# Plan: `run_geolat_consistency.py` on a Small Cluster

## Problem

`run_geolat_consistency.py` requires 12+ replica machines because weaker
consistency models (eventual, causal, etc.) place up to 10 replicas to bring
them closer to clients. We only have 5 machines: 1 driver + 4 workers.

## Approach: Virtual Replicas via IP Aliases + tc on NIC and Loopback

Assign IP aliases to each AR machine so every virtual replica gets a unique
IP. Apply tc/netem on **both** the physical NIC (inter-machine delay) and
**loopback** (intra-machine delay between co-located replicas).

GigaPaxos natively supports multiple ARs on the same host — each AR runs as
a separate JVM with its own port, HTTP frontend (port + 300), and randomly
allocated Docker container port.

## Physical Layout (5 machines)

```
Machine 1 (10.10.1.1)  — AR host, up to 4 virtual replicas
Machine 2 (10.10.1.2)  — AR host, up to 3 virtual replicas
Machine 3 (10.10.1.3)  — AR host, up to 3 virtual replicas
Machine 4 (10.10.1.4)  — RC (control plane)
Machine 5              — Driver: runs script, latency proxy, client
```

## Virtual IP Assignment

Round-robin across 3 AR machines. Each virtual replica gets a unique alias IP.
The first 3 replicas use the machines' primary IPs; additional replicas use
aliases in the 10.10.1.{11..} range (avoiding .4 which is the RC).

| Replica | Physical machine | IP (alias)  | AR port | HTTP port |
|---------|-----------------|-------------|---------|-----------|
| 0       | Machine 1       | 10.10.1.1   | 2000    | 2300      |
| 1       | Machine 2       | 10.10.1.2   | 2000    | 2300      |
| 2       | Machine 3       | 10.10.1.3   | 2000    | 2300      |
| 3       | Machine 1       | 10.10.1.11  | 2000    | 2300      |
| 4       | Machine 2       | 10.10.1.12  | 2000    | 2300      |
| 5       | Machine 3       | 10.10.1.13  | 2000    | 2300      |
| 6       | Machine 1       | 10.10.1.21  | 2000    | 2300      |
| 7       | Machine 2       | 10.10.1.22  | 2000    | 2300      |
| 8       | Machine 3       | 10.10.1.23  | 2000    | 2300      |
| 9       | Machine 1       | 10.10.1.31  | 2000    | 2300      |

Each alias IP binds its own JVM on port 2000 (no port conflict since different
IPs). HTTP frontend at port 2300 per IP, Docker ports randomly allocated.

## Traffic Paths and tc/netem

| Source            | Destination          | Kernel path    | tc device  |
|-------------------|----------------------|----------------|------------|
| Machine 1 (.1)    | Machine 2 (.2)       | physical NIC   | `eth0`     |
| Machine 1 (.1)    | Machine 2 alias (.12)| physical NIC   | `eth0`     |
| Machine 1 (.1)    | Machine 1 alias (.11)| **loopback**   | `lo`       |

### Inter-machine delay (physical NIC)

Same as current `apply_inter_server_latency()` — per-destination-IP filters on
the physical NIC. Each (source machine, destination virtual IP) pair gets a
tc/netem class with delay based on geographic distance.

The NIC device name is auto-detected via `detect_nic()` (stored in
`nic_by_host`), not hardcoded.

```bash
# On machine 1 (nic=enp195s0np0, auto-detected): delay to virtual replicas on other machines
NIC=enp195s0np0  # from nic_by_host["10.10.1.1"]
tc qdisc add dev $NIC root handle 1: htb default 99
tc class add dev $NIC parent 1: classid 1:99 htb rate 100gbit

# To replica 1 on machine 2 (10.10.1.2, geo: Seattle), 50ms one-way
tc class add dev $NIC parent 1: classid 1:1 htb rate 100gbit
tc qdisc add dev $NIC parent 1:1 handle 11: netem delay 50.00ms
tc filter add dev $NIC parent 1: protocol ip prio 1 u32 \
  match ip dst 10.10.1.2/32 flowid 1:1

# To replica 4 on machine 2 (10.10.1.12, geo: Miami), 30ms one-way
tc class add dev $NIC parent 1: classid 1:2 htb rate 100gbit
tc qdisc add dev $NIC parent 1:2 handle 12: netem delay 30.00ms
tc filter add dev $NIC parent 1: protocol ip prio 2 u32 \
  match ip dst 10.10.1.12/32 flowid 1:2

# ... repeat for all virtual IPs on other machines
```

### Intra-machine delay (loopback)

Traffic between alias IPs on the same machine routes through loopback,
bypassing the physical NIC. Apply tc/netem on `lo` with per-destination-IP
filters.

```bash
# On machine 1 (lo): delay to co-located virtual replicas
tc qdisc add dev lo root handle 1: htb default 99
tc class add dev lo parent 1: classid 1:99 htb rate 100gbit

# To alias .11 (geo: LA), avg one-way delay from co-located replicas = 40ms
tc class add dev lo parent 1: classid 1:1 htb rate 100gbit
tc qdisc add dev lo parent 1:1 handle 11: netem delay 40.00ms
tc filter add dev lo parent 1: protocol ip prio 1 u32 \
  match ip dst 10.10.1.11/32 flowid 1:1

# To alias .21 (geo: Chicago), avg one-way delay = 15ms
tc class add dev lo parent 1: classid 1:2 htb rate 100gbit
tc qdisc add dev lo parent 1:2 handle 12: netem delay 15.00ms
tc filter add dev lo parent 1: protocol ip prio 2 u32 \
  match ip dst 10.10.1.21/32 flowid 1:2
```

### Per-pair accuracy (optional enhancement)

The above uses dst-only filters — the delay to a destination alias is the same
from all co-located senders. For full per-pair accuracy, match both src and dst:

```bash
# NYC (.1) → LA (.11): 40ms one-way
tc filter add dev lo parent 1: protocol ip prio 1 u32 \
  match ip src 10.10.1.1/32 match ip dst 10.10.1.11/32 flowid 1:1

# Chicago (.21) → LA (.11): 28ms one-way
tc filter add dev lo parent 1: protocol ip prio 2 u32 \
  match ip src 10.10.1.21/32 match ip dst 10.10.1.11/32 flowid 1:2
```

With at most 4 replicas per machine: 4×3 = 12 filter rules. Manageable.

**Caveat**: per-pair filtering requires NIO to use the alias IP as source. NIO
binds to the configured address, but outgoing connection source IP is set by
kernel route lookup. If unreliable, fall back to dst-only filtering with
average delay (bounded error due to round-robin geographic spread).

## Why This Is Accurate Per Consistency Model

| Model | Replicas | Consensus? | Impact of co-location |
|-------|----------|------------|----------------------|
| linearizability | 3 (one per machine) | Paxos | **None** — no co-location |
| sequential | 3 rw + up to 7 ro | Paxos (writes) | rw replicas on separate machines; ro replicas serve reads locally |
| causal/pram | up to 10 | No consensus | Requests handled locally |
| client-centric | up to 10 | No consensus | Requests handled locally |
| eventual | up to 10 | No consensus | Requests handled locally |

Round-robin ensures the first 3 replicas (Paxos quorum for linearizability/
sequential) always land on separate physical machines with proper inter-machine
WAN emulation. The loopback tc handles intra-machine delay for additional
replicas.

Client-perceived latency is dominated by the latency proxy (client ↔ replica
geographic delay), which works correctly since each virtual replica has its own
IP and geolocation in the config.

## Script Changes

### CLI arguments

```python
parser.add_argument("--ar-hosts",
    default="10.10.1.1,10.10.1.2,10.10.1.3",
    help="Comma-separated physical AR host IPs")
parser.add_argument("--control-plane-host",
    default="10.10.1.4",
    help="Control plane (RC) host IP")
parser.add_argument("--max-replicas", type=int, default=10,
    help="Max virtual replicas across all AR hosts")
parser.add_argument("--consistency",
    default=None,
    help="Comma-separated list of consistency models to run "
         "(default: all 9). Example: --consistency linearizability,sequential,eventual")
parser.add_argument("--num-warmup", type=int, default=500,
    help="Number of warmup requests per client location (default: 500)")
parser.add_argument("--num-requests", type=int, default=1000,
    help="Number of measurement requests per client location (default: 1000)")
# --net-device removed: auto-detected per host via detect_nic()
```

### New functions

#### `detect_nic(host, peer_ip)`
Auto-detect the physical NIC on a given host by querying the kernel routing
table. This avoids hardcoding interface names, which vary across CloudLab
machine types (e.g., `enp65s0f0np0`, `enp195s0np0`, `ens1f1np1`).

```python
def detect_nic(host, peer_ip):
    """Detect the physical NIC on `host` used to reach `peer_ip`."""
    result = subprocess.run(
        ["ssh", host, f"ip -o route get {peer_ip}"],
        capture_output=True, text=True, check=True,
    )
    # Output: "10.10.1.2 dev enp195s0np0 src 10.10.1.1 ..."
    parts = result.stdout.strip().split()
    dev_idx = parts.index("dev")
    return parts[dev_idx + 1]
```

Called once per physical AR host at script start. Returns a `{host: nic_name}`
mapping used by `setup_ip_aliases`, `apply_inter_server_latency`, and
`apply_intra_machine_latency`.

```python
# Example: detect NIC on each AR host using a peer on another machine
nic_by_host = {}
for i, host in enumerate(ar_hosts):
    peer = ar_hosts[(i + 1) % len(ar_hosts)]
    nic_by_host[host] = detect_nic(host, peer)
# Result: {"10.10.1.1": "enp195s0np0", "10.10.1.2": "enp195s0np0", ...}
```

#### `setup_ip_aliases(ar_hosts, virtual_ip_map, nic_by_host)`
- SSH to each AR machine
- Add alias IPs: `ip addr add 10.10.1.X/24 dev <nic_by_host[host]>`
- Called once at script start

#### `teardown_ip_aliases(ar_hosts, virtual_ip_map, nic_by_host)`
- SSH to each AR machine
- Remove alias IPs: `ip addr del 10.10.1.X/24 dev <nic_by_host[host]>`
- Called at script exit (in `finally` block)

#### `get_virtual_ip(replica_index, ar_hosts)`
- Round-robin mapping: `physical_host = ar_hosts[index % len(ar_hosts)]`
- Primary IPs for first `len(ar_hosts)` replicas, aliases for the rest
- Alias scheme: `10.10.1.{10*(slot+1) + host_index + 1}`
  - Slot 0: .1, .2, .3 (primary)
  - Slot 1: .11, .12, .13
  - Slot 2: .21, .22, .23
  - Slot 3: .31

#### `apply_intra_machine_latency(physical_host, co_located_replicas)`
- Compute pairwise geographic delays between co-located replicas
- SSH to `physical_host` and apply tc/netem on `lo` with per-dst-IP filters
- For each alias IP, use average one-way delay from other co-located replicas

### Modified functions

#### `prepare_xdn_config_file()`
- Use virtual IPs from `get_virtual_ip()` instead of sequential `10.10.1.{1..N}`
- Return `virtual_ip → physical_host` mapping alongside `server_address_by_name`

#### `apply_inter_server_latency()`
- SSH to **physical host** (not virtual IP) to configure tc
- Use `nic_by_host[host]` for the device name (auto-detected, not hardcoded)
- Only apply rules for cross-machine pairs (skip same-machine pairs)
- Call `apply_intra_machine_latency()` for same-machine pairs

#### `reset_inter_server_latency()`
- Also reset tc on `lo` in addition to the physical NIC

#### `reset_xdn_cloudlab_cluster()`
- Iterate over physical AR hosts + alias IPs, not `10.10.1.{1..N}`
- Kill processes on all virtual IPs, clean up state

#### `get_protocol_aware_replica_placement()`
- Add `max_replicas` parameter to cap replica count
- `linearizability`: keep 3 (must be >= 3 for Paxos majority)
- `sequential`: `min(computed, max_replicas)`
- `eventual`: `min(10, max_replicas)`

### Validation

- `assert len(ar_hosts) >= 3` — need 3 physical machines for Paxos quorum
- `assert max_replicas >= 3` — minimum for any consistency model
- `assert max_replicas <= len(ar_hosts) * 4` — practical limit per machine

## Cleanup and Isolation

The script must guarantee that **all emulated state is cleaned up** regardless
of success or failure — tc rules, IP aliases, XDN processes, Docker containers,
and latency proxy. A stale tc rule on loopback or a leftover alias IP would
corrupt subsequent experiments or normal SSH/system traffic.

### Cleanup function: `cleanup_all()`

A single function that unconditionally removes all emulated state:

```python
def cleanup_all(ar_hosts, nic_by_host, virtual_ip_map):
    """Remove all tc rules, IP aliases, processes, and containers.

    Safe to call multiple times (idempotent). Each step uses
    check=False / ignore errors so a failure in one step does not
    prevent the remaining steps from executing.
    """
    for host in ar_hosts:
        nic = nic_by_host.get(host, "")
        # 1. Remove tc rules on physical NIC and loopback
        if nic:
            _ssh(host, f"sudo tc qdisc del dev {nic} root 2>/dev/null || true")
        _ssh(host, "sudo tc qdisc del dev lo root 2>/dev/null || true")

        # 2. Remove IP aliases added by this script
        alias_ips = [ip for ip, phys in virtual_ip_map.items() if phys == host]
        for alias_ip in alias_ips:
            if nic:
                _ssh(host, f"sudo ip addr del {alias_ip}/24 dev {nic} 2>/dev/null || true")

        # 3. Kill XDN processes and containers
        _ssh(host, "sudo fuser -k 2000/tcp 2>/dev/null || true")
        _ssh(host, "sudo fuser -k 2300/tcp 2>/dev/null || true")
        _ssh(host, "sudo rm -rf /tmp/gigapaxos /tmp/xdn /dev/shm/xdn")
        _ssh(host, (
            "containers=$(docker ps -a -q); "
            "if [ -n \"$containers\" ]; then docker stop $containers && docker rm $containers; fi"
        ), check=False)
        _ssh(host, "docker network prune --force > /dev/null 2>&1", check=False)

    # 4. Clean up control plane
    _ssh(control_plane_host, "sudo fuser -k 3000/tcp 2>/dev/null || true")
    _ssh(control_plane_host, "sudo rm -rf /tmp/gigapaxos")

    # 5. Kill latency proxy on driver
    os.system("fuser -s -k 8080/tcp 2>/dev/null || true")
```

### Invocation pattern: `try/finally` at every level

The script has two nested loops: outer loop over consistency models, inner
implicit loop over the setup/measure/teardown phases. Cleanup must happen at
both levels.

#### Top-level: script entry point

```python
# Detect NICs and build virtual IP map once
nic_by_host = {host: detect_nic(host, ...) for host in ar_hosts}
virtual_ip_map = build_virtual_ip_map(ar_hosts, max_replicas)

try:
    setup_ip_aliases(ar_hosts, virtual_ip_map, nic_by_host)

    for consistency in consistency_models:
        for read_ratio in read_ratios:
            try:
                # setup cluster, emulate latency, deploy, measure
                ...
            except Exception as e:
                print(f"ERROR in {consistency}/{read_ratio}: {e}")
                traceback.print_exc()
            finally:
                # Per-iteration cleanup: stop cluster, remove tc, kill proxy
                try:
                    destroy_service(...)
                except Exception:
                    pass
                reset_xdn_cloudlab_cluster(...)
                reset_inter_server_latency(ar_hosts, nic_by_host)
                os.system("fuser -s -k 8080/tcp 2>/dev/null || true")
finally:
    # Unconditional full cleanup — runs on success, failure, or KeyboardInterrupt
    cleanup_all(ar_hosts, nic_by_host, virtual_ip_map)
```

#### Key properties

1. **Per-iteration `finally`**: each (consistency, read_ratio) iteration cleans
   up its own tc rules, XDN cluster, and latency proxy. This prevents state
   from one iteration leaking into the next.

2. **Top-level `finally`**: catches any unhandled exception, `KeyboardInterrupt`
   (Ctrl-C), or `SystemExit` and runs `cleanup_all()` which removes IP aliases,
   all tc rules, and all processes. This is the safety net.

3. **Idempotent cleanup**: every cleanup command uses `2>/dev/null || true` or
   `check=False`, so calling `cleanup_all()` after per-iteration cleanup is
   harmless (no errors from already-removed rules).

4. **IP aliases removed last**: aliases are set up once at script start and
   removed in the top-level `finally`. Per-iteration cleanup does NOT remove
   aliases (they're reused across iterations).

### Signal handling (optional hardening)

For extra safety against `kill -9` or OOM kills (where `finally` blocks don't
run), register an `atexit` handler:

```python
import atexit

def _atexit_cleanup():
    try:
        cleanup_all(ar_hosts, nic_by_host, virtual_ip_map)
    except Exception:
        pass

atexit.register(_atexit_cleanup)
```

Note: `atexit` does NOT run on `SIGKILL` (kill -9). For that case, the next
script invocation should call `cleanup_all()` at startup as a precaution.

### Startup pre-clean

As a defense against stale state from a previously killed run, call
`cleanup_all()` **before** setting up anything:

```python
# Pre-clean any stale state from a previous aborted run
print("Pre-cleaning stale state from previous runs ...")
cleanup_all(ar_hosts, nic_by_host, virtual_ip_map)

try:
    setup_ip_aliases(...)
    ...
finally:
    cleanup_all(...)
```

This ensures the script is fully isolated and idempotent — it can be run,
killed, and re-run without manual cleanup.

## Verbose Logging

Use Python's `logging` module with timestamped output at INFO level. Every
significant step prints to both console and a log file in the results directory.

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),                          # console
        logging.FileHandler(results_dir / "run.log"),     # persistent log
    ],
)
log = logging.getLogger("geolat")
```

Log at each phase boundary:
```python
log.info("=" * 60)
log.info("  Consistency: %s | Read ratio: %d%% | Replicas: %d", consistency, read_ratio, num_replicas)
log.info("=" * 60)
log.info("[Phase 1] Cleaning up previous cluster ...")
log.info("[Phase 2] Generating config for %d replicas ...", num_replicas)
log.info("[Phase 3] Starting XDN cluster ...")
log.info("[Phase 4] Emulating WAN latency ...")
log.info("[Phase 5] Verifying WAN emulation ...")
log.info("[Phase 6] Starting latency proxy ...")
log.info("[Phase 7] Deploying service '%s' ...", service_name)
log.info("[Phase 8] Warmup (%d requests per location) ...", num_warmup)
log.info("[Phase 9] Measurement (%d requests per location) ...", num_requests)
log.info("[Phase 10] Collecting results ...")
log.info("[Phase 11] Cleaning up ...")
```

## Output Directory and Files

Timestamped results directory, consistent with other eval scripts:

```
eval/results/geolat_consistency_YYYYMMDD_HHMMSS/
├── run.log                                          # full verbose log
├── eval_geolat_consistency_summary.csv              # summary: one row per (consistency, read_ratio)
├── eval_geolat_consistency_raw.csv                  # raw: one row per request (for CDF plots)
├── wan_verification.log                             # ping RTT verification results
├── config_linearizability_0.properties              # generated GigaPaxos configs (for reproducibility)
├── config_sequential_20.properties
├── ...
└── plots/
    └── cons_replica_{consistency}_{read_ratio}.pdf  # replica placement maps
```

### Output CSV files

The script produces four CSV files. The first two match the exact formats
expected by the paper's plotting scripts in `reflex-paper/data/`:

#### 1. `eval_consistency_latency_cdf.csv` (for Figure 20a)

Pre-computed CDF points per consistency model. This is the format expected by
`reflex-paper/data/plot_eval_consistency_latency_cdf.py`.

```csv
consistency,latency_ms,cdf
linearizability,12.3,0.01
linearizability,15.1,0.02
...
sequential,8.2,0.01
...
eventual,5.1,0.01
...
```

Generated from the raw measurements at the 80% read ratio (the paper's workload)
by sorting latencies per consistency model and computing the empirical CDF:

```python
def compute_cdf_csv(raw_rows, target_read_ratio=80):
    """Generate CDF CSV rows from raw measurements at a specific read ratio."""
    cdf_rows = []
    for consistency in SERIES_ORDER:
        latencies = sorted(
            float(r["latency_ms"]) for r in raw_rows
            if r["consistency"] == consistency
            and int(r["read_ratio"]) == target_read_ratio
        )
        n = len(latencies)
        for i, lat in enumerate(latencies):
            cdf_rows.append({
                "consistency": consistency,
                "latency_ms": f"{lat:.2f}",
                "cdf": f"{(i + 1) / n:.6f}",
            })
    return cdf_rows
```

#### 2. `eval_consistency_top3_cities.csv` (for Figure 20b)

Average latency per (city, consistency) for the top-3 metro areas. This is the
format expected by `reflex-paper/data/plot_eval_consistency_top3_cities.py`.

```csv
city,consistency,avg_latency_ms
New York,linearizability,45.2
New York,sequential,32.1
New York,eventual,12.5
Los Angeles,linearizability,38.7
Los Angeles,sequential,25.3
Los Angeles,eventual,8.2
Chicago,linearizability,42.1
Chicago,sequential,28.9
Chicago,eventual,10.1
```

Generated from the raw measurements at the 80% read ratio:

```python
TOP_3_CITIES = ("New York", "Los Angeles", "Chicago")

def compute_top3_cities_csv(raw_rows, target_read_ratio=80):
    """Generate top-3 cities CSV from raw measurements."""
    city_rows = []
    for city in TOP_3_CITIES:
        for consistency in SERIES_ORDER:
            latencies = [
                float(r["latency_ms"]) for r in raw_rows
                if r["consistency"] == consistency
                and int(r["read_ratio"]) == target_read_ratio
                and r["client_city"] == city
            ]
            if latencies:
                city_rows.append({
                    "city": city,
                    "consistency": consistency,
                    "avg_latency_ms": f"{statistics.mean(latencies):.2f}",
                })
    return city_rows
```

#### 3. `eval_geolat_consistency_summary.csv`

One row per (consistency, read_ratio) combination with aggregate statistics:

```csv
consistency,read_ratio,num_replicas,num_clients,num_requests_per_client,avg_latency_ms,median_latency_ms,stddev_latency_ms,p90_latency_ms,p95_latency_ms,p99_latency_ms
linearizability,0,3,100,1000,45.2,43.1,8.3,55.2,62.1,78.4
sequential,20,5,100,1000,32.1,30.5,6.2,40.3,45.1,52.8
...
```

#### 4. `eval_geolat_consistency_raw.csv`

One row per individual request measurement (all read ratios, all cities):

```csv
consistency,read_ratio,client_city,client_lat,client_lon,target_replica,is_read,latency_ms
linearizability,80,New York,40.71,-74.01,10.10.1.1:2300,false,43.2
linearizability,80,New York,40.71,-74.01,10.10.1.1:2300,true,12.1
sequential,80,Los Angeles,34.05,-118.24,10.10.1.2:2300,true,8.3
...
```

This raw format enables:
- Recomputing CDF at any percentile granularity
- Per-city latency breakdown beyond top-3
- Read vs write latency comparison
- Post-hoc analysis with different read ratios

### Generating paper figures

After the script completes, copy the CDF and top-3 CSVs to the paper's data
directory and run the plotting scripts:

```bash
cp results/geolat_consistency_*/eval_consistency_latency_cdf.csv \
   ../reflex-paper/data/eval_consistency_latency_cdf.csv
cp results/geolat_consistency_*/eval_consistency_top3_cities.csv \
   ../reflex-paper/data/eval_consistency_top3_cities.csv

cd ../reflex-paper
python data/plot_eval_consistency_latency_cdf.py
python data/plot_eval_consistency_top3_cities.py
```

## WAN Emulation Verification

After applying tc/netem rules, verify the emulated latency is correct before
proceeding to measurement. This catches misconfigurations early.

#### `verify_wan_emulation(replica_ips, expected_delays, tolerance_ms=5)`

```python
def verify_wan_emulation(replica_ips, expected_delays, tolerance_ms=5):
    """Ping between replica pairs and verify RTT matches expected 2*one_way_delay.

    Args:
        replica_ips: list of virtual replica IPs
        expected_delays: dict of (src_ip, dst_ip) -> one_way_delay_ms
        tolerance_ms: acceptable deviation from expected RTT

    Returns:
        True if all pairs within tolerance, False otherwise.
        Logs each verification result.
    """
    all_ok = True
    for (src_ip, dst_ip), one_way_ms in expected_delays.items():
        expected_rtt_ms = 2 * one_way_ms
        # Find the physical host for src_ip
        physical_host = virtual_ip_to_physical[src_ip]
        result = subprocess.run(
            ["ssh", physical_host,
             f"ping -c 5 -I {src_ip} {dst_ip} -q"],
            capture_output=True, text=True, check=False,
        )
        # Parse avg RTT from: rtt min/avg/max/mdev = 99.5/100.2/101.1/0.5 ms
        match = re.search(r"rtt.*= [\d.]+/([\d.]+)/", result.stdout)
        if not match:
            log.warning("  FAIL: ping %s -> %s: no response", src_ip, dst_ip)
            all_ok = False
            continue
        actual_rtt_ms = float(match.group(1))
        deviation = abs(actual_rtt_ms - expected_rtt_ms)
        status = "OK" if deviation <= tolerance_ms else "FAIL"
        log.info("  %s: %s -> %s  expected_rtt=%.1fms  actual_rtt=%.1fms  dev=%.1fms",
                 status, src_ip, dst_ip, expected_rtt_ms, actual_rtt_ms, deviation)
        if deviation > tolerance_ms:
            all_ok = False
    return all_ok
```

Called after `apply_inter_server_latency()` and `apply_intra_machine_latency()`.
Verifies a sample of pairs (e.g., all inter-machine pairs + one intra-machine
pair per host). Results are written to `wan_verification.log`.

If verification fails, the script logs a warning but continues (the emulation
may still be approximately correct). With `--strict-wan-verify` flag, the script
aborts on failure.

## Warmup

Each (consistency, read_ratio) iteration must warm up before measurement to
ensure JVM JIT compilation and container readiness. The warmup mirrors the
measurement workload but results are discarded.

```python
# Phase 8: Warmup
log.info("[Phase 8] Warmup: %d requests per client location ...", args.num_warmup)
warmup_session = requests.Session()
for client in clients:
    service_target_url = client['ServiceTargetUrl']
    headers = {"Content-Type": "application/json", "XDN": service_name,
               "X-Client-Location": f"{client['Latitude']};{client['Longitude']}"}

    # Generate read/write mix matching the read_ratio
    warmup_ops = generate_read_write_mix(args.num_warmup, read_ratio)

    for i, is_read in enumerate(warmup_ops):
        try:
            if is_read:
                warmup_session.get(service_target_url, headers=headers,
                                   timeout=5, proxies=request_proxies)
            else:
                warmup_session.post(service_target_url, headers=headers,
                                    data=post_data[i % num_tasks],
                                    timeout=5, proxies=request_proxies)
        except Exception as e:
            log.debug("Warmup error: %s", e)
warmup_session.close()
log.info("  Warmup complete.")
```

**Warmup sizing**: default 500 requests per client location. This ensures:
- JVM Paxos code path gets JIT-compiled (~100+ invocations needed, as discovered
  in the microbenchmark investigation)
- Docker container's SQLite is initialized and warmed
- Latency proxy connections are established
- TCP connection pools are warmed

The `--num-warmup` flag allows tuning. For quick testing, use `--num-warmup 50`.

## Consistency Model Selection

The `--consistency` flag filters which models to run:

```python
ALL_CONSISTENCY_MODELS = [
    "linearizability", "sequential", "causal", "pram",
    "monotonic_reads", "writes_follow_reads", "read_your_writes",
    "monotonic_writes", "eventual",
]

if args.consistency:
    models = [m.strip() for m in args.consistency.split(",")]
    for m in models:
        if m not in ALL_CONSISTENCY_MODELS:
            log.error("Unknown consistency model: %s", m)
            log.error("Valid models: %s", ", ".join(ALL_CONSISTENCY_MODELS))
            sys.exit(1)
else:
    models = ALL_CONSISTENCY_MODELS

log.info("Consistency models to measure: %s", models)
```

## Measurement Loop (updated)

```python
# Phase 9: Measurement
log.info("[Phase 9] Measurement: %d requests per client location ...", args.num_requests)
raw_rows = []
measure_session = requests.Session()
for client in clients:
    log.info("  Client: %s (count=%d, replica=%s)",
             client['City'], client['Count'], client['TargetReplicaName'])
    service_target_url = client['ServiceTargetUrl']
    headers = {
        "Content-Type": "application/json",
        "XDN": service_name,
        "X-Client-Location": f"{client['Latitude']};{client['Longitude']}",
    }
    measure_ops = generate_read_write_mix(args.num_requests, read_ratio)

    for client_id in range(client['Count']):
        prev_cookie = None
        for i, is_read in enumerate(measure_ops):
            start = time.perf_counter()
            try:
                if is_read:
                    resp = measure_session.get(
                        service_target_url, headers=headers,
                        timeout=5, cookies=prev_cookie, proxies=request_proxies)
                else:
                    resp = measure_session.post(
                        service_target_url, headers=headers,
                        data=post_data[i % num_tasks],
                        timeout=5, cookies=prev_cookie, proxies=request_proxies)
                prev_cookie = resp.cookies
            except Exception as e:
                log.debug("Measurement error: %s", e)
            latency_ms = (time.perf_counter() - start) * 1000.0

            # Record raw measurement
            raw_rows.append({
                "consistency": consistency,
                "read_ratio": read_ratio,
                "client_city": client['City'],
                "client_lat": client['Latitude'],
                "client_lon": client['Longitude'],
                "target_replica": client['TargetReplicaHostPort'],
                "is_read": is_read,
                "latency_ms": f"{latency_ms:.2f}",
            })
measure_session.close()
```

## Example Runs

```bash
# Full run: all 9 models, all read ratios
python run_geolat_consistency.py \
    --ar-hosts 10.10.1.1,10.10.1.2,10.10.1.3 \
    --control-plane-host 10.10.1.4 \
    --max-replicas 10

# Quick test: only linearizability and eventual
python run_geolat_consistency.py \
    --ar-hosts 10.10.1.1,10.10.1.2,10.10.1.3 \
    --control-plane-host 10.10.1.4 \
    --consistency linearizability,eventual \
    --num-warmup 100 --num-requests 200

# Single model debug run
python run_geolat_consistency.py \
    --ar-hosts 10.10.1.1,10.10.1.2,10.10.1.3 \
    --control-plane-host 10.10.1.4 \
    --consistency sequential \
    --num-warmup 50 --num-requests 100
```

## Files to Modify

1. **`eval/run_geolat_consistency.py`** — all changes above
2. **`eval/README.md`** — update machine requirements and add usage example
