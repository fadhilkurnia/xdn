# Evaluation Scripts

This folder contains scripts for benchmarking XDN and baseline replication
systems across multiple applications. Scripts are organized by application
determinism: **deterministic** apps use active replication (Paxos/RSM) and
**non-deterministic** apps use primary-backup replication.

## Deterministic Apps (Active Replication)

Use `run_load_ar_*.py` to deploy a service and sweep through request rates,
collecting load-latency numbers.

### `run_load_ar_reflex_app.py`

Starts an XDN cluster (active replication via Paxos), deploys a deterministic
Dockerized service, and runs load tests at each requested rate.

```
python eval/run_load_ar_reflex_app.py <xdn-config> <docker-image> \
    --rates 100,200,500,1000 --duration 30s
```

Results go to `eval/results/load_ar_<app>_reflex_<timestamp>/` (e.g.,
`load_ar_bookcatalog_reflex_20260314_150000/`) with per-rate files (`rate<N>.txt`).
A summary CSV is written inside the same directory as `reflex_load_results.csv`.

### `run_load_ar_distdb_app.py`

Launches a demo app backed by a distributed database (rqlite or TiKV) on the
active replicas and runs the same load sweep.

```
python eval/run_load_ar_distdb_app.py <xdn-config> <docker-image> \
    --rates 100,200,500,1000 --duration 30s
```

Results go to `eval/results/load_ar_<app>_distdb_<backend>_<timestamp>/` (e.g.,
`load_ar_bookcatalog_distdb_rqlite_20260314_150000/`). A summary CSV is written
inside the same directory as `<backend>_load_results.csv`.

### `run_load_ar_oebs_app.py`

Deploys a demo app to Kubernetes with an OpenEBS PVC, port-forwards the service
locally, and runs load tests.

```
python eval/run_load_ar_oebs_app.py <docker-image> --rates 100,200 --duration 1m
```

Results go to `eval/results/load_ar_<app>_oebs_<timestamp>/` (e.g.,
`load_ar_bookcatalog_oebs_20260314_150000/`). A summary CSV is written
inside the same directory as `oebs_load_results.csv`. Per-rate raw outputs are
stored as `*_summary.json`/`*_timeseries.csv` (k6) or `*_latency.txt` (Go).

### `run_load_ar_drbd_app.py`

Creates a 3-node DRBD resource, formats and mounts the replicated block device
on the primary node, starts the container, and runs load tests.

```
python eval/run_load_ar_drbd_app.py <xdn-config> <docker-image> \
    --backing-device /dev/sdb --force-primary --force-create-md
```

Results go to `eval/results/load_ar_<app>_drbd_<timestamp>/` (e.g.,
`load_ar_bookcatalog_drbd_20260314_150000/`). A summary CSV is written inside
the same directory as `drbd_load_results.csv`.

### `run_load_ar_criu_app.py`

Deploys a service using CRIU-based checkpoint/restore on the active replicas
and runs load tests at each requested rate.

```
python eval/run_load_ar_criu_app.py <xdn-config> <docker-image> \
    --rates 100,200,500,1000 --duration 30s
```

Results go to `eval/results/load_ar_<app>_criu_<timestamp>/` (e.g.,
`load_ar_bookcatalog_criu_20260314_150000/`).

### `run_load_ar_eval.sh`

Orchestration script that runs all active replication benchmarks end-to-end:
XDN (reflex), distributed DB, OpenEBS, and CRIU baselines for all apps, then
collects results and generates plots.

```
./eval/run_load_ar_eval.sh                     # run all benchmarks
./eval/run_load_ar_eval.sh --step reflex       # run only XDN active replication
./eval/run_load_ar_eval.sh --step distdb       # run only distributed DB baselines
./eval/run_load_ar_eval.sh --step oebs         # run only OpenEBS baselines
./eval/run_load_ar_eval.sh --step criu         # run only CRIU baselines
./eval/run_load_ar_eval.sh --step collect      # only collect/organize existing results
./eval/run_load_ar_eval.sh --step plot         # only generate plots
```

Environment variables `RATES` and `DURATION` can override the defaults
(e.g., `RATES=100,200,500 DURATION=60s ./eval/run_load_ar_eval.sh`).

## Non-Deterministic Apps (Primary-Backup)

These scripts benchmark non-deterministic applications under primary-backup
replication. Each script covers one replication system and accepts an `--app`
flag to select the application. They run per-rate isolated benchmarks (fresh
cluster deployment per rate point) and produce `rate<N>.txt` files.

### Available apps

| App name | Description | Docker images |
|----------|-------------|---------------|
| `wordpress` | WordPress + MySQL | `mysql:8.4.0`, `wordpress:6.5.4-apache` |
| `bookcatalog` | BookCatalog-ND (SQLite) | `fadhilkurnia/xdn-bookcatalog-nd` |
| `tpcc` | TPC-C (PostgreSQL + Flask) | `postgres:17.4-bookworm`, `fadhilkurnia/xdn-tpcc` |
| `hotelres` | Hotel-Reservation (MongoDB) | `mongo:8.0.5-rc2-noble`, `fadhilkurnia/xdn-hotel-reservation` |
| `synth` | Synthetic Workload (SQLite) | `fadhilkurnia/xdn-synth-workload` |

### `run_load_pb_reflex_app.py`

XDN Primary-Backup via FUSELOG state-diff replication. Fresh cluster per rate point.

```
python eval/run_load_pb_reflex_app.py --app wordpress --rates 100,200,500,1000
python eval/run_load_pb_reflex_app.py --app bookcatalog --duration 30
python eval/run_load_pb_reflex_app.py --app tpcc --sanity-check
```

Apps: `wordpress`, `bookcatalog`, `tpcc`, `hotelres`.
Extra flags: `--sanity-check`, `--sample-latency`, `--jfr`, `--apache-timing`.

### `run_load_pb_openebs_app.py`

OpenEBS Mayastor 3-replica block-level NVMe-oF replication on Kubernetes.

```
python eval/run_load_pb_openebs_app.py --app wordpress --rates 100,200,500
python eval/run_load_pb_openebs_app.py --app bookcatalog --skip-k8s --skip-openebs
```

Apps: `wordpress`, `bookcatalog`, `tpcc`, `hotelres`, `synth`.
Extra flags: `--skip-k8s`, `--skip-disk-prep`, `--skip-openebs`, `--skip-deploy`.

### `run_load_pb_distdb_app.py`

Native distributed database replication baselines. The DB backend is
auto-selected based on the app:

| App | Backend | Description |
|-----|---------|-------------|
| `wordpress` | MySQL semi-sync | `rpl_semi_sync_source_wait_for_replica_count=1` |
| `tpcc` | PostgreSQL sync | `synchronous_commit=on` with streaming replicas |
| `hotelres` | MongoDB replica set | `w:majority` write concern |
| `bookcatalog` | rqlite (Raft) | Each SQL txn = separate Raft consensus round |
| `synth` | rqlite (Raft) | Same as bookcatalog |

```
python eval/run_load_pb_distdb_app.py --app wordpress --rates 100,200,500
python eval/run_load_pb_distdb_app.py --app tpcc --skip-db
```

Extra flags: `--skip-teardown`, `--skip-db`, `--skip-app`, `--skip-seed`.

### `run_load_pb_criu_app.py`

CRIU checkpoint/restore baseline using `BaselineCriuReplica.java` proxy.

```
python eval/run_load_pb_criu_app.py --app wordpress --rates 1,100,200
python eval/run_load_pb_criu_app.py --app bookcatalog
```

Apps: `wordpress`, `bookcatalog`.

### Synth validation scripts

The synthetic workload scripts run parametric experiments (vary txns, ops,
write_size, autocommit) to validate XDN's sync-granularity hypothesis.
They are kept as separate scripts since their experiment structure differs
from the standard rate sweep:

| Script | System |
|--------|--------|
| `validate_load_pb_synth_reflex.py` | XDN Primary-Backup |
| `validate_load_pb_synth_openebs.py` | OpenEBS |
| `validate_load_pb_synth_rqlite.py` | rqlite |

### Output location

Results go to `eval/results/load_pb_<app>_<system>_<timestamp>/` with a
convenience symlink at `eval/results/load_pb_<app>_<system>/` pointing to
the latest run. Each directory contains:

- `rate<N>.txt` — per-rate metrics from the Go load generator
- `screen_rate<N>.log` — cluster console log for that rate point
- `screen.log` — concatenated screen logs
- `gigapaxos_config.properties` — effective GigaPaxos config used
- `jvm_args.txt` — JVM args used for the run

### Legacy scripts

The original per-app-per-system scripts are preserved in `eval/legacy_pb_scripts/`
with deprecation warnings. They will be removed in a future release.

## Geo-Distributed Latency

### `run_geolat_v2.py`

Measures end-to-end latency from 3 US cities (New York, Chicago, Los Angeles)
under 5 deployment approaches. All latencies are computed from geographic
coordinates using 3.1x speed-of-light inflation via `tc` netem emulation on
CloudLab.

**Baselines** (single deployment, measure from all 3 cities):
- `single_region`: 3 replicas in Virginia (us-east-1), 2ms inter-AZ RTT
- `multi_region`: 3 replicas across Virginia, Ohio, Oregon (Spanner-like)

**ReFlex** (per-city deployment — replicas placed near each city separately):
- `reflex_lin`: 3 replicas near the target city (linearizability)
- `reflex_seq`: 3+ replicas near the target city, Flexible Paxos (sequential)
- `reflex_evt`: 3 replicas near the target city (eventual, local reads+writes)

Each ReFlex approach deploys a fresh cluster optimized for one city at a time,
reflecting the real use case where each user's service gets replicas placed
near their location. Consistency protocol is verified via `/replica/info`
after each deployment.

```
python eval/run_geolat_v2.py \
    --ar-hosts 10.10.1.1,10.10.1.2,10.10.1.3 \
    --control-plane-host 10.10.1.4

python eval/run_geolat_v2.py \
    --approaches single_region,multi_region,reflex_lin \
    --num-warmup 100 --num-requests 200
```

Results go to `eval/results/geolat_v2_<timestamp>/` with CSVs:
`eval_geo_latency_cdf.csv`, `eval_geo_per_city_latency.csv`,
`eval_geo_raw.csv`, `eval_geo_summary.csv`.

### `run_geolat_consistency.py` (v1)

Original 50-city geo-latency experiment measuring only ReFlex consistency
levels across multiple read ratios. Still usable for the v1 paper figures.

## Microbenchmarks

### `run_microbench_breakdown_bookcatalog.py`

Measures the contribution of each optimization level to max throughput with
the deterministic bookcatalog app. Tests four levels: no optimization,
in-memory state, Paxos batching, and execution batching.

```
python eval/run_microbench_breakdown_bookcatalog.py [--rates 100,200,...] [--levels 0,1,2,3]
```

Results go to `eval/results/bookcatalog_optbreakdown_<timestamp>/`.

### `run_microbench_coordination_granularity.py`

Measures per-request latency as a function of SQL statements per request,
demonstrating ReFlex's coordination granularity advantage. Uses the tpcc-java
service with configurable `txns` parameter. Sends sequential requests (one at
a time) to isolate pure coordination cost.

```
python eval/run_microbench_coordination_granularity.py --system reflex
python eval/run_microbench_coordination_granularity.py --system pgsync-txn
python eval/run_microbench_coordination_granularity.py --system pgsync-stmt
python eval/run_microbench_coordination_granularity.py --system openebs
```

Systems: `reflex` (1 round/request), `pgsync-txn` (1 sync/commit),
`pgsync-stmt` (1 sync/SQL statement), `openebs` (1 round/fsync).
Extra flags: `--sanity-check`, `--txns 1,2,3,5,8,10,15`, `--n-samples 50`,
`--n-warmup 10`.
Results go to `eval/results/microbench_coordination_<system>_<timestamp>/`.
Use `plot_coordination_granularity.py` to combine results into a paper figure.

## Debugging / Investigation

These `investigate_*.py` scripts are for debugging and profiling, not final
measurements.

| Script | Purpose |
|--------|---------|
| `investigate_pb_overhead.py` | Layer-by-layer overhead decomposition using debug headers |
| `investigate_tpcc_pb_finesweep.py` | Fine-grained rate sweep for TPC-C PB |
| `investigate_tpcc_pb_batched.py` | TPC-C with batch accumulation |
| `investigate_tpcc_pb_rsync.py` | TPC-C with RSYNC recorder |
| `investigate_tpcc_pb_unreplicated.py` | TPC-C unreplicated baseline |
| `investigate_tpcc_pb_dde.py` | TPC-C with direct execute bypass |
| `investigate_tpcc_pb_skiprepl.py` | TPC-C with skipped replication |
| `investigate_tpcc_pb_instrumented.py` | TPC-C with instrumented timing |
| `investigate_tpcc_pb_accum.py` | TPC-C capture accumulation sweep |
| `investigate_parse_pb_timing.py` | Parse XDN screen log into a timing breakdown report |
| `investigate_parse_paxos_batch_size.py` | Parse PaxosSlotBatch log lines from screen logs |
| `investigate_parse_paxos_proposal_size.py` | Parse PaxosProposalSize log lines from screen logs |

## Combining Results Across Systems

### `generate_eval_csvs.py`

Aggregates `rate<N>.txt` files from multiple systems for the same application
into a single CSV. This is the main script for combining results before plotting.

```
python eval/generate_eval_csvs.py
```

It reads from `eval/results/` subdirectories (using the symlinks described above)
and produces:

**For deterministic apps (active replication):**
- `eval/results/eval_load_ar_bookcatalog.csv` — xdn_ar, openebs, rqlite
- `eval/results/eval_load_ar_todo.csv` — xdn_ar, openebs, rqlite
- `eval/results/eval_load_ar_webkv.csv` — xdn_ar, openebs, tikv

**For non-deterministic apps (primary-backup):**
- `eval/results/eval_load_pb_bookcatalog-nd.csv` — xdn_pb, openebs, rqlite, criu
- `eval/results/eval_load_pb_wordpress.csv` — xdn_pb, openebs, semisync
- `eval/results/eval_load_pb_tpcc.csv` — xdn_pb, openebs, pgsync

Each CSV has columns: `system, target_load_rps, achieved_load_rps,
actual_throughput_rps, total_requests_sent, total_successful_responses,
min_latency_ms, max_latency_ms, average_latency_ms, median_latency_ms,
p90_latency_ms, p95_latency_ms, p99_latency_ms`.

### `generate_wordpress_csv.py`

Generates per-metric comparison CSVs specifically for WordPress systems. Reads
from `load_pb_wordpress_reflex/`, `load_pb_wordpress_openebs/`,
`load_pb_wordpress_semisync/` symlinks and produces files like
`eval/results/wordpress_comparison_throughput_rps.csv`,
`wordpress_comparison_avg_latency_ms.csv`, etc.

## Plotting

- `plot_paper_comparison.py` — Paper-ready PDFs for all apps, output to `reflex-paper/figures/`
- `plot_load_latency.py` — Load-latency curves from a single results directory
- `plot_comparison.py` — Side-by-side standalone vs XDN-PB comparison
- `plot_tpcc_comparison.py` — TPC-C 3-way comparison
- `plot_hotelres_comparison.py` — Hotel-Reservation 3-way comparison
- `plot_synth_comparison.py` — Synthetic workload parametric experiments
- `plot_fuselog_comparison.py` — FUSELOG recorder variant comparison

## Utilities

### `get_latency_at_rate.go`

Go load generator with Poisson arrivals. Sends POST requests at a target rate
for a fixed duration and prints latency percentiles.

```
go run eval/get_latency_at_rate.go <url> <json_payload> <duration_seconds> <target_rate>
```

### `init_openebs.py`

Bootstraps OpenEBS storage in the Kubernetes cluster.

```
python eval/init_openebs.py <node1> <node2> ...
```

### `init_drbd.py`

Prepares nodes for DRBD by installing packages and loading kernel modules.

```
python eval/init_drbd.py <node1> <node2> <node3> --backing-device /dev/sdb
```

## Typical Workflow

**Active replication (deterministic apps):**

1. Run `./eval/run_load_ar_eval.sh` to benchmark all systems, or run individual
   `run_load_ar_*.py` scripts for specific baselines
2. Run `generate_eval_csvs.py` to aggregate results into combined CSVs
3. Run the appropriate `plot_*.py` script to generate figures

**Primary-backup (non-deterministic apps):**

1. Run `run_load_pb_<system>_app.py --app <app>` for each system to produce
   `rate<N>.txt` files in timestamped directories under `eval/results/`
2. Ensure symlinks (`eval/results/load_pb_<app>_<system>/`) point to the latest runs
3. Run `generate_eval_csvs.py` to aggregate results into combined CSVs
4. Run the appropriate `plot_*.py` script to generate figures
