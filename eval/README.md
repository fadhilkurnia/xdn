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
replication. They run per-rate isolated benchmarks (fresh cluster deployment
per rate point) and produce `rate<N>.txt` files.

### Naming convention

Scripts follow the pattern `run_load_pb_<app>_<system>.py` where `<app>` is the
application (e.g., `bookcatalog`, `wordpress`, `tpcc`, `hotelres`, `synth`) and
`<system>` is the replication system (e.g., `reflex`, `openebs`, `rqlite`,
`criu`, `semisync`, `pgsync`).

### Output location

Results go to `eval/results/load_pb_<app>_<system>_<timestamp>/` with a
convenience symlink at `eval/results/load_pb_<app>_<system>/` pointing to
the latest run. Each directory contains:

- `rate<N>.txt` — per-rate metrics from the Go load generator
- `screen_rate<N>.log` — cluster console log for that rate point
- `screen.log` — concatenated screen logs

The `rate<N>.txt` files contain key-value metrics:

```
total_requests_sent: <int>
total_successful_responses: <int>
actual_achieved_rate_rps: <float>
actual_throughput_rps: <float>
average_latency_ms: <float>
median_latency_ms: <float>
p90_latency_ms: <float>
p95_latency_ms: <float>
p99_latency_ms: <float>
```

### Available scripts

| Script | App | System |
|--------|-----|--------|
| `run_load_pb_bookcatalog_reflex.py` | BookCatalog-ND (SQLite) | XDN Primary-Backup |
| `run_load_pb_bookcatalog_openebs.py` | BookCatalog-ND | OpenEBS |
| `run_load_pb_bookcatalog_rqlite.py` | BookCatalog-ND | rqlite |
| `run_load_pb_bookcatalog_criu.py` | BookCatalog-ND | CRIU |
| `run_load_pb_wordpress_reflex.py` | WordPress | XDN Primary-Backup |
| `run_load_pb_wordpress_openebs.py` | WordPress | OpenEBS |
| `run_load_pb_wordpress_semisync.py` | WordPress | MySQL Semi-Sync |
| `run_load_pb_tpcc_reflex.py` | TPC-C (PostgreSQL) | XDN Primary-Backup |
| `run_load_pb_tpcc_openebs.py` | TPC-C | OpenEBS |
| `run_load_pb_tpcc_pgsync.py` | TPC-C | PostgreSQL Sync Repl |
| `run_load_pb_hotelres_reflex.py` | Hotel-Reservation | XDN Primary-Backup |
| `run_load_pb_hotelres_openebs.py` | Hotel-Reservation | OpenEBS |
| `run_load_pb_synth_reflex.py` | Synthetic Workload | XDN Primary-Backup |
| `run_load_pb_synth_openebs.py` | Synthetic Workload | OpenEBS |
| `run_load_pb_synth_rqlite.py` | Synthetic Workload | rqlite |

## Microbenchmarks

### `run_microbench_breakdown_bookcatalog.py`

Measures the contribution of each optimization level to max throughput with
the deterministic bookcatalog app. Tests four levels: no optimization,
in-memory state, Paxos batching, and execution batching.

```
python eval/run_microbench_breakdown_bookcatalog.py [--rates 100,200,...] [--levels 0,1,2,3]
```

Results go to `eval/results/bookcatalog_optbreakdown_<timestamp>/`.

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

1. Run `run_load_pb_<app>_<system>.py` for each system to produce `rate<N>.txt`
   files in timestamped directories under `eval/results/`
2. Ensure symlinks (`eval/results/load_pb_<app>_<system>/`) point to the latest runs
3. Run `generate_eval_csvs.py` to aggregate results into combined CSVs
4. Run the appropriate `plot_*.py` script to generate figures
