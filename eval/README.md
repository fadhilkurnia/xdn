# Evaluation Scripts

This folder contains helper scripts for deploying demo apps (OpenEBS-backed or
distributed DB-backed) and running load tests against them.

## Scripts

### `run_load_oebs_app.py`

Deploys supported demo apps to Kubernetes with OpenEBS PVCs, port-forwards the
service locally, and runs load tests at one or more request rates.

**Usage (Go, default):**

```
python eval/run_load_oebs_app.py <docker-images...> --rates 100,200 --duration 1m
```

**Usage (k6):**

```
python eval/run_load_oebs_app.py <docker-images...> --load-generator k6 --rates 100,200 --duration 30s
```

Results are written to `eval/results/oebs_load_results.csv`. Per-rate raw outputs
are stored as `*_summary.json`/`*_timeseries.csv` (k6) or `*_latency.txt` (Go).

### `run_load_distdb_app.py`

Launches a demo app backed by a distributed DB (rqlite or TiKV), starts the
database services on the active replicas, and runs load tests with k6 or Go.

**Usage (Go, default):**

```
python eval/run_load_distdb_app.py <xdn-config.properties> <docker-image> --rates 100,200 --duration 30s
```

**Usage (k6):**

```
python eval/run_load_distdb_app.py <xdn-config.properties> <docker-image> --load-generator k6 --rates 100,200 --duration 30s
```

### `get_latency_at_rate.go`

Sends POST requests to a target URL at a Poisson arrival rate for a fixed
duration and prints latency percentiles plus success rates.

**Usage:**

```
go run eval/get_latency_at_rate.go <url> <json_payload> <duration_seconds> <target_rate>
```

### `init_openebs.py`

Bootstraps OpenEBS storage in the Kubernetes cluster. Run before the load tests
if the storage class or control plane is not already installed.

**Usage:**

```
python eval/init_openebs.py <node1> <node2> ...
```

**Usage (skip node/disk prep):**

```
python eval/init_openebs.py <node1> <node2> ... --skip-prep
```
