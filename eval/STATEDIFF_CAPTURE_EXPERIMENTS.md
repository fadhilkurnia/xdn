# Statediff Capture Experiments

This document defines three single-node experiments to run on CloudLab worker `10.10.1.4` in support of the "Transparent and Efficient Statediff Capture" section in [reflex-paper/sections/_4_blackbox.tex](/users/fadhil/xdn/reflex-paper/sections/_4_blackbox.tex) and [reflex-paper/sections/_7_evaluation.tex](/users/fadhil/xdn/reflex-paper/sections/_7_evaluation.tex).

The three evaluated approaches are:
- `fuselog`: XDN's FUSE-based statediff capture
- `btrfs`: incremental diff using readonly snapshots plus `btrfs send`
- `rsync`: incremental diff using `--write-batch`

## Experiment 1

Goal: break down the latency of statediff capture for a realistic SQLite-backed service.

Application:
- Docker image `fadhilkurnia/xdn-bookcatalog`
- State directory `/app/data`
- Workload `POST /api/books`

Method:
- Run the service locally on `10.10.1.4`.
- Keep the experiment low-load and single-threaded.
- Pre-seed the database with a fixed number of books so the state is non-trivial.
- For each measured request:
  1. measure HTTP request latency
  2. immediately capture the incremental statediff using the current baseline
  3. record artifact size

Metrics:
- `request_ms`: end-to-end HTTP latency as observed by the local client
- `capture_ms`: time spent generating the incremental statediff
- `artifact_bytes`: size of the captured diff artifact
- `total_ms`: `request_ms + capture_ms`

Interpretation:
- `fuselog` should have the smallest `capture_ms`
- `rsync` should be much larger because it must scan the whole state
- `btrfs` should be in the middle because it pays snapshot metadata overhead

## Experiment 2

Goal: show how capture latency grows when the state change itself becomes larger, even when the initial state is already large.

Application:
- [services/filewriter](/users/fadhil/xdn/services/filewriter)
- State file `state.bin`
- Request `POST /?size=<bytes>`

Method:
- Start the service with a large initial state, e.g. `1024 MB`.
- Vary only the per-request write size.
- Keep the initial state fixed for all baselines.
- After each request, capture the statediff using the selected baseline.

Suggested write sizes:
- `8`
- `64`
- `512`
- `4096`
- `16384`
- `65536`
- `262144`
- `1048576`

Metrics:
- `request_ms`
- `capture_ms`
- `artifact_bytes`

Interpretation:
- `fuselog` should rise with actual changed bytes, not total state size
- `btrfs` should show extent-level step behavior
- `rsync` should stay dominated by whole-state scanning even though the write size changes

## Experiment 3

Goal: show how capture latency changes as the total state size grows, while keeping the per-request state change fixed.

Application:
- [services/filewriter](/users/fadhil/xdn/services/filewriter)
- State file `state.bin`
- Request `POST /?size=4096`

Method:
- Keep the per-request write size fixed at `4096` bytes.
- Vary only the initial state size across a sweep of large files.
- Reinitialize the state for each baseline and state-size point.
- Place the fuselog working tree under `/dev/shm/` so the fuselog path runs in memory.

Suggested state sizes in MB:
- `64`
- `128`
- `256`
- `512`
- `1024`

Metrics:
- `request_ms`
- `capture_ms`
- `artifact_bytes`

Interpretation:
- `fuselog` should stay close to flat because the changed region is fixed at `4 KB`
- `btrfs` should remain mostly flat or mildly step-shaped from metadata overhead
- `rsync` should grow with total state size because it scans and batches against the whole tree

## Controls

Keep these constant across baselines:
- same worker machine: `10.10.1.4`
- same local client placement: run the client on the same node
- same Docker image and state directory
- same number of warmup requests and measured requests
- same initial seeded state before each baseline
- same filesystem cache policy unless deliberately dropped between trials

Recommended defaults:
- `bookcatalog`: `100` measured requests, `200` seeded books
- `filewriter`: `20` requests per write size, `1024 MB` initial state

## Automation

Use:
- [statediff_capture_driver.py](/users/fadhil/xdn/eval/statediff_capture_driver.py) from the driver node
- [statediff_capture_worker.py](/users/fadhil/xdn/eval/statediff_capture_worker.py) on `10.10.1.4`

Typical flow:

```bash
cd /users/fadhil/xdn/eval

# stage files and prepare dependencies on 10.10.1.4
python3 statediff_capture_driver.py prep

# experiment 1: bookcatalog latency breakdown
python3 statediff_capture_driver.py bookcatalog

# experiment 2: filewriter on large initial state
python3 statediff_capture_driver.py filewriter --state-mb 1024

# experiment 3: fixed 4 KB write, varied total state size
python3 statediff_capture_driver.py filewriter-state-size
```

Results are written on the worker under:
- `/tmp/xdn-statediff-results/bookcatalog/`
- `/tmp/xdn-statediff-results/filewriter/`
- `/tmp/xdn-statediff-results/filewriter_state_size/`

Each run emits:
- per-request CSV
- summary JSON

## Notes

- `btrfs` is not currently installed on `10.10.1.4`; the `prep` step installs `btrfs-progs`.
- `fuselog` requires `allow_other`; `/etc/fuse.conf` must allow it.
- The Btrfs baseline uses a loopback filesystem, so it does not depend on the worker's root filesystem type.
